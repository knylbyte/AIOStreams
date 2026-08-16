import { Readable } from 'node:stream';
import { createLogger } from '../../logging/logger.js';
import { createSegmentReadStream } from './segment-read-stream-factory.js';
import type { SegmentBufferingSource } from './segments-stream.js';
import {
  SpoolingSegmentsStream,
  type SpoolingSegmentArtifactSource,
  type SpoolingSegmentLogicalRange,
} from './spooling-segments-stream.js';
import type { SharedSegment } from './segment-arena.js';
import type {
  SegmentArtifact,
  SegmentRangeMetadataFetchOptions,
  SegmentRangeMetadata,
  SegmentRangeLayout,
} from './segment-artifact.js';
import { ZeroSegmentArtifact } from './segment-artifact.js';
import { isImplausibleYencFileSize, YencMetadataError } from './yenc.js';
import { definitiveLossKind, NntpError } from '../nntp/errors.js';
import { CommandPriority, EngineOptions, NzbSegmentRef } from '../types.js';
import type { HoleHooks } from '../holes.js';
import {
  resolveEngineResourcePlan,
  type EngineResourcePlan,
} from '../resource-plan.js';
import { resolveSegmentStreamQueuePlan } from '../stream-queue-budget.js';

const logger = createLogger('usenet/file-stream');

export interface FileSource {
  segments: NzbSegmentRef[];
  /** Best-effort filename. */
  filename?: string;
  /**
   * Pre-known decoded size (e.g. from NZB inspection / a parent archive's
   * member sizes). When set, {@link FileStream.open} skips the size probe
   * entirely; no segment is fetched until the first {@link FileStream.readAt}.
   * Critical for archive inspection, which opens one stream per volume.
   */
  knownSize?: number;
}

/** Narrow pool surface used by direct and random-access file reads. */
export interface FileStreamPool
  extends SegmentBufferingSource, SpoolingSegmentArtifactSource {
  fetchSegmentShared(
    segment: NzbSegmentRef,
    nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority
  ): Promise<SharedSegment>;
  fetchSegmentRangeMetadata(
    segment: NzbSegmentRef,
    nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority,
    options?: SegmentRangeMetadataFetchOptions
  ): Promise<SegmentRangeMetadata>;
}

/**
 * Common surface for a seekable, byte-range-servable stream. Implemented by both
 * {@link FileStream} (a plain NZB file) and the archive inner-file stream, so
 * the byte-serving route and engine handle either transparently.
 */
export interface SeekableStream {
  readonly filename?: string;
  size(): number;
  open(signal?: AbortSignal): Promise<void>;
  createReadStream(range?: { start?: number; end?: number }): Readable;
  readAt(offset: number, length: number): Promise<Buffer>;
  /**
   * Zero-alloc variant of {@link readAt}: write into `dst` at `dstOffset`,
   * returning bytes written (fewer than `length` only at EOF). Optional; see
   * `RandomAccess.readAtInto` for the contract and rationale.
   */
  readAtInto?(
    dst: Buffer,
    dstOffset: number,
    offset: number,
    length: number
  ): Promise<number>;
}

interface KnownRange {
  /** Half-open decoded byte range [begin, end) of this segment in the file. */
  begin: number;
  end: number;
  /** Provenance keeps logical offsets separate from yEnc-assertable offsets. */
  origin:
    | 'global-yenc-range'
    | 'global-yenc-derived'
    | 'standalone-prefix'
    | 'logical-grid'
    | 'estimate';
}

function isExactSpoolingLogicalRange(range: KnownRange): boolean {
  return (
    range.origin === 'global-yenc-range' ||
    range.origin === 'global-yenc-derived' ||
    range.origin === 'standalone-prefix'
  );
}

function isAssertableYencRange(range: KnownRange): boolean {
  return range.origin === 'global-yenc-range';
}

function spoolingLayoutForRange(
  range: KnownRange | undefined
): SegmentRangeLayout | undefined {
  if (
    range?.origin === 'global-yenc-range' ||
    range?.origin === 'global-yenc-derived'
  ) {
    return 'global-range';
  }
  return range?.origin === 'standalone-prefix' ? 'standalone-part' : undefined;
}

interface LocatedSegment {
  readonly segmentIndex: number;
  readonly segmentStartByte: number;
  readonly initialArtifact?: SegmentArtifact;
}

/**
 * Memo of the window-boundary segment on the readAt path: consecutive archive
 * windows (and the CBC IV read just before each window) re-touch the segment
 * straddling the boundary, and the memo spares them an arena/disk round-trip.
 * Holds a copy, never a pin: FileStream has no close/destroy hook, so a pin
 * held here would leak an arena slot.
 */
export interface SegmentMemo {
  owner?: FileStream;
  index: number;
  begin: number;
  end: number;
  len: number;
  /** Lazily allocated, grown only when a larger body appears; reused in place. */
  buf?: Buffer;
}

/**
 * Seekable view over a single NZB file. Resolves byte offsets to segment
 * indices using interpolation search (cheap because probed segments are cached)
 * and serves arbitrary HTTP ranges via {@link SegmentsStream}.
 */
export class FileStream implements SeekableStream {
  private knownRanges = new Map<number, KnownRange>();
  private _size = 0;
  private avgDecodedSize = 0;
  /**
   * Confirmed uniform part size: locked once a segment's measured range sits
   * exactly at `index × partLength` (posters emit fixed-size parts, so this
   * locks on the first non-zero segment touched). Locked seeks compute the
   * target segment arithmetically (no interpolation misprobes, each of which
   * costs a full segment fetch). Every result is still verified by the located
   * segment's own yEnc range before serving.
   */
  private lockedPartSize: number | undefined;
  private opened = false;
  /** Whether {@link _size} is exact (measured/known), not a ratio estimate. */
  private sizeExact = false;
  /** Exact layout learned only from bounded metadata or completed artifacts. */
  private spoolingLayout: SegmentRangeLayout | undefined;
  /** See {@link SegmentMemo}; shared when injected, own slot otherwise. */
  private memo?: SegmentMemo;
  /** Playback hole handling (zero-fill policy owner + persisted holes). */
  private holes?: { hooks: HoleHooks; fileIndex: number };
  private readonly resourcePlan: EngineResourcePlan;

  constructor(
    private pool: FileStreamPool,
    private source: FileSource,
    private nzbHash: string,
    private opts: EngineOptions,
    memo?: SegmentMemo,
    holes?: { hooks: HoleHooks; fileIndex: number },
    resourcePlan?: EngineResourcePlan
  ) {
    this.memo = memo;
    this.holes = holes;
    this.resourcePlan = resourcePlan ?? resolveEngineResourcePlan(opts);
  }

  get filename(): string | undefined {
    return this.source.filename;
  }

  size(): number {
    return this._size;
  }

  /**
   * Determine the file's decoded size, fetching as little as possible.
   *
   * - With a pre-known size (archive volumes, from NZB inspection): fetch
   *   NOTHING: the header bytes are pulled lazily by the first {@link readAt}.
   * - Otherwise probe only the FIRST segment and trust its yEnc `=ybegin size=`
   *   (the total file size is present in every segment). We deliberately do NOT
   *   fetch the last segment: it would be a second round-trip per file purely to
   *   refine the size, and on a multi-volume archive that doubles the
   *   inspection's article fetches.
   * - Only when the yEnc header lacks a size do we fall back to the last
   *   segment's part end for an exact value.
   */
  async open(signal?: AbortSignal): Promise<void> {
    if (this.opened) return;
    const startedAt = Date.now();
    const segments = this.source.segments;
    if (segments.length === 0) {
      throw new Error('cannot open file stream: no segments');
    }

    if (this.source.knownSize && this.source.knownSize > 0) {
      this._size = this.source.knownSize;
      // Callers only pass exact sizes here (PAR2 descriptors / probed part
      // ends); estimates would corrupt archive offset maps anyway.
      this.sizeExact = true;
      // Float on purpose: flooring biases the estimate low, which makes far
      // seeks overshoot by a segment or two (each a wasted full fetch).
      this.avgDecodedSize = Math.max(1, this._size / segments.length);
      this.opened = true;
      return;
    }

    // Spooling uses the bounded scalar probe; buffering preserves the existing
    // shared-segment locator exactly. No body ownership escapes this helper.
    const first = await this.fetchOpeningMetadata(0, signal);
    const firstLayout =
      this.resourcePlan.mode === 'segment_spooling'
        ? this.requireSpoolingMetadataLayout(first)
        : undefined;
    if (
      firstLayout === 'standalone-part' &&
      this.resourcePlan.mode === 'segment_spooling'
    ) {
      await this.buildStandalonePrefixRanges(0, first, signal);
      this.opened = true;
      this.logOpened(startedAt);
      return;
    }
    if (firstLayout) this.acceptSpoolingLayout(firstLayout);
    const firstBegin = first.byteRange?.[0] ?? 0;
    const firstEnd = first.byteRange?.[1] ?? first.decodedSize ?? 0;
    this.knownRanges.set(0, {
      begin: firstBegin,
      end: firstEnd,
      origin: first.byteRange ? 'global-yenc-range' : 'estimate',
    });
    this.avgDecodedSize = firstEnd - firstBegin || first.decodedSize || 1;

    const encodedSize = segments.reduce((acc, s) => acc + (s.bytes ?? 0), 0);
    const trustYencSize =
      first.fileSize !== undefined &&
      !isImplausibleYencFileSize(first.fileSize, segments.length, {
        encodedSize,
        firstPartLen: firstEnd - firstBegin,
        yencTotalParts: first.totalParts,
      });

    if (segments.length === 1) {
      // A single part spans the whole file, so its decoded end IS the exact
      // size; prefer it over a (possibly bogus) `=ybegin size=`.
      this._size = firstEnd || first.fileSize || first.decodedSize || 0;
      this.sizeExact = firstEnd > 0;
    } else if (trustYencSize) {
      // yEnc `=ybegin size=` is the exact total file size; no last fetch needed.
      this._size = first.fileSize!;
      this.sizeExact = true;
    } else {
      // No (or implausible) yEnc size: fall back to the last segment's part end
      // (exact) or a ratio estimate.
      const lastIdx = segments.length - 1;
      const last = await this.fetchOpeningMetadata(lastIdx, signal);
      if (this.resourcePlan.mode === 'segment_spooling') {
        this.acceptSpoolingLayout(this.requireSpoolingMetadataLayout(last));
      }
      if (last.byteRange) {
        this.knownRanges.set(lastIdx, {
          begin: last.byteRange[0],
          end: last.byteRange[1],
          origin: 'global-yenc-range',
        });
        this._size = last.byteRange[1];
        this.sizeExact = true;
      } else {
        this._size = this.avgDecodedSize * segments.length;
        this.sizeExact = false;
      }
    }
    this.opened = true;
    this.logOpened(startedAt);
  }

  private logOpened(startedAt: number): void {
    logger.debug(
      {
        nzbHash: this.nzbHash,
        filename: this.source.filename,
        size: this._size,
        segments: this.source.segments.length,
        latency: Date.now() - startedAt,
      },
      'opened file stream'
    );
  }

  /**
   * Random-access read of `length` bytes at `offset`. Used by archive header
   * parsers (RAR/7z) to cheaply probe arbitrary regions via interpolation seek.
   * Returns fewer bytes than requested only when the range hits EOF.
   *
   * Unlike {@link createReadStream} (which prefetches a parallel window for
   * playback), this fetches **only** the segments overlapping the requested
   * range, sequentially. A small header probe therefore costs ~one segment, not
   * a full read-ahead-window prefetch burst; this is critical for the archive
   * parser, which issues many tiny reads across volume boundaries.
   */
  async readAt(offset: number, length: number): Promise<Buffer> {
    if (!this.opened) {
      throw new Error('FileStream.open() must be called before reading');
    }
    if (length <= 0) return Buffer.alloc(0);
    const start = Math.max(0, offset);
    const end = Math.min(this._size, start + length);
    if (end <= start) return Buffer.alloc(0);
    const dst = Buffer.allocUnsafe(end - start);
    const written = await this.readAtInto(dst, 0, offset, length);
    return written === dst.length ? dst : dst.subarray(0, written);
  }

  /**
   * {@link readAt} into a caller-owned buffer: the archive serve path's hot
   * loop. Copies each contributing segment's slice straight into `dst` with
   * no intermediate allocation.
   */
  async readAtInto(
    dst: Buffer,
    dstOffset: number,
    offset: number,
    length: number,
    /** Stops the segment walk and cancels queued fetches (abandoned window). */
    signal?: AbortSignal
  ): Promise<number> {
    if (!this.opened) {
      throw new Error('FileStream.open() must be called before reading');
    }
    if (length <= 0) return 0;
    const start = Math.max(0, offset);
    const end = Math.min(this._size, start + length);
    if (end <= start) return 0;

    const segments = this.source.segments;
    let written = 0;
    let pos = start;
    let { segmentIndex } = await this.locateSegment(pos);

    while (pos < end && segmentIndex < segments.length) {
      let begin: number;
      let segEnd: number;
      const memo = this.memo;
      if (
        memo &&
        memo.owner === this &&
        memo.index === segmentIndex &&
        memo.buf
      ) {
        ({ begin, end: segEnd } = memo);
        const buf = memo.buf;
        this.knownRanges.set(segmentIndex, {
          begin,
          end: segEnd,
          origin: this.knownRanges.get(segmentIndex)?.origin ?? 'estimate',
        });
        if (begin >= end) break;
        if (segEnd > pos) {
          const within = Math.max(0, pos - begin);
          const take = Math.min(end, segEnd) - pos;
          if (take > 0) {
            buf.copy(dst, dstOffset + written, within, within + take);
            written += take;
            pos += take;
          }
        }
      } else {
        // Everything between the pin and release() is one synchronous block
        // (the arena contract).
        const h = await this.pool.fetchSegmentShared(
          segments[segmentIndex],
          this.nzbHash,
          signal,
          CommandPriority.High
        );
        try {
          const body = h.data.body;
          begin = h.data.byteRange?.[0] ?? segmentIndex * this.avgDecodedSize;
          segEnd = h.data.byteRange?.[1] ?? begin + body.length;
          this.knownRanges.set(segmentIndex, {
            begin,
            end: segEnd,
            origin: h.data.byteRange ? 'global-yenc-range' : 'estimate',
          });
          // The located segment must contain `pos`; subsequent segments start
          // at their own `begin`. Guard against a gap/overshoot just in case.
          if (begin >= end) break;
          if (segEnd > pos) {
            const within = Math.max(0, pos - begin);
            const take = Math.min(end, segEnd) - pos;
            if (take > 0) {
              body.copy(dst, dstOffset + written, within, within + take);
              written += take;
              pos += take;
            }
          }
          // Memoize only the window-boundary segment (extends past this
          // read's end), as a copy, never a retained pin.
          if (segEnd >= end && body.length > 0) {
            const slot = (this.memo ??= {
              index: -1,
              begin: 0,
              end: 0,
              len: 0,
            });
            if (!slot.buf || slot.buf.length < body.length) {
              slot.buf = Buffer.allocUnsafe(Math.max(1 << 20, body.length));
            }
            body.copy(slot.buf, 0);
            slot.owner = this;
            slot.index = segmentIndex;
            slot.begin = begin;
            slot.end = segEnd;
            slot.len = body.length;
          }
        } finally {
          h.release();
        }
      }
      segmentIndex++;
    }
    return written;
  }

  /**
   * Serve a half-open byte range [start, end). `end` defaults to file size.
   */
  createReadStream(range?: { start?: number; end?: number }): Readable {
    if (!this.opened) {
      throw new Error('FileStream.open() must be called before reading');
    }
    const start = Math.max(0, range?.start ?? 0);
    const end = Math.min(this._size, range?.end ?? this._size);
    const length = Math.max(0, end - start);
    logger.trace(
      { nzbHash: this.nzbHash, start, end, length },
      'serving byte range'
    );

    if (length === 0) {
      return Readable.from([]);
    }

    // Find the segment containing `start`.
    return this.openRangeStream(start, length);
  }

  private openRangeStream(start: number, length: number): Readable {
    // Deferred passthrough: do the (async) interpolation search, then wire up a
    // SegmentsStream. We use a PassThrough-like Readable that begins emitting
    // once the start segment is located.
    let inner: Readable | undefined;
    let detachRelay: (() => void) | undefined;
    let releaseRelayLease: (() => void) | undefined;
    const locatorController = new AbortController();
    const relayQueuePlan =
      this.resourcePlan.mode === 'segment_spooling' &&
      this.resourcePlan.segmentSpooling
        ? resolveSegmentStreamQueuePlan(
            this.resourcePlan.segmentSpooling.readerHighWaterMarkBytes
          )
        : undefined;
    const relayHighWaterMark = relayQueuePlan?.highWaterMarkBytes;
    const out = new Readable({
      ...(relayHighWaterMark === undefined
        ? {}
        : { highWaterMark: relayHighWaterMark }),
      read() {
        // `_read()` is demand even for consumers using paused/readable mode;
        // `isPaused()` only describes flowing-mode state. Resume the bounded
        // inner producer whenever the relay has room again.
        inner?.resume();
      },
      destroy(error, callback) {
        if (!locatorController.signal.aborted) {
          locatorController.abort(error ?? new Error('Range stream closed'));
        }
        detachRelay?.();
        detachRelay = undefined;
        // A destroyed relay no longer exposes queued chunks. Discard its
        // bounded queue before returning the shared stream-memory lease.
        out.pause();
        while (out.readableLength > 0) {
          const before = out.readableLength;
          out.read(Math.min(before, out.readableHighWaterMark));
          if (out.readableLength >= before) break;
        }
        releaseRelayLease?.();
        releaseRelayLease = undefined;
        const current = inner;
        inner = undefined;
        if (!current || current.closed) {
          callback(error);
          return;
        }
        current.once('close', () => callback(error));
        if (!current.destroyed) current.destroy(error ?? undefined);
      },
    });

    const requestedAt = Date.now();
    let firstByteSeen = false;
    void this.locateSegmentForDirectStream(start, locatorController.signal)
      .then(async ({ segmentIndex, segmentStartByte, initialArtifact }) => {
        if (out.destroyed || locatorController.signal.aborted) {
          await initialArtifact?.release();
          return;
        }
        const segments = this.source.segments.slice(segmentIndex);
        // Playback hole handling: local task index → absolute segment index.
        const holes = this.holes;
        const knownAbs = holes?.hooks.knownHoles?.(holes.fileIndex);
        const knownLocal =
          knownAbs && knownAbs.size > 0
            ? new Set(
                [...knownAbs]
                  .map((a) => a - segmentIndex)
                  .filter((l) => l >= 0 && l < segments.length)
              )
            : undefined;
        const spoolingPlan = this.resourcePlan.segmentSpooling;
        let created: Readable;
        try {
          created = createSegmentReadStream(
            {
              pool: this.pool,
              segments,
              nzbHash: this.nzbHash,
              sizeForSegment:
                this.resourcePlan.mode === 'segment_spooling'
                  ? (local) =>
                      this.exactSpoolingSegmentSize(segmentIndex + local)
                  : holes
                    ? (local) => this.exactSegmentSize(segmentIndex + local)
                    : undefined,
              byteRangeForSegment:
                this.resourcePlan.mode === 'segment_spooling'
                  ? (local) =>
                      this.authoritativeSpoolingByteRange(segmentIndex + local)
                  : undefined,
              onHole: holes
                ? (local, bytes, kind) =>
                    holes.hooks.onHole({
                      nzbFileIndex: holes.fileIndex,
                      segmentIndex: segmentIndex + local,
                      targetOffset:
                        this.resourcePlan.mode === 'segment_spooling'
                          ? this.spoolingSegmentStartByte(segmentIndex + local)
                          : this.segmentStartByte(segmentIndex + local),
                      bytes,
                      kind,
                    })
                : undefined,
              knownHoles: knownLocal,
              // The read-ahead window IS the per-stream parallelism: a stream keeps
              // up to `prefetchSegments` segment fetches in flight ahead of the read
              // cursor, and the global download semaphore (Σ provider connections)
              // caps how many of those actually run at once. So a lone stream can use
              // the whole account, while concurrent streams fair-share it via that
              // semaphore; there is no separate per-stream connection cap.
              maxPrefetchSegments: this.opts.prefetchSegments,
              // Buffer sized to the same window so completed-but-not-yet-emitted
              // segments can ride out per-segment latency jitter without stalling
              // dispatch.
              bufferingBufferSizeBytes: Math.max(
                this.avgDecodedSize * this.opts.prefetchSegments,
                1
              ),
              spoolingReaderHighWaterMarkBytes:
                spoolingPlan?.readerHighWaterMarkBytes,
              spoolingRelayHighWaterMarkBytes:
                spoolingPlan?.readerHighWaterMarkBytes,
              spoolingFirstSegmentStartByte:
                this.resourcePlan.mode === 'segment_spooling'
                  ? segmentStartByte
                  : undefined,
              spoolingFileEndByte:
                this.resourcePlan.mode === 'segment_spooling'
                  ? this._size
                  : undefined,
              spoolingLayoutHint:
                this.resourcePlan.mode === 'segment_spooling'
                  ? this.spoolingLayout
                  : undefined,
              logicalRangeForSegment:
                this.resourcePlan.mode === 'segment_spooling'
                  ? (local) => this.spoolingLogicalRange(segmentIndex + local)
                  : undefined,
              initialSpoolingArtifact: initialArtifact,
              skipBytes: start - segmentStartByte,
              limitBytes: length,
              priority: CommandPriority.High,
              signal: locatorController.signal,
            },
            this.resourcePlan.mode
          );
        } catch (error) {
          await initialArtifact?.release();
          throw error;
        }
        inner = created;
        const current = inner;
        if (current instanceof SpoolingSegmentsStream) {
          // Chunks move from the inner queue into this relay. Keep the same
          // hard stream-memory reservation until the relay itself drains or
          // discards those chunks, even if the inner producer has closed.
          releaseRelayLease = current.retainStreamMemoryLease();
        }
        let terminal = false;
        const onData = (chunk: Buffer): void => {
          if (!firstByteSeen) {
            firstByteSeen = true;
            logger.debug(
              {
                nzbHash: this.nzbHash,
                start,
                length,
                latency: Date.now() - requestedAt,
              },
              'range first byte'
            );
          }
          if (!out.push(chunk)) current.pause();
        };
        const onEnd = (): void => {
          terminal = true;
          detachRelay?.();
          detachRelay = undefined;
          inner = undefined;
          out.push(null);
        };
        const onError = (error: Error): void => {
          terminal = true;
          out.destroy(error);
        };
        const onClose = (): void => {
          if (!terminal && !out.destroyed) {
            out.destroy(new Error('Segment read stream closed before EOF'));
          }
        };
        const onPause = (): void => {
          current.pause();
        };
        const onResume = (): void => {
          current.resume();
        };
        detachRelay = () => {
          current.removeListener('data', onData);
          current.removeListener('end', onEnd);
          current.removeListener('error', onError);
          current.removeListener('close', onClose);
          out.removeListener('pause', onPause);
          out.removeListener('resume', onResume);
        };
        out.on('pause', onPause);
        out.on('resume', onResume);
        current.on('end', onEnd);
        current.on('error', onError);
        current.on('close', onClose);
        current.on('data', onData);
      })
      .catch((err) => {
        if (!out.destroyed) {
          out.destroy(err instanceof Error ? err : new Error(String(err)));
        }
      });

    return out;
  }

  private locateSegmentForDirectStream(
    targetByte: number,
    signal: AbortSignal
  ): Promise<LocatedSegment> {
    return this.resourcePlan.mode === 'segment_spooling'
      ? this.locateSpoolingSegment(targetByte, signal)
      : this.locateSegment(targetByte, signal);
  }

  /**
   * Locate the segment containing `targetByte` via interpolation search over
   * decoded byte ranges. Returns the segment index and its decoded start byte.
   */
  private async locateSegment(
    targetByte: number,
    signal?: AbortSignal
  ): Promise<LocatedSegment> {
    const segments = this.source.segments;
    if (segments.length === 1) {
      return {
        segmentIndex: 0,
        segmentStartByte: this.knownRanges.get(0)?.begin ?? 0,
      };
    }

    let lo = 0;
    let hi = segments.length - 1;

    // Use known endpoints to bound the search.
    const firstRange = this.knownRanges.get(0);
    if (firstRange && targetByte < firstRange.end) {
      return { segmentIndex: 0, segmentStartByte: firstRange.begin };
    }

    let guard = 0;
    while (lo <= hi && guard++ < segments.length + 8) {
      // Interpolate an index guess: exact arithmetic once the uniform part
      // size is locked, the running average estimate otherwise.
      const est = this.lockedPartSize ?? Math.max(1, this.avgDecodedSize);
      let guess = Math.floor(targetByte / est);
      guess = Math.min(hi, Math.max(lo, guess));

      const range = await this.rangeForSegment(guess, signal);
      if (targetByte < range.begin) {
        hi = guess - 1;
        // Refine avg estimate downward.
        this.avgDecodedSize = Math.max(1, range.begin / Math.max(1, guess));
      } else if (targetByte >= range.end) {
        lo = guess + 1;
        this.avgDecodedSize = Math.max(1, range.end / Math.max(1, guess + 1));
      } else {
        return { segmentIndex: guess, segmentStartByte: range.begin };
      }
    }

    // Fallback: linear clamp to the bounded region.
    const idx = Math.min(segments.length - 1, Math.max(0, lo));
    const range = await this.rangeForSegment(idx, signal);
    return { segmentIndex: idx, segmentStartByte: range.begin };
  }

  /**
   * Uniform part length, when proven: the locked grid (a measured non-first
   * range landing exactly on `index × len`), else the first segment's
   * measured length corroborated by an EXACT total size + segment count that
   * only a uniform grid of that length satisfies. yEnc posters emit
   * fixed-size parts, so corroboration failing means "don't trust it".
   */
  private partGridSize(): number | undefined {
    if (this.spoolingLayout === 'standalone-part') return undefined;
    if (this.lockedPartSize !== undefined && this.lockedPartSize > 0) {
      return this.lockedPartSize;
    }
    const first = this.knownRanges.get(0);
    if (!first || first.begin !== 0 || !this.sizeExact) return undefined;
    const len = first.end - first.begin;
    const n = this.source.segments.length;
    if (len <= 0) return undefined;
    return len * (n - 1) < this._size && this._size <= len * n
      ? len
      : undefined;
  }

  /**
   * Decoded start byte of segment `index`, when guaranteed without a fetch.
   * Defined whenever {@link exactSegmentSize} approves a pad (same sources).
   */
  private segmentStartByte(index: number): number | undefined {
    const known = this.knownRanges.get(index);
    if (known) return known.begin;
    const part = this.partGridSize();
    return part !== undefined ? index * part : undefined;
  }

  /**
   * Exact decoded size of segment `index`, or undefined when it cannot be
   * guaranteed. Zero-fill padding relies on this: a wrong length silently
   * shifts every later byte, which is worse than the stream dying.
   */
  private exactSegmentSize(index: number): number | undefined {
    const known = this.knownRanges.get(index);
    if (known) return known.end - known.begin;
    const part = this.partGridSize();
    if (part === undefined) return undefined;
    const n = this.source.segments.length;
    if (index < 0 || index >= n) return undefined;
    if (index < n - 1) return part;
    if (!this.sizeExact) return undefined;
    const last = this._size - part * (n - 1);
    return last > 0 && last <= part ? last : undefined;
  }

  /** Add the trivial exact one-part known-size case used for early tailing. */
  private exactSpoolingSegmentSize(index: number): number | undefined {
    const known = this.knownRanges.get(index);
    if (known && isExactSpoolingLogicalRange(known)) {
      return known.end - known.begin;
    }
    return index === 0 &&
      this.source.segments.length === 1 &&
      this.sizeExact &&
      this._size > 0
      ? this._size
      : undefined;
  }

  /** Exact direct-spooling offset; buffering estimates are never eligible. */
  private spoolingSegmentStartByte(index: number): number | undefined {
    const known = this.knownRanges.get(index);
    if (known && isExactSpoolingLogicalRange(known)) return known.begin;
    return undefined;
  }

  private authoritativeSpoolingByteRange(
    index: number
  ): readonly [number, number] | undefined {
    const range = this.knownRanges.get(index);
    if (!range || !isAssertableYencRange(range)) return undefined;
    return [range.begin, range.end];
  }

  private spoolingLogicalRange(
    index: number
  ): SpoolingSegmentLogicalRange | undefined {
    const range = this.knownRanges.get(index);
    const layout = spoolingLayoutForRange(range);
    if (!range || !layout) return undefined;
    return { range: [range.begin, range.end], layout };
  }

  /** Buffer-free locator used only by the direct segment-spooling stream. */
  private async locateSpoolingSegment(
    targetByte: number,
    signal: AbortSignal
  ): Promise<LocatedSegment> {
    const segments = this.source.segments;
    if (targetByte === 0 || segments.length === 1) {
      const firstRange = this.knownRanges.get(0);
      if (
        firstRange?.origin === 'global-yenc-range' &&
        firstRange.begin !== 0
      ) {
        throw new YencMetadataError(
          'inconsistent_layout',
          'The first global yEnc range does not begin at file offset zero'
        );
      }
      return {
        segmentIndex: 0,
        segmentStartByte: 0,
      };
    }

    let lo = 0;
    let hi = segments.length - 1;
    const firstRange = this.knownRanges.get(0);
    if (
      firstRange &&
      isExactSpoolingLogicalRange(firstRange) &&
      targetByte < firstRange.end
    ) {
      return { segmentIndex: 0, segmentStartByte: firstRange.begin };
    }

    let guard = 0;
    while (lo <= hi && guard++ < segments.length + 8) {
      const estimate = this.lockedPartSize ?? Math.max(1, this.avgDecodedSize);
      const guess = Math.min(
        hi,
        Math.max(lo, Math.floor(targetByte / estimate))
      );
      const range = await this.rangeForSpoolingSegment(guess, signal);
      if (targetByte < range.begin) {
        hi = guess - 1;
        this.avgDecodedSize = Math.max(1, range.begin / Math.max(1, guess));
      } else if (targetByte >= range.end) {
        lo = guess + 1;
        this.avgDecodedSize = Math.max(1, range.end / Math.max(1, guess + 1));
      } else {
        return this.fetchLocatedSpoolingArtifact(guess, range, signal);
      }
    }

    const index = Math.min(segments.length - 1, Math.max(0, lo));
    const range = await this.rangeForSpoolingSegment(index, signal);
    return this.fetchLocatedSpoolingArtifact(index, range, signal);
  }

  private async rangeForSpoolingSegment(
    index: number,
    signal: AbortSignal
  ): Promise<KnownRange> {
    const cached = this.knownRanges.get(index);
    if (cached && isExactSpoolingLogicalRange(cached)) {
      const layout = spoolingLayoutForRange(cached);
      if (layout) this.acceptSpoolingLayout(layout);
      return cached;
    }

    try {
      const metadata = await this.pool.fetchSegmentRangeMetadata(
        this.source.segments[index],
        this.nzbHash,
        signal,
        CommandPriority.High,
        {
          requireByteRange: this.source.segments.length > 1,
          allowStandalonePart: true,
        }
      );
      const layout = this.requireSpoolingMetadataLayout(metadata);
      if (layout === 'standalone-part') {
        await this.buildStandalonePrefixRanges(index, metadata, signal);
        const standalone = this.knownRanges.get(index);
        if (!standalone) {
          throw new YencMetadataError(
            'invalid_header',
            'Standalone yEnc prefix map omitted the requested segment'
          );
        }
        return standalone;
      }
      this.acceptSpoolingLayout(layout);
      return this.recordMeasuredRange(index, {
        byteRange: metadata.byteRange,
        decodedSize: metadata.decodedSize ?? 0,
        origin: 'global-yenc-range',
      });
    } catch (error) {
      const synthesized = this.synthesizeSpoolingHoleRange(index, error);
      if (synthesized) return synthesized;
      throw error;
    }
  }

  private async fetchLocatedSpoolingArtifact(
    index: number,
    range: KnownRange,
    signal: AbortSignal
  ): Promise<LocatedSegment> {
    await this.assertLocatedSpoolingPredecessor(index, range, signal);
    const expectedLength = range.end - range.begin;
    if (!Number.isSafeInteger(expectedLength) || expectedLength <= 0) {
      throw new Error('Spooling locator resolved an invalid segment range');
    }
    let artifact: SegmentArtifact | undefined;
    try {
      artifact = await this.pool.fetchSegmentArtifact(
        this.source.segments[index],
        this.nzbHash,
        signal,
        CommandPriority.High,
        {
          expectedLength,
          expectedByteRange: this.authoritativeSpoolingByteRange(index),
          allowGrowing: true,
        }
      );
      if (signal.aborted) {
        const abandoned = artifact;
        artifact = undefined;
        await abandoned.release();
        throw new NntpError('connection', 'aborted');
      }
      return {
        segmentIndex: index,
        segmentStartByte: range.begin,
        initialArtifact: artifact,
      };
    } catch (error) {
      await artifact?.release();
      const kind = definitiveLossKind(error);
      if (kind !== undefined) {
        const zero = this.createLocatedHoleArtifact(
          index,
          expectedLength,
          kind
        );
        if (zero) {
          return {
            segmentIndex: index,
            segmentStartByte: range.begin,
            initialArtifact: zero,
          };
        }
      }
      throw error;
    }
  }

  private async assertLocatedSpoolingPredecessor(
    index: number,
    range: KnownRange,
    signal: AbortSignal
  ): Promise<void> {
    if (spoolingLayoutForRange(range) !== 'global-range') return;
    if (index === 0) {
      if (range.begin !== 0) {
        throw new YencMetadataError(
          'inconsistent_layout',
          'The first global yEnc range does not begin at file offset zero'
        );
      }
      return;
    }

    let previous = this.knownRanges.get(index - 1);
    if (spoolingLayoutForRange(previous) !== 'global-range') {
      previous = await this.rangeForSpoolingSegment(index - 1, signal);
    }
    if (
      !previous ||
      spoolingLayoutForRange(previous) !== 'global-range' ||
      previous.end !== range.begin
    ) {
      throw new YencMetadataError(
        'inconsistent_layout',
        'The target global yEnc range has a gap or overlap at its predecessor boundary'
      );
    }
  }

  private createLocatedHoleArtifact(
    index: number,
    bytes: number,
    kind: 'missing' | 'undecodable'
  ): ZeroSegmentArtifact | undefined {
    const holes = this.holes;
    if (
      !holes ||
      holes.hooks.onHole({
        nzbFileIndex: holes.fileIndex,
        segmentIndex: index,
        targetOffset: this.spoolingSegmentStartByte(index),
        bytes,
        kind,
      }) !== 'pad'
    ) {
      return undefined;
    }
    return new ZeroSegmentArtifact(bytes);
  }

  private requireSpoolingMetadataLayout(
    metadata: SegmentRangeMetadata
  ): SegmentRangeLayout {
    const range = metadata.byteRange;
    const inferred = range
      ? 'global-range'
      : metadata.totalParts !== undefined && metadata.totalParts > 1
        ? undefined
        : 'standalone-part';
    const layout = metadata.layout ?? inferred;
    const fileSize = metadata.fileSize;
    const decodedSize = metadata.decodedSize;
    if (
      layout === 'global-range' &&
      range !== undefined &&
      Number.isSafeInteger(range[0]) &&
      Number.isSafeInteger(range[1]) &&
      range[0] >= 0 &&
      range[1] > range[0] &&
      Number.isSafeInteger(fileSize) &&
      fileSize !== undefined &&
      fileSize > 0 &&
      range[1] <= fileSize
    ) {
      return layout;
    }
    if (
      layout === 'standalone-part' &&
      range === undefined &&
      (metadata.totalParts === undefined || metadata.totalParts <= 1) &&
      Number.isSafeInteger(fileSize) &&
      fileSize !== undefined &&
      fileSize > 0 &&
      Number.isSafeInteger(decodedSize) &&
      decodedSize !== undefined &&
      decodedSize === fileSize
    ) {
      return layout;
    }
    throw new YencMetadataError(
      'invalid_header',
      'yEnc metadata cannot establish an exact segment layout'
    );
  }

  private acceptSpoolingLayout(layout: SegmentRangeLayout): void {
    if (this.spoolingLayout === undefined) {
      this.spoolingLayout = layout;
      return;
    }
    if (this.spoolingLayout !== layout) {
      throw new YencMetadataError(
        'inconsistent_layout',
        'Logical file mixes global yEnc ranges with standalone parts'
      );
    }
  }

  /** Build an exact finite scalar prefix map without fetching any BODY. */
  private async buildStandalonePrefixRanges(
    seedIndex: number,
    seedMetadata: SegmentRangeMetadata,
    signal?: AbortSignal
  ): Promise<void> {
    this.acceptSpoolingLayout('standalone-part');
    const ranges: KnownRange[] = [];
    let cursor = 0;
    for (let index = 0; index < this.source.segments.length; index++) {
      const metadata =
        index === seedIndex
          ? seedMetadata
          : await this.pool.fetchSegmentRangeMetadata(
              this.source.segments[index],
              this.nzbHash,
              signal,
              CommandPriority.High,
              { requireByteRange: true, allowStandalonePart: true }
            );
      const layout = this.requireSpoolingMetadataLayout(metadata);
      if (layout !== 'standalone-part') {
        throw new YencMetadataError(
          'inconsistent_layout',
          'Logical file mixes standalone parts with global yEnc ranges'
        );
      }
      const length = metadata.decodedSize;
      if (
        length === undefined ||
        !Number.isSafeInteger(length) ||
        length <= 0 ||
        !Number.isSafeInteger(cursor + length)
      ) {
        throw new YencMetadataError(
          'invalid_header',
          'Standalone yEnc part has an invalid exact length'
        );
      }
      ranges.push({
        begin: cursor,
        end: cursor + length,
        origin: 'standalone-prefix',
      });
      cursor += length;
    }
    if (
      this.source.knownSize !== undefined &&
      this.source.knownSize > 0 &&
      this.source.knownSize !== cursor
    ) {
      throw new YencMetadataError(
        'inconsistent_layout',
        'Standalone yEnc prefix size conflicts with the exact file size'
      );
    }
    for (let index = 0; index < ranges.length; index++) {
      this.knownRanges.set(index, ranges[index]);
    }
    this._size = cursor;
    this.sizeExact = true;
    this.avgDecodedSize = cursor / ranges.length;
  }

  private async fetchOpeningMetadata(
    index: number,
    signal?: AbortSignal
  ): Promise<SegmentRangeMetadata> {
    if (this.resourcePlan.mode === 'segment_spooling') {
      return this.pool.fetchSegmentRangeMetadata(
        this.source.segments[index],
        this.nzbHash,
        signal,
        CommandPriority.High,
        {
          requireByteRange: this.source.segments.length > 1,
          allowStandalonePart: true,
        }
      );
    }
    const shared = await this.pool.fetchSegmentShared(
      this.source.segments[index],
      this.nzbHash,
      signal,
      CommandPriority.High
    );
    try {
      return {
        byteRange: shared.data.byteRange,
        fileSize: shared.data.fileSize,
        totalParts: shared.data.totalParts,
        name: shared.data.name,
        decodedSize: shared.data.size,
        layout: shared.data.byteRange ? 'global-range' : 'standalone-part',
      };
    } finally {
      shared.release();
    }
  }

  private async rangeForSegment(
    index: number,
    signal?: AbortSignal
  ): Promise<KnownRange> {
    const cached = this.knownRanges.get(index);
    if (cached) return cached;
    // Metadata-only; released immediately.
    let data: { byteRange?: [number, number]; size: number };
    try {
      const h = await this.pool.fetchSegmentShared(
        this.source.segments[index],
        this.nzbHash,
        signal,
        CommandPriority.High
      );
      data = h.data;
      h.release();
    } catch (err) {
      // A seek landing ON a hole must not kill the locate: with a proven part
      // grid the segment's range is known without its bytes. The actual read
      // of the hole is then the padding policy's problem, not the seek's.
      const synthesized = this.synthesizeHoleRange(index, err);
      if (synthesized) return synthesized;
      throw err;
    }
    return this.recordMeasuredRange(index, {
      byteRange: data.byteRange,
      decodedSize: data.size,
      origin: data.byteRange ? 'global-yenc-range' : 'estimate',
    });
  }

  private recordMeasuredRange(
    index: number,
    data: {
      readonly byteRange?: readonly [number, number];
      readonly decodedSize: number;
      readonly origin: KnownRange['origin'];
    }
  ): KnownRange {
    const begin = data.byteRange?.[0] ?? index * this.avgDecodedSize;
    const end = data.byteRange?.[1] ?? begin + data.decodedSize;
    const range: KnownRange = { begin, end, origin: data.origin };
    this.knownRanges.set(index, range);
    // Lock the uniform part size when a measured non-first range lands exactly
    // on the fixed-size grid; a later contradiction unlocks it.
    if (data.byteRange) {
      const len = end - begin;
      if (this.lockedPartSize !== undefined) {
        if (
          index < this.source.segments.length - 1 &&
          begin !== index * this.lockedPartSize
        ) {
          this.lockedPartSize = undefined;
        }
      } else if (index > 0 && len > 0 && begin === index * len) {
        this.lockedPartSize = len;
      }
    }
    return range;
  }

  private synthesizeHoleRange(
    index: number,
    error: unknown
  ): KnownRange | undefined {
    const kind = definitiveLossKind(error);
    if (kind === undefined) return undefined;
    const part = this.partGridSize();
    if (part === undefined) return undefined;
    const begin = index * part;
    const exact = this.exactSegmentSize(index);
    const end = exact
      ? begin + exact
      : Math.min(begin + part, this._size || begin + part);
    const range: KnownRange = { begin, end, origin: 'logical-grid' };
    this.knownRanges.set(index, range);
    logger.debug(
      { nzbHash: this.nzbHash, index, begin, end, kind },
      'segment unservable on all providers; synthesized grid range for seek'
    );
    return range;
  }

  /**
   * Spooling holes require an exact range. A missing middle part is derivable
   * only between two measured contiguous-neighbour boundaries; the final part
   * may additionally use the exact file end. No single-part grid heuristic is
   * promoted to a byte range, and a leading hole remains fail-closed.
   */
  private synthesizeSpoolingHoleRange(
    index: number,
    error: unknown
  ): KnownRange | undefined {
    const kind = definitiveLossKind(error);
    if (kind === undefined) return undefined;
    if (this.spoolingLayout !== 'global-range' || index <= 0) return undefined;
    const previous = this.knownRanges.get(index - 1);
    if (!previous || !isAssertableYencRange(previous)) return undefined;
    let end: number | undefined;
    if (index === this.source.segments.length - 1 && this.sizeExact) {
      end = this._size;
    } else {
      const next = this.knownRanges.get(index + 1);
      if (next && isAssertableYencRange(next)) end = next.begin;
    }
    const begin = previous.end;
    if (
      end === undefined ||
      !Number.isSafeInteger(begin) ||
      !Number.isSafeInteger(end) ||
      end <= begin
    ) {
      return undefined;
    }
    const range: KnownRange = {
      begin,
      end,
      origin: 'global-yenc-derived',
    };
    this.knownRanges.set(index, range);
    logger.debug(
      { nzbHash: this.nzbHash, index, begin, end: range.end, kind },
      'segment unservable on all providers; derived exact neighbour range'
    );
    return range;
  }
}
