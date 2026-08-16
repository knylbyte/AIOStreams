import { addAbortSignal, Readable } from 'node:stream';
import { createReadStream } from 'node:fs';
import { createLogger } from '../../logging/logger.js';
import type { DiskFileLease } from '../../utils/disk-backed-cache.js';
import type { DecodedSegmentMetadata } from './streaming-yenc-article-decoder.js';
import type { SharedSegment } from './segment-arena.js';
import type { GrowingSpoolArtifact } from '../spool/growing-artifact.js';
import { UsenetSpoolError } from '../spool/errors.js';
import {
  SEGMENT_STREAM_MAX_CHUNK_BYTES,
  resolveSegmentStreamQueuePlan,
} from '../stream-queue-budget.js';

const MEBIBYTE_BYTES = 1024 * 1024;
/** Matches the largest planned Block-1 spool reader HWM and bounds owned data. */
const MAX_ARTIFACT_READER_HIGH_WATER_MARK_BYTES = 2 * MEBIBYTE_BYTES;
const logger = createLogger('usenet/segment-artifact');

/** Bounded range options shared by every decoded-segment storage variant. */
export interface SegmentArtifactReadOptions {
  readonly start?: number;
  readonly endExclusive?: number;
  readonly signal?: AbortSignal;
  readonly highWaterMark?: number;
}

/** Where the decoded bytes backing a {@link SegmentArtifact} currently live. */
export type SegmentArtifactStorage = 'arena' | 'disk-cache' | 'spool' | 'zero';

/** How bounded yEnc metadata maps one article into its logical NZB file. */
export type SegmentRangeLayout = 'global-range' | 'standalone-part';

/** Scalar-only metadata used by the buffer-free direct spooling locator. */
export interface SegmentRangeMetadata {
  readonly byteRange?: readonly [number, number];
  readonly fileSize?: number;
  readonly totalParts?: number;
  readonly name?: string;
  readonly decodedSize?: number;
  /** Explicit on strict probes; legacy/fake sources may omit it for inference. */
  readonly layout?: SegmentRangeLayout;
}

/** Strictness required by a file-offset locator using scalar yEnc metadata. */
export interface SegmentRangeMetadataFetchOptions {
  /** Require global `=ypart`, unless a strict standalone part is allowed. */
  readonly requireByteRange?: boolean;
  /** Accept an exact single-part `=ybegin size` as a local standalone length. */
  readonly allowStandalonePart?: boolean;
}

/** Delivery policy for one independently validated artifact waiter. */
export interface SegmentArtifactFetchOptions {
  /** Exact file-grid length asserted independently after producer completion. */
  readonly expectedLength?: number;
  /** Exact authoritative global yEnc range asserted independently per waiter. */
  readonly expectedByteRange?: readonly [number, number];
  /**
   * Permit resolution after the first committed byte. Successful reader EOF
   * still waits for BODY, decoder, sink and final-length validation.
   */
  readonly allowGrowing?: boolean;
}

/**
 * One independently owned reference to decoded segment bytes.
 *
 * Each handle creates at most one reader. Reader close releases the handle;
 * callers that never open a reader must call {@link release}. Release is
 * idempotent and resolves only after storage owned by the final reference has
 * been cleaned up.
 */
export interface SegmentArtifact {
  readonly metadata: DecodedSegmentMetadata;
  readonly length: number;
  readonly storage: SegmentArtifactStorage;
  /**
   * Every emitted Buffer is bounded by `SEGMENT_STREAM_MAX_CHUNK_BYTES` and
   * owns no materially larger backing allocation hidden behind a short view.
   */
  createReadStream(options?: SegmentArtifactReadOptions): Readable;
  release(): Promise<void>;
}

/**
 * Block-8 extension point for file-backed persistent L2 hits. Implementations
 * must return an independently releasable file lease and must not materialize
 * the decoded body in a Buffer.
 */
export interface SegmentArtifactCacheLookup {
  /** Explicit false lets the producer skip even a transient promotion lease. */
  readonly promotionEnabled?: boolean;
  acquire(
    messageId: string,
    signal?: AbortSignal
  ): Promise<SegmentArtifact | undefined>;
  /**
   * Best-effort promotion of one complete transient spool file. Implementations
   * must bound concurrency and must not retain `sourcePath` after settlement.
   */
  promote?(
    messageId: string,
    metadata: DecodedSegmentMetadata,
    sourcePath: string
  ): Promise<boolean>;
}

interface ValidatedReadRange {
  readonly start: number;
  readonly endExclusive: number;
  readonly highWaterMark: number;
}

function validateReadRange(
  length: number,
  options: SegmentArtifactReadOptions
): ValidatedReadRange {
  const start = options.start ?? 0;
  const endExclusive = options.endExclusive ?? length;
  const highWaterMark = options.highWaterMark ?? SEGMENT_STREAM_MAX_CHUNK_BYTES;
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(endExclusive) ||
    start < 0 ||
    endExclusive < start ||
    endExclusive > length
  ) {
    throw new RangeError('Segment artifact range is outside decoded bytes');
  }
  if (
    !Number.isSafeInteger(highWaterMark) ||
    highWaterMark <= 0 ||
    highWaterMark > MAX_ARTIFACT_READER_HIGH_WATER_MARK_BYTES
  ) {
    throw new RangeError(
      'Segment artifact highWaterMark must be a safe positive integer no larger than 2 MiB'
    );
  }
  resolveSegmentStreamQueuePlan(highWaterMark);
  return { start, endExclusive, highWaterMark };
}

class BufferRangeReadable extends Readable {
  private position: number;

  constructor(
    private readonly body: Buffer,
    start: number,
    private readonly endExclusive: number,
    highWaterMark: number
  ) {
    super({ highWaterMark });
    this.position = start;
  }

  override _read(size: number): void {
    if (this.position >= this.endExclusive) {
      this.push(null);
      return;
    }
    const bytes = Math.min(
      Math.max(1, size),
      SEGMENT_STREAM_MAX_CHUNK_BYTES,
      this.endExclusive - this.position
    );
    const chunk = Buffer.allocUnsafeSlow(bytes);
    this.body.copy(chunk, 0, this.position, this.position + bytes);
    this.position += bytes;
    this.push(chunk);
  }
}

function metadataFromShared(shared: SharedSegment): DecodedSegmentMetadata {
  return {
    byteRange: shared.data.byteRange,
    fileSize: shared.data.fileSize,
    totalParts: shared.data.totalParts,
    name: shared.data.name,
    size: shared.data.size,
  };
}

/**
 * A single-reader artifact that keeps one SegmentArena pin alive while copying
 * each emitted chunk into independent owned memory. No view into the recyclable
 * arena slot escapes `_read`; chunks are at most 64 KiB and the internal
 * Readable high-water mark is capped at 2 MiB.
 */
export class ArenaSegmentArtifact implements SegmentArtifact {
  readonly metadata: DecodedSegmentMetadata;
  readonly length: number;
  readonly storage = 'arena' as const;

  private reader: Readable | undefined;
  private released = false;
  private pinReleased = false;
  private releasePromise: Promise<void> | undefined;
  private releaseDone: (() => void) | undefined;

  constructor(private readonly shared: SharedSegment) {
    this.metadata = metadataFromShared(shared);
    this.length = shared.data.body.length;
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    if (this.released) {
      throw new Error('Arena segment artifact has been released');
    }
    if (this.reader) {
      throw new Error('Segment artifact handles support one reader');
    }
    const range = validateReadRange(this.length, options);
    const reader = new BufferRangeReadable(
      this.shared.data.body,
      range.start,
      range.endExclusive,
      range.highWaterMark
    );
    this.reader = reader;
    reader.once('close', () => this.finishRelease());
    if (options.signal) addAbortSignal(options.signal, reader);
    return reader;
  }

  release(): Promise<void> {
    if (this.releasePromise) return this.releasePromise;
    const done = Promise.withResolvers<void>();
    this.releasePromise = done.promise;
    this.releaseDone = done.resolve;
    this.released = true;
    if (this.reader && !this.reader.destroyed) this.reader.destroy();
    else this.finishRelease();
    return this.releasePromise;
  }

  private finishRelease(): void {
    if (!this.released) this.released = true;
    if (!this.pinReleased) {
      this.pinReleased = true;
      this.shared.release();
    }
    this.releaseDone?.();
    this.releaseDone = undefined;
    this.releasePromise ??= Promise.resolve();
  }
}

class EmptyRangeReadable extends Readable {
  override _read(): void {
    this.push(null);
  }
}

/**
 * One-reader view over only the body range of a persistent serialized cache
 * entry. The generic file lease keeps physical eviction deferred until the
 * reader closes; neither construction nor range reads materialize the body.
 */
export class DiskSegmentArtifact implements SegmentArtifact {
  readonly storage = 'disk-cache' as const;

  private reader: Readable | undefined;
  private readerClosed = false;
  private released = false;
  private releasePromise: Promise<void> | undefined;
  private fileReleasePromise: Promise<void> | undefined;

  constructor(
    private readonly fileLease: DiskFileLease,
    readonly metadata: DecodedSegmentMetadata,
    private readonly bodyOffset: number,
    readonly length: number
  ) {
    if (
      !Number.isSafeInteger(bodyOffset) ||
      bodyOffset < 0 ||
      !Number.isSafeInteger(length) ||
      length < 0 ||
      bodyOffset + length !== fileLease.serializedBytes
    ) {
      throw new RangeError('Disk segment artifact body range is invalid');
    }
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    if (this.released) {
      throw new Error('Disk segment artifact has been released');
    }
    if (this.reader) {
      throw new Error('Segment artifact handles support one reader');
    }
    const range = validateReadRange(this.length, options);
    const rangeLength = range.endExclusive - range.start;
    const reader =
      rangeLength === 0
        ? new EmptyRangeReadable({ highWaterMark: range.highWaterMark })
        : createReadStream(this.fileLease.path, {
            start: this.bodyOffset + range.start,
            end: this.bodyOffset + range.endExclusive - 1,
            highWaterMark: Math.min(
              range.highWaterMark,
              SEGMENT_STREAM_MAX_CHUNK_BYTES
            ),
          });
    this.reader = reader;
    reader.once('close', () => {
      this.readerClosed = true;
      this.released = true;
      this.releasePromise ??= this.releaseFile();
      void this.releasePromise.catch((error: unknown) => {
        logger.warn({ err: error }, 'failed to release disk segment file');
      });
    });
    if (options.signal) addAbortSignal(options.signal, reader);
    return reader;
  }

  release(): Promise<void> {
    if (this.releasePromise) return this.releasePromise;
    this.released = true;
    if (this.reader && !this.readerClosed) {
      const done = Promise.withResolvers<void>();
      this.releasePromise = done.promise;
      this.reader.once('close', () => {
        this.releaseFile().then(done.resolve, done.reject);
      });
      if (!this.reader.destroyed) this.reader.destroy();
      return this.releasePromise;
    }
    this.releasePromise = this.releaseFile();
    return this.releasePromise;
  }

  private releaseFile(): Promise<void> {
    this.fileReleasePromise ??= this.fileLease.release();
    return this.fileReleasePromise;
  }
}

/**
 * SegmentArtifact adapter over one growing or complete transient spool file.
 * `releaseReference` belongs to the single-flight owner and disposes the file
 * only after its final independently delivered handle is released.
 */
export class GrowingSpoolArtifactAdapter implements SegmentArtifact {
  readonly storage = 'spool' as const;

  private reader: Readable | undefined;
  private releasePromise: Promise<void> | undefined;
  private released = false;

  constructor(
    private readonly artifact: GrowingSpoolArtifact,
    private readonly initialMetadata: DecodedSegmentMetadata,
    private readonly releaseReference: () => Promise<void>,
    private readonly producerCompletion?: Promise<DecodedSegmentMetadata>,
    private readonly currentMetadata?: () => DecodedSegmentMetadata
  ) {}

  get metadata(): DecodedSegmentMetadata {
    return this.currentMetadata?.() ?? this.initialMetadata;
  }

  get length(): number {
    return this.metadata.size;
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    if (this.released) {
      throw new Error('Spool segment artifact has been released');
    }
    if (this.reader) {
      throw new Error('Segment artifact handles support one reader');
    }
    const lengthAtOpen = this.length;
    const byteRangeAtOpen = this.metadata.byteRange;
    validateReadRange(lengthAtOpen, options);
    const completion = this.producerCompletion?.then((metadata) => {
      if (metadata.size !== lengthAtOpen) {
        throw new UsenetSpoolError(
          'USENET_SPOOL_IO',
          'Decoded segment length differs from the exact file range'
        );
      }
      const finalByteRange = metadata.byteRange;
      if (
        (byteRangeAtOpen === undefined) !== (finalByteRange === undefined) ||
        (byteRangeAtOpen !== undefined &&
          finalByteRange !== undefined &&
          (finalByteRange[0] !== byteRangeAtOpen[0] ||
            finalByteRange[1] !== byteRangeAtOpen[1]))
      ) {
        throw new UsenetSpoolError(
          'USENET_SPOOL_METADATA_MISMATCH',
          'Decoded segment metadata changed after reader publication'
        );
      }
    });
    void completion?.catch(() => undefined);
    const reader = this.artifact.createReadStream({
      ...options,
      completion,
    });
    this.reader = reader;
    reader.once('close', () => {
      void this.release().catch((error: unknown) => {
        logger.warn({ err: error }, 'failed to release spool artifact reader');
      });
    });
    return reader;
  }

  release(): Promise<void> {
    if (this.releasePromise) return this.releasePromise;
    this.released = true;
    if (this.reader && !this.reader.destroyed) this.reader.destroy();
    this.releasePromise = this.releaseReference();
    return this.releasePromise;
  }
}

class ZeroReadable extends Readable {
  private remaining: number;

  constructor(length: number, highWaterMark: number) {
    super({ highWaterMark });
    this.remaining = length;
  }

  override _read(size: number): void {
    if (this.remaining === 0) {
      this.push(null);
      return;
    }
    const bytes = Math.min(
      this.remaining,
      SEGMENT_STREAM_MAX_CHUNK_BYTES,
      Math.max(1, size)
    );
    this.remaining -= bytes;
    this.push(Buffer.alloc(bytes));
  }
}

/**
 * A sparse logical segment that emits independently owned zero chunks of at
 * most 64 KiB. Its Readable queue is bounded by the 2 MiB HWM cap and no
 * allocation scales with the logical hole length.
 */
export class ZeroSegmentArtifact implements SegmentArtifact {
  readonly metadata: DecodedSegmentMetadata;
  readonly storage = 'zero' as const;

  private reader: Readable | undefined;
  private released = false;

  constructor(readonly length: number) {
    if (!Number.isSafeInteger(length) || length < 0) {
      throw new RangeError(
        'Zero segment artifact length must be a safe non-negative integer'
      );
    }
    this.metadata = { size: length };
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    if (this.released) {
      throw new Error('Zero segment artifact has been released');
    }
    if (this.reader) {
      throw new Error('Segment artifact handles support one reader');
    }
    const range = validateReadRange(this.length, options);
    const reader = new ZeroReadable(
      range.endExclusive - range.start,
      range.highWaterMark
    );
    this.reader = reader;
    reader.once('close', () => {
      this.released = true;
    });
    if (options.signal) addAbortSignal(options.signal, reader);
    return reader;
  }

  release(): Promise<void> {
    if (this.released) return Promise.resolve();
    this.released = true;
    if (this.reader && !this.reader.destroyed) this.reader.destroy();
    return Promise.resolve();
  }
}
