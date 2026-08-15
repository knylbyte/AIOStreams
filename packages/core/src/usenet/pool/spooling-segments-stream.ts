import { Readable } from 'node:stream';
import { createLogger } from '../../logging/logger.js';
import { definitiveLossKind, NntpError } from '../nntp/errors.js';
import type { HoleDecision, HoleKind } from '../holes.js';
import type { ByteLease } from './byte-budget.js';
import {
  ZeroSegmentArtifact,
  type SegmentArtifact,
  type SegmentArtifactFetchOptions,
} from './segment-artifact.js';
import type { CommandPriority, NzbSegmentRef } from '../types.js';

const logger = createLogger('usenet/spooling-segments');

/** Narrow source contract needed by the ordered spool reader. */
export interface SpoolingSegmentArtifactSource {
  fetchSegmentArtifact(
    segment: NzbSegmentRef,
    nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority,
    options?: SegmentArtifactFetchOptions
  ): Promise<SegmentArtifact>;
  acquireSegmentStreamMemory(
    priority: CommandPriority,
    signal?: AbortSignal
  ): Promise<ByteLease>;
}

export interface SpoolingSegmentsStreamOptions {
  readonly pool: SpoolingSegmentArtifactSource;
  /** Segments in exact file order, starting with the range's first segment. */
  readonly segments: readonly NzbSegmentRef[];
  readonly nzbHash: string;
  /** Hard bound on planned fetch/artifact tasks retained by this stream. */
  readonly maxPrefetchSegments: number;
  readonly readerHighWaterMarkBytes: number;
  /** Bytes discarded from the first relevant artifact. */
  readonly skipBytes?: number;
  /** Exact post-skip output cap. */
  readonly limitBytes?: number;
  readonly priority: CommandPriority;
  readonly signal?: AbortSignal;
  readonly sizeForSegment?: (idx: number) => number | undefined;
  readonly onHole?: (
    idx: number,
    bytes: number,
    kind: HoleKind
  ) => HoleDecision;
  readonly knownHoles?: ReadonlySet<number>;
  /** Located complete or growing artifact for local segment zero; ownership transfers. */
  readonly initialArtifact?: SegmentArtifact;
}

interface PlannedSegment {
  readonly idx: number;
  settled: Promise<void>;
  artifact?: SegmentArtifact;
  error?: unknown;
  done: boolean;
  releasing: boolean;
}

interface ActiveArtifactReader {
  readonly task: PlannedSegment;
  readonly reader: Readable;
  readonly onData: (chunk: Buffer) => void;
  readonly onEnd: () => void;
  readonly onError: (error: Error) => void;
  readonly onClose: () => void;
  remainingBytes: number;
  terminal: boolean;
}

function isNonNegativeSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

/**
 * Ordered, file-backed segment output for `segment_spooling`.
 *
 * Invariants:
 *
 * - `planned.size <= maxPrefetchSegments`; completed future artifacts retain
 *   only disk/file references and never complete segment Buffers;
 * - only local segment zero may resolve as growing; every future task waits
 *   for complete provider failover and producer validation;
 * - exactly one artifact reader may feed the outer Readable at a time;
 * - output order is monotonically increasing by local segment index;
 * - the stream-level memory lease covers the inner and outer Readable HWMs,
 *   is acquired before dispatch, and remains held until producer resources
 *   are detached AND every retained output queue is drained or destroyed;
 * - satisfying a finite byte range stops further output immediately, but
 *   successful EOF is linearized only by the active artifact reader's
 *   producer-validated `end` event;
 * - every normal EOF, abort and error path removes all cross-stream listeners.
 */
export class SpoolingSegmentsStream extends Readable {
  private readonly pool: SpoolingSegmentArtifactSource;
  private readonly segments: readonly NzbSegmentRef[];
  private readonly nzbHash: string;
  private readonly maxPrefetchSegments: number;
  private readonly readerHighWaterMarkBytes: number;
  private readonly priority: CommandPriority;
  private readonly externalSignal: AbortSignal | undefined;
  private readonly sizeForSegment:
    | ((idx: number) => number | undefined)
    | undefined;
  private readonly onHole:
    | ((idx: number, bytes: number, kind: HoleKind) => HoleDecision)
    | undefined;
  private readonly knownHoles: ReadonlySet<number> | undefined;
  private readonly controller = new AbortController();
  private readonly planned = new Map<number, PlannedSegment>();
  private initialArtifact: SegmentArtifact | undefined;

  private onExternalAbort: (() => void) | undefined;
  private streamLease: ByteLease | undefined;
  private active: ActiveArtifactReader | undefined;
  private nextPlan = 0;
  private nextEmit = 0;
  private skipRemaining: number;
  private limitRemaining: number;
  private startPromise: Promise<void> | undefined;
  private producerCleanupPromise: Promise<void> | undefined;
  private ending = false;
  private backpressured = false;
  private rangeSatisfied = false;
  private streamLifecycleEnded = false;
  private streamLeaseRetained = false;

  constructor(options: SpoolingSegmentsStreamOptions) {
    if (!isPositiveSafeInteger(options.maxPrefetchSegments)) {
      throw new RangeError('Spooling prefetch must be a safe positive integer');
    }
    if (!isPositiveSafeInteger(options.readerHighWaterMarkBytes)) {
      throw new RangeError(
        'Spooling reader high-water mark must be a safe positive integer'
      );
    }
    const skipBytes = options.skipBytes ?? 0;
    const limitBytes = options.limitBytes ?? Number.POSITIVE_INFINITY;
    if (!isNonNegativeSafeInteger(skipBytes)) {
      throw new RangeError('Spooling skipBytes must be a safe integer');
    }
    if (
      limitBytes !== Number.POSITIVE_INFINITY &&
      !isNonNegativeSafeInteger(limitBytes)
    ) {
      throw new RangeError('Spooling limitBytes must be a safe integer');
    }
    super({
      highWaterMark: options.readerHighWaterMarkBytes,
      autoDestroy: true,
    });
    this.pool = options.pool;
    this.segments = options.segments;
    this.nzbHash = options.nzbHash;
    this.maxPrefetchSegments = options.maxPrefetchSegments;
    this.readerHighWaterMarkBytes = options.readerHighWaterMarkBytes;
    this.priority = options.priority;
    this.externalSignal = options.signal;
    this.sizeForSegment = options.sizeForSegment;
    this.onHole = options.onHole;
    this.knownHoles = options.knownHoles;
    this.initialArtifact = options.initialArtifact;
    this.skipRemaining = skipBytes;
    this.limitRemaining = limitBytes;

    if (options.signal?.aborted) {
      queueMicrotask(() => this.destroy(this.abortError()));
    } else if (options.signal) {
      this.onExternalAbort = () => this.destroy(this.abortError());
      options.signal.addEventListener('abort', this.onExternalAbort, {
        once: true,
      });
    }
  }

  override _read(): void {
    if (this.ending || this.destroyed) return;
    this.backpressured = false;
    this.active?.reader.resume();
    if (!this.startPromise) {
      this.startPromise = this.startOnce();
    } else {
      this.pump();
    }
  }

  override pause(): this {
    this.active?.reader.pause();
    return super.pause();
  }

  override resume(): this {
    const result = super.resume();
    if (!this.backpressured) this.active?.reader.resume();
    return result;
  }

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    void this.cleanupProducer(error).then(
      () => this.finishDestroy(error, callback),
      (cleanupError: unknown) => {
        this.finishDestroy(
          error ??
            (cleanupError instanceof Error
              ? cleanupError
              : new Error(String(cleanupError))),
          callback
        );
      }
    );
  }

  /**
   * Keep the stream-memory lease alive for a bounded relay queue which takes
   * ownership of emitted chunks. The returned release callback is idempotent;
   * it must be called only after that queue is drained or destroyed.
   */
  retainStreamMemoryLease(): () => void {
    if (this.streamLeaseRetained) {
      throw new Error('Spooling stream memory lease already has a relay owner');
    }
    this.streamLeaseRetained = true;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.streamLeaseRetained = false;
      this.releaseStreamLeaseIfUnused();
    };
  }

  private async startOnce(): Promise<void> {
    if (this.limitRemaining === 0 || this.segments.length === 0) {
      this.finishNormally();
      return;
    }
    try {
      const lease = await this.pool.acquireSegmentStreamMemory(
        this.priority,
        this.controller.signal
      );
      if (this.ending || this.destroyed) {
        lease.release();
        return;
      }
      this.streamLease = lease;
      this.planMore();
      this.pump();
    } catch (error) {
      if (this.ending || this.destroyed) return;
      this.destroy(error instanceof Error ? error : new Error(String(error)));
    }
  }

  private planMore(): void {
    while (
      !this.ending &&
      this.planned.size < this.maxPrefetchSegments &&
      this.nextPlan < this.segments.length
    ) {
      const idx = this.nextPlan++;
      const task: PlannedSegment = {
        idx,
        settled: Promise.resolve(),
        done: false,
        releasing: false,
      };
      this.planned.set(idx, task);

      if (idx === 0 && this.initialArtifact) {
        task.artifact = this.initialArtifact;
        this.initialArtifact = undefined;
        task.done = true;
        continue;
      }

      const knownHole = this.knownHoles?.has(idx)
        ? this.createHoleArtifact(idx, 'missing')
        : undefined;
      if (knownHole) {
        task.artifact = knownHole;
        task.done = true;
        continue;
      }

      const expectedLength = this.sizeForSegment?.(idx);
      task.settled = this.pool
        .fetchSegmentArtifact(
          this.segments[idx],
          this.nzbHash,
          this.controller.signal,
          this.priority,
          {
            expectedLength,
            allowGrowing: idx === 0,
          }
        )
        .then(
          (artifact) => {
            task.artifact = artifact;
          },
          (error: unknown) => {
            task.error = error;
          }
        )
        .finally(() => {
          task.done = true;
          if (!this.ending && !this.destroyed) this.pump();
        });
    }
  }

  private pump(): void {
    if (this.ending || this.destroyed || !this.streamLease || this.active) {
      return;
    }
    const task = this.planned.get(this.nextEmit);
    if (!task) {
      if (this.nextEmit >= this.segments.length) this.finishNormally();
      return;
    }
    if (!task.done || task.releasing) return;

    if (task.error !== undefined) {
      const kind = definitiveLossKind(task.error);
      const zero =
        kind === undefined
          ? undefined
          : this.createHoleArtifact(task.idx, kind);
      if (!zero) {
        const failure =
          task.error instanceof Error
            ? task.error
            : new Error(String(task.error));
        logger.debug(
          { nzbHash: this.nzbHash, segmentIndex: task.idx, err: failure },
          'spooling segment task failed'
        );
        this.destroy(failure);
        return;
      }
      task.error = undefined;
      task.artifact = zero;
    }

    const artifact = task.artifact;
    if (!artifact) {
      this.destroy(new Error('Spooling segment task settled without storage'));
      return;
    }
    this.startArtifactReader(task, artifact);
  }

  private startArtifactReader(
    task: PlannedSegment,
    artifact: SegmentArtifact
  ): void {
    const start = Math.min(this.skipRemaining, artifact.length);
    this.skipRemaining -= start;
    const available = artifact.length - start;
    const take = Number.isFinite(this.limitRemaining)
      ? Math.min(available, this.limitRemaining)
      : available;
    if (take <= 0) {
      void this.releaseCurrent(task);
      return;
    }

    let reader: Readable;
    try {
      reader = artifact.createReadStream({
        start,
        endExclusive: start + take,
        signal: this.controller.signal,
        highWaterMark: this.readerHighWaterMarkBytes,
      });
    } catch (error) {
      this.destroy(error instanceof Error ? error : new Error(String(error)));
      return;
    }
    const active: ActiveArtifactReader = {
      task,
      reader,
      remainingBytes: take,
      terminal: false,
      onData: (chunk) => this.onArtifactData(active, chunk),
      onEnd: () => this.onArtifactEnd(active),
      onError: (error) => this.onArtifactError(active, error),
      onClose: () => this.onArtifactClose(active),
    };
    this.active = active;
    reader.on('data', active.onData);
    reader.once('end', active.onEnd);
    reader.once('error', active.onError);
    reader.once('close', active.onClose);
  }

  private onArtifactData(active: ActiveArtifactReader, chunk: Buffer): void {
    if (this.active !== active || active.terminal || this.ending) return;
    if (!Buffer.isBuffer(chunk)) {
      this.destroy(
        new TypeError('Segment artifact reader emitted non-Buffer data')
      );
      return;
    }
    if (chunk.length > active.remainingBytes) {
      this.destroy(new Error('Segment artifact reader exceeded its range'));
      return;
    }
    active.remainingBytes -= chunk.length;
    let output = chunk;
    if (output.length > this.limitRemaining) {
      output = output.subarray(0, this.limitRemaining);
    }
    this.limitRemaining -= output.length;
    if (output.length > 0 && !this.push(output)) {
      this.backpressured = true;
      active.reader.pause();
    }
    if (this.limitRemaining === 0) {
      // The reader's endExclusive matches the satisfied range. It may already
      // have emitted every requested byte while its producer-completion gate
      // is still pending, so only its validated `end` may finish this stream.
      this.rangeSatisfied = true;
    }
  }

  private onArtifactEnd(active: ActiveArtifactReader): void {
    if (this.active !== active || active.terminal || this.ending) return;
    active.terminal = true;
    if (active.remainingBytes !== 0) {
      this.destroy(new Error('Segment artifact reader ended before its range'));
      return;
    }
    void this.releaseCurrent(active.task, this.rangeSatisfied);
  }

  private onArtifactError(active: ActiveArtifactReader, error: Error): void {
    if (this.active !== active || active.terminal || this.ending) return;
    active.terminal = true;
    this.destroy(error);
  }

  private onArtifactClose(active: ActiveArtifactReader): void {
    if (this.active !== active || active.terminal || this.ending) return;
    active.terminal = true;
    this.destroy(new Error('Segment artifact reader closed before end'));
  }

  private async releaseCurrent(
    task: PlannedSegment,
    finishAfterRelease = false
  ): Promise<void> {
    if (task.releasing) return;
    task.releasing = true;
    const active = this.active;
    if (active?.task === task) {
      this.detachActiveReader(active);
      this.active = undefined;
    }
    try {
      await task.artifact?.release();
    } catch (error) {
      if (!this.ending && !this.destroyed) {
        this.destroy(error instanceof Error ? error : new Error(String(error)));
      }
      return;
    }
    if (this.ending || this.destroyed) return;
    this.planned.delete(task.idx);
    if (finishAfterRelease) {
      this.finishNormally();
      return;
    }
    this.nextEmit++;
    this.planMore();
    this.pump();
  }

  private createHoleArtifact(
    idx: number,
    kind: HoleKind
  ): ZeroSegmentArtifact | undefined {
    const bytes = this.sizeForSegment?.(idx);
    if (
      bytes === undefined ||
      bytes <= 0 ||
      this.onHole?.(idx, bytes, kind) !== 'pad'
    ) {
      return undefined;
    }
    logger.warn(
      { nzbHash: this.nzbHash, segmentIndex: idx, bytes, kind },
      kind === 'undecodable'
        ? 'spooling segment undecodable on all providers; zero-filled'
        : 'spooling segment missing on all providers; zero-filled'
    );
    return new ZeroSegmentArtifact(bytes);
  }

  private finishNormally(): void {
    if (this.ending || this.destroyed) return;
    this.ending = true;
    void this.cleanupProducer()
      .then(() => {
        if (!this.destroyed) this.push(null);
      })
      .catch((error: unknown) => {
        if (!this.destroyed) {
          this.destroy(
            error instanceof Error ? error : new Error(String(error))
          );
        }
      });
  }

  private cleanupProducer(reason?: Error | null): Promise<void> {
    if (this.producerCleanupPromise) return this.producerCleanupPromise;
    this.ending = true;
    this.removeExternalAbortListener();
    if (!this.controller.signal.aborted) this.controller.abort(reason);
    const active = this.active;
    if (active) {
      active.terminal = true;
      this.detachActiveReader(active);
      this.active = undefined;
      this.discardReadableQueue(active.reader);
      if (!active.reader.destroyed) active.reader.destroy();
    }

    this.producerCleanupPromise = (async () => {
      await Promise.allSettled(
        this.startPromise === undefined ? [] : [this.startPromise]
      );
      const tasks = [...this.planned.values()];
      await Promise.allSettled(tasks.map((task) => task.settled));
      const initialArtifact = this.initialArtifact;
      this.initialArtifact = undefined;
      const releases = await Promise.allSettled([
        ...tasks.map((task) => task.artifact?.release()),
        initialArtifact?.release(),
      ]);
      this.planned.clear();
      const failedRelease = releases.find(
        (result): result is PromiseRejectedResult =>
          result.status === 'rejected'
      );
      if (failedRelease) throw failedRelease.reason;
    })();
    return this.producerCleanupPromise;
  }

  private finishDestroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    // On normal auto-destroy, `end` proves the queue was consumed. Explicit
    // destroy/error may leave queued Buffers behind, so discard them in chunks
    // no larger than the configured HWM before returning their budget.
    if (!this.readableEnded) this.discardReadableQueue(this);
    this.streamLifecycleEnded = true;
    this.releaseStreamLeaseIfUnused();
    callback(error);
  }

  private discardReadableQueue(reader: Readable): void {
    reader.pause();
    while (reader.readableLength > 0) {
      const before = reader.readableLength;
      reader.read(Math.min(before, reader.readableHighWaterMark));
      if (reader.readableLength >= before) break;
    }
  }

  private releaseStreamLeaseIfUnused(): void {
    if (!this.streamLifecycleEnded || this.streamLeaseRetained) return;
    this.streamLease?.release();
    this.streamLease = undefined;
  }

  private detachActiveReader(active: ActiveArtifactReader): void {
    active.reader.removeListener('data', active.onData);
    active.reader.removeListener('end', active.onEnd);
    active.reader.removeListener('error', active.onError);
    active.reader.removeListener('close', active.onClose);
  }

  private removeExternalAbortListener(): void {
    if (this.externalSignal && this.onExternalAbort) {
      this.externalSignal.removeEventListener('abort', this.onExternalAbort);
      this.onExternalAbort = undefined;
    }
  }

  private abortError(): NntpError {
    return new NntpError('connection', 'aborted');
  }
}
