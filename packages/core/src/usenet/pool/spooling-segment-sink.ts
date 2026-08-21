import assert from 'node:assert/strict';
import type { ByteLease } from './byte-budget.js';
import type {
  DirectDecodeByteSink,
  DirectDecodeCommitOptions,
} from './streaming-yenc-article-decoder.js';
import { UsenetSpoolError } from '../spool/errors.js';
import type { SegmentSpoolingHotpathCounters } from './hotpath-counters.js';

/**
 * Yielding after each settled write breaks long chains of writer/drain
 * microtasks. The writer advances `committedBytes` and the lifecycle observer
 * publishes the first artifact before this turn boundary, so first-byte
 * delivery remains eligible immediately.
 */
const SETTLED_BATCHES_PER_EVENT_LOOP_TURN = 1;
/** Fixed per-batch bound for extra owner-fair turns under real contention. */
const MAX_COOPERATIVE_YIELDS_PER_BATCH = 16;

type DecodeBatchOwnership =
  | 'idle'
  | 'local'
  | 'awaiting-reservation'
  | 'writer-owned';

/** Narrow artifact ownership surface consumed by the direct decode sink. */
export interface SpoolingSinkArtifact {
  readonly reservedBytes: number;
  readonly committedBytes: number;
  write(chunk: Buffer, lease: ByteLease): boolean;
  grow(bytes: number): Promise<void>;
  complete(): Promise<void>;
  fail(error: Error): void;
}

/** Fixed-cost first-write hook used by production-path latency measurement. */
export interface SpoolingSinkLifecycleObserver {
  onFirstSinkCommit?(): void;
  /** True while distinct playback owners need a cooperative decode turn. */
  shouldYieldBeforeNextDecodeInput?(): boolean;
}

function asError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new UsenetSpoolError('USENET_SPOOL_IO', 'Segment spool sink failed', {
        cause: error,
      });
}

/**
 * Backpressure boundary from the streaming yEnc decoder into one spool file.
 *
 * The caller holds one global memory lease of `leasedWindowBytes` for this
 * sink's complete lifetime. Each accepted Buffer receives an exact logical
 * child lease inside that reserved window; the writer releases that child only
 * after its asynchronous write settles. Native decode fills the free suffix of
 * one unpooled two-chunk batch. The first payload is flushed promptly; steady
 * state publishes up to two decoder inputs per write. Writer settlement never
 * speculatively reserves a hypothetical next batch: a local batch with actual
 * decoded payload first proves its exact disk reservation, then transfers the
 * same Buffer view to the writer without copying. At most one local batch, one
 * reservation operation, and one writer-owned batch can exist at a time. The
 * fixed resource order is: lifetime download-memory lease, local decoded
 * batch, optional spool reservation, then writer child lease. Waiting for
 * spool capacity acquires no additional memory or file owner, so concurrent
 * sinks cannot form a hold-and-wait cycle.
 */
export class SpoolingSegmentSink implements DirectDecodeByteSink {
  readonly directDecode = true as const;
  readonly maxDecodeInputBytes: number;
  private retainedBytes = 0;
  private batch: Buffer | undefined;
  private batchBytes = 0;
  private batchOwnership: DecodeBatchOwnership = 'idle';
  private pendingChunk: Buffer | undefined;
  private decodeTargetBytes = 0;
  private firstPayloadPending = true;
  private failure: Error | undefined;
  private drainListener: (() => void) | undefined;
  private readyForNextWrite = true;
  private pendingOperation: Promise<void> = Promise.resolve();
  private writeSettlement: PromiseWithResolvers<void> | undefined;
  private secondaryFailure: Error | undefined;
  private endPromise: Promise<void> | undefined;
  private inputComplete = false;
  private ending = false;
  private closed = false;
  private sinkCommitNotified = false;
  private settledBatches = 0;
  private cooperativeYieldPending = false;
  private cooperativeYieldsForBatch = 0;
  private cooperativeYieldHandle: ReturnType<typeof setImmediate> | undefined;

  constructor(
    private readonly artifact: SpoolingSinkArtifact,
    private readonly leasedWindowBytes: number,
    private readonly requiredHeadroomBytes: number,
    private readonly hotpathCounters?: SegmentSpoolingHotpathCounters,
    private readonly lifecycleObserver?: SpoolingSinkLifecycleObserver
  ) {
    if (
      !Number.isSafeInteger(leasedWindowBytes) ||
      leasedWindowBytes <= 0 ||
      !Number.isSafeInteger(requiredHeadroomBytes) ||
      requiredHeadroomBytes <= 0 ||
      requiredHeadroomBytes > leasedWindowBytes
    ) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool sink received an invalid leased memory window'
      );
    }
    if (leasedWindowBytes < 2 * requiredHeadroomBytes) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool decode batch requires two decoder windows'
      );
    }
    this.maxDecodeInputBytes = requiredHeadroomBytes;
    this.assertInvariants();
  }

  acquireDecodeTarget(maxDecodedBytes: number): Buffer {
    this.assertWritable();
    if (
      !Number.isSafeInteger(maxDecodedBytes) ||
      maxDecodedBytes <= 0 ||
      maxDecodedBytes > this.maxDecodeInputBytes ||
      this.decodeTargetBytes !== 0
    ) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool decoder requested an invalid direct output target'
      );
    }
    const batch = this.decodeBatch();
    if (maxDecodedBytes > batch.length - this.batchBytes) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool decode batch has no guaranteed suffix'
      );
    }
    this.decodeTargetBytes = maxDecodedBytes;
    return batch.subarray(this.batchBytes, this.batchBytes + maxDecodedBytes);
  }

  commitDecoded(bytes: number, options: DirectDecodeCommitOptions): boolean {
    if (
      typeof options.inputBoundary !== 'boolean' ||
      typeof options.articleEnded !== 'boolean'
    ) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Segment spool decoder supplied invalid commit metadata'
      );
    }
    if (
      !Number.isSafeInteger(bytes) ||
      bytes < 0 ||
      bytes > this.decodeTargetBytes
    ) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool decoder committed an invalid direct output length'
      );
    }
    this.decodeTargetBytes = 0;
    this.batchBytes += bytes;
    if (bytes > 0) this.batchOwnership = 'local';
    if (options.articleEnded) this.inputComplete = true;
    if (!options.inputBoundary || this.batchBytes === 0) {
      this.assertInvariants();
      return true;
    }
    const remaining = this.leasedWindowBytes - this.batchBytes;
    if (this.firstPayloadPending || remaining < this.requiredHeadroomBytes) {
      this.firstPayloadPending = false;
      return this.flushDecodeBatch(false, options.articleEnded);
    }
    if (
      !this.cooperativeYieldPending &&
      this.cooperativeYieldsForBatch < MAX_COOPERATIVE_YIELDS_PER_BATCH &&
      this.lifecycleObserver?.shouldYieldBeforeNextDecodeInput?.() === true
    ) {
      // Keep the local half-batch in its already leased backing while yielding
      // transport ownership to another stream. No write or child lease exists
      // yet, so the next input can still complete the same bounded batch.
      this.cooperativeYieldPending = true;
      this.cooperativeYieldsForBatch++;
      this.assertInvariants();
      return false;
    }
    this.assertInvariants();
    return true;
  }

  /** Transfer one decoder-owned Buffer and its exact logical lease to disk. */
  write(chunk: Buffer): boolean {
    this.assertWritable();
    if (!Number.isSafeInteger(chunk.length) || chunk.length <= 0) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool chunks must contain a safe positive byte count'
      );
    }
    if (
      chunk.length > this.leasedWindowBytes ||
      this.batchOwnership !== 'idle'
    ) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool decoder exceeded its leased memory window'
      );
    }

    this.batchBytes = chunk.length;
    this.batchOwnership = 'local';
    this.pendingChunk = chunk;
    return this.publishLocalBatch();
  }

  onceDrain(listener: () => void): void {
    if (this.drainListener) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Segment spool sink already has a drain listener'
      );
    }
    if (
      this.failure ||
      (this.readyForNextWrite && !this.cooperativeYieldPending)
    ) {
      queueMicrotask(listener);
      return;
    }
    this.drainListener = listener;
    if (this.cooperativeYieldPending) {
      this.cooperativeYieldHandle = setImmediate(() => {
        this.cooperativeYieldHandle = undefined;
        this.cooperativeYieldPending = false;
        this.emitDrain();
      });
    }
  }

  end(): Promise<void> {
    this.endPromise ??= this.endOnce();
    return this.endPromise;
  }

  private async endOnce(): Promise<void> {
    this.ending = true;
    this.inputComplete = true;
    try {
      this.cancelCooperativeYield();
      if (this.decodeTargetBytes !== 0) {
        throw new UsenetSpoolError(
          'USENET_MEMORY_BUDGET',
          'Segment spool sink ended with an active decode target'
        );
      }
      if (this.batchOwnership === 'local') {
        this.flushDecodeBatch(true, true);
      }
      await this.awaitBatchSettlement();
      if (this.failure) throw this.terminalFailure();
      if (
        this.retainedBytes !== 0 ||
        this.batchOwnership !== 'idle' ||
        this.batchBytes !== 0
      ) {
        throw new UsenetSpoolError(
          'USENET_MEMORY_BUDGET',
          'Segment spool sink ended with retained decoder bytes'
        );
      }
      await this.artifact.complete();
    } finally {
      this.closed = true;
    }
  }

  fail(error: Error): void {
    if (this.failure) return;
    this.failure = error;
    this.cancelCooperativeYield();
    if (this.batchOwnership === 'idle' || this.batchOwnership === 'local') {
      this.clearLocalBatch();
      this.readyForNextWrite = true;
    }
    this.decodeTargetBytes = 0;
    this.artifact.fail(error);
    this.emitDrain();
  }

  private createChunkLease(bytes: number): ByteLease {
    assert.equal(this.batchOwnership, 'writer-owned');
    assert.equal(this.retainedBytes, bytes);
    this.assertInvariants();
    let released = false;
    return {
      bytes,
      release: () => {
        if (released) return;
        released = true;
        this.retainedBytes -= bytes;
        this.clearLocalBatch();
        if (!this.sinkCommitNotified && this.artifact.committedBytes > 0) {
          this.sinkCommitNotified = true;
          this.lifecycleObserver?.onFirstSinkCommit?.();
        }
        if (this.failure || this.inputComplete || this.ending || this.closed) {
          this.finishWriterSettlement();
        } else {
          this.scheduleWriterSettlement();
        }
        this.assertInvariants();
      },
    };
  }

  private scheduleWriterSettlement(): void {
    this.settledBatches++;
    const yieldToEventLoop =
      this.settledBatches % SETTLED_BATCHES_PER_EVENT_LOOP_TURN === 0;
    this.pendingOperation = yieldToEventLoop
      ? new Promise<void>((resolve) => setImmediate(resolve)).then(() => {
          this.finishWriterSettlement();
        })
      : Promise.resolve().then(() => {
          this.finishWriterSettlement();
        });
  }

  private finishWriterSettlement(): void {
    this.readyForNextWrite = true;
    this.writeSettlement?.resolve();
    this.writeSettlement = undefined;
    this.emitDrain();
  }

  private publishLocalBatch(): false {
    assert.equal(this.batchOwnership, 'local');
    const chunk = this.pendingChunk;
    assert(chunk);
    assert.equal(chunk.length, this.batchBytes);
    this.readyForNextWrite = false;
    const available =
      this.artifact.reservedBytes - this.artifact.committedBytes;
    if (available >= chunk.length) {
      this.publishWriterOwnedBatch();
      return false;
    }

    const missing = chunk.length - available;
    this.batchOwnership = 'awaiting-reservation';
    if (this.hotpathCounters) {
      this.hotpathCounters.spoolGrowthRequests++;
      this.hotpathCounters.spoolGrowthBytes += missing;
      this.hotpathCounters.growthRequestsWithDecodedPayload++;
    }
    let growth: Promise<void>;
    try {
      growth = this.artifact.grow(missing);
    } catch (error) {
      const failure = this.recordOperationalFailure(error);
      this.clearLocalBatch();
      this.readyForNextWrite = true;
      this.emitDrain();
      throw failure;
    }
    this.pendingOperation = growth.then(
      () => {
        if (this.failure) {
          this.clearLocalBatch();
          this.readyForNextWrite = true;
          this.emitDrain();
          return;
        }
        try {
          this.publishWriterOwnedBatch();
        } catch (error) {
          this.recordOperationalFailure(error);
          if (this.batchOwnership !== 'idle') this.clearLocalBatch();
          this.readyForNextWrite = true;
          this.emitDrain();
        }
      },
      (error: unknown) => {
        this.recordOperationalFailure(error);
        this.clearLocalBatch();
        this.readyForNextWrite = true;
        this.emitDrain();
      }
    );
    this.assertInvariants();
    return false;
  }

  private publishWriterOwnedBatch(): void {
    assert(
      this.batchOwnership === 'local' ||
        this.batchOwnership === 'awaiting-reservation'
    );
    const chunk = this.pendingChunk;
    assert(chunk);
    const available =
      this.artifact.reservedBytes - this.artifact.committedBytes;
    if (available < chunk.length) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CAPACITY',
        'Segment spool reservation did not cover the decoded payload'
      );
    }
    this.batchOwnership = 'writer-owned';
    this.retainedBytes = chunk.length;
    this.writeSettlement = Promise.withResolvers<void>();
    const lease = this.createChunkLease(chunk.length);
    try {
      this.artifact.write(chunk, lease);
      if (this.hotpathCounters) {
        this.hotpathCounters.decodedBatchesCommitted++;
        this.hotpathCounters.sinkDrainCycles++;
      }
    } catch (error) {
      const failure = this.recordOperationalFailure(error);
      lease.release();
      throw failure;
    }
    this.assertInvariants();
  }

  private async awaitBatchSettlement(): Promise<void> {
    while (
      this.batchOwnership === 'awaiting-reservation' ||
      this.batchOwnership === 'writer-owned' ||
      this.writeSettlement
    ) {
      await this.pendingOperation;
      const settlement = this.writeSettlement;
      if (settlement) await settlement.promise;
    }
    await this.pendingOperation;
  }

  private recordOperationalFailure(error: unknown): Error {
    const failure = asError(error);
    if (this.failure) {
      if (failure !== this.failure) this.secondaryFailure ??= failure;
    } else {
      this.failure = failure;
      this.artifact.fail(failure);
    }
    return failure;
  }

  private clearLocalBatch(): void {
    assert.equal(this.retainedBytes, 0);
    this.batchBytes = 0;
    this.batchOwnership = 'idle';
    this.pendingChunk = undefined;
    this.cooperativeYieldsForBatch = 0;
  }

  private emitDrain(): void {
    const listener = this.drainListener;
    if (!listener) return;
    this.drainListener = undefined;
    queueMicrotask(listener);
  }

  private assertWritable(
    allowEnding = false,
    allowInputComplete = false
  ): void {
    if (this.failure) throw this.terminalFailure();
    if (
      this.closed ||
      (this.ending && !allowEnding) ||
      (this.inputComplete && !allowInputComplete)
    ) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Segment spool sink is ending'
      );
    }
    if (
      !this.readyForNextWrite ||
      this.retainedBytes !== 0 ||
      (this.batchOwnership !== 'idle' && this.batchOwnership !== 'local')
    ) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool producer wrote before its drain notification'
      );
    }
  }

  private assertInvariants(): void {
    assert(Number.isSafeInteger(this.retainedBytes));
    assert(this.retainedBytes >= 0);
    assert(this.retainedBytes <= this.leasedWindowBytes);
    assert(Number.isSafeInteger(this.batchBytes));
    assert(this.batchBytes >= 0);
    assert(this.batchBytes <= this.leasedWindowBytes);
    assert(Number.isSafeInteger(this.decodeTargetBytes));
    assert(this.decodeTargetBytes >= 0);
    assert(this.decodeTargetBytes <= this.maxDecodeInputBytes);
    assert(this.cooperativeYieldsForBatch >= 0);
    assert(this.cooperativeYieldsForBatch <= MAX_COOPERATIVE_YIELDS_PER_BATCH);
    if (this.closed) assert(this.ending);
    if (this.ending) assert(this.inputComplete);
    if (this.cooperativeYieldPending) {
      assert.equal(this.readyForNextWrite, true);
      assert.equal(this.retainedBytes, 0);
      assert(this.batchBytes > 0);
      assert.equal(this.batchOwnership, 'local');
    }
    switch (this.batchOwnership) {
      case 'idle':
        assert.equal(this.batchBytes, 0);
        assert.equal(this.retainedBytes, 0);
        assert.equal(this.pendingChunk, undefined);
        break;
      case 'local':
        assert(this.batchBytes > 0);
        assert.equal(this.retainedBytes, 0);
        break;
      case 'awaiting-reservation':
        assert(this.batchBytes > 0);
        assert.equal(this.retainedBytes, 0);
        assert.equal(this.readyForNextWrite, false);
        assert.equal(this.pendingChunk?.length, this.batchBytes);
        break;
      case 'writer-owned':
        assert(this.batchBytes > 0);
        assert.equal(this.retainedBytes, this.batchBytes);
        assert.equal(this.readyForNextWrite, false);
        assert.equal(this.pendingChunk?.length, this.batchBytes);
        break;
    }
  }

  private decodeBatch(): Buffer {
    if (!this.batch) {
      this.batch = Buffer.allocUnsafeSlow(this.leasedWindowBytes);
      if (this.hotpathCounters) {
        this.hotpathCounters.yencOutputBackingAllocations++;
      }
    }
    return this.batch;
  }

  private flushDecodeBatch(
    allowEnding = false,
    allowInputComplete = false
  ): false {
    this.assertWritable(allowEnding, allowInputComplete);
    if (this.batchBytes <= 0 || this.decodeTargetBytes !== 0) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool attempted to flush an invalid decode batch'
      );
    }
    const batch = this.decodeBatch();
    this.pendingChunk = batch.subarray(0, this.batchBytes);
    return this.publishLocalBatch();
  }

  private cancelCooperativeYield(): void {
    const handle = this.cooperativeYieldHandle;
    this.cooperativeYieldHandle = undefined;
    this.cooperativeYieldPending = false;
    if (handle) clearImmediate(handle);
  }

  private terminalFailure(): Error {
    const failure = this.failure;
    assert(failure);
    const secondary = this.secondaryFailure;
    if (!secondary) return failure;
    return new AggregateError(
      [failure, secondary],
      'Segment spool sink failed and cleanup also failed',
      { cause: failure }
    );
  }
}
