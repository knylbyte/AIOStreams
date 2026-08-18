import assert from 'node:assert/strict';
import type { ByteLease } from './byte-budget.js';
import type { DirectDecodeByteSink } from './streaming-yenc-article-decoder.js';
import { UsenetSpoolError } from '../spool/errors.js';
import type { SegmentSpoolingHotpathCounters } from './hotpath-counters.js';

const MEBIBYTE_BYTES = 1024 * 1024;

/** Narrow artifact ownership surface consumed by the direct decode sink. */
export interface SpoolingSinkArtifact {
  readonly reservedBytes: number;
  readonly committedBytes: number;
  write(chunk: Buffer, lease: ByteLease): boolean;
  grow(bytes: number): Promise<void>;
  complete(): Promise<void>;
  fail(error: Error): void;
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
 * state publishes up to two decoder inputs per write and resumes only after
 * both that write and any required disk-reservation growth complete.
 */
export class SpoolingSegmentSink implements DirectDecodeByteSink {
  readonly directDecode = true as const;
  readonly maxDecodeInputBytes: number;
  private retainedBytes = 0;
  private batch: Buffer | undefined;
  private batchBytes = 0;
  private decodeTargetBytes = 0;
  private firstPayloadPending = true;
  private failure: Error | undefined;
  private drainListener: (() => void) | undefined;
  private readyForNextWrite = true;
  private preparing: Promise<void> = Promise.resolve();
  private writeSettlement: PromiseWithResolvers<void> | undefined;
  private secondaryFailure: Error | undefined;

  constructor(
    private readonly artifact: SpoolingSinkArtifact,
    private readonly leasedWindowBytes: number,
    private readonly requiredHeadroomBytes: number,
    private readonly hotpathCounters?: SegmentSpoolingHotpathCounters
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

  commitDecoded(bytes: number, inputBoundary: boolean): boolean {
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
    if (!inputBoundary || this.batchBytes === 0) {
      this.assertInvariants();
      return true;
    }
    const remaining = this.leasedWindowBytes - this.batchBytes;
    if (this.firstPayloadPending || remaining < this.requiredHeadroomBytes) {
      this.firstPayloadPending = false;
      return this.flushDecodeBatch();
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
    if (chunk.length > this.leasedWindowBytes || this.retainedBytes !== 0) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool decoder exceeded its leased memory window'
      );
    }

    this.batchBytes = chunk.length;
    this.writeSettlement = Promise.withResolvers<void>();
    const lease = this.createChunkLease(chunk.length);
    this.readyForNextWrite = false;
    try {
      this.artifact.write(chunk, lease);
      if (this.hotpathCounters) {
        this.hotpathCounters.decodedBatchesCommitted++;
        this.hotpathCounters.sinkDrainCycles++;
      }
    } catch (error) {
      const failure = asError(error);
      this.fail(failure);
      lease.release();
      throw failure;
    }
    this.assertInvariants();
    // One retained chunk is the hard local high-water mark. The connection
    // resumes only after the writer releases its exact child lease.
    return false;
  }

  onceDrain(listener: () => void): void {
    if (this.drainListener) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Segment spool sink already has a drain listener'
      );
    }
    if (this.failure || this.readyForNextWrite) {
      queueMicrotask(listener);
      return;
    }
    this.drainListener = listener;
  }

  async end(): Promise<void> {
    if (this.decodeTargetBytes !== 0) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool sink ended with an active decode target'
      );
    }
    if (this.batchBytes > 0) this.flushDecodeBatch();
    const settlement = this.writeSettlement;
    if (settlement) await settlement.promise;
    await this.preparing;
    if (this.failure) throw this.terminalFailure();
    if (this.retainedBytes !== 0) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool sink ended with retained decoder bytes'
      );
    }
    await this.artifact.complete();
  }

  fail(error: Error): void {
    if (this.failure) return;
    this.failure = error;
    if (this.retainedBytes === 0) this.batchBytes = 0;
    this.decodeTargetBytes = 0;
    this.artifact.fail(error);
    this.emitDrain();
  }

  private createChunkLease(bytes: number): ByteLease {
    this.retainedBytes += bytes;
    this.assertInvariants();
    let released = false;
    return {
      bytes,
      release: () => {
        if (released) return;
        released = true;
        this.retainedBytes -= bytes;
        this.batchBytes = 0;
        if (this.failure) {
          // A writer-owned child may settle after the producer has already
          // failed. Growth belongs only to a live next-write admission; do not
          // resurrect it after terminal failure. Settlement still wakes every
          // drain/end owner exactly once.
          this.writeSettlement?.resolve();
          this.writeSettlement = undefined;
          this.emitDrain();
        } else {
          this.startPreparingNextWrite();
        }
        this.assertInvariants();
      },
    };
  }

  private startPreparingNextWrite(): void {
    this.preparing = this.prepareNextWrite().then(
      () => {
        this.writeSettlement?.resolve();
        this.writeSettlement = undefined;
        if (this.failure) return;
        this.readyForNextWrite = true;
        this.emitDrain();
      },
      (error: unknown) => {
        const failure = asError(error);
        if (this.failure) {
          if (failure !== this.failure) this.secondaryFailure ??= failure;
        } else {
          this.failure = failure;
          this.artifact.fail(failure);
        }
        this.writeSettlement?.resolve();
        this.writeSettlement = undefined;
        this.emitDrain();
      }
    );
  }

  private async prepareNextWrite(): Promise<void> {
    const available =
      this.artifact.reservedBytes - this.artifact.committedBytes;
    if (available >= this.requiredHeadroomBytes) return;
    const missing = this.requiredHeadroomBytes - available;
    const growth = Math.max(MEBIBYTE_BYTES, missing);
    await this.artifact.grow(growth);
  }

  private emitDrain(): void {
    const listener = this.drainListener;
    if (!listener) return;
    this.drainListener = undefined;
    queueMicrotask(listener);
  }

  private assertWritable(): void {
    if (this.failure) throw this.terminalFailure();
    if (!this.readyForNextWrite || this.retainedBytes !== 0) {
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
    if (this.retainedBytes > 0)
      assert.equal(this.retainedBytes, this.batchBytes);
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

  private flushDecodeBatch(): false {
    this.assertWritable();
    if (this.batchBytes <= 0 || this.decodeTargetBytes !== 0) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment spool attempted to flush an invalid decode batch'
      );
    }
    const batch = this.decodeBatch();
    const bytes = this.batchBytes;
    this.writeSettlement = Promise.withResolvers<void>();
    const lease = this.createChunkLease(bytes);
    this.readyForNextWrite = false;
    try {
      this.artifact.write(batch.subarray(0, bytes), lease);
      if (this.hotpathCounters) {
        this.hotpathCounters.decodedBatchesCommitted++;
        this.hotpathCounters.sinkDrainCycles++;
      }
    } catch (error) {
      const failure = asError(error);
      this.fail(failure);
      lease.release();
      throw failure;
    }
    this.assertInvariants();
    return false;
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
