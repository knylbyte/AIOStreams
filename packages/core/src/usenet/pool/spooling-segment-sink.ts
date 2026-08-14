import assert from 'node:assert/strict';
import type { ByteLease } from './byte-budget.js';
import type { BackpressuredByteSink } from './streaming-yenc-article-decoder.js';
import type { GrowingSpoolArtifact } from '../spool/growing-artifact.js';
import { UsenetSpoolError } from '../spool/errors.js';

const MEBIBYTE_BYTES = 1024 * 1024;

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
 * after its asynchronous write settles. This sink deliberately returns false
 * after every owned chunk, limiting retention to one chunk and resuming only
 * after both its write and any required disk-reservation growth complete.
 */
export class SpoolingSegmentSink implements BackpressuredByteSink {
  private retainedBytes = 0;
  private failure: Error | undefined;
  private drainListener: (() => void) | undefined;
  private readyForNextWrite = true;
  private preparing: Promise<void> = Promise.resolve();

  constructor(
    private readonly artifact: GrowingSpoolArtifact,
    private readonly leasedWindowBytes: number,
    private readonly requiredHeadroomBytes: number
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
    this.assertInvariants();
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

    const lease = this.createChunkLease(chunk.length);
    this.readyForNextWrite = false;
    try {
      this.artifact.write(chunk, lease);
    } catch (error) {
      lease.release();
      throw error;
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
    await this.preparing;
    if (this.failure) throw this.failure;
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
        this.startPreparingNextWrite();
        this.assertInvariants();
      },
    };
  }

  private startPreparingNextWrite(): void {
    this.preparing = this.prepareNextWrite().then(
      () => {
        if (this.failure) return;
        this.readyForNextWrite = true;
        this.emitDrain();
      },
      (error: unknown) => {
        const failure = asError(error);
        this.failure = failure;
        this.artifact.fail(failure);
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
    if (this.failure) throw this.failure;
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
  }
}
