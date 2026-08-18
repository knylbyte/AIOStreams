import assert from 'node:assert/strict';
import type { ByteLease } from '../pool/byte-budget.js';
import { classifySpoolFileError, UsenetSpoolError } from './errors.js';
import type { ManagedSpoolFile } from './types.js';
import type { SegmentSpoolingHotpathCounters } from '../pool/hotpath-counters.js';

interface QueuedChunk {
  readonly chunk: Buffer;
  readonly lease: ByteLease;
}

export interface SpoolWriterOptions {
  readonly file: ManagedSpoolFile;
  readonly maxQueueBytes: number;
  readonly onCommitted: (bytes: number) => void;
  readonly onFailed: (error: Error) => void;
  readonly hotpathCounters?: SegmentSpoolingHotpathCounters;
}

function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

/**
 * Ordered, byte-bounded async writer for one growing spool file.
 *
 * An accepted chunk transfers ownership of both the Buffer and its exact-size
 * {@link ByteLease} to the writer. The lease is released after the write
 * settles. `committedBytes` advances only after the complete chunk has been
 * written, so readers never observe a partial or out-of-order prefix.
 */
export class SpoolWriter {
  private readonly file: ManagedSpoolFile;
  private readonly maxQueueBytes: number;
  private readonly lowWaterMarkBytes: number;
  private readonly onCommitted: (bytes: number) => void;
  private readonly onFailed: (error: Error) => void;
  private readonly hotpathCounters: SegmentSpoolingHotpathCounters | undefined;
  private readonly queue: QueuedChunk[] = [];
  private queuedBytes = 0;
  private committedBytes = 0;
  private pumping = false;
  private ending = false;
  private failure: Error | undefined;
  private drainListener: (() => void) | undefined;
  private readonly closed = Promise.withResolvers<void>();
  private closePromise: Promise<void> | undefined;
  private endPromise: Promise<void> | undefined;

  constructor(options: SpoolWriterOptions) {
    if (!isPositiveSafeInteger(options.maxQueueBytes)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Writer queue maxQueueBytes must be a safe positive integer'
      );
    }
    this.file = options.file;
    this.maxQueueBytes = options.maxQueueBytes;
    this.lowWaterMarkBytes = Math.floor(options.maxQueueBytes / 2);
    this.onCommitted = options.onCommitted;
    this.onFailed = options.onFailed;
    this.hotpathCounters = options.hotpathCounters;
    this.assertInvariants();
  }

  /** Bytes written as one contiguous, reader-visible prefix. */
  get committed(): number {
    return this.committedBytes;
  }

  /** Bytes already committed or accepted for ordered writing. */
  get scheduledBytes(): number {
    return this.committedBytes + this.queuedBytes;
  }

  /** Bytes whose memory leases remain owned by this writer. */
  get bufferedBytes(): number {
    return this.queuedBytes;
  }

  /**
   * Accept a pre-leased chunk. `false` means the queue reached its high-water
   * mark and the producer must wait for {@link onceDrain} before another write.
   * A write that would cross the hard queue bound is rejected without taking
   * ownership of the chunk or lease.
   */
  write(chunk: Buffer, lease: ByteLease): boolean {
    this.assertWritable();
    if (!isPositiveSafeInteger(chunk.length) || lease.bytes !== chunk.length) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Writer chunks require an exact, positive ByteLease'
      );
    }
    if (lease.bytes > this.maxQueueBytes - this.queuedBytes) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Writer queue byte limit would be exceeded'
      );
    }

    this.queue.push({ chunk, lease });
    this.queuedBytes += lease.bytes;
    this.assertInvariants();
    this.startPump();
    return this.queuedBytes < this.maxQueueBytes;
  }

  /** Register the single producer continuation for the next low-water event. */
  onceDrain(listener: () => void): void {
    this.assertWritable();
    if (this.drainListener) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'A writer drain listener is already registered'
      );
    }
    if (this.queuedBytes <= this.lowWaterMarkBytes) {
      queueMicrotask(listener);
      return;
    }
    this.drainListener = listener;
  }

  /** Drain accepted writes, close the file, and reject if any write failed. */
  end(): Promise<void> {
    if (!this.endPromise) {
      this.ending = true;
      this.startPump();
      this.endPromise = this.awaitSuccessfulClose();
    }
    return this.endPromise;
  }

  /** Fail idempotently, release queued leases, and close after in-flight I/O. */
  fail(error: Error): void {
    if (this.failure) return;
    this.transitionFailure(error);
    this.releaseQueuedChunks();
    this.startPump();
  }

  /** Resolves after the file handle closes, irrespective of success/failure. */
  settled(): Promise<void> {
    return this.closed.promise;
  }

  private async awaitSuccessfulClose(): Promise<void> {
    await this.closed.promise;
    if (this.failure) throw this.failure;
  }

  private startPump(): void {
    if (this.pumping || this.closePromise) return;
    this.pumping = true;
    void this.runPump()
      .catch((error: unknown) => {
        this.transitionFailure(
          classifySpoolFileError(error, 'writing a spool file')
        );
        this.releaseQueuedChunks();
      })
      .finally(() => {
        this.pumping = false;
        if (this.failure) this.releaseQueuedChunks();
        if (this.ending || this.failure) {
          void this.closeFile();
        } else if (this.queue.length > 0) {
          this.startPump();
        }
      });
  }

  private async runPump(): Promise<void> {
    while (!this.failure) {
      const entry = this.queue.shift();
      if (!entry) return;
      try {
        if (this.hotpathCounters) this.hotpathCounters.spoolWriteOperations++;
        await this.writeFully(entry.chunk, this.committedBytes);
        if (!this.failure) {
          this.committedBytes += entry.chunk.length;
          this.onCommitted(entry.chunk.length);
        }
      } catch (error) {
        this.transitionFailure(
          classifySpoolFileError(error, 'writing a spool file')
        );
      } finally {
        this.queuedBytes -= entry.lease.bytes;
        entry.lease.release();
        this.emitDrainIfNeeded();
        this.assertInvariants();
      }
    }
  }

  private async writeFully(chunk: Buffer, position: number): Promise<void> {
    let offset = 0;
    while (offset < chunk.length) {
      const requested = chunk.length - offset;
      const result = await this.file.handle.write(
        chunk,
        offset,
        requested,
        position + offset
      );
      if (this.hotpathCounters) {
        this.hotpathCounters.spoolWriteSyscalls++;
        this.hotpathCounters.spoolBytesWritten += result.bytesWritten;
        if (result.bytesWritten < requested) {
          this.hotpathCounters.spoolShortWrites++;
        }
      }
      if (
        !isPositiveSafeInteger(result.bytesWritten) ||
        result.bytesWritten > chunk.length - offset
      ) {
        throw new UsenetSpoolError(
          'USENET_SPOOL_IO',
          'Spool write returned an invalid byte count'
        );
      }
      offset += result.bytesWritten;
    }
  }

  private transitionFailure(error: Error): void {
    if (this.failure) return;
    this.failure = error;
    this.ending = true;
    const drainListener = this.drainListener;
    this.drainListener = undefined;
    this.onFailed(error);
    if (drainListener) queueMicrotask(drainListener);
  }

  private releaseQueuedChunks(): void {
    while (this.queue.length > 0) {
      const entry = this.queue.shift();
      assert(entry, 'non-empty writer queue must have a head');
      this.queuedBytes -= entry.lease.bytes;
      entry.lease.release();
    }
    this.assertInvariants();
  }

  private emitDrainIfNeeded(): void {
    if (
      !this.failure &&
      this.queuedBytes <= this.lowWaterMarkBytes &&
      this.drainListener
    ) {
      const listener = this.drainListener;
      this.drainListener = undefined;
      listener();
    }
  }

  private closeFile(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closePromise = (async () => {
      try {
        await this.file.close();
      } catch (error) {
        this.transitionFailure(
          classifySpoolFileError(error, 'closing a spool file')
        );
      } finally {
        this.closed.resolve();
      }
    })();
    return this.closePromise;
  }

  private assertWritable(): void {
    if (this.failure) throw this.failure;
    if (this.ending || this.closePromise) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Spool writer no longer accepts chunks'
      );
    }
  }

  private assertInvariants(): void {
    assert(Number.isSafeInteger(this.committedBytes));
    assert(Number.isSafeInteger(this.queuedBytes));
    assert(this.committedBytes >= 0);
    assert(this.queuedBytes >= 0);
    assert(this.queuedBytes <= this.maxQueueBytes);
    assert(this.queue.length <= this.maxQueueBytes);
  }
}
