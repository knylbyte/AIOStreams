import { Readable } from 'node:stream';
import {
  classifySpoolFileError,
  spoolAbortError,
  UsenetSpoolError,
} from './errors.js';
import type { GrowingReadableSource, ManagedSpoolFile } from './types.js';
import { resolveSegmentStreamQueuePlan } from '../stream-queue-budget.js';

export interface GrowingFileReaderOptions {
  readonly source: GrowingReadableSource;
  readonly start: number;
  readonly endExclusive?: number;
  readonly highWaterMark: number;
  readonly signal?: AbortSignal;
  readonly completion?: Promise<void>;
  readonly onClosed: () => void;
  readonly onReadBytes?: (bytes: number) => void;
}

function isNonNegativeSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

/**
 * Range-aware reader for a file whose committed prefix grows over time.
 * Reads never cross the source's `committedBytes`; when caught up, the reader
 * waits on an abortable source notification instead of polling. Each output
 * chunk owns one exact, unpooled allocation and bounded short reads fill that
 * same target before it becomes visible to the Readable queue.
 */
export class GrowingFileReader extends Readable {
  private readonly source: GrowingReadableSource;
  private readonly endExclusive: number | undefined;
  private readonly readBytes: number;
  private readonly onClosed: () => void;
  private readonly controller = new AbortController();
  private readonly userSignal: AbortSignal | undefined;
  private readonly completion: Promise<void> | undefined;
  private readonly onReadBytes: ((bytes: number) => void) | undefined;
  private userAbort: (() => void) | undefined;
  private position: number;
  private reading = false;
  private openPromise: Promise<ManagedSpoolFile> | undefined;
  private file: ManagedSpoolFile | undefined;
  private cleanupDone = false;

  constructor(options: GrowingFileReaderOptions) {
    if (!isNonNegativeSafeInteger(options.start)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Reader start must be a safe non-negative integer'
      );
    }
    if (
      options.endExclusive !== undefined &&
      (!isNonNegativeSafeInteger(options.endExclusive) ||
        options.endExclusive < options.start)
    ) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Reader endExclusive must be a safe integer at or after start'
      );
    }
    if (!isPositiveSafeInteger(options.highWaterMark)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Reader highWaterMark must be a safe positive integer'
      );
    }
    const queuePlan = resolveSegmentStreamQueuePlan(options.highWaterMark);
    super({ highWaterMark: options.highWaterMark, autoDestroy: true });
    this.source = options.source;
    this.position = options.start;
    this.endExclusive = options.endExclusive;
    this.readBytes = queuePlan.maxChunkBytes;
    this.onClosed = options.onClosed;
    this.userSignal = options.signal;
    this.completion = options.completion;
    this.onReadBytes = options.onReadBytes;

    if (options.signal?.aborted) {
      queueMicrotask(() =>
        this.destroy(spoolAbortError(options.signal?.reason))
      );
    } else if (options.signal) {
      this.userAbort = () => {
        this.destroy(spoolAbortError(options.signal?.reason));
      };
      options.signal.addEventListener('abort', this.userAbort, { once: true });
    }
  }

  override _read(): void {
    if (this.reading || this.destroyed) return;
    this.reading = true;
    void this.pump()
      .catch((error: unknown) => {
        if (!this.destroyed) {
          this.destroy(
            error instanceof Error
              ? error
              : new UsenetSpoolError(
                  'USENET_SPOOL_IO',
                  'Growing spool reader failed',
                  { cause: error }
                )
          );
        }
      })
      .finally(() => {
        this.reading = false;
      });
  }

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    if (!this.controller.signal.aborted) this.controller.abort(error);
    if (this.userSignal && this.userAbort) {
      this.userSignal.removeEventListener('abort', this.userAbort);
      this.userAbort = undefined;
    }

    void this.closeFile(error)
      .then((closeError) => callback(closeError))
      .catch((closeError: unknown) => {
        callback(
          closeError instanceof Error
            ? closeError
            : new Error(String(closeError))
        );
      });
  }

  private async pump(): Promise<void> {
    if (this.endExclusive !== undefined && this.position >= this.endExclusive) {
      await this.finishValidated();
      return;
    }
    const initialSnapshot = this.source.snapshot();
    if (initialSnapshot.error) throw initialSnapshot.error;
    if (initialSnapshot.state === 'disposed') {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Growing spool artifact was disposed'
      );
    }
    const initialRangeEnd = this.endExclusive ?? Number.MAX_SAFE_INTEGER;
    if (
      initialSnapshot.state === 'complete' &&
      this.position >= Math.min(initialSnapshot.committedBytes, initialRangeEnd)
    ) {
      await this.finishValidated();
      return;
    }
    await this.ensureFile();

    while (!this.destroyed) {
      const snapshot = this.source.snapshot();
      if (snapshot.error) throw snapshot.error;
      if (snapshot.state === 'disposed') {
        throw new UsenetSpoolError(
          'USENET_SPOOL_CLOSED',
          'Growing spool artifact was disposed'
        );
      }

      const rangeEnd = this.endExclusive ?? Number.MAX_SAFE_INTEGER;
      const readableEnd = Math.min(snapshot.committedBytes, rangeEnd);
      if (this.position < readableEnd) {
        const size = Math.min(this.readBytes, readableEnd - this.position);
        // An unpooled target owns exactly `size` backing bytes. Short reads are
        // filled into this same allocation, so no tiny queued view can retain
        // a larger temporary read buffer (or a shared slab) outside the lease.
        const buffer = Buffer.allocUnsafeSlow(size);
        const file = this.file;
        if (!file) {
          throw new UsenetSpoolError(
            'USENET_SPOOL_IO',
            'Growing spool reader lost its file lease'
          );
        }
        let filledBytes = 0;
        while (filledBytes < size) {
          if (this.destroyed || this.controller.signal.aborted) {
            throw spoolAbortError(this.controller.signal.reason);
          }
          const remainingBytes = size - filledBytes;
          const result = await file.handle.read(
            buffer,
            filledBytes,
            remainingBytes,
            this.position + filledBytes
          );
          if (
            !Number.isSafeInteger(result.bytesRead) ||
            result.bytesRead <= 0 ||
            result.bytesRead > remainingBytes
          ) {
            throw new UsenetSpoolError(
              'USENET_SPOOL_IO',
              'Committed spool data is missing or truncated'
            );
          }
          filledBytes += result.bytesRead;
        }
        if (this.destroyed || this.controller.signal.aborted) {
          throw spoolAbortError(this.controller.signal.reason);
        }
        this.position += size;
        this.onReadBytes?.(size);
        if (!this.push(buffer)) return;
        continue;
      }

      if (this.position >= rangeEnd || snapshot.state === 'complete') {
        await this.finishValidated();
        return;
      }
      await this.source.waitForChange(this.position, this.controller.signal);
    }
  }

  private async finishValidated(): Promise<void> {
    await this.completion;
    if (!this.destroyed) this.push(null);
  }

  private ensureFile(): Promise<ManagedSpoolFile> {
    if (!this.openPromise) {
      this.openPromise = this.source
        .openReadableFile(this.controller.signal)
        .then((file) => {
          this.file = file;
          return file;
        });
    }
    return this.openPromise;
  }

  private async closeFile(originalError: Error | null): Promise<Error | null> {
    if (this.cleanupDone) return originalError;
    this.cleanupDone = true;
    let closeError: Error | null = originalError;
    try {
      const file = this.file ?? (await this.openPromise);
      await file?.close();
    } catch (error) {
      if (!closeError) {
        closeError = classifySpoolFileError(error, 'closing a spool reader');
      }
    } finally {
      this.file = undefined;
      this.onClosed();
    }
    return closeError;
  }
}
