import { Readable } from 'node:stream';
import {
  classifySpoolFileError,
  spoolAbortError,
  UsenetSpoolError,
} from './errors.js';
import type { GrowingReadableSource, ManagedSpoolFile } from './types.js';

export interface GrowingFileReaderOptions {
  readonly source: GrowingReadableSource;
  readonly start: number;
  readonly endExclusive?: number;
  readonly highWaterMark: number;
  readonly signal?: AbortSignal;
  readonly completion?: Promise<void>;
  readonly onClosed: () => void;
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
 * waits on an abortable source notification instead of polling.
 */
export class GrowingFileReader extends Readable {
  private readonly source: GrowingReadableSource;
  private readonly endExclusive: number | undefined;
  private readonly readBytes: number;
  private readonly onClosed: () => void;
  private readonly controller = new AbortController();
  private readonly userSignal: AbortSignal | undefined;
  private readonly completion: Promise<void> | undefined;
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
    super({ highWaterMark: options.highWaterMark, autoDestroy: true });
    this.source = options.source;
    this.position = options.start;
    this.endExclusive = options.endExclusive;
    this.readBytes = options.highWaterMark;
    this.onClosed = options.onClosed;
    this.userSignal = options.signal;
    this.completion = options.completion;

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
        const buffer = Buffer.allocUnsafe(size);
        const file = this.file;
        if (!file) {
          throw new UsenetSpoolError(
            'USENET_SPOOL_IO',
            'Growing spool reader lost its file lease'
          );
        }
        const result = await file.handle.read(buffer, 0, size, this.position);
        if (
          !Number.isSafeInteger(result.bytesRead) ||
          result.bytesRead <= 0 ||
          result.bytesRead > size
        ) {
          throw new UsenetSpoolError(
            'USENET_SPOOL_IO',
            'Committed spool data is missing or truncated'
          );
        }
        this.position += result.bytesRead;
        if (!this.push(buffer.subarray(0, result.bytesRead))) return;
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
