import assert from 'node:assert/strict';
import type { ByteLease } from '../pool/byte-budget.js';
import {
  classifySpoolFileError,
  isMissingSpoolError,
  spoolAbortError,
  UsenetSpoolError,
} from './errors.js';
import { GrowingFileReader } from './growing-readable.js';
import type {
  GrowingArtifactSnapshot,
  GrowingFileReadOptions,
  GrowingReadableSource,
  ManagedSpoolFile,
  OpenManagedSpoolFile,
  SpoolArtifactState,
  SpoolBudgetLease,
  SpoolFileSystem,
  SpoolPromotionLease,
} from './types.js';
import { SpoolWriter } from './writer.js';

interface ChangeWaiter {
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
  readonly signal: AbortSignal;
  readonly onAbort: () => void;
}

export interface GrowingSpoolArtifactOptions {
  readonly partialPath: string;
  readonly readyPath: string;
  readonly reservation: SpoolBudgetLease;
  readonly writerQueueBytes: number;
  readonly readerHighWaterMarkBytes: number;
  readonly maxReaders: number;
  readonly fileSystem: SpoolFileSystem;
  readonly openFile: OpenManagedSpoolFile;
  readonly signal?: AbortSignal;
  readonly onDisposed: () => void;
}

function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

/**
 * One transient decoded-segment file, readable while its ordered writer grows.
 * The on-disk format is the raw contiguous decoded payload without a header:
 * `.partial` while growing, then an atomic rename to `.ready` on completion.
 *
 * State transitions are monotonic:
 * `created -> writing -> complete|failed -> disposed` (an empty artifact may
 * transition directly from `created` to `complete`). `committedBytes` advances
 * only after a full queued write succeeds and never exceeds the disk lease.
 * Explicit disposal aborts readers/writer, waits for reader and promotion
 * references, removes both path variants, and only then releases disk budget.
 */
export class GrowingSpoolArtifact implements GrowingReadableSource {
  private stateValue: SpoolArtifactState = 'created';
  private committedValue = 0;
  private failure: Error | undefined;
  private readonly partialPath: string;
  private readonly readyPath: string;
  private readonly reservation: SpoolBudgetLease;
  private readonly readerHighWaterMarkBytes: number;
  private readonly maxReaders: number;
  private readonly fileSystem: SpoolFileSystem;
  private readonly openFile: OpenManagedSpoolFile;
  private readonly onDisposed: () => void;
  private readonly writer: SpoolWriter;
  private readonly readers = new Set<GrowingFileReader>();
  private readonly changeWaiters = new Set<ChangeWaiter>();
  private promotionReferences = 0;
  private referenceDrain: PromiseWithResolvers<void> | undefined;
  private completePromise: Promise<void> | undefined;
  private disposePromise: Promise<void> | undefined;

  private constructor(
    options: GrowingSpoolArtifactOptions,
    writerFile: ManagedSpoolFile
  ) {
    if (!isPositiveSafeInteger(options.readerHighWaterMarkBytes)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Reader high-water mark must be a safe positive integer'
      );
    }
    if (!isPositiveSafeInteger(options.maxReaders)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Artifact maxReaders must be a safe positive integer'
      );
    }
    this.partialPath = options.partialPath;
    this.readyPath = options.readyPath;
    this.reservation = options.reservation;
    this.readerHighWaterMarkBytes = options.readerHighWaterMarkBytes;
    this.maxReaders = options.maxReaders;
    this.fileSystem = options.fileSystem;
    this.openFile = options.openFile;
    this.onDisposed = options.onDisposed;
    this.writer = new SpoolWriter({
      file: writerFile,
      maxQueueBytes: options.writerQueueBytes,
      onCommitted: (bytes) => this.commitWritten(bytes),
      onFailed: (error) => this.transitionFailed(error),
    });
    this.assertInvariants();
  }

  /** Create the secure `.partial` file and hold one global writer-file permit. */
  static async create(
    options: GrowingSpoolArtifactOptions
  ): Promise<GrowingSpoolArtifact> {
    const writerFile = await options.openFile(
      options.partialPath,
      'wx+',
      0o600,
      options.signal
    );
    try {
      return new GrowingSpoolArtifact(options, writerFile);
    } catch (error) {
      await writerFile.close();
      throw error;
    }
  }

  get state(): SpoolArtifactState {
    return this.stateValue;
  }

  get committedBytes(): number {
    return this.committedValue;
  }

  get reservedBytes(): number {
    return this.reservation.reservedBytes;
  }

  /** Increase this artifact's hard disk reservation before scheduling data. */
  grow(
    bytes: number,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<void> {
    this.assertAcceptingWrites();
    return this.reservation.grow(bytes, options);
  }

  /**
   * Schedule an owned, pre-leased decoded chunk. The call is synchronous and
   * preserves producer order; grow the disk reservation before retrying a
   * `USENET_SPOOL_CAPACITY` failure.
   */
  write(chunk: Buffer, lease: ByteLease): boolean {
    this.assertAcceptingWrites();
    if (
      chunk.length >
      this.reservation.reservedBytes - this.writer.scheduledBytes
    ) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CAPACITY',
        'Chunk would exceed the artifact disk reservation'
      );
    }
    const acceptsMore = this.writer.write(chunk, lease);
    if (this.stateValue === 'created') this.stateValue = 'writing';
    this.assertInvariants();
    return acceptsMore;
  }

  onceDrain(listener: () => void): void {
    this.writer.onceDrain(listener);
  }

  /** Drain, close, and atomically rename `.partial` to `.ready`. */
  complete(): Promise<void> {
    if (!this.completePromise) this.completePromise = this.completeOnce();
    return this.completePromise;
  }

  /** Propagate one typed writer failure to all current and future readers. */
  fail(error: Error): void {
    if (
      this.stateValue === 'complete' ||
      this.stateValue === 'disposed' ||
      this.failure
    ) {
      return;
    }
    const classified = classifySpoolFileError(error, 'writing a spool file');
    this.transitionFailed(classified);
    this.writer.fail(classified);
  }

  /** Create a separately counted range reader over the growing file. */
  createReadStream(options: GrowingFileReadOptions = {}): GrowingFileReader {
    if (this.stateValue === 'disposed') {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Cannot read a disposed spool artifact'
      );
    }
    if (this.readers.size >= this.maxReaders) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_OPEN_FILE_LIMIT',
        'Artifact reader limit reached'
      );
    }
    const highWaterMark =
      options.highWaterMark ?? this.readerHighWaterMarkBytes;
    if (highWaterMark > this.readerHighWaterMarkBytes) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Reader highWaterMark exceeds the artifact resource plan'
      );
    }
    let reader: GrowingFileReader;
    reader = new GrowingFileReader({
      source: this,
      start: options.start ?? 0,
      endExclusive: options.endExclusive,
      highWaterMark,
      signal: options.signal,
      onClosed: () => this.readerClosed(reader),
    });
    this.readers.add(reader);
    this.assertInvariants();
    return reader;
  }

  /** Retain a complete `.ready` file for a future cache-promotion operation. */
  acquirePromotion(): SpoolPromotionLease {
    if (this.stateValue !== 'complete') {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Cache promotion requires a complete spool artifact'
      );
    }
    this.promotionReferences++;
    this.assertInvariants();
    let released = false;
    return {
      path: this.readyPath,
      release: () => {
        if (released) return;
        released = true;
        this.promotionReferences--;
        this.referencesChanged();
        this.assertInvariants();
      },
    };
  }

  /**
   * Idempotently terminate I/O and remove the artifact after all references
   * close. The disk lease is released only after both path variants are gone.
   */
  dispose(): Promise<void> {
    if (!this.disposePromise) this.disposePromise = this.disposeOnce();
    return this.disposePromise;
  }

  snapshot(): GrowingArtifactSnapshot {
    return {
      state: this.stateValue,
      committedBytes: this.committedValue,
      error:
        this.failure ??
        (this.stateValue === 'disposed'
          ? new UsenetSpoolError(
              'USENET_SPOOL_CLOSED',
              'Spool artifact was disposed'
            )
          : undefined),
    };
  }

  waitForChange(position: number, signal: AbortSignal): Promise<void> {
    try {
      if (signal.aborted) throw spoolAbortError(signal.reason);
      const snapshot = this.snapshot();
      if (snapshot.error) throw snapshot.error;
      if (snapshot.committedBytes > position || snapshot.state === 'complete') {
        return Promise.resolve();
      }
      if (this.changeWaiters.size >= this.maxReaders) {
        throw new UsenetSpoolError(
          'USENET_SPOOL_OPEN_FILE_LIMIT',
          'Growing-reader waiter limit reached'
        );
      }
    } catch (error) {
      return Promise.reject(error);
    }

    return new Promise<void>((resolve, reject) => {
      const waiter: ChangeWaiter = {
        resolve,
        reject,
        signal,
        onAbort: () => {
          if (!this.changeWaiters.delete(waiter)) return;
          signal.removeEventListener('abort', waiter.onAbort);
          reject(spoolAbortError(signal.reason));
        },
      };
      signal.addEventListener('abort', waiter.onAbort, { once: true });
      this.changeWaiters.add(waiter);
      this.assertInvariants();
    });
  }

  async openReadableFile(signal: AbortSignal): Promise<ManagedSpoolFile> {
    if (signal.aborted) throw spoolAbortError(signal.reason);
    const preferred =
      this.stateValue === 'complete' ? this.readyPath : this.partialPath;
    const alternate =
      preferred === this.partialPath ? this.readyPath : this.partialPath;
    try {
      return await this.openFile(preferred, 'r', undefined, signal);
    } catch (error) {
      if (!isMissingSpoolError(error)) throw error;
      return this.openFile(alternate, 'r', undefined, signal);
    }
  }

  private async completeOnce(): Promise<void> {
    this.assertCompletable();
    try {
      await this.writer.end();
      if (this.failure) throw this.failure;
      await this.fileSystem.rename(this.partialPath, this.readyPath);
      if (this.failure) throw this.failure;
      if (this.stateValue === 'disposed') {
        throw new UsenetSpoolError(
          'USENET_SPOOL_CLOSED',
          'Spool artifact was disposed while completing'
        );
      }
      this.stateValue = 'complete';
      this.notifyChangeWaiters();
      this.assertInvariants();
    } catch (error) {
      const classified = classifySpoolFileError(
        error,
        'finalizing a spool file'
      );
      this.transitionFailed(classified);
      throw classified;
    }
  }

  private commitWritten(bytes: number): void {
    this.reservation.recordWritten(bytes);
    this.committedValue += bytes;
    this.notifyChangeWaiters();
    this.assertInvariants();
  }

  private transitionFailed(error: Error): void {
    if (this.failure || this.stateValue === 'disposed') return;
    this.failure = error;
    this.stateValue = 'failed';
    this.notifyChangeWaiters();
    this.assertInvariants();
  }

  private notifyChangeWaiters(): void {
    const error = this.snapshot().error;
    for (const waiter of this.changeWaiters) {
      this.changeWaiters.delete(waiter);
      waiter.signal.removeEventListener('abort', waiter.onAbort);
      if (error) waiter.reject(error);
      else waiter.resolve();
    }
  }

  private readerClosed(reader: GrowingFileReader): void {
    this.readers.delete(reader);
    this.referencesChanged();
    this.assertInvariants();
  }

  private referencesChanged(): void {
    if (
      this.readers.size === 0 &&
      this.promotionReferences === 0 &&
      this.referenceDrain
    ) {
      this.referenceDrain.resolve();
      this.referenceDrain = undefined;
    }
  }

  private waitForReferences(): Promise<void> {
    if (this.readers.size === 0 && this.promotionReferences === 0) {
      return Promise.resolve();
    }
    this.referenceDrain ??= Promise.withResolvers<void>();
    return this.referenceDrain.promise;
  }

  private async disposeOnce(): Promise<void> {
    const completed = this.stateValue === 'complete';
    const completion = this.completePromise;
    const disposeError =
      this.failure ??
      new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Spool artifact was disposed'
      );
    this.stateValue = 'disposed';
    if (!completed) this.writer.fail(disposeError);
    this.notifyChangeWaiters();
    for (const reader of this.readers) reader.destroy(disposeError);

    await this.writer.settled();
    if (completion) await Promise.allSettled([completion]);
    await this.waitForReferences();
    await this.removeFile(this.partialPath);
    await this.removeFile(this.readyPath);
    this.reservation.release();
    this.onDisposed();
    this.assertInvariants();
  }

  private async removeFile(filePath: string): Promise<void> {
    try {
      await this.fileSystem.rm(filePath, { force: true });
    } catch (error) {
      if (!isMissingSpoolError(error)) {
        throw classifySpoolFileError(error, 'removing a spool file');
      }
    }
  }

  private assertAcceptingWrites(): void {
    if (this.failure) throw this.failure;
    if (this.stateValue !== 'created' && this.stateValue !== 'writing') {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Spool artifact no longer accepts writes'
      );
    }
  }

  private assertCompletable(): void {
    if (this.failure) throw this.failure;
    if (this.stateValue === 'complete') return;
    if (this.stateValue !== 'created' && this.stateValue !== 'writing') {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Spool artifact cannot be completed in its current state'
      );
    }
  }

  private assertInvariants(): void {
    assert(Number.isSafeInteger(this.committedValue));
    assert(this.committedValue >= 0);
    assert(this.committedValue <= this.reservation.writtenBytes);
    assert(this.reservation.writtenBytes <= this.reservation.reservedBytes);
    assert(this.readers.size <= this.maxReaders);
    assert(this.changeWaiters.size <= this.maxReaders);
    assert(this.promotionReferences >= 0);
  }
}
