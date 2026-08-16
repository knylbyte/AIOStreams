import assert from 'node:assert/strict';
import { ByteBudget, ByteBudgetError } from './byte-budget.js';
import { CommandPriority } from '../types.js';
import type { SegmentSpoolingPlan } from '../resource-plan.js';
import { SpoolManager } from '../spool/manager.js';
import { UsenetSpoolError } from '../spool/errors.js';
import type { ByteLease } from './byte-budget.js';
import type { SegmentArtifactCacheLookup } from './segment-artifact.js';

export interface SegmentSpoolingRuntimeOptions {
  readonly plan: SegmentSpoolingPlan;
  readonly engineId: string;
  readonly cacheRoot?: string;
  readonly artifactCache?: SegmentArtifactCacheLookup;
  readonly memoryBudget?: ByteBudget;
  readonly spoolManager?: SpoolManager;
}

type MemoryRequestKind = 'download' | 'stream';

interface MemoryWaiter {
  readonly bytes: number;
  readonly kind: MemoryRequestKind;
  readonly priority: CommandPriority;
  readonly signal?: AbortSignal;
  readonly resolve: (lease: ByteLease) => void;
  readonly reject: (error: unknown) => void;
  onAbort?: () => void;
}

const MAX_MEMORY_WAITERS = 1024;
const MAX_CONTENDED_HIGH_GRANTS = 3;

function abortError(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  const error = new Error('Segment-spooling memory acquisition aborted', {
    cause: signal.reason,
  });
  error.name = 'AbortError';
  return error;
}

/**
 * Engine-lifetime owners shared by every segment-spooling fetch. The optional
 * artifact cache is deliberately file-backed-only and remains unimplemented
 * until Block 8.
 */
export class SegmentSpoolingRuntime {
  readonly plan: SegmentSpoolingPlan;
  readonly memoryBudget: ByteBudget;
  readonly spoolManager: SpoolManager;
  readonly artifactCache: SegmentArtifactCacheLookup | undefined;

  /** Caps aggregate stream queues at half the global transient RAM budget. */
  private readonly streamAdmissionMaxBytes: number;
  private streamAdmissionUsedBytes = 0;
  private readonly highDownloadWaiters: MemoryWaiter[] = [];
  private readonly highStreamWaiters: MemoryWaiter[] = [];
  private readonly lowDownloadWaiters: MemoryWaiter[] = [];
  private readonly lowStreamWaiters: MemoryWaiter[] = [];
  private contendedHighGrants = 0;
  private nextKind: MemoryRequestKind = 'download';
  private closedError: Error | undefined;
  private closePromise: Promise<void> | undefined;

  constructor(options: SegmentSpoolingRuntimeOptions) {
    this.plan = options.plan;
    this.memoryBudget =
      options.memoryBudget ?? new ByteBudget(options.plan.memoryBudgetBytes);
    this.streamAdmissionMaxBytes = Math.floor(
      options.plan.memoryBudgetBytes / 2
    );
    this.spoolManager =
      options.spoolManager ??
      new SpoolManager({
        plan: options.plan,
        engineId: options.engineId,
        cacheRoot: options.cacheRoot,
      });
    this.artifactCache = options.artifactCache;
  }

  /** Acquire the global per-download window with stable spool error mapping. */
  async acquireDownloadMemory(
    priority: CommandPriority,
    signal?: AbortSignal
  ): Promise<ByteLease> {
    try {
      return await this.acquireMemory(
        'download',
        this.plan.perDownloadBaseLeaseBytes,
        priority,
        signal
      );
    } catch (error) {
      if (signal?.aborted) throw error;
      if (error instanceof UsenetSpoolError) throw error;
      if (error instanceof ByteBudgetError) {
        if (error.code === 'BYTE_BUDGET_CLOSED') {
          throw new UsenetSpoolError(
            'USENET_SPOOL_CLOSED',
            'Segment-spooling runtime is closed',
            { cause: error }
          );
        }
        throw new UsenetSpoolError(
          'USENET_MEMORY_BUDGET',
          'Segment-spooling memory budget could not grant a download window',
          { cause: error }
        );
      }
      throw error;
    }
  }

  /**
   * Atomically reserve every bounded Readable queue owned by one output path.
   * Direct streams request `2H` (artifact reader + ordered stream), while the
   * FileStream path requests `3H` to include its relay. Admission and global
   * accounting are checked in the same synchronous grant; no partial lease is
   * held while another budget is awaited.
   */
  async acquireStreamMemory(
    bytes: number,
    priority: CommandPriority,
    signal?: AbortSignal
  ): Promise<ByteLease> {
    try {
      if (
        !Number.isSafeInteger(bytes) ||
        bytes <= 0 ||
        bytes > this.plan.perStreamBufferBytes ||
        bytes > this.streamAdmissionMaxBytes
      ) {
        throw new UsenetSpoolError(
          'USENET_MEMORY_BUDGET',
          'Segment stream requested an invalid memory window'
        );
      }
      return await this.acquireMemory('stream', bytes, priority, signal);
    } catch (error) {
      if (signal?.aborted) throw error;
      if (error instanceof UsenetSpoolError) throw error;
      if (error instanceof ByteBudgetError) {
        if (error.code === 'BYTE_BUDGET_CLOSED') {
          throw new UsenetSpoolError(
            'USENET_SPOOL_CLOSED',
            'Segment-spooling runtime is closed',
            { cause: error }
          );
        }
        throw new UsenetSpoolError(
          'USENET_MEMORY_BUDGET',
          'Segment-spooling memory budget could not grant a stream window',
          { cause: error }
        );
      }
      throw error;
    }
  }

  /** Stop new memory waiters and idempotently dispose the complete spool. */
  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    const error = new UsenetSpoolError(
      'USENET_SPOOL_CLOSED',
      'Segment-spooling runtime is closed'
    );
    this.closedError = error;
    this.rejectMemoryWaiters(error);
    this.memoryBudget.close(error);
    this.closePromise = this.spoolManager.close();
    return this.closePromise;
  }

  private acquireMemory(
    kind: MemoryRequestKind,
    bytes: number,
    priority: CommandPriority,
    signal?: AbortSignal
  ): Promise<ByteLease> {
    if (this.closedError) return Promise.reject(this.closedError);
    if (priority !== CommandPriority.High && priority !== CommandPriority.Low) {
      return Promise.reject(
        new ByteBudgetError(
          'BYTE_BUDGET_INVALID_PRIORITY',
          'Memory priority must be CommandPriority.High or CommandPriority.Low'
        )
      );
    }
    if (!Number.isSafeInteger(bytes) || bytes <= 0) {
      return Promise.reject(
        new ByteBudgetError(
          'BYTE_BUDGET_INVALID_BYTES',
          'Requested memory must be a safe positive integer'
        )
      );
    }
    if (bytes > this.memoryBudget.stats().maxBytes) {
      return Promise.reject(
        new ByteBudgetError(
          'BYTE_BUDGET_REQUEST_TOO_LARGE',
          'Requested memory exceeds the global segment-spooling budget'
        )
      );
    }
    if (signal?.aborted) return Promise.reject(abortError(signal));

    if (this.memoryWaitingCount === 0) {
      const immediate = this.tryGrantMemory(kind, bytes);
      if (immediate) return Promise.resolve(immediate);
    }
    if (this.memoryWaitingCount >= MAX_MEMORY_WAITERS) {
      return Promise.reject(
        new ByteBudgetError(
          'BYTE_BUDGET_QUEUE_FULL',
          'Segment-spooling memory waiter capacity reached'
        )
      );
    }

    return new Promise<ByteLease>((resolve, reject) => {
      const waiter: MemoryWaiter = {
        bytes,
        kind,
        priority,
        signal,
        resolve,
        reject,
      };
      if (signal) {
        waiter.onAbort = () => {
          if (!this.removeMemoryWaiter(waiter)) return;
          this.removeMemoryAbortListener(waiter);
          reject(abortError(signal));
          this.drainMemoryWaiters();
        };
        signal.addEventListener('abort', waiter.onAbort, { once: true });
      }
      this.memoryQueue(waiter.kind, waiter.priority).push(waiter);
      this.drainMemoryWaiters();
    });
  }

  private tryGrantMemory(
    kind: MemoryRequestKind,
    bytes: number
  ): ByteLease | undefined {
    if (
      kind === 'stream' &&
      bytes > this.streamAdmissionMaxBytes - this.streamAdmissionUsedBytes
    ) {
      return undefined;
    }
    const globalLease = this.memoryBudget.tryAcquire(bytes);
    if (!globalLease) return undefined;
    if (kind === 'stream') this.streamAdmissionUsedBytes += bytes;
    this.assertMemoryInvariants();
    let released = false;
    return {
      bytes,
      release: () => {
        if (released) return;
        released = true;
        if (kind === 'stream') this.streamAdmissionUsedBytes -= bytes;
        globalLease.release();
        this.assertMemoryInvariants();
        this.drainMemoryWaiters();
      },
    };
  }

  private drainMemoryWaiters(): void {
    while (!this.closedError && this.memoryWaitingCount > 0) {
      const waiter = this.pickMemoryWaiter();
      if (!waiter) return;
      const lease = this.tryGrantMemory(waiter.kind, waiter.bytes);
      if (!lease) return;
      const queue = this.memoryQueue(waiter.kind, waiter.priority);
      assert.equal(queue[0], waiter);
      queue.shift();
      this.removeMemoryAbortListener(waiter);
      const lowWaiting =
        this.lowDownloadWaiters.length > 0 || this.lowStreamWaiters.length > 0;
      if (waiter.priority === CommandPriority.High && lowWaiting) {
        this.contendedHighGrants++;
      } else {
        this.contendedHighGrants = 0;
      }
      this.nextKind = waiter.kind === 'download' ? 'stream' : 'download';
      waiter.resolve(lease);
    }
  }

  private pickMemoryWaiter(): MemoryWaiter | undefined {
    const lowWaiting =
      this.lowDownloadWaiters.length > 0 || this.lowStreamWaiters.length > 0;
    if (lowWaiting && this.contendedHighGrants >= MAX_CONTENDED_HIGH_GRANTS) {
      return this.fittingMemoryHead(CommandPriority.Low);
    }
    return (
      this.fittingMemoryHead(CommandPriority.High) ??
      this.fittingMemoryHead(CommandPriority.Low)
    );
  }

  private fittingMemoryHead(
    priority: CommandPriority
  ): MemoryWaiter | undefined {
    const download = this.memoryQueue('download', priority)[0];
    const stream = this.memoryQueue('stream', priority)[0];
    const downloadFits = download ? this.memoryRequestFits(download) : false;
    const streamFits = stream ? this.memoryRequestFits(stream) : false;
    if (downloadFits && streamFits) {
      return this.nextKind === 'download' ? download : stream;
    }
    if (downloadFits) return download;
    if (streamFits) return stream;
    return undefined;
  }

  private memoryRequestFits(waiter: MemoryWaiter): boolean {
    const available =
      this.memoryBudget.stats().maxBytes - this.memoryBudget.stats().usedBytes;
    if (waiter.bytes > available) return false;
    return (
      waiter.kind === 'download' ||
      waiter.bytes <=
        this.streamAdmissionMaxBytes - this.streamAdmissionUsedBytes
    );
  }

  private memoryQueue(
    kind: MemoryRequestKind,
    priority: CommandPriority
  ): MemoryWaiter[] {
    if (priority === CommandPriority.High) {
      return kind === 'download'
        ? this.highDownloadWaiters
        : this.highStreamWaiters;
    }
    return kind === 'download'
      ? this.lowDownloadWaiters
      : this.lowStreamWaiters;
  }

  private get memoryWaitingCount(): number {
    return (
      this.highDownloadWaiters.length +
      this.highStreamWaiters.length +
      this.lowDownloadWaiters.length +
      this.lowStreamWaiters.length
    );
  }

  private removeMemoryWaiter(waiter: MemoryWaiter): boolean {
    const queue = this.memoryQueue(waiter.kind, waiter.priority);
    const index = queue.indexOf(waiter);
    if (index < 0) return false;
    queue.splice(index, 1);
    return true;
  }

  private removeMemoryAbortListener(waiter: MemoryWaiter): void {
    if (waiter.signal && waiter.onAbort) {
      waiter.signal.removeEventListener('abort', waiter.onAbort);
      waiter.onAbort = undefined;
    }
  }

  private rejectMemoryWaiters(error: Error): void {
    for (const queue of [
      this.highDownloadWaiters,
      this.highStreamWaiters,
      this.lowDownloadWaiters,
      this.lowStreamWaiters,
    ]) {
      while (queue.length > 0) {
        const waiter = queue.shift();
        assert(waiter);
        this.removeMemoryAbortListener(waiter);
        waiter.reject(error);
      }
    }
  }

  private assertMemoryInvariants(): void {
    assert(Number.isSafeInteger(this.streamAdmissionUsedBytes));
    assert(this.streamAdmissionUsedBytes >= 0);
    assert(this.streamAdmissionUsedBytes <= this.streamAdmissionMaxBytes);
    assert(
      this.memoryBudget.stats().usedBytes <= this.memoryBudget.stats().maxBytes
    );
    assert(this.memoryWaitingCount <= MAX_MEMORY_WAITERS);
  }
}
