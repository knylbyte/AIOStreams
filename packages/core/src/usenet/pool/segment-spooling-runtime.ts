import assert from 'node:assert/strict';
import { ByteBudget, ByteBudgetError } from './byte-budget.js';
import { CommandPriority } from '../types.js';
import type { SegmentSpoolingPlan } from '../resource-plan.js';
import { SpoolManager } from '../spool/manager.js';
import { UsenetSpoolError } from '../spool/errors.js';
import type { ByteBudgetStats, ByteLease } from './byte-budget.js';
import type { SpoolManagerStats } from '../spool/types.js';
import type { SegmentArtifactCacheLookup } from './segment-artifact.js';
import { resolveSegmentStreamMemoryBytes } from '../stream-queue-budget.js';
import type {
  SegmentStreamCleanupCause,
  UsenetResourceEventObserver,
  UsenetResourceLifecycleEvent,
} from './resource-events.js';
import { ResourceEventLogAggregator } from './resource-events.js';
import { createLogger } from '../../logging/logger.js';
import { SegmentSpoolingHotpathCounters } from './hotpath-counters.js';

const logger = createLogger('usenet/resources');

export interface SegmentSpoolingRuntimeOptions {
  readonly plan: SegmentSpoolingPlan;
  readonly engineId: string;
  readonly cacheRoot?: string;
  readonly artifactCache?: SegmentArtifactCacheLookup;
  readonly memoryBudget?: ByteBudget;
  readonly spoolManager?: SpoolManager;
  readonly clock?: () => number;
  readonly onEvent?: UsenetResourceEventObserver;
  readonly hotpathCounters?: SegmentSpoolingHotpathCounters;
}

/** Resource-owner snapshot used by the engine dashboard contract. */
export interface SegmentSpoolingRuntimeStats {
  readonly memory: ByteBudgetStats;
  readonly spool: SpoolManagerStats;
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
  readonly enqueuedAt: number;
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
 * artifact cache exposes persistent file leases and bounded best-effort
 * promotion without materializing decoded bodies.
 */
export class SegmentSpoolingRuntime {
  readonly plan: SegmentSpoolingPlan;
  readonly memoryBudget: ByteBudget;
  readonly spoolManager: SpoolManager;
  readonly artifactCache: SegmentArtifactCacheLookup | undefined;
  readonly hotpathCounters: SegmentSpoolingHotpathCounters | undefined;

  /** Caps aggregate stream queues at half the global transient RAM budget. */
  private readonly streamAdmissionMaxBytes: number;
  private streamAdmissionUsedBytes = 0;
  private activeStreamLeases = 0;
  private activePromotions = 0;
  private readonly highDownloadWaiters: MemoryWaiter[] = [];
  private readonly highStreamWaiters: MemoryWaiter[] = [];
  private readonly lowDownloadWaiters: MemoryWaiter[] = [];
  private readonly lowStreamWaiters: MemoryWaiter[] = [];
  private contendedHighGrants = 0;
  private nextKind: MemoryRequestKind = 'download';
  private closedError: Error | undefined;
  private closePromise: Promise<void> | undefined;
  private readonly clock: () => number;
  private readonly onEvent: UsenetResourceEventObserver | undefined;
  private readonly resourceEventLogger: ResourceEventLogAggregator;

  constructor(options: SegmentSpoolingRuntimeOptions) {
    this.plan = options.plan;
    this.clock = options.clock ?? Date.now;
    this.onEvent = options.onEvent;
    this.hotpathCounters = options.hotpathCounters;
    this.resourceEventLogger = new ResourceEventLogAggregator(
      {
        debugEnabled: () => logger.isLevelEnabled('debug'),
        debug: (fields, message) => logger.debug(fields, message),
        warn: (fields, message) => logger.warn(fields, message),
        emitted: () => {
          if (this.hotpathCounters) {
            this.hotpathCounters.resourceLogRecordsEmitted++;
          }
        },
        suppressed: () => {
          if (this.hotpathCounters) {
            this.hotpathCounters.resourceLogRecordsSuppressed++;
          }
        },
      },
      this.clock
    );
    this.memoryBudget =
      options.memoryBudget ?? new ByteBudget(options.plan.memoryBudgetBytes);
    this.streamAdmissionMaxBytes = Math.floor(
      options.plan.memoryBudgetBytes / 2
    );
    const fileStreamWindowBytes = resolveSegmentStreamMemoryBytes(
      options.plan.readerHighWaterMarkBytes,
      options.plan.readerHighWaterMarkBytes
    );
    if (
      fileStreamWindowBytes > options.plan.perStreamBufferBytes ||
      fileStreamWindowBytes > this.streamAdmissionMaxBytes
    ) {
      throw new UsenetSpoolError(
        'USENET_MEMORY_BUDGET',
        'Segment-spooling resource plan cannot cover the hard stream queues'
      );
    }
    this.spoolManager =
      options.spoolManager ??
      new SpoolManager({
        plan: options.plan,
        engineId: options.engineId,
        cacheRoot: options.cacheRoot,
        clock: this.clock,
        onEvent: (event) => this.observeEvent(event),
        hotpathCounters: this.hotpathCounters,
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
   * Direct streams request two hard queue capacities (artifact reader +
   * ordered stream), while the FileStream path includes a third capacity for
   * its relay. Admission and global accounting are checked in the same
   * synchronous grant; no partial lease is held while another budget awaits.
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

  /**
   * Best-effort admission for bounded background promotion queues. Promotion
   * never queues and never bypasses an already waiting stream/download.
   */
  tryAcquirePromotionMemory(bytes: number): ByteLease | undefined {
    if (
      this.closedError ||
      this.memoryWaitingCount > 0 ||
      !Number.isSafeInteger(bytes) ||
      bytes <= 0 ||
      bytes > this.plan.memoryBudgetBytes
    ) {
      return undefined;
    }
    const globalLease = this.memoryBudget.tryAcquire(bytes);
    if (!globalLease) return undefined;
    let released = false;
    return {
      bytes: globalLease.bytes,
      release: () => {
        if (released) return;
        released = true;
        globalLease.release();
        // Runtime waiters are intentionally kept outside ByteBudget so stream
        // admission and cross-kind fairness can be decided atomically. A
        // promotion therefore owns the matching lost-wakeup bridge as well.
        this.drainMemoryWaiters();
      },
    };
  }

  /**
   * Synchronous, queue-free admission for best-effort persistent promotion.
   * Foreground waiters always win; while a playback stream owns memory, at
   * most one already admitted promotion may consume disk/CPU resources.
   */
  tryStartPromotion(
    foregroundDownloadWaiting: boolean
  ): (() => void) | undefined {
    const spool = this.spoolManager.stats();
    if (
      this.closedError ||
      foregroundDownloadWaiting ||
      this.memoryWaitingCount > 0 ||
      spool.budget.waiting > 0 ||
      spool.files.waiting > 0 ||
      (this.activeStreamLeases > 0 && this.activePromotions >= 1)
    ) {
      if (this.hotpathCounters) {
        this.hotpathCounters.promotionsSkippedForForegroundPressure++;
      }
      return undefined;
    }
    this.activePromotions++;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.activePromotions--;
      assert(this.activePromotions >= 0);
    };
  }

  recordPromotion(outcome: 'success' | 'skipped' | 'failed'): void {
    this.observeEvent({ type: 'promotion_result', outcome });
  }

  recordStreamCleanup(cause: SegmentStreamCleanupCause): void {
    this.observeEvent({ type: 'stream_cleanup', cause });
  }

  /** Point-in-time accounting from the actual global resource owners. */
  stats(): SegmentSpoolingRuntimeStats {
    const memory = this.memoryBudget.stats();
    return {
      memory: {
        ...memory,
        // Runtime queues intentionally sit outside ByteBudget so admission and
        // cross-kind fairness are atomic; expose their true combined depth.
        waiting: memory.waiting + this.memoryWaitingCount,
      },
      spool: this.spoolManager.stats(),
    };
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
    this.closePromise = this.closeOnce();
    return this.closePromise;
  }

  private async closeOnce(): Promise<void> {
    try {
      await this.spoolManager.close();
    } finally {
      this.resourceEventLogger.flush();
    }
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
        enqueuedAt: this.clock(),
      };
      if (signal) {
        waiter.onAbort = () => {
          if (!this.removeMemoryWaiter(waiter)) return;
          this.removeMemoryAbortListener(waiter);
          this.emitMemoryWaitEnd(waiter, 'aborted');
          reject(abortError(signal));
          this.drainMemoryWaiters();
        };
        signal.addEventListener('abort', waiter.onAbort, { once: true });
      }
      this.memoryQueue(waiter.kind, waiter.priority).push(waiter);
      this.observeEvent({
        type: 'memory_wait_start',
        kind: waiter.kind,
        bytes: waiter.bytes,
        priority: waiter.priority,
        queueDepth: this.memoryWaitingCount,
      });
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
    if (kind === 'stream') {
      this.streamAdmissionUsedBytes += bytes;
      this.activeStreamLeases++;
    }
    this.assertMemoryInvariants();
    let released = false;
    return {
      bytes,
      release: () => {
        if (released) return;
        released = true;
        if (kind === 'stream') {
          this.streamAdmissionUsedBytes -= bytes;
          this.activeStreamLeases--;
          assert(this.activeStreamLeases >= 0);
        }
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
      this.emitMemoryWaitEnd(waiter, 'granted');
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
        this.emitMemoryWaitEnd(waiter, 'closed');
        waiter.reject(error);
      }
    }
  }

  private assertMemoryInvariants(): void {
    assert(Number.isSafeInteger(this.streamAdmissionUsedBytes));
    assert(this.streamAdmissionUsedBytes >= 0);
    assert(this.streamAdmissionUsedBytes <= this.streamAdmissionMaxBytes);
    assert(Number.isSafeInteger(this.activeStreamLeases));
    assert(this.activeStreamLeases >= 0);
    assert(Number.isSafeInteger(this.activePromotions));
    assert(this.activePromotions >= 0);
    assert(
      this.memoryBudget.stats().usedBytes <= this.memoryBudget.stats().maxBytes
    );
    assert(this.memoryWaitingCount <= MAX_MEMORY_WAITERS);
  }

  private emitMemoryWaitEnd(
    waiter: MemoryWaiter,
    outcome: 'granted' | 'aborted' | 'closed'
  ): void {
    this.observeEvent({
      type: 'memory_wait_end',
      kind: waiter.kind,
      bytes: waiter.bytes,
      priority: waiter.priority,
      queueDepth: this.memoryWaitingCount,
      waitMs: Math.max(0, this.clock() - waiter.enqueuedAt),
      outcome,
    });
  }

  private observeEvent(event: UsenetResourceLifecycleEvent): void {
    if (this.hotpathCounters) this.hotpathCounters.resourceEventsObserved++;
    try {
      this.onEvent?.(event);
    } catch {
      // Observability is not part of the ownership path.
    }
    this.resourceEventLogger.observe(event);
  }
}
