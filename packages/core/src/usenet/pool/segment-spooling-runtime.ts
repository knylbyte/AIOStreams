import { ByteBudget, ByteBudgetError } from './byte-budget.js';
import type { CommandPriority } from '../types.js';
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
  private readonly streamAdmissionBudget: ByteBudget;
  private closePromise: Promise<void> | undefined;

  constructor(options: SegmentSpoolingRuntimeOptions) {
    this.plan = options.plan;
    this.memoryBudget =
      options.memoryBudget ?? new ByteBudget(options.plan.memoryBudgetBytes);
    this.streamAdmissionBudget = new ByteBudget(
      Math.floor(options.plan.memoryBudgetBytes / 2)
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
      return await this.memoryBudget.acquire(
        this.plan.perDownloadBaseLeaseBytes,
        { priority, signal }
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
   * Reserve both bounded Readable queues owned by one output stream: the
   * active artifact reader and its ordered outer stream. The configured
   * per-stream value remains the cap from which this HWM is derived; reserving
   * that complete cap up front could consume the global budget before any
   * download lease is able to make progress.
   */
  async acquireStreamMemory(
    priority: CommandPriority,
    signal?: AbortSignal
  ): Promise<ByteLease> {
    const bytes = 2 * this.plan.readerHighWaterMarkBytes;
    let admissionLease: ByteLease | undefined;
    try {
      admissionLease = await this.streamAdmissionBudget.acquire(bytes, {
        priority,
        signal,
      });
      const globalLease = await this.memoryBudget.acquire(bytes, {
        priority,
        signal,
      });
      let released = false;
      return {
        bytes: globalLease.bytes,
        release: () => {
          if (released) return;
          released = true;
          globalLease.release();
          admissionLease?.release();
          admissionLease = undefined;
        },
      };
    } catch (error) {
      admissionLease?.release();
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
    this.streamAdmissionBudget.close(error);
    this.memoryBudget.close(error);
    this.closePromise = this.spoolManager.close();
    return this.closePromise;
  }
}
