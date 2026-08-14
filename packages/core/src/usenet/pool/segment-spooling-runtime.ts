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

  private closePromise: Promise<void> | undefined;

  constructor(options: SegmentSpoolingRuntimeOptions) {
    this.plan = options.plan;
    this.memoryBudget =
      options.memoryBudget ?? new ByteBudget(options.plan.memoryBudgetBytes);
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

  /** Stop new memory waiters and idempotently dispose the complete spool. */
  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    const error = new UsenetSpoolError(
      'USENET_SPOOL_CLOSED',
      'Segment-spooling runtime is closed'
    );
    this.memoryBudget.close(error);
    this.closePromise = this.spoolManager.close();
    return this.closePromise;
  }
}
