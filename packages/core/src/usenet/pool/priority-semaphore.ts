import { CommandPriority } from '../types.js';
import type { SegmentSpoolingHotpathCounters } from './hotpath-counters.js';

export const MAX_ACTIVE_SEMAPHORE_OWNERS = 128;
export const MAX_SEMAPHORE_WAITERS = 65_536;
export const MAX_SEMAPHORE_WAITERS_PER_OWNER = 1024;
const MAX_OWNER_KEY_LENGTH = 128;
const ANONYMOUS_OWNER = '';

export type PrioritySemaphoreErrorCode =
  | 'SEMAPHORE_GLOBAL_CAPACITY'
  | 'SEMAPHORE_OWNER_CAPACITY'
  | 'SEMAPHORE_ACTIVE_OWNER_CAPACITY'
  | 'SEMAPHORE_INVALID_OWNER'
  | 'SEMAPHORE_INVALID_PRIORITY'
  | 'SEMAPHORE_CLOSED'
  | 'SEMAPHORE_ABORTED';

/** Stable internal error taxonomy for bounded semaphore admission. */
export class PrioritySemaphoreError extends Error {
  override readonly cause?: unknown;

  constructor(
    readonly code: PrioritySemaphoreErrorCode,
    message: string,
    options: { readonly cause?: unknown } = {}
  ) {
    super(message);
    this.name = 'PrioritySemaphoreError';
    if (options.cause !== undefined) this.cause = options.cause;
    Error.captureStackTrace?.(this, PrioritySemaphoreError);
  }
}

interface PrioritySemaphoreOptions {
  readonly maxWaiters?: number;
  readonly maxWaitersPerOwner?: number;
  readonly maxActiveOwners?: number;
  readonly hotpathCounters?: SegmentSpoolingHotpathCounters;
}

interface WaiterNode {
  readonly priority: CommandPriority;
  readonly ownerKey: string;
  readonly resolve: (release: () => void) => void;
  readonly reject: (error: Error) => void;
  readonly signal?: AbortSignal;
  owner: OwnerQueue;
  previous?: WaiterNode;
  next?: WaiterNode;
  onAbort?: () => void;
  queued: boolean;
}

class OwnerQueue {
  count: number;
  head?: WaiterNode;
  tail?: WaiterNode;
  previousOwner: OwnerQueue;
  nextOwner: OwnerQueue;

  constructor(
    readonly priority: CommandPriority,
    readonly ownerKey: string
  ) {
    this.count = 0;
    this.previousOwner = this;
    this.nextOwner = this;
  }
}

interface PriorityQueueState {
  readonly owners: Map<string, OwnerQueue>;
  cursor?: OwnerQueue;
}

interface OwnerAccounting {
  queues: number;
  waiters: number;
}

function positiveSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a safe positive integer`);
  }
  return value;
}

/**
 * Priority-aware, owner-fair counting semaphore.
 *
 * Waiters live in intrusive FIFO deques, one per owner and priority. The owner
 * deques form circular rings, so enqueue, grant selection, arbitrary abort and
 * owner retirement are O(1) without scanning the global waiter population or
 * shifting arrays. At most 128 distinct queued owners (including the anonymous
 * compatibility owner), 1,024 waiters per owner and 65,536 waiters globally
 * are retained by default.
 *
 * High-priority playback is strongly favoured but does not strictly starve Low
 * when `highShare < 1`: the deterministic rolling accumulator retains the
 * existing weighted High/Low turns. FIFO remains strict within each owner.
 */
export class PrioritySemaphore {
  private readonly max: number;
  private limit: number;
  private inUseCount = 0;
  private waitingCount = 0;
  private readonly high: PriorityQueueState = { owners: new Map() };
  private readonly low: PriorityQueueState = { owners: new Map() };
  /** Bounded cross-priority accounting for each distinct queued owner. */
  private readonly ownerAccounting = new Map<string, OwnerAccounting>();
  private readonly maxWaiters: number;
  private readonly maxWaitersPerOwner: number;
  private readonly maxActiveOwners: number;
  private readonly hotpathCounters: SegmentSpoolingHotpathCounters | undefined;
  /** Per-100 odds a contended grant goes to Low (0 = strict priority). */
  private readonly lowOdds: number;
  private lowAccumulator = 0;
  private closedError: Error | undefined;

  constructor(
    permits: number,
    highShare = 1,
    options: PrioritySemaphoreOptions = {}
  ) {
    this.max = positiveSafeInteger(permits, 'semaphore permits');
    this.limit = this.max;
    this.maxWaiters = positiveSafeInteger(
      options.maxWaiters ?? MAX_SEMAPHORE_WAITERS,
      'semaphore waiter capacity'
    );
    this.maxWaitersPerOwner = positiveSafeInteger(
      options.maxWaitersPerOwner ?? MAX_SEMAPHORE_WAITERS_PER_OWNER,
      'semaphore owner waiter capacity'
    );
    this.maxActiveOwners = positiveSafeInteger(
      options.maxActiveOwners ?? MAX_ACTIVE_SEMAPHORE_OWNERS,
      'semaphore active owner capacity'
    );
    if (this.maxWaitersPerOwner > this.maxWaiters) {
      throw new RangeError(
        'semaphore owner waiter capacity cannot exceed global capacity'
      );
    }
    this.hotpathCounters = options.hotpathCounters;
    const clamped = Math.min(1, Math.max(0, highShare));
    this.lowOdds = Math.round((1 - clamped) * 100);
  }

  get inUse(): number {
    return this.inUseCount;
  }

  get capacity(): number {
    return this.max;
  }

  get effectiveLimit(): number {
    return this.limit;
  }

  get waiting(): number {
    return this.waitingCount;
  }

  /** Distinct owner keys currently retained by a queued priority ring. */
  get activeOwners(): number {
    return this.ownerAccounting.size;
  }

  acquire(
    priority: CommandPriority = CommandPriority.High,
    signal?: AbortSignal,
    ownerKey = ANONYMOUS_OWNER
  ): Promise<() => void> {
    if (this.closedError) return Promise.reject(this.closedError);
    if (signal?.aborted) return Promise.reject(this.abortedError(signal));
    if (priority !== CommandPriority.High && priority !== CommandPriority.Low) {
      return Promise.reject(
        new PrioritySemaphoreError(
          'SEMAPHORE_INVALID_PRIORITY',
          'invalid semaphore priority'
        )
      );
    }
    if (
      typeof ownerKey !== 'string' ||
      ownerKey.length > MAX_OWNER_KEY_LENGTH
    ) {
      return Promise.reject(
        new PrioritySemaphoreError(
          'SEMAPHORE_INVALID_OWNER',
          'invalid semaphore owner key'
        )
      );
    }
    if (this.inUseCount < this.limit && this.waitingCount === 0) {
      this.inUseCount++;
      this.recordGrant();
      return Promise.resolve(this.makeRelease());
    }
    if (this.waitingCount >= this.maxWaiters) {
      return Promise.reject(
        this.capacityError(
          'SEMAPHORE_GLOBAL_CAPACITY',
          'semaphore waiter capacity reached'
        )
      );
    }

    const state = this.state(priority);
    const existingOwner = state.owners.get(ownerKey);
    if (
      (this.ownerAccounting.get(ownerKey)?.waiters ?? 0) >=
      this.maxWaitersPerOwner
    ) {
      return Promise.reject(
        this.capacityError(
          'SEMAPHORE_OWNER_CAPACITY',
          'semaphore owner waiter capacity reached'
        )
      );
    }
    if (
      !existingOwner &&
      !this.ownerAccounting.has(ownerKey) &&
      this.ownerAccounting.size >= this.maxActiveOwners
    ) {
      return Promise.reject(
        this.capacityError(
          'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
          'semaphore active owner capacity reached'
        )
      );
    }

    return new Promise<() => void>((resolve, reject) => {
      const owner = existingOwner ?? this.addOwner(state, priority, ownerKey);
      const waiter: WaiterNode = {
        priority,
        ownerKey,
        owner,
        resolve,
        reject,
        signal,
        queued: true,
      };
      this.appendWaiter(owner, waiter);
      if (signal) {
        waiter.onAbort = () => {
          if (!waiter.queued) return;
          this.removeWaiter(waiter);
          this.hotpathCounters?.semaphoreAbort();
          reject(this.abortedError(signal));
          this.drainAvailable();
        };
        signal.addEventListener('abort', waiter.onAbort, { once: true });
      }
      this.updatePeaks();
      this.drainAvailable();
    });
  }

  throttleTo(value: number): void {
    const next = Math.max(1, Math.min(this.max, Math.floor(value)));
    if (next === this.limit) return;
    this.limit = next;
    this.drainAvailable();
  }

  restore(): void {
    this.throttleTo(this.max);
  }

  close(error?: Error): void {
    if (this.closedError) return;
    this.closedError =
      error instanceof PrioritySemaphoreError &&
      error.code === 'SEMAPHORE_CLOSED'
        ? error
        : new PrioritySemaphoreError(
            'SEMAPHORE_CLOSED',
            error?.message ?? 'semaphore closed',
            { cause: error }
          );
    this.rejectState(this.high, this.closedError);
    this.rejectState(this.low, this.closedError);
    this.ownerAccounting.clear();
    this.waitingCount = 0;
  }

  private makeRelease(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.inUseCount--;
      this.drainAvailable();
    };
  }

  private drainAvailable(): void {
    while (
      !this.closedError &&
      this.inUseCount < this.limit &&
      this.waitingCount > 0
    ) {
      this.grantNext();
    }
  }

  private grantNext(): void {
    const state = this.pickPriorityState();
    const owner = state.cursor;
    if (!owner) return;
    const waiter = owner.head;
    if (!waiter) {
      throw new Error('semaphore owner ring contains an empty queue');
    }
    this.removeWaiter(waiter, true);
    this.hotpathCounters?.semaphoreOwnerTurn();
    this.inUseCount++;
    this.recordGrant();
    waiter.resolve(this.makeRelease());
  }

  private pickPriorityState(): PriorityQueueState {
    const highWaiting = this.high.cursor !== undefined;
    const lowWaiting = this.low.cursor !== undefined;
    if (!highWaiting) return this.low;
    if (!lowWaiting || this.lowOdds === 0) return this.high;
    this.lowAccumulator += this.lowOdds;
    if (this.lowAccumulator >= 100) {
      this.lowAccumulator -= 100;
      return this.low;
    }
    return this.high;
  }

  private state(priority: CommandPriority): PriorityQueueState {
    return priority === CommandPriority.High ? this.high : this.low;
  }

  private addOwner(
    state: PriorityQueueState,
    priority: CommandPriority,
    ownerKey: string
  ): OwnerQueue {
    const owner = new OwnerQueue(priority, ownerKey);
    const cursor = state.cursor;
    if (!cursor) {
      state.cursor = owner;
    } else {
      const tail = cursor.previousOwner;
      owner.previousOwner = tail;
      owner.nextOwner = cursor;
      tail.nextOwner = owner;
      cursor.previousOwner = owner;
    }
    state.owners.set(ownerKey, owner);
    const accounting = this.ownerAccounting.get(ownerKey);
    if (accounting) accounting.queues++;
    else this.ownerAccounting.set(ownerKey, { queues: 1, waiters: 0 });
    return owner;
  }

  private removeOwner(owner: OwnerQueue): void {
    const state = this.state(owner.priority);
    if (owner.nextOwner === owner) {
      state.cursor = undefined;
    } else {
      owner.previousOwner.nextOwner = owner.nextOwner;
      owner.nextOwner.previousOwner = owner.previousOwner;
      if (state.cursor === owner) state.cursor = owner.nextOwner;
    }
    state.owners.delete(owner.ownerKey);
    const accounting = this.ownerAccounting.get(owner.ownerKey);
    if (accounting) {
      accounting.queues--;
      if (accounting.queues === 0 && accounting.waiters === 0) {
        this.ownerAccounting.delete(owner.ownerKey);
      }
    }
    owner.previousOwner = owner;
    owner.nextOwner = owner;
  }

  private appendWaiter(owner: OwnerQueue, waiter: WaiterNode): void {
    waiter.previous = owner.tail;
    if (owner.tail) owner.tail.next = waiter;
    else owner.head = waiter;
    owner.tail = waiter;
    owner.count++;
    this.waitingCount++;
    const accounting = this.ownerAccounting.get(owner.ownerKey);
    if (!accounting) {
      throw new Error('semaphore owner accounting is missing');
    }
    accounting.waiters++;
  }

  private removeWaiter(waiter: WaiterNode, advanceOwnerTurn = false): void {
    if (!waiter.queued) return;
    waiter.queued = false;
    const owner = waiter.owner;
    if (waiter.previous) waiter.previous.next = waiter.next;
    else owner.head = waiter.next;
    if (waiter.next) waiter.next.previous = waiter.previous;
    else owner.tail = waiter.previous;
    waiter.previous = undefined;
    waiter.next = undefined;
    owner.count--;
    this.waitingCount--;
    const accounting = this.ownerAccounting.get(owner.ownerKey);
    if (!accounting || accounting.waiters <= 0) {
      throw new Error('semaphore owner waiter accounting underflow');
    }
    accounting.waiters--;
    if (waiter.signal && waiter.onAbort) {
      waiter.signal.removeEventListener('abort', waiter.onAbort);
      waiter.onAbort = undefined;
    }
    if (owner.count === 0) this.removeOwner(owner);
    else if (advanceOwnerTurn) {
      this.state(owner.priority).cursor = owner.nextOwner;
    }
  }

  private rejectState(state: PriorityQueueState, error: Error): void {
    for (const owner of state.owners.values()) {
      let waiter = owner.head;
      while (waiter) {
        const next = waiter.next;
        waiter.queued = false;
        if (waiter.signal && waiter.onAbort) {
          waiter.signal.removeEventListener('abort', waiter.onAbort);
        }
        waiter.reject(error);
        waiter = next;
      }
      owner.head = undefined;
      owner.tail = undefined;
      owner.count = 0;
      owner.previousOwner = owner;
      owner.nextOwner = owner;
    }
    state.owners.clear();
    state.cursor = undefined;
  }

  private updatePeaks(): void {
    this.hotpathCounters?.semaphoreQueued(
      this.ownerAccounting.size,
      this.waitingCount
    );
  }

  private recordGrant(): void {
    this.hotpathCounters?.semaphoreGrant();
  }

  private capacityError(
    code: Extract<
      PrioritySemaphoreErrorCode,
      | 'SEMAPHORE_GLOBAL_CAPACITY'
      | 'SEMAPHORE_OWNER_CAPACITY'
      | 'SEMAPHORE_ACTIVE_OWNER_CAPACITY'
    >,
    message: string
  ): PrioritySemaphoreError {
    this.hotpathCounters?.semaphoreCapacityReject();
    return new PrioritySemaphoreError(code, message);
  }

  private abortedError(signal: AbortSignal): PrioritySemaphoreError {
    return new PrioritySemaphoreError('SEMAPHORE_ABORTED', 'aborted', {
      cause: signal.reason,
    });
  }
}
