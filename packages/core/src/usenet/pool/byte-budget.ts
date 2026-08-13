import assert from 'node:assert/strict';
import { CommandPriority } from '../types.js';

/** A byte reservation owned by one caller until {@link release} is invoked. */
export interface ByteLease {
  readonly bytes: number;
  /** Releases the reservation exactly once; subsequent calls are no-ops. */
  release(): void;
}

/** Immutable snapshot of a {@link ByteBudget}. */
export interface ByteBudgetStats {
  readonly maxBytes: number;
  readonly usedBytes: number;
  readonly waiting: number;
  readonly peakBytes: number;
}

type ByteBudgetErrorCode =
  | 'BYTE_BUDGET_INVALID_BYTES'
  | 'BYTE_BUDGET_REQUEST_TOO_LARGE'
  | 'BYTE_BUDGET_INVALID_PRIORITY'
  | 'BYTE_BUDGET_INVALID_MAX_WAITERS'
  | 'BYTE_BUDGET_QUEUE_FULL'
  | 'BYTE_BUDGET_CLOSED';

/** Typed input or lifecycle failure raised by {@link ByteBudget}. */
export class ByteBudgetError extends Error {
  constructor(
    readonly code: ByteBudgetErrorCode,
    message: string
  ) {
    super(message);
    this.name = 'ByteBudgetError';
    Error.captureStackTrace?.(this, ByteBudgetError);
  }
}

interface QueuedWaiter {
  readonly bytes: number;
  readonly priority: CommandPriority;
  readonly resolve: (lease: ByteLease) => void;
  readonly reject: (reason: unknown) => void;
  readonly signal?: AbortSignal;
  onAbort?: () => void;
}

const MAX_CONTENDED_HIGH_GRANTS = 3;
/**
 * Four pending operations per maximum planned open spool file (256) leaves
 * headroom for readers, writers, and downloads while bounding retained
 * callbacks and AbortSignal listeners.
 */
const DEFAULT_MAX_WAITERS = 1024;

function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

function abortReason(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  const error = new Error('Byte budget acquisition aborted', {
    cause: signal.reason,
  });
  error.name = 'AbortError';
  return error;
}

/**
 * A hard byte-cap with abortable, priority-aware leases.
 *
 * Invariants, asserted after every accounting mutation:
 *
 * - `usedBytes` is a safe integer in `[0, maxBytes]`.
 * - every granted lease is counted exactly once until its idempotent release.
 * - each priority queue is FIFO and contains only live pending acquires.
 * - the combined queues contain at most the configured `maxWaiters`.
 *
 * High priority is favoured under sustained contention, but cannot starve Low:
 * before a Low turn is owed, a fitting High head wins, or a fitting Low head
 * uses capacity that an oversized High head cannot currently use. After three
 * contended High grants, the Low head owns the next turn; if it does not fit,
 * capacity is reserved instead of allowing High to bypass it. Only the two
 * queue heads are considered, preserving FIFO within each priority.
 * Pending acquires are bounded by `maxWaiters`, which defaults to 1024.
 */
export class ByteBudget {
  private readonly maxBytes: number;
  private readonly maxWaiters: number;
  private usedBytes = 0;
  private peakBytes = 0;
  private readonly highWaiters: QueuedWaiter[] = [];
  private readonly lowWaiters: QueuedWaiter[] = [];
  private contendedHighGrants = 0;
  private closedError: Error | undefined;

  constructor(
    maxBytes: number,
    options: {
      readonly maxWaiters?: number;
    } = {}
  ) {
    if (!isPositiveSafeInteger(maxBytes)) {
      throw new ByteBudgetError(
        'BYTE_BUDGET_INVALID_BYTES',
        'Byte budget maxBytes must be a finite, safe, positive integer'
      );
    }
    const maxWaiters = options.maxWaiters ?? DEFAULT_MAX_WAITERS;
    if (!isPositiveSafeInteger(maxWaiters)) {
      throw new ByteBudgetError(
        'BYTE_BUDGET_INVALID_MAX_WAITERS',
        'Byte budget maxWaiters must be a finite, safe, positive integer'
      );
    }
    this.maxBytes = maxBytes;
    this.maxWaiters = maxWaiters;
    this.assertInvariants();
  }

  /**
   * Acquires `bytes`, waiting without polling when capacity is unavailable.
   * The returned promise rejects if the signal aborts before the grant, the
   * budget closes, or the request is invalid or larger than the hard cap.
   */
  acquire(
    bytes: number,
    options: {
      signal?: AbortSignal;
      priority?: CommandPriority;
    } = {}
  ): Promise<ByteLease> {
    try {
      this.validateRequest(bytes);
      this.assertOpen();
      this.validatePriority(options.priority);
    } catch (error) {
      return Promise.reject(error);
    }

    if (options.signal?.aborted) {
      return Promise.reject(abortReason(options.signal));
    }

    const priority = options.priority ?? CommandPriority.High;
    if (this.waitingCount === 0 && bytes <= this.maxBytes - this.usedBytes) {
      return Promise.resolve(this.grant(bytes));
    }
    if (this.waitingCount >= this.maxWaiters) {
      return Promise.reject(
        new ByteBudgetError(
          'BYTE_BUDGET_QUEUE_FULL',
          `Byte budget waiter queue reached its ${this.maxWaiters}-request limit`
        )
      );
    }

    return new Promise<ByteLease>((resolve, reject) => {
      const waiter: QueuedWaiter = {
        bytes,
        priority,
        resolve,
        reject,
        signal: options.signal,
      };

      const signal = options.signal;
      if (signal) {
        waiter.onAbort = () => {
          if (!this.removeWaiter(waiter)) return;
          this.removeAbortListener(waiter);
          reject(abortReason(signal));
          this.drain();
        };
        signal.addEventListener('abort', waiter.onAbort, { once: true });
      }

      this.queueFor(priority).push(waiter);
      this.assertInvariants();
      this.drain();
    });
  }

  /**
   * Acquires immediately or returns `null`. Existing waiters are never
   * bypassed, so this method cannot undermine FIFO or priority fairness.
   */
  tryAcquire(bytes: number): ByteLease | null {
    this.validateRequest(bytes);
    this.assertOpen();
    if (this.waitingCount > 0 || bytes > this.maxBytes - this.usedBytes) {
      return null;
    }
    return this.grant(bytes);
  }

  /** Returns an immutable point-in-time accounting snapshot. */
  stats(): ByteBudgetStats {
    return {
      maxBytes: this.maxBytes,
      usedBytes: this.usedBytes,
      waiting: this.waitingCount,
      peakBytes: this.peakBytes,
    };
  }

  /**
   * Permanently closes the budget and rejects every pending waiter. Active
   * leases remain accounted until their owners release them. Closing twice is
   * harmless; the first close reason remains authoritative.
   */
  close(
    error: Error = new ByteBudgetError(
      'BYTE_BUDGET_CLOSED',
      'Byte budget is closed'
    )
  ): void {
    if (this.closedError) return;
    this.closedError = error;
    this.rejectQueue(this.highWaiters, error);
    this.rejectQueue(this.lowWaiters, error);
    this.assertInvariants();
  }

  private get waitingCount(): number {
    return this.highWaiters.length + this.lowWaiters.length;
  }

  private validateRequest(bytes: number): void {
    if (!isPositiveSafeInteger(bytes)) {
      throw new ByteBudgetError(
        'BYTE_BUDGET_INVALID_BYTES',
        'Requested bytes must be a finite, safe, positive integer'
      );
    }
    if (bytes > this.maxBytes) {
      throw new ByteBudgetError(
        'BYTE_BUDGET_REQUEST_TOO_LARGE',
        `Requested ${bytes} bytes exceeds the ${this.maxBytes}-byte budget`
      );
    }
  }

  private validatePriority(priority: CommandPriority | undefined): void {
    if (
      priority !== undefined &&
      priority !== CommandPriority.High &&
      priority !== CommandPriority.Low
    ) {
      throw new ByteBudgetError(
        'BYTE_BUDGET_INVALID_PRIORITY',
        'Byte budget priority must be CommandPriority.High or CommandPriority.Low'
      );
    }
  }

  private assertOpen(): void {
    if (this.closedError) throw this.closedError;
  }

  private queueFor(priority: CommandPriority): QueuedWaiter[] {
    return priority === CommandPriority.Low
      ? this.lowWaiters
      : this.highWaiters;
  }

  private removeWaiter(waiter: QueuedWaiter): boolean {
    const queue = this.queueFor(waiter.priority);
    const index = queue.indexOf(waiter);
    if (index < 0) return false;
    queue.splice(index, 1);
    this.assertInvariants();
    return true;
  }

  private removeAbortListener(waiter: QueuedWaiter): void {
    if (waiter.signal && waiter.onAbort) {
      waiter.signal.removeEventListener('abort', waiter.onAbort);
      waiter.onAbort = undefined;
    }
  }

  private rejectQueue(queue: QueuedWaiter[], error: Error): void {
    while (queue.length > 0) {
      const waiter = queue.shift();
      assert(waiter, 'non-empty byte-budget queue must have a head');
      this.removeAbortListener(waiter);
      waiter.reject(error);
    }
  }

  private drain(): void {
    while (!this.closedError && this.waitingCount > 0) {
      const bothPrioritiesWaiting =
        this.highWaiters.length > 0 && this.lowWaiters.length > 0;
      const availableBytes = this.maxBytes - this.usedBytes;
      const queue = this.pickQueue(availableBytes);
      const waiter = queue?.[0];
      if (!waiter) return;

      queue.shift();
      this.assertInvariants();
      this.removeAbortListener(waiter);
      if (bothPrioritiesWaiting && waiter.priority === CommandPriority.High) {
        this.contendedHighGrants++;
      } else {
        this.contendedHighGrants = 0;
      }
      waiter.resolve(this.grant(waiter.bytes));
    }
  }

  /** Selects only between queue heads; `undefined` reserves current capacity. */
  private pickQueue(availableBytes: number): QueuedWaiter[] | undefined {
    const highHead = this.highWaiters[0];
    const lowHead = this.lowWaiters[0];

    if (!highHead) {
      return lowHead && lowHead.bytes <= availableBytes
        ? this.lowWaiters
        : undefined;
    }
    if (!lowHead) {
      return highHead.bytes <= availableBytes ? this.highWaiters : undefined;
    }
    if (this.contendedHighGrants >= MAX_CONTENDED_HIGH_GRANTS) {
      return lowHead.bytes <= availableBytes ? this.lowWaiters : undefined;
    }
    if (highHead.bytes <= availableBytes) return this.highWaiters;
    return lowHead.bytes <= availableBytes ? this.lowWaiters : undefined;
  }

  private grant(bytes: number): ByteLease {
    assert(
      bytes <= this.maxBytes - this.usedBytes,
      'byte-budget grant must fit within remaining capacity'
    );
    this.usedBytes += bytes;
    this.peakBytes = Math.max(this.peakBytes, this.usedBytes);
    this.assertInvariants();

    let released = false;
    return {
      bytes,
      release: () => {
        if (released) return;
        released = true;
        this.release(bytes);
      },
    };
  }

  private release(bytes: number): void {
    assert(
      bytes <= this.usedBytes,
      'byte-budget release cannot exceed accounted usage'
    );
    this.usedBytes -= bytes;
    this.assertInvariants();
    this.drain();
  }

  private assertInvariants(): void {
    assert(
      Number.isSafeInteger(this.usedBytes),
      'byte-budget usedBytes must remain a safe integer'
    );
    assert(this.usedBytes >= 0, 'byte-budget usedBytes cannot be negative');
    assert(
      this.usedBytes <= this.maxBytes,
      'byte-budget usedBytes cannot exceed maxBytes'
    );
    assert(
      this.waitingCount <= this.maxWaiters,
      'byte-budget waiter count cannot exceed maxWaiters'
    );
  }
}
