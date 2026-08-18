import { CommandPriority } from '../types.js';

interface QueuedWaiter {
  priority: CommandPriority;
  ownerKey: string;
  resolve: (release: () => void) => void;
  reject: (err: Error) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

const ANONYMOUS_OWNER = '';
const MAX_WAITERS = 65_536;
const MAX_WAITERS_PER_OWNER = 1024;
const MAX_OWNER_KEY_LENGTH = 128;

/**
 * A counting semaphore whose waiters are served by priority (lower enum value
 * first → High before Low), owner round-robin within a priority, and FIFO
 * within each owner. Used as the global download budget (gates BODY/ARTICLE).
 *
 * High-priority playback is strongly favoured but does NOT strictly starve
 * Low-priority background work (health checks / inspect / seek probes): when both
 * classes are waiting, `highShare` of contended grants go to High and the rest to
 * Low, chosen by a deterministic rolling accumulator. `highShare = 1` restores
 * strict priority. With only one class waiting, the share is irrelevant;
 * the head is served.
 *
 * The ceiling is dynamic: {@link throttleTo} temporarily lowers the effective
 * limit (down to ≥1) and {@link restore} returns it to the hard `max`. The
 * per-provider connection pool uses this to back off when a provider reports its
 * account connection limit ("too many connections"): new acquires queue instead
 * of dialing past the ceiling, without ever exceeding the configured maximum.
 */
export class PrioritySemaphore {
  /** Hard ceiling (configured maximum); `limit` never exceeds this. */
  private readonly max: number;
  /** Current effective ceiling (≤ max); lowered while throttled. */
  private limit: number;
  /** Permits currently leased out. */
  private inUseCount = 0;
  private waiters: QueuedWaiter[] = [];
  private readonly highOwners: string[] = [];
  private readonly lowOwners: string[] = [];
  private highOwnerCursor = 0;
  private lowOwnerCursor = 0;
  /** Per-100 odds a contended grant goes to Low (0 = strict priority). */
  private readonly lowOdds: number;
  /** Accumulator driving the deterministic High/Low pick. */
  private lowAcc = 0;
  private closedError: Error | undefined;

  /**
   * @param permits   hard capacity.
   * @param highShare share (0..1) of contended grants reserved for High; `1`
   *                  (default) = strict priority, never serve Low while High waits.
   */
  constructor(permits: number, highShare = 1) {
    this.max = permits;
    this.limit = permits;
    const clamped = Math.min(1, Math.max(0, highShare));
    this.lowOdds = Math.round((1 - clamped) * 100);
  }

  get inUse(): number {
    return this.inUseCount;
  }

  /** The hard ceiling (configured maximum), unaffected by throttling. */
  get capacity(): number {
    return this.max;
  }

  /** The current effective ceiling (≤ {@link capacity} while throttled). */
  get effectiveLimit(): number {
    return this.limit;
  }

  get waiting(): number {
    return this.waiters.length;
  }

  /**
   * Acquire one permit. `ownerKey` is bounded and drives fair turns among
   * active streams; omitted callers share the anonymous FIFO owner. Resolves
   * with an idempotent release function and rejects if the signal aborts.
   */
  acquire(
    priority: CommandPriority = CommandPriority.High,
    signal?: AbortSignal,
    ownerKey = ANONYMOUS_OWNER
  ): Promise<() => void> {
    if (this.closedError) return Promise.reject(this.closedError);
    if (signal?.aborted) {
      return Promise.reject(new Error('aborted'));
    }
    if (
      typeof ownerKey !== 'string' ||
      ownerKey.length > MAX_OWNER_KEY_LENGTH
    ) {
      return Promise.reject(new Error('invalid semaphore owner key'));
    }
    if (this.inUseCount < this.limit && this.waiters.length === 0) {
      this.inUseCount++;
      return Promise.resolve(this.makeRelease());
    }
    if (this.waiters.length >= MAX_WAITERS) {
      return Promise.reject(new Error('semaphore waiter capacity reached'));
    }
    let ownerWaiters = 0;
    for (const waiter of this.waiters) {
      if (waiter.ownerKey === ownerKey) ownerWaiters++;
    }
    if (ownerWaiters >= MAX_WAITERS_PER_OWNER) {
      return Promise.reject(
        new Error('semaphore owner waiter capacity reached')
      );
    }
    return new Promise<() => void>((resolve, reject) => {
      const waiter: QueuedWaiter = {
        priority,
        ownerKey,
        resolve: (release) => resolve(release),
        reject,
        signal,
      };
      if (signal) {
        waiter.onAbort = () => {
          const idx = this.waiters.indexOf(waiter);
          if (idx !== -1) {
            this.waiters.splice(idx, 1);
            this.removeIdleOwner(waiter.priority, waiter.ownerKey);
          }
          reject(new Error('aborted'));
          this.drainAvailable();
        };
        signal.addEventListener('abort', waiter.onAbort, { once: true });
      }
      // Global insertion order plus the owner ring gives FIFO within each
      // owner and round-robin service across contending owners.
      this.waiters.push(waiter);
      const owners = this.ownerOrder(priority);
      if (!owners.includes(ownerKey)) owners.push(ownerKey);
      this.drainAvailable();
    });
  }

  /**
   * Set the effective ceiling to `n` (clamped to [1, max]). Lowering never
   * preempts in-flight permits; it just stops new grants until releases bring
   * `inUse` back under it. Raising immediately wakes queued waiters up to the new
   * limit, so it doubles as the recovery/step-up primitive.
   */
  throttleTo(n: number): void {
    const next = Math.max(1, Math.min(this.max, Math.floor(n)));
    if (next === this.limit) return;
    this.limit = next;
    // Grant any freed headroom to queued waiters (share-weighted High/Low).
    while (
      !this.closedError &&
      this.inUseCount < this.limit &&
      this.waiters.length > 0
    ) {
      this.grantNext();
    }
  }

  /** Restore the effective ceiling to the hard maximum and wake any waiters. */
  restore(): void {
    this.throttleTo(this.max);
  }

  /** Reject every queued acquire and permanently reject future acquires. */
  close(error: Error): void {
    if (this.closedError) return;
    this.closedError = error;
    const waiters = this.waiters;
    this.waiters = [];
    this.highOwners.length = 0;
    this.lowOwners.length = 0;
    this.highOwnerCursor = 0;
    this.lowOwnerCursor = 0;
    for (const waiter of waiters) {
      if (waiter.signal && waiter.onAbort) {
        waiter.signal.removeEventListener('abort', waiter.onAbort);
      }
      waiter.reject(error);
    }
  }

  private makeRelease(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.release();
    };
  }

  private release(): void {
    this.inUseCount--;
    // Hand the freed permit to the next waiter, but only while we're within the
    // (possibly throttled) limit, so a lowered ceiling actually holds.
    if (
      !this.closedError &&
      this.inUseCount < this.limit &&
      this.waiters.length > 0
    ) {
      this.grantNext();
    }
  }

  /** Grant one permit to a waiter chosen by the High/Low share, then resolve it. */
  private grantNext(): void {
    const idx = this.pickWaiterIndex();
    if (idx < 0) return;
    const w = this.waiters.splice(idx, 1)[0];
    this.advanceOwnerTurn(w.priority, w.ownerKey);
    if (w.signal && w.onAbort) {
      w.signal.removeEventListener('abort', w.onAbort);
    }
    this.inUseCount++;
    w.resolve(this.makeRelease());
  }

  /**
   * Index of the waiter to serve. High/Low selection retains the configured
   * deterministic share. Within that class, owners receive round-robin turns;
   * the first queued waiter for the selected owner preserves owner-local FIFO.
   */
  private pickWaiterIndex(): number {
    if (this.waiters.length === 0) return -1;
    const highWaiting = this.highOwners.length > 0;
    const lowWaiting = this.lowOwners.length > 0;
    if (!highWaiting) return this.pickOwnerWaiterIndex(CommandPriority.Low);
    if (!lowWaiting || this.lowOdds <= 0) {
      return this.pickOwnerWaiterIndex(CommandPriority.High);
    }
    this.lowAcc += this.lowOdds;
    if (this.lowAcc >= 100) {
      this.lowAcc -= 100;
      return this.pickOwnerWaiterIndex(CommandPriority.Low);
    }
    return this.pickOwnerWaiterIndex(CommandPriority.High);
  }

  private ownerOrder(priority: CommandPriority): string[] {
    return priority === CommandPriority.High ? this.highOwners : this.lowOwners;
  }

  private ownerCursor(priority: CommandPriority): number {
    return priority === CommandPriority.High
      ? this.highOwnerCursor
      : this.lowOwnerCursor;
  }

  private setOwnerCursor(priority: CommandPriority, cursor: number): void {
    if (priority === CommandPriority.High) this.highOwnerCursor = cursor;
    else this.lowOwnerCursor = cursor;
  }

  private pickOwnerWaiterIndex(priority: CommandPriority): number {
    const owners = this.ownerOrder(priority);
    if (owners.length === 0) return -1;
    const cursor = this.ownerCursor(priority) % owners.length;
    const ownerKey = owners[cursor];
    return this.waiters.findIndex(
      (waiter) => waiter.priority === priority && waiter.ownerKey === ownerKey
    );
  }

  private advanceOwnerTurn(priority: CommandPriority, ownerKey: string): void {
    const owners = this.ownerOrder(priority);
    const ownerIndex = owners.indexOf(ownerKey);
    if (ownerIndex < 0) return;
    let cursor = owners.length === 0 ? 0 : (ownerIndex + 1) % owners.length;
    const ownerStillWaiting = this.waiters.some(
      (waiter) => waiter.priority === priority && waiter.ownerKey === ownerKey
    );
    if (!ownerStillWaiting) {
      owners.splice(ownerIndex, 1);
      if (owners.length === 0) cursor = 0;
      else if (ownerIndex < cursor) cursor--;
      if (cursor >= owners.length) cursor = 0;
    }
    this.setOwnerCursor(priority, cursor);
  }

  private removeIdleOwner(priority: CommandPriority, ownerKey: string): void {
    if (
      this.waiters.some(
        (waiter) => waiter.priority === priority && waiter.ownerKey === ownerKey
      )
    ) {
      return;
    }
    const owners = this.ownerOrder(priority);
    const index = owners.indexOf(ownerKey);
    if (index < 0) return;
    let cursor = this.ownerCursor(priority);
    owners.splice(index, 1);
    if (index < cursor) cursor--;
    if (cursor >= owners.length) cursor = 0;
    this.setOwnerCursor(priority, Math.max(0, cursor));
  }

  private drainAvailable(): void {
    while (
      !this.closedError &&
      this.inUseCount < this.limit &&
      this.waiters.length > 0
    ) {
      this.grantNext();
    }
  }
}
