import assert from 'node:assert/strict';
import {
  classifySpoolFileError,
  spoolAbortError,
  UsenetSpoolError,
} from './errors.js';
import type {
  SpoolBudgetLease,
  SpoolBudgetStats,
  SpoolStatFs,
} from './types.js';
import type { UsenetResourceEventObserver } from '../pool/resource-events.js';
import { CommandPriority } from '../types.js';

const DEFAULT_MAX_WAITERS = 1024;

interface BudgetWaiter {
  readonly bytes: number;
  readonly reservation?: SpoolReservation;
  readonly signal?: AbortSignal;
  readonly priority: CommandPriority;
  readonly resolve: (lease: SpoolBudgetLease) => void;
  readonly reject: (error: Error) => void;
  onAbort?: () => void;
  readonly enqueuedAt: number;
}

export interface SpoolBudgetOptions {
  readonly maxBytes: number;
  readonly minFreeDiskBytes: number;
  readonly statfs: () => Promise<SpoolStatFs>;
  readonly maxWaiters?: number;
  readonly clock?: () => number;
  readonly onEvent?: UsenetResourceEventObserver;
}

function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

function isNonNegativeSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

function availableBytes(stats: SpoolStatFs): number {
  const blocks = BigInt(stats.bavail);
  const blockBytes = BigInt(stats.bsize);
  if (blocks < 0n || blockBytes < 0n) {
    throw new UsenetSpoolError(
      'USENET_SPOOL_IO',
      'Filesystem reported an invalid negative free-space value'
    );
  }
  const bytes = blocks * blockBytes;
  return bytes > BigInt(Number.MAX_SAFE_INTEGER)
    ? Number.MAX_SAFE_INTEGER
    : Number(bytes);
}

/**
 * Hard global reservation budget for transient spool files.
 *
 * Invariants:
 *
 * - `0 <= actualBytes <= reservedBytes <= maxBytes`.
 * - pending reservations are bounded and FIFO.
 * - free-space checks include every reserved-but-not-yet-written byte, so
 *   concurrent reservations cannot collectively consume the configured disk
 *   safety margin.
 * - a lease releases accounting exactly once, after its owner removed the file.
 */
export class SpoolBudget {
  private readonly maxBytes: number;
  private readonly minFreeDiskBytes: number;
  private readonly statfs: () => Promise<SpoolStatFs>;
  private readonly maxWaiters: number;
  private reservedBytes = 0;
  private actualBytes = 0;
  private peakReservedBytes = 0;
  private peakActualBytes = 0;
  private readonly waiters: BudgetWaiter[] = [];
  private draining = false;
  private drainRequested = false;
  private closedError: Error | undefined;
  private readonly clock: () => number;
  private readonly onEvent: UsenetResourceEventObserver | undefined;

  constructor(options: SpoolBudgetOptions) {
    if (!isPositiveSafeInteger(options.maxBytes)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Spool budget maxBytes must be a safe positive integer'
      );
    }
    if (!isNonNegativeSafeInteger(options.minFreeDiskBytes)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Spool budget minFreeDiskBytes must be a safe non-negative integer'
      );
    }
    const maxWaiters = options.maxWaiters ?? DEFAULT_MAX_WAITERS;
    if (!isPositiveSafeInteger(maxWaiters)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Spool budget maxWaiters must be a safe positive integer'
      );
    }
    this.maxBytes = options.maxBytes;
    this.minFreeDiskBytes = options.minFreeDiskBytes;
    this.statfs = options.statfs;
    this.maxWaiters = maxWaiters;
    this.clock = options.clock ?? Date.now;
    this.onEvent = options.onEvent;
    this.assertInvariants();
  }

  /** Reserve initial capacity, waiting event-driven when the hard cap is busy. */
  reserve(
    bytes: number,
    options: {
      readonly signal?: AbortSignal;
      readonly priority?: CommandPriority;
    } = {}
  ): Promise<SpoolBudgetLease> {
    try {
      this.validateBytes(bytes);
      this.assertOpen();
      if (bytes > this.maxBytes) {
        throw this.capacityError(bytes);
      }
    } catch (error) {
      return Promise.reject(error);
    }
    return this.enqueue(
      bytes,
      undefined,
      options.signal,
      options.priority ?? CommandPriority.High
    );
  }

  /** Immutable accounting snapshot for diagnostics and deterministic tests. */
  stats(): SpoolBudgetStats {
    return {
      maxBytes: this.maxBytes,
      reservedBytes: this.reservedBytes,
      actualBytes: this.actualBytes,
      peakReservedBytes: this.peakReservedBytes,
      peakActualBytes: this.peakActualBytes,
      waiting: this.waiters.length,
    };
  }

  /** Permanently reject queued/new reservations; active leases remain releasable. */
  close(
    error: Error = new UsenetSpoolError(
      'USENET_SPOOL_CLOSED',
      'Spool budget is closed'
    )
  ): void {
    if (this.closedError) return;
    this.closedError = error;
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift();
      assert(waiter, 'non-empty spool-budget queue must have a head');
      this.removeAbortListener(waiter);
      this.emitWaitEnd(waiter, 'closed');
      waiter.reject(error);
    }
    this.assertInvariants();
  }

  private grow(
    reservation: SpoolReservation,
    bytes: number,
    signal?: AbortSignal,
    priority = CommandPriority.High
  ): Promise<void> {
    try {
      this.validateBytes(bytes);
      this.assertOpen();
      reservation.assertActive();
      if (bytes > this.maxBytes - reservation.reservedBytes) {
        throw this.capacityError(bytes);
      }
    } catch (error) {
      return Promise.reject(error);
    }
    return this.enqueue(bytes, reservation, signal, priority).then(
      () => undefined
    );
  }

  private enqueue(
    bytes: number,
    reservation: SpoolReservation | undefined,
    signal: AbortSignal | undefined,
    priority: CommandPriority
  ): Promise<SpoolBudgetLease> {
    if (signal?.aborted) return Promise.reject(spoolAbortError(signal.reason));
    if (this.waiters.length >= this.maxWaiters) {
      return Promise.reject(
        new UsenetSpoolError(
          'USENET_SPOOL_CAPACITY',
          `Spool budget waiter queue reached its ${this.maxWaiters}-request limit`
        )
      );
    }

    return new Promise<SpoolBudgetLease>((resolve, reject) => {
      const waiter: BudgetWaiter = {
        bytes,
        reservation,
        signal,
        priority,
        resolve,
        reject,
        enqueuedAt: this.clock(),
      };
      if (signal) {
        waiter.onAbort = () => {
          const index = this.waiters.indexOf(waiter);
          if (index < 0) return;
          this.waiters.splice(index, 1);
          this.removeAbortListener(waiter);
          this.emitWaitEnd(waiter, 'aborted');
          reject(spoolAbortError(signal.reason));
          this.assertInvariants();
          this.drain();
        };
        signal.addEventListener('abort', waiter.onAbort, { once: true });
      }
      this.waiters.push(waiter);
      this.emit({
        type: 'spool_wait_start',
        kind: 'spool',
        bytes,
        priority,
        queueDepth: this.waiters.length,
      });
      this.assertInvariants();
      this.drain();
    });
  }

  private drain(): void {
    if (this.closedError) return;
    if (this.draining) {
      this.drainRequested = true;
      return;
    }
    this.draining = true;
    this.drainRequested = false;
    void this.drainLoop()
      .catch((error: unknown) => {
        this.close(classifySpoolFileError(error, 'checking spool capacity'));
      })
      .finally(() => {
        this.draining = false;
        if (this.drainRequested) this.drain();
      });
  }

  private async drainLoop(): Promise<void> {
    while (!this.closedError) {
      const waiter = this.waiters[0];
      if (!waiter) return;
      const reservation = waiter.reservation;
      if (reservation && !reservation.active) {
        this.shiftWaiter(waiter);
        this.emitWaitEnd(waiter, 'closed');
        waiter.reject(
          new UsenetSpoolError(
            'USENET_SPOOL_CLOSED',
            'Cannot grow a released spool reservation'
          )
        );
        continue;
      }
      if (
        reservation &&
        waiter.bytes > this.maxBytes - reservation.reservedBytes
      ) {
        this.shiftWaiter(waiter);
        this.emitWaitEnd(waiter, 'capacity_rejected');
        waiter.reject(this.capacityError(waiter.bytes));
        continue;
      }
      if (waiter.bytes > this.maxBytes - this.reservedBytes) return;

      try {
        await this.assertFreeDisk(waiter.bytes);
      } catch (error) {
        if (this.waiters[0] !== waiter) continue;
        this.shiftWaiter(waiter);
        this.emitWaitEnd(waiter, 'disk_rejected');
        waiter.reject(
          error instanceof UsenetSpoolError
            ? error
            : classifySpoolFileError(error, 'checking free spool space')
        );
        continue;
      }
      if (this.closedError || this.waiters[0] !== waiter) continue;
      if (waiter.bytes > this.maxBytes - this.reservedBytes) continue;

      this.shiftWaiter(waiter);
      this.reservedBytes += waiter.bytes;
      this.peakReservedBytes = Math.max(
        this.peakReservedBytes,
        this.reservedBytes
      );
      const lease =
        reservation ?? this.createReservation(waiter.bytes, waiter.priority);
      if (reservation) reservation.addReserved(waiter.bytes);
      this.assertInvariants();
      this.emitWaitEnd(waiter, 'granted');
      waiter.resolve(lease);
    }
  }

  private async assertFreeDisk(requestedBytes: number): Promise<void> {
    let stats: SpoolStatFs;
    try {
      stats = await this.statfs();
    } catch (error) {
      throw classifySpoolFileError(error, 'checking free spool space');
    }
    const freeBytes = availableBytes(stats);
    const unwrittenReservation = this.reservedBytes - this.actualBytes;
    const requiredBeforeMargin = unwrittenReservation + requestedBytes;
    if (
      requiredBeforeMargin > freeBytes ||
      this.minFreeDiskBytes > freeBytes - requiredBeforeMargin
    ) {
      this.emit({
        type: 'disk_safety_warning',
        requestedBytes,
        freeBytes,
        requiredBytes: requiredBeforeMargin,
        minFreeDiskBytes: this.minFreeDiskBytes,
      });
      throw new UsenetSpoolError(
        'USENET_SPOOL_DISK_FULL',
        'Spool reservation would violate the configured free-disk margin'
      );
    }
  }

  private shiftWaiter(waiter: BudgetWaiter): void {
    const shifted = this.waiters.shift();
    assert.equal(shifted, waiter, 'spool-budget grants must remain FIFO');
    this.removeAbortListener(waiter);
    this.assertInvariants();
  }

  private removeAbortListener(waiter: BudgetWaiter): void {
    if (waiter.signal && waiter.onAbort) {
      waiter.signal.removeEventListener('abort', waiter.onAbort);
      waiter.onAbort = undefined;
    }
  }

  private emitWaitEnd(
    waiter: BudgetWaiter,
    outcome:
      | 'granted'
      | 'aborted'
      | 'closed'
      | 'capacity_rejected'
      | 'disk_rejected'
  ): void {
    this.emit({
      type: 'spool_wait_end',
      kind: 'spool',
      bytes: waiter.bytes,
      priority: waiter.priority,
      queueDepth: this.waiters.length,
      waitMs: Math.max(0, this.clock() - waiter.enqueuedAt),
      outcome,
    });
  }

  private emit(event: Parameters<UsenetResourceEventObserver>[0]): void {
    try {
      this.onEvent?.(event);
    } catch {
      // Observability must never affect resource ownership.
    }
  }

  private recordWritten(reservation: SpoolReservation, bytes: number): void {
    reservation.assertActive();
    this.validateBytes(bytes);
    if (bytes > reservation.reservedBytes - reservation.writtenBytes) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CAPACITY',
        'Committed spool bytes exceed their reservation'
      );
    }
    reservation.addWritten(bytes);
    this.actualBytes += bytes;
    this.peakActualBytes = Math.max(this.peakActualBytes, this.actualBytes);
    this.assertInvariants();
  }

  private createReservation(
    initialBytes: number,
    priority: CommandPriority
  ): SpoolReservation {
    let reservation: SpoolReservation;
    reservation = new SpoolReservation(
      initialBytes,
      (bytes, signal) => this.grow(reservation, bytes, signal, priority),
      (bytes) => this.recordWritten(reservation, bytes),
      () => this.release(reservation)
    );
    return reservation;
  }

  private release(reservation: SpoolReservation): void {
    if (!reservation.active) return;
    reservation.deactivate();
    for (let index = this.waiters.length - 1; index >= 0; index--) {
      const waiter = this.waiters[index];
      if (waiter.reservation !== reservation) continue;
      this.waiters.splice(index, 1);
      this.removeAbortListener(waiter);
      this.emitWaitEnd(waiter, 'closed');
      waiter.reject(
        new UsenetSpoolError(
          'USENET_SPOOL_CLOSED',
          'Spool reservation was released while growth was pending'
        )
      );
    }
    this.reservedBytes -= reservation.reservedBytes;
    this.actualBytes -= reservation.writtenBytes;
    this.assertInvariants();
    this.drain();
  }

  private validateBytes(bytes: number): void {
    if (!isPositiveSafeInteger(bytes)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Spool byte counts must be safe positive integers'
      );
    }
  }

  private assertOpen(): void {
    if (this.closedError) throw this.closedError;
  }

  private capacityError(bytes: number): UsenetSpoolError {
    return new UsenetSpoolError(
      'USENET_SPOOL_CAPACITY',
      `Cannot reserve ${bytes} additional bytes within the spool budget`
    );
  }

  private assertInvariants(): void {
    assert(Number.isSafeInteger(this.reservedBytes));
    assert(Number.isSafeInteger(this.actualBytes));
    assert(this.actualBytes >= 0);
    assert(this.reservedBytes >= this.actualBytes);
    assert(this.reservedBytes <= this.maxBytes);
    assert(this.waiters.length <= this.maxWaiters);
  }
}

class SpoolReservation implements SpoolBudgetLease {
  private reserved = 0;
  private written = 0;
  active = true;

  constructor(
    initialBytes: number,
    private readonly growReservation: (
      bytes: number,
      signal?: AbortSignal
    ) => Promise<void>,
    private readonly recordReservationWrite: (bytes: number) => void,
    private readonly releaseReservation: () => void
  ) {
    this.reserved = initialBytes;
  }

  get reservedBytes(): number {
    return this.reserved;
  }

  get writtenBytes(): number {
    return this.written;
  }

  grow(
    bytes: number,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<void> {
    return this.growReservation(bytes, options.signal);
  }

  recordWritten(bytes: number): void {
    this.recordReservationWrite(bytes);
  }

  release(): void {
    this.releaseReservation();
  }

  assertActive(): void {
    if (!this.active) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Spool reservation has already been released'
      );
    }
  }

  addReserved(bytes: number): void {
    this.reserved += bytes;
  }

  addWritten(bytes: number): void {
    this.written += bytes;
  }

  deactivate(): void {
    this.active = false;
  }
}
