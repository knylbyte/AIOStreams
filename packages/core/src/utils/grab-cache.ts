import { DiskBackedCache } from './disk-backed-cache.js';
import { getCacheFolder } from './general.js';

/** Where every grab cache (NZB bodies, torrent metadata, …) lives on disk. */
export const GRAB_CACHE_DIR = (): string => getCacheFolder();

const DEFAULT_MAX_GRAB_FLIGHTS = 256;
const DEFAULT_MAX_GRAB_WAITERS_PER_KEY = 64;

export interface GrabCacheOptions<V> {
  /** Namespace (subdirectory + dashboard label). Must be filesystem-safe. */
  name: string;
  /** Base directory; defaults to the shared cache root. */
  dir?: string;
  /** L1 (in-memory) byte budget. */
  maxMemBytes: number;
  /** L2 (on-disk) byte budget. */
  maxDiskBytes: number;
  serialize: (value: V) => Buffer;
  deserialize: (buf: Buffer) => V;
  sizeOf: (value: V) => number;
  /** Process-wide producer bound. */
  maxFlights?: number;
  /** Per-key request waiter bound. */
  maxWaitersPerKey?: number;
}

export interface GrabFetchOptions {
  /** Request-local cancellation; never owns the shared producer. */
  readonly signal?: AbortSignal;
}

export type GrabCacheErrorCode =
  | 'GRAB_CACHE_CLOSED'
  | 'GRAB_CACHE_FLIGHT_CAPACITY'
  | 'GRAB_CACHE_WAITER_CAPACITY'
  | 'GRAB_CACHE_INVALID_LIMIT';

/** Stable lifecycle/capacity failure for the bounded shared-grab owner. */
export class GrabCacheError extends Error {
  constructor(readonly code: GrabCacheErrorCode) {
    super(
      code === 'GRAB_CACHE_CLOSED'
        ? 'grab cache is closed'
        : code === 'GRAB_CACHE_FLIGHT_CAPACITY'
          ? 'too many grab producers are active'
          : code === 'GRAB_CACHE_WAITER_CAPACITY'
            ? 'too many requests are waiting for this grab'
            : 'grab cache limits must be safe positive integers'
    );
    this.name = 'GrabCacheError';
  }
}

interface GrabWaiter<V> {
  readonly resolve: (value: V) => void;
  readonly reject: (error: unknown) => void;
  readonly signal?: AbortSignal;
  onAbort?: () => void;
  settled: boolean;
}

type GrabOutcome<V> =
  | { readonly ok: true; readonly value: V }
  | { readonly ok: false; readonly error: unknown };

interface GrabFlight<V> {
  readonly controller: AbortController;
  readonly waiters: Set<GrabWaiter<V>>;
  readonly result: Promise<V>;
  readonly resolveResult: (value: V) => void;
  readonly rejectResult: (error: unknown) => void;
  task: Promise<void>;
  outcome?: GrabOutcome<V>;
}

function abortReason(signal: AbortSignal): unknown {
  return (
    signal.reason ?? new DOMException('The operation was aborted', 'AbortError')
  );
}

/**
 * Bounded, closeable, disk-backed shared-grab owner.
 *
 * Each key owns one producer and one owner AbortSignal. Request signals only
 * own their waiters, so one disconnected client cannot cancel work needed by
 * another. `close()` fences new cache hits synchronously, rejects all waiters,
 * aborts every owner exactly once, awaits producer finalizers, and finally
 * closes the backing cache so no write can cross the persistence barrier.
 */
export class GrabCache<V> {
  private readonly cache: DiskBackedCache<V>;
  private readonly inflight = new Map<string, GrabFlight<V>>();
  private readonly maxFlights: number;
  private readonly maxWaitersPerKey: number;
  private closedError: Error | undefined;
  private closePromise: Promise<void> | undefined;

  constructor(opts: GrabCacheOptions<V>) {
    this.maxFlights = opts.maxFlights ?? DEFAULT_MAX_GRAB_FLIGHTS;
    this.maxWaitersPerKey =
      opts.maxWaitersPerKey ?? DEFAULT_MAX_GRAB_WAITERS_PER_KEY;
    if (
      !Number.isSafeInteger(this.maxFlights) ||
      this.maxFlights <= 0 ||
      !Number.isSafeInteger(this.maxWaitersPerKey) ||
      this.maxWaitersPerKey <= 0
    ) {
      throw new GrabCacheError('GRAB_CACHE_INVALID_LIMIT');
    }
    this.cache = new DiskBackedCache<V>({
      name: opts.name,
      dir: opts.dir ?? GRAB_CACHE_DIR(),
      maxMemBytes: opts.maxMemBytes,
      maxDiskBytes: opts.maxDiskBytes,
      serialize: opts.serialize,
      deserialize: opts.deserialize,
      sizeOf: opts.sizeOf,
    });
  }

  get activeFlights(): number {
    return this.inflight.size;
  }

  get waitingRequests(): number {
    let waiting = 0;
    for (const flight of this.inflight.values()) waiting += flight.waiters.size;
    return waiting;
  }

  /** Cached value for `key` (L1→L2), or `undefined` on a miss. */
  async cached(key: string): Promise<V | undefined> {
    if (this.closedError) throw this.closedError;
    const value = await this.cache.getAsync(key);
    if (this.closedError) throw this.closedError;
    return value;
  }

  /** Existing owner result for compatibility with lazy torrent callers. */
  inFlight(key: string): Promise<V> | undefined {
    return this.inflight.get(key)?.result;
  }

  /** Return a cached value or join/start one owner-controlled producer. */
  async fetch(
    key: string,
    produce: (signal: AbortSignal) => Promise<V>,
    options: GrabFetchOptions = {}
  ): Promise<V> {
    if (options.signal?.aborted) throw abortReason(options.signal);
    if (this.closedError) throw this.closedError;

    let flight = this.inflight.get(key);
    if (!flight) {
      if (this.inflight.size >= this.maxFlights) {
        throw new GrabCacheError('GRAB_CACHE_FLIGHT_CAPACITY');
      }
      flight = this.createFlight(key, produce);
    }
    if (flight.waiters.size >= this.maxWaitersPerKey) {
      throw new GrabCacheError('GRAB_CACHE_WAITER_CAPACITY');
    }
    return this.addWaiter(flight, options.signal);
  }

  /** Idempotent process-lifecycle barrier for producers and cache writers. */
  close(error: Error = new GrabCacheError('GRAB_CACHE_CLOSED')): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closedError = error;
    const flights = [...this.inflight.values()];
    for (const flight of flights) {
      this.settleFlight(flight, { ok: false, error });
      if (!flight.controller.signal.aborted) flight.controller.abort(error);
    }
    this.closePromise = Promise.all(flights.map((flight) => flight.task))
      .then(() => this.cache.close())
      .then(() => undefined);
    return this.closePromise;
  }

  private createFlight(
    key: string,
    produce: (signal: AbortSignal) => Promise<V>
  ): GrabFlight<V> {
    const result = Promise.withResolvers<V>();
    const flight: GrabFlight<V> = {
      controller: new AbortController(),
      waiters: new Set(),
      result: result.promise,
      resolveResult: result.resolve,
      rejectResult: result.reject,
      task: Promise.resolve(),
    };
    this.inflight.set(key, flight);
    const operation = Promise.resolve().then(async () => {
      flight.controller.signal.throwIfAborted();
      const hit = await this.cache.getAsync(key);
      flight.controller.signal.throwIfAborted();
      if (this.closedError) throw this.closedError;
      if (hit !== undefined) return hit;
      const value = await produce(flight.controller.signal);
      flight.controller.signal.throwIfAborted();
      if (this.closedError) throw this.closedError;
      this.cache.set(key, value);
      if (this.closedError) throw this.closedError;
      return value;
    });
    flight.task = operation
      .then(
        (value) => this.settleFlight(flight, { ok: true, value }),
        (error: unknown) => this.settleFlight(flight, { ok: false, error })
      )
      .finally(() => {
        if (this.inflight.get(key) === flight) this.inflight.delete(key);
      });
    void result.promise.catch(() => undefined);
    void flight.task.catch(() => undefined);
    return flight;
  }

  private addWaiter(
    flight: GrabFlight<V>,
    signal: AbortSignal | undefined
  ): Promise<V> {
    if (flight.outcome) {
      return flight.outcome.ok
        ? Promise.resolve(flight.outcome.value)
        : Promise.reject(flight.outcome.error);
    }
    return new Promise<V>((resolve, reject) => {
      const waiter: GrabWaiter<V> = {
        resolve,
        reject,
        signal,
        settled: false,
      };
      const onAbort = (): void => {
        if (!signal) return;
        this.settleWaiter(flight, waiter, {
          ok: false,
          error: abortReason(signal),
        });
      };
      if (signal) waiter.onAbort = onAbort;
      flight.waiters.add(waiter);
      if (signal && onAbort) {
        signal.addEventListener('abort', onAbort, { once: true });
        if (signal.aborted) onAbort();
      }
      if (flight.outcome) this.settleWaiter(flight, waiter, flight.outcome);
    });
  }

  private settleFlight(flight: GrabFlight<V>, outcome: GrabOutcome<V>): void {
    if (flight.outcome) return;
    flight.outcome = outcome;
    if (outcome.ok) flight.resolveResult(outcome.value);
    else flight.rejectResult(outcome.error);
    for (const waiter of [...flight.waiters]) {
      this.settleWaiter(flight, waiter, outcome);
    }
  }

  private settleWaiter(
    flight: GrabFlight<V>,
    waiter: GrabWaiter<V>,
    outcome: GrabOutcome<V>
  ): void {
    if (waiter.settled) return;
    waiter.settled = true;
    flight.waiters.delete(waiter);
    if (waiter.signal && waiter.onAbort) {
      waiter.signal.removeEventListener('abort', waiter.onAbort);
    }
    if (outcome.ok) waiter.resolve(outcome.value);
    else waiter.reject(outcome.error);
  }
}
