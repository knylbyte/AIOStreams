import { createLogger } from '../../logging/logger.js';
import { ConnectionOptions, NntpConnection } from './connection.js';
import { NntpError } from './errors.js';
import { YencDecodeError } from '../pool/yenc.js';
import { UsenetSpoolError } from '../spool/errors.js';
import {
  CommandPriority,
  ProviderConfig,
  ProviderPoolInfo,
  ProviderState,
} from '../types.js';

const logger = createLogger('usenet/worker-pool');

export interface WorkerPoolOptions extends ConnectionOptions {
  circuitBreakerThreshold: number;
  circuitBreakerCooldownMs: number;
  /** Max in-flight commands per connection (1 = sequential). */
  pipelineDepth: number;
  /**
   * Share (0..1) of contended connection grants reserved for High-priority
   * playback; the rest go to Low background work so it isn't starved. `1` = strict
   * priority.
   */
  streamingPriority: number;
  /** Invoked on every failed dial attempt. */
  onDialError?: (err: unknown) => void;
}

/** Cancel handle for the pool's single periodic maintenance task. */
export interface WorkerPoolScheduledTask {
  cancel(): void;
  unref?(): void;
}

/**
 * Narrow scheduler seam for deterministic keepalive tests. Production uses one
 * native interval; implementations must not schedule overlapping callbacks.
 */
export interface WorkerPoolScheduler {
  every(intervalMs: number, callback: () => void): WorkerPoolScheduledTask;
}

/**
 * One unit of work submitted to a provider: the `run` closure performs the
 * actual `BODY`/`STAT` (and decode) on a ready connection. The worker pool owns
 * only the connection lifecycle (dialing, auth, pipelining, reconnect, throttle)
 * and never touches yEnc/cache/affinity (those stay in {@link MultiProviderPool}).
 */
export interface WorkResult<T = unknown> {
  value: T;
  bytes: number;
  /** Transfer-only wall-clock (excludes queue/connect wait), for throughput stats. */
  durationMs: number;
}

export interface WorkRequest<T = unknown> {
  priority: CommandPriority;
  run: (
    conn: NntpConnection,
    execution: WorkExecution
  ) => Promise<{ value: T; bytes: number }>;
  resolve: (r: WorkResult<T>) => void;
  reject: (err: unknown) => void;
  /**
   * The request performs bounded local preparation after assignment and marks
   * the provider transfer only when its command has entered the connection
   * FIFO. Queue/connect/preparation time is excluded from throughput metrics.
   */
  explicitTransferStart?: boolean;
  /**
   * Cancels while queued or assigned pre-wire; once its provider command has
   * entered the connection FIFO, the transfer completes normally.
   */
  signal?: AbortSignal;
}

interface QueuedWorkRequest<T = unknown> extends WorkRequest<T> {
  /** Installed while assigned but before the provider command is enqueued. */
  cancelPreWire?: (error: NntpError) => boolean;
}

/** A single connection slot owned by the pool (lazily dialed). */
interface Slot {
  conn: NntpConnection | null;
  /** A dial is in progress for this slot. */
  connecting: boolean;
  /** Consecutive failures on this slot's connection (per-connection breaker). */
  failures: number;
  /**
   * Requests dequeued onto this connection, including local preparation and
   * commands already present in `conn.inFlight`. Together with `maintenance`
   * this is the sole logical pipeline occupancy; `conn.inFlight` must never be
   * added to either counter.
   */
  assigned: number;
  /** Active provider-maintenance commands occupying this connection's FIFO. */
  maintenance: number;
  /** Idempotent release for the one possible active maintenance command. */
  releaseMaintenance?: () => void;
  /**
   * Assignment-order fence for commands not yet appended to the FIFO. Its
   * length is hard-bounded by this slot's `pipelineDepth`.
   */
  commandTurns: CommandTurn[];
}

/** Per-assignment timing control supplied to work closures. */
export interface WorkExecution {
  /**
   * Wait until every earlier assignment has either enqueued its command or
   * failed pre-wire. This preserves command FIFO when preparations complete
   * out of order.
   */
  waitForCommandTurn(signal?: AbortSignal): Promise<void>;
  /** Idempotently mark the instant the provider command entered the FIFO. */
  markTransferStarted(): void;
}

interface CommandTurn {
  ready: boolean;
  completed: boolean;
  failure?: Error;
  start?: () => void;
  cancel?: (error: NntpError) => boolean;
  resolve?: () => void;
  reject?: (error: Error) => void;
  signal?: AbortSignal;
  onAbort?: () => void;
}

/** Additive-increase step interval for the adaptive connection-limit throttle. */
const THROTTLE_STEP_MS = 5_000;
/** Keepalive DATE interval on otherwise-idle warm connections. */
const KEEPALIVE_MS = 30_000;
/** Backoff after a transient dial/connection failure before re-dialing. */
const DIAL_BACKOFF_MS = 1_000;
/**
 * Auth-recovery cooldown for a provider that authenticated successfully before
 * and then started failing auth
 */
const AUTH_RECOVERY_BASE_MS = 60_000;
const AUTH_RECOVERY_FACTOR = 5;
const AUTH_RECOVERY_CAP_MS = 5 * 60_000;

const SYSTEM_WORKER_POOL_SCHEDULER: WorkerPoolScheduler = {
  every(intervalMs, callback) {
    const timer = setInterval(callback, intervalMs);
    return {
      cancel: () => clearInterval(timer),
      unref: () => timer.unref?.(),
    };
  },
};

/**
 * Per-provider pool of long-lived **worker connections** that pull from priority
 * and normal request queues (rather than callers leasing a connection per request).
 * Connections persist and are reused across requests (no lease churn), keep
 * warm via keepalive, pipeline up to `pipelineDepth` commands each, reconnect
 * with backoff, and throttle their own count on a "too many connections" reply.
 */
export class ProviderWorkerPool {
  private slots: Slot[];
  private prioQ: QueuedWorkRequest[] = [];
  private normalQ: QueuedWorkRequest[] = [];
  private state: ProviderState;
  private closed = false;

  /** Adaptive connection ceiling (≤ maxConnections); lowered on a limit hit. */
  private allowed: number;
  private throttleTimer?: ReturnType<typeof setInterval>;
  private keepaliveTask?: WorkerPoolScheduledTask;
  private trippedUntil = 0;

  private lastDialOkAt = 0;
  private lastDialError?: { at: number; kind: string; message: string };

  /** A successful authenticated dial has happened at least once. */
  private everOnline = false;
  /**
   * Epoch ms when an `auth_failed` provider may attempt a recovery probe. `0`
   * means a terminal latch (never authenticated = treat as bad creds). Set to a
   * future time only once {@link everOnline} - a transient post-online auth fail.
   */
  private authClearAt = 0;
  /** Current auth-recovery backoff (grows per consecutive failure). */
  private authBackoffMs = 0;

  /** EWMAs used by the multi-pool for provider ordering. */
  private serviceTimeEwmaMs = 0;
  private missRateEwma = 0;
  /** Measured per-fetch throughput (bytes/ms), 0 until first sampled. */
  private throughputEwma = 0;

  /** Per-100 odds a contended pull serves Low (0 = strict priority). */
  private readonly lowOdds: number;
  private lowAcc = 0;

  constructor(
    readonly config: ProviderConfig,
    private opts: WorkerPoolOptions,
    scheduler: WorkerPoolScheduler = SYSTEM_WORKER_POOL_SCHEDULER
  ) {
    const clamped = Math.min(1, Math.max(0, opts.streamingPriority));
    this.lowOdds = Math.round((1 - clamped) * 100);
    const max = Math.max(1, config.maxConnections);
    this.allowed = max;
    this.slots = Array.from({ length: max }, () => ({
      conn: null,
      connecting: false,
      failures: 0,
      assigned: 0,
      maintenance: 0,
      commandTurns: [],
    }));
    this.state = config.enabled === false ? 'disabled' : 'online';
    this.keepaliveTask = scheduler.every(KEEPALIVE_MS, () => this.keepalive());
    this.keepaliveTask.unref?.();
  }

  get id(): string {
    return this.config.id;
  }
  /** Human-friendly label for logs: display name, falling back to the stable id. */
  get label(): string {
    return this.config.name ?? this.config.id;
  }
  get isBackup(): boolean {
    return !!this.config.isBackup;
  }
  get depth(): number {
    return Math.max(1, this.opts.pipelineDepth);
  }

  get tripped(): boolean {
    if (this.state === 'auth_failed' || this.state === 'disabled') return true;
    return this.trippedUntil > Date.now();
  }
  get throttled(): boolean {
    return this.allowed < Math.max(1, this.config.maxConnections);
  }

  /** Total pipeline slots free right now (used for least-busy provider ordering). */
  get freeSlots(): number {
    if (this.state !== 'online') return 0;
    return Math.max(0, this.allowed * this.depth - this.occupancyTotal());
  }

  get inFlight(): number {
    return this.occupancyTotal();
  }
  /**
   * Mean time to complete a whole article fetch
   */
  get avgServiceTimeMs(): number {
    return this.serviceTimeEwmaMs;
  }
  get missRate(): number {
    return this.missRateEwma;
  }
  get throughput(): number {
    return this.throughputEwma;
  }

  recordServiceTime(ms: number): void {
    this.serviceTimeEwmaMs =
      this.serviceTimeEwmaMs === 0
        ? ms
        : this.serviceTimeEwmaMs * 0.8 + ms * 0.2;
  }
  recordOutcome(missing: boolean): void {
    this.missRateEwma = this.missRateEwma * 0.8 + (missing ? 1 : 0) * 0.2;
  }
  recordThroughput(bytes: number, ms: number): void {
    if (ms <= 0 || bytes <= 0) return;
    const rate = bytes / ms;
    this.throughputEwma =
      this.throughputEwma === 0 ? rate : this.throughputEwma * 0.8 + rate * 0.2;
  }

  /**
   * Submit one request; resolves/rejects via its own callbacks. Rejects with
   * `auth_failed`/`no_providers` immediately when the provider is unusable so the
   * multi-pool fails over (it never claims "article missing").
   */
  submit<T>(
    req: Omit<WorkRequest<T>, 'resolve' | 'reject'>
  ): Promise<WorkResult<T>> {
    return new Promise((resolve, reject) => {
      if (this.closed || this.state === 'disabled') {
        reject(
          new NntpError('no_providers', 'provider unavailable', {
            provider: this.label,
          })
        );
        return;
      }
      if (this.authBlocked()) {
        reject(
          new NntpError('auth_failed', 'provider authentication failed', {
            provider: this.label,
          })
        );
        return;
      }
      if (req.signal?.aborted) {
        reject(
          new NntpError('connection', 'aborted', { provider: this.label })
        );
        return;
      }
      const full: QueuedWorkRequest<T> = {
        ...req,
        resolve,
        reject,
      };
      if (req.signal) {
        const signal = req.signal;
        const onAbort = (): void => {
          const abortError = new NntpError('connection', 'aborted', {
            provider: this.label,
          });
          for (const q of [this.prioQ, this.normalQ]) {
            const i = q.indexOf(full as QueuedWorkRequest);
            if (i === -1) continue;
            q.splice(i, 1);
            full.reject(abortError);
            return;
          }
          full.cancelPreWire?.(abortError);
        };
        signal.addEventListener('abort', onAbort);
        // Detach on settle: stream-lifetime signals outlive many requests, and
        // leftover listeners accumulate for the whole stream otherwise.
        const detach = (): void => signal.removeEventListener('abort', onAbort);
        full.resolve = (r) => {
          detach();
          resolve(r);
        };
        full.reject = (err) => {
          detach();
          reject(err);
        };
      }
      (req.priority === CommandPriority.High ? this.prioQ : this.normalQ).push(
        full as QueuedWorkRequest
      );
      this.dispatch();
    });
  }

  /**
   * Submit work whose provider-attempt storage must be prepared asynchronously.
   * The existing priority queue assigns and counts a pipeline slot before
   * `prepare` starts, preserving FIFO and `pipelineDepth`. Provider service
   * timing starts only when `run` calls `markTransferStarted` after command
   * enqueue. A prepared attempt is disposed exactly once on every failure or
   * pre-wire abort; success transfers its ownership through the result.
   */
  submitPrepared<T, Prepared>(req: {
    priority: CommandPriority;
    prepare: () => Promise<Prepared>;
    run: (
      conn: NntpConnection,
      prepared: Prepared,
      markTransferStarted: () => void
    ) => Promise<{ value: T; bytes: number }>;
    dispose: (prepared: Prepared, error: Error) => Promise<void>;
    signal?: AbortSignal;
  }): Promise<WorkResult<T>> {
    return this.submit<T>({
      priority: req.priority,
      signal: req.signal,
      explicitTransferStart: true,
      run: async (conn, execution) => {
        let prepared: { readonly value: Prepared } | undefined;
        try {
          prepared = { value: await req.prepare() };
          if (req.signal?.aborted) {
            throw new NntpError('connection', 'aborted', {
              provider: this.label,
            });
          }
          await execution.waitForCommandTurn(req.signal);
          if (this.closed || req.signal?.aborted) {
            throw new NntpError('connection', 'aborted', {
              provider: this.label,
            });
          }
          const result = await req.run(
            conn,
            prepared.value,
            execution.markTransferStarted
          );
          prepared = undefined;
          return result;
        } catch (error) {
          const failure =
            error instanceof Error
              ? error
              : new NntpError('connection', 'provider request failed', {
                  provider: this.label,
                  cause: error,
                });
          if (prepared) {
            const owned = prepared.value;
            prepared = undefined;
            await req.dispose(owned, failure);
          }
          throw failure;
        }
      },
    });
  }

  private hasWork(): boolean {
    return this.prioQ.length > 0 || this.normalQ.length > 0;
  }

  private openConns(): number {
    let n = 0;
    for (const s of this.slots) if (s.conn || s.connecting) n++;
    return n;
  }

  private slotOccupancy(slot: Slot): number {
    return slot.assigned + slot.maintenance;
  }

  /**
   * Logical provider-pipeline occupancy. Provider assignments and maintenance
   * commands are disjoint owners of slots; `conn.inFlight` is observational
   * only and must never be added to this value.
   */
  private occupancyTotal(): number {
    let n = 0;
    for (const slot of this.slots) n += this.slotOccupancy(slot);
    return n;
  }

  /**
   * Peek the next dispatchable request and dequeue it.
   */
  private pullFor(): QueuedWorkRequest | undefined {
    const fits = (q: QueuedWorkRequest[]): boolean => q.length > 0;
    const hasHigh = fits(this.prioQ);
    const hasLow = fits(this.normalQ);
    // When both classes have compatible work, divert `1 - streamingPriority` of
    // pulls to Low so background work isn't starved by continuous playback.
    if (hasHigh && hasLow && this.lowOdds > 0) {
      this.lowAcc += this.lowOdds;
      if (this.lowAcc >= 100) {
        this.lowAcc -= 100;
        return this.normalQ.shift();
      }
      return this.prioQ.shift();
    }
    if (hasHigh) return this.prioQ.shift();
    if (hasLow) return this.normalQ.shift();
    return undefined;
  }

  /** Assign as much queued work as connections + the pipeline depth allow. */
  private dispatch(): void {
    if (this.closed) return;
    if (this.state === 'disabled') {
      this.failAllQueued(
        new NntpError('no_providers', 'provider disabled', {
          provider: this.label,
        })
      );
      return;
    }
    // While the auth cooldown is in effect (or terminally latched), fail the
    // queue so the multi-pool fails over. Once the cooldown elapses we fall
    // through and the dial loop below fires a single recovery probe.
    if (this.authBlocked()) {
      this.failAllQueued(
        new NntpError('auth_failed', 'provider authentication failed', {
          provider: this.label,
        })
      );
      return;
    }
    // 1) Fill EXISTING usable connections' pipelines with compatible work first.
    for (const slot of this.slots) {
      if (!this.hasWork()) break;
      if (slot.connecting) continue;
      if (!slot.conn || !slot.conn.isUsable) {
        slot.conn = null;
        continue;
      }
      while (
        this.slotOccupancy(slot) < this.depth &&
        slot.conn.canAccept(this.depth)
      ) {
        const req = this.pullFor();
        if (!req) break;
        slot.assigned++;
        this.fireTransfer(slot, req);
      }
    }
    // 2) Open new connections only for demand the current pool can't serve.
    //    Dialing is async (work isn't consumed until a socket is ready), so the
    //    cap must count connections ALREADY connecting/open; otherwise each
    //    re-dispatch dials another full batch and we blast `maxConnections`
    //    sockets at the provider (choking it / tripping its limit). Total
    //    connections are held to ⌈(in-flight + queued)/depth⌉ ≈ the real
    //    concurrency.
    const queued = this.prioQ.length + this.normalQ.length;
    if (queued === 0) return;
    const demand = this.occupancyTotal() + queued;
    const wantConns = Math.min(this.allowed, Math.ceil(demand / this.depth));
    let toDial = wantConns - this.openConns();
    // Recovering from `auth_failed` (cooldown elapsed): dial only a single probe
    // to re-test the credentials.
    if (this.state === 'auth_failed') {
      toDial = Math.max(0, 1 - this.openConns());
    }
    for (const slot of this.slots) {
      if (toDial <= 0) break;
      if (slot.connecting || slot.conn) continue;
      this.beginDial(slot);
      toDial--;
    }
  }

  private beginDial(slot: Slot): void {
    slot.connecting = true;
    NntpConnection.connect(this.config, this.opts).then(
      (conn) => {
        slot.connecting = false;
        if (this.closed) {
          conn.quit();
          return;
        }
        slot.conn = conn;
        slot.failures = 0;
        this.everOnline = true;
        this.lastDialOkAt = Date.now();
        if (this.state !== 'online') {
          logger.info({ provider: this.label }, 'provider back online');
          this.state = 'online';
          // A clean authenticated dial clears any auth-recovery cooldown.
          this.authClearAt = 0;
          this.authBackoffMs = 0;
        }
        this.dispatch();
      },
      (err) => {
        slot.connecting = false;
        this.onDialError(slot, err);
      }
    );
  }

  private onDialError(slot: Slot, err: unknown): void {
    this.lastDialError = {
      at: Date.now(),
      kind: err instanceof NntpError ? err.kind : 'unknown',
      message: err instanceof Error ? err.message : String(err),
    };
    this.opts.onDialError?.(err);
    if (err instanceof NntpError && err.kind === 'auth_failed') {
      const firstTime = this.state !== 'auth_failed';
      this.state = 'auth_failed';
      if (this.everOnline) {
        // Proven-good account: a transient auth glitch, not bad creds. Cool down
        // and let a later request fire a single recovery probe (no busy-dial).
        this.armAuthCooldown();
        if (firstTime) {
          logger.warn(
            { provider: this.label, err, retryInMs: this.authBackoffMs },
            'provider auth failed after being online; cooling down'
          );
        }
      } else {
        // Never authenticated = real bad credentials. Latch terminally (fail
        // fast); recovery comes from a credential re-save rebuilding the engine.
        this.authClearAt = 0;
        if (firstTime) {
          logger.warn(
            { provider: this.label, err },
            'provider authentication failed (check credentials)'
          );
        }
      }
      this.dispatch(); // fails the queue (authBlocked)
      return;
    }
    // A recovery probe that failed for a non-auth reason: re-arm the cooldown so
    // a proven-good-but-currently-unreachable provider doesn't busy-dial.
    if (this.state === 'auth_failed' && this.everOnline) this.armAuthCooldown();
    if (err instanceof NntpError && err.kind === 'connection_limit') {
      this.throttleOnLimit();
      // Re-dispatch after a short backoff (queued work waits behind the gate).
      setTimeout(() => this.dispatch(), DIAL_BACKOFF_MS).unref?.();
      return;
    }
    this.recordConnFailure(slot, err);
    setTimeout(() => this.dispatch(), DIAL_BACKOFF_MS).unref?.();
  }

  private fireTransfer(slot: Slot, req: QueuedWorkRequest): void {
    const conn = slot.conn!;
    const turn: CommandTurn = {
      ready: slot.commandTurns.length === 0,
      completed: false,
    };
    slot.commandTurns.push(turn);
    if (slot.commandTurns.length > this.depth) {
      throw new Error('provider command-turn accounting overflow');
    }
    let started: number | undefined;
    let commandEnqueued = false;
    let assignmentReleased = false;
    const releaseAssignment = (): void => {
      if (assignmentReleased) return;
      assignmentReleased = true;
      this.completeCommandTurn(slot, turn);
      req.cancelPreWire = undefined;
      slot.assigned--;
      if (slot.assigned < 0) {
        throw new Error('provider worker assignment accounting underflow');
      }
    };
    const transfer = Promise.withResolvers<{
      value: unknown;
      bytes: number;
    }>();
    let transferSettled = false;
    const settleTransfer = (
      outcome:
        | {
            readonly ok: true;
            readonly value: { value: unknown; bytes: number };
          }
        | { readonly ok: false; readonly error: unknown }
    ): void => {
      if (transferSettled) return;
      transferSettled = true;
      req.cancelPreWire = undefined;
      turn.cancel = undefined;
      if (outcome.ok) transfer.resolve(outcome.value);
      else transfer.reject(outcome.error);
    };
    const cancelPreWire = (error: NntpError): boolean => {
      if (commandEnqueued || transferSettled) return false;
      turn.start = undefined;
      this.completeCommandTurn(slot, turn, error);
      settleTransfer({ ok: false, error });
      return true;
    };
    req.cancelPreWire = cancelPreWire;
    turn.cancel = cancelPreWire;
    const execution: WorkExecution = {
      waitForCommandTurn: (signal) => this.waitForCommandTurn(turn, signal),
      markTransferStarted: () => {
        if (commandEnqueued || transferSettled) return;
        commandEnqueued = true;
        req.cancelPreWire = undefined;
        turn.cancel = undefined;
        started ??= Date.now();
        this.completeCommandTurn(slot, turn);
      },
    };
    const invoke = (): void => {
      turn.start = undefined;
      if (transferSettled) return;
      if (this.closed) {
        cancelPreWire(
          new NntpError('no_providers', 'pool closed', {
            provider: this.label,
          })
        );
        return;
      }
      if (req.signal?.aborted) {
        cancelPreWire(
          new NntpError('connection', 'aborted', { provider: this.label })
        );
        return;
      }
      try {
        if (!req.explicitTransferStart) started = Date.now();
        const pending = req.run(conn, execution);
        // Existing buffering/HEAD/STAT work synchronously queues its command
        // before returning its Promise. Prepared work marks from the exact
        // connection enqueue callback instead.
        if (!req.explicitTransferStart) execution.markTransferStarted();
        pending.then(
          (value) => settleTransfer({ ok: true, value }),
          (error: unknown) => settleTransfer({ ok: false, error })
        );
      } catch (error) {
        settleTransfer({ ok: false, error });
      }
    };
    if (req.explicitTransferStart || turn.ready) {
      invoke();
    } else {
      turn.start = invoke;
    }
    transfer.promise.then(
      (res) => {
        const durationMs = started === undefined ? 0 : Date.now() - started;
        releaseAssignment();
        slot.failures = 0;
        // Refresh staleness on real work so purge only reaps genuinely idle
        // connections (touch-at-connect-only redialed active streams every
        // stale interval). Keepalive DATEs deliberately don't touch.
        conn.touch();
        this.recordServiceTime(durationMs);
        if (res.bytes > 0) this.recordThroughput(res.bytes, durationMs);
        this.recordOutcome(false);
        req.resolve({ ...res, durationMs });
        this.dispatch();
      },
      (err) => {
        releaseAssignment();
        this.onTransferError(slot, req, err, started !== undefined);
      }
    );
  }

  private waitForCommandTurn(
    turn: CommandTurn,
    signal: AbortSignal | undefined
  ): Promise<void> {
    if (turn.failure) return Promise.reject(turn.failure);
    if (turn.completed) return Promise.resolve();
    if (signal?.aborted) {
      return Promise.reject(
        new NntpError('connection', 'aborted', { provider: this.label })
      );
    }
    if (turn.ready) return Promise.resolve();
    const deferred = Promise.withResolvers<void>();
    turn.resolve = deferred.resolve;
    turn.reject = deferred.reject;
    if (signal) {
      const onAbort = (): void => {
        if (turn.onAbort !== onAbort) return;
        const error = new NntpError('connection', 'aborted', {
          provider: this.label,
        });
        if (turn.cancel?.(error)) return;
        this.clearCommandTurnWaiter(turn);
        deferred.reject(error);
      };
      turn.signal = signal;
      turn.onAbort = onAbort;
      signal.addEventListener('abort', onAbort, { once: true });
    }
    return deferred.promise;
  }

  private clearCommandTurnWaiter(turn: CommandTurn): void {
    if (turn.signal && turn.onAbort) {
      turn.signal.removeEventListener('abort', turn.onAbort);
    }
    turn.signal = undefined;
    turn.onAbort = undefined;
    turn.resolve = undefined;
    turn.reject = undefined;
  }

  /** Remove one bounded assignment-order turn and advance its successor. */
  private completeCommandTurn(
    slot: Slot,
    turn: CommandTurn,
    failure?: Error
  ): void {
    if (turn.completed) return;
    turn.completed = true;
    turn.failure = failure;
    turn.start = undefined;
    turn.cancel = undefined;
    const reject = turn.reject;
    this.clearCommandTurnWaiter(turn);
    const index = slot.commandTurns.indexOf(turn);
    if (index === -1) return;
    slot.commandTurns.splice(index, 1);
    if (failure) reject?.(failure);
    if (index !== 0) return;
    const next = slot.commandTurns[0];
    if (!next || next.ready) return;
    next.ready = true;
    const resolve = next.resolve;
    this.clearCommandTurnWaiter(next);
    resolve?.();
    next.start?.();
  }

  private onTransferError(
    slot: Slot,
    req: WorkRequest,
    err: unknown,
    transferStarted: boolean
  ): void {
    if (this.closed) {
      req.reject(err);
      return;
    }
    // Content-level outcomes leave the connection healthy.
    if (err instanceof NntpError && err.kind === 'article_not_found') {
      this.recordOutcome(true);
      req.reject(err);
      this.dispatch();
      return;
    }
    if (err instanceof YencDecodeError) {
      req.reject(err);
      this.dispatch();
      return;
    }
    // Spool capacity/I/O/memory failures are local sink failures, not evidence
    // that the provider or its circuit is unhealthy.
    if (err instanceof UsenetSpoolError) {
      if (slot.conn && !slot.conn.isUsable) slot.conn = null;
      req.reject(err);
      this.dispatch();
      return;
    }
    // A caller abort before command enqueue is local cancellation for direct
    // and prepared work alike. It leaves a usable provider connection healthy.
    if (
      !transferStarted &&
      err instanceof NntpError &&
      err.kind === 'connection' &&
      err.message === 'aborted' &&
      slot.conn?.isUsable
    ) {
      req.reject(err);
      this.dispatch();
      return;
    }
    // Other local preparation/turn failures also leave a usable provider
    // connection untouched and must not contribute to its circuit breaker.
    if (req.explicitTransferStart && !transferStarted && slot.conn?.isUsable) {
      req.reject(err);
      this.dispatch();
      return;
    }
    // Connection-level failure: the connection has likely destroyed itself.
    if (err instanceof NntpError && err.kind === 'connection_limit') {
      this.throttleOnLimit();
    } else {
      this.recordConnFailure(slot, err);
    }
    if (slot.conn && !slot.conn.isUsable) slot.conn = null;
    req.reject(err);
    this.dispatch();
  }

  // ---- connection-limit throttle (adaptive ceiling, AIMD recovery) ----------

  private throttleOnLimit(): void {
    const target = Math.max(1, this.openConns());
    const wasThrottled = this.throttled;
    this.allowed = Math.min(this.allowed, Math.max(1, target));
    if (!wasThrottled) {
      logger.debug(
        { provider: this.label, throttledTo: this.allowed },
        'provider connection limit hit; throttling connection ceiling'
      );
    }
    if (!this.throttleTimer) {
      this.throttleTimer = setInterval(() => {
        if (this.closed || !this.throttled) {
          if (this.throttleTimer) clearInterval(this.throttleTimer);
          this.throttleTimer = undefined;
          return;
        }
        this.allowed = Math.min(
          Math.max(1, this.config.maxConnections),
          this.allowed + 1
        );
        this.dispatch();
      }, THROTTLE_STEP_MS);
      this.throttleTimer.unref?.();
    }
  }

  /**
   * Whether the pool must reject auth-bound work synchronously right now: while
   * `auth_failed` and either terminally latched (never authenticated = bad creds,
   * `authClearAt === 0`) or still inside the recovery cooldown. False once the
   * cooldown elapses, so the next dispatch can fire a single recovery probe.
   */
  private authBlocked(): boolean {
    if (this.state !== 'auth_failed') return false;
    if (this.authClearAt === 0) return true; // never authenticated = terminal
    return Date.now() < this.authClearAt; // proven-good = blocked during cooldown
  }

  /** Grow the auth-recovery backoff and arm the next probe time. */
  private armAuthCooldown(): void {
    this.authBackoffMs = Math.min(
      AUTH_RECOVERY_CAP_MS,
      this.authBackoffMs
        ? this.authBackoffMs * AUTH_RECOVERY_FACTOR
        : AUTH_RECOVERY_BASE_MS
    );
    this.authClearAt = Date.now() + this.authBackoffMs;
  }

  private recordConnFailure(slot: Slot, err: unknown): void {
    if (this.closed) return;
    slot.failures++;
    if (slot.failures >= this.opts.circuitBreakerThreshold) {
      const wasTripped = this.trippedUntil > Date.now();
      this.trippedUntil = Date.now() + this.opts.circuitBreakerCooldownMs;
      if (!wasTripped) {
        logger.warn(
          {
            provider: this.label,
            failures: slot.failures,
            err,
          },
          'provider circuit breaker tripped'
        );
      }
      // Fail queued work with a transient error so callers fail over to the
      // next provider
      this.failAllQueued(
        new NntpError('connection', 'provider circuit breaker tripped', {
          provider: this.label,
        })
      );
    }
  }

  private failAllQueued(err: NntpError): void {
    const all = [...this.prioQ, ...this.normalQ];
    this.prioQ = [];
    this.normalQ = [];
    for (const req of all) req.reject(err);
  }

  /** Periodic DATE on idle warm connections so the server doesn't reap them. */
  private keepalive(): void {
    if (this.closed) return;
    if (this.hasWork()) this.dispatch();
    for (const slot of this.slots) {
      const conn = slot.conn;
      if (!conn || !conn.isUsable || this.slotOccupancy(slot) > 0) continue;
      slot.maintenance++;
      if (this.slotOccupancy(slot) > this.depth) {
        throw new Error('provider maintenance accounting overflow');
      }
      let released = false;
      const releaseMaintenance = (): void => {
        if (released) return;
        released = true;
        if (slot.releaseMaintenance === releaseMaintenance) {
          slot.releaseMaintenance = undefined;
        }
        slot.maintenance--;
        if (slot.maintenance < 0) {
          throw new Error('provider maintenance accounting underflow');
        }
      };
      slot.releaseMaintenance = releaseMaintenance;
      conn
        .date(undefined, this.opts.idleConnectionMs)
        .catch(() => {
          conn.destroy();
          if (slot.conn === conn) slot.conn = null;
        })
        .finally(() => {
          releaseMaintenance();
          this.dispatch();
        });
    }
  }

  /** Close connections idle past their stale deadline (called periodically). */
  purgeStaleIdles(): void {
    for (const slot of this.slots) {
      const conn = slot.conn;
      if (
        conn &&
        this.slotOccupancy(slot) === 0 &&
        (conn.isStale() || !conn.isUsable)
      ) {
        conn.quit();
        slot.conn = null;
      }
    }
    // Backstop for any missed-dispatch edge: queued work must never outlive a
    // purge interval without a dispatch attempt.
    if (this.hasWork()) this.dispatch();
  }

  info(): ProviderPoolInfo {
    let total = 0;
    let acquired = 0;
    for (const s of this.slots) {
      if (s.conn || s.connecting) total++;
      if (this.slotOccupancy(s) > 0) acquired++;
    }
    return {
      id: this.config.id,
      name: this.config.name,
      state: this.tripped && this.state === 'online' ? 'offline' : this.state,
      total,
      idle: Math.max(0, total - acquired),
      acquired,
      available: Math.max(0, this.allowed - total),
      max: this.config.maxConnections,
      tripped: this.tripped,
      throttled: this.throttled,
      isBackup: this.isBackup,
      freeSlots: this.freeSlots,
      throughput: Math.round(this.throughputEwma * this.depth * 1000),
      queued: this.prioQ.length + this.normalQ.length,
      lastDialOkAt: this.lastDialOkAt || undefined,
      lastDialError: this.lastDialError,
    };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.throttleTimer) clearInterval(this.throttleTimer);
    this.keepaliveTask?.cancel();
    this.throttleTimer = undefined;
    this.keepaliveTask = undefined;
    const closeError = new NntpError('no_providers', 'pool closed', {
      provider: this.label,
    });
    for (const slot of this.slots) {
      // Reverse order prevents completing an earlier turn from starting a later
      // one while shutdown is cancelling the bounded pre-wire set.
      for (const turn of [...slot.commandTurns].reverse()) {
        turn.cancel?.(closeError);
      }
      slot.releaseMaintenance?.();
      if (slot.conn) slot.conn.quit();
      slot.conn = null;
    }
    this.failAllQueued(closeError);
  }
}
