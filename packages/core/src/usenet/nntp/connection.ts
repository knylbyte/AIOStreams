import net from 'node:net';
import tls from 'node:tls';
import { createLogger } from '../../logging/logger.js';
import type { ProviderConfig } from '../types.js';
import {
  NntpError,
  classifyNntpStatus,
  isConnectionLimitResponse,
} from './errors.js';
import {
  CRLF,
  DOT_TERMINATOR,
  NntpOnreadParser,
  parseStatusLine,
  statusClass,
} from './protocol.js';
import {
  NNTP_READ_CARRY_MAX_BYTES,
  NNTP_READ_CARRY_MAX_CHUNKS,
  NNTP_READ_WINDOW_BYTES,
} from './read-carry.js';

const logger = createLogger('usenet/connection');

export interface ConnectionOptions {
  dialTimeoutMs: number;
  idleConnectionMs: number;
  /**
   * Receives the server's response latency (ms) for an article fetch: the gap
   * between writing `BODY` and reading its status line, before a single payload
   * byte.
   */
  onLatencySample?: (ms: number) => void;
  /** Injectable monotonic-enough wall clock for deterministic timeout tests. */
  clock?: () => number;
  /** Injectable single-shot scheduler used only for response timers. */
  scheduleTimeout?: (
    callback: () => void,
    delayMs: number
  ) => {
    cancel(): void;
    unref?(): void;
  };
  /**
   * Deterministic transport seam for tests: invoked after the local pause is
   * published and may synchronously deliver callbacks already queued by TLS.
   * Production leaves it unset.
   */
  onLocalPause?: (deliverLateRead: (chunk: Buffer) => boolean) => void;
  /** Optional bounded telemetry seam; never receives payload bytes. */
  onLateRead?: (stats: {
    readonly bytes: number;
    readonly chunks: number;
    readonly limitBytes: number;
  }) => void;
}

/**
 * Lifecycle contract for a streamed NNTP BODY payload. `write(false)` follows
 * Node writable semantics: the chunk was accepted, but the whole connection
 * must pause until the one-shot drain callback fires. Implementations must not
 * retain the raw chunk view beyond `write`; it aliases parser scratch.
 */
export interface BackpressuredBodyConsumer {
  write(chunk: Buffer): boolean;
  onceDrain(listener: () => void): void;
  end(): Promise<void>;
  fail(error: Error): void;
}

/**
 * One in-flight request on a (possibly pipelined) connection. Several may be
 * queued at once: their commands are written back-to-back and the responses are
 * matched to requests strictly FIFO, since NNTP delivers them in request order.
 *
 * A `body` request is a TWO-STAGE state machine: first a status line, then (on
 * a 2xx) the multiline payload. Unlike a sequential `command()`, the status of
 * request N+1 cannot be read until request N's body has fully drained.
 * `line` requests (STAT/DATE/GROUP/AUTH) are single-stage.
 */
interface PipelineRequest {
  /** `line` = status line only; `body` = status line then multiline payload. */
  kind: 'line' | 'body';
  /** Sub-state within a `body` request. */
  stage: 'status' | 'payload';
  resolve: (value: any) => void;
  reject: (err: Error) => void;
  /** Rolling inactivity budget (ms); reset on every inbound byte. */
  stallTimeoutMs: number;
  /** Total wall-clock budget (ms), or `Infinity` for stall-only (no deadline). */
  totalTimeoutMs: number;
  /** Absolute epoch-ms deadline (`writtenAt + totalTimeoutMs`), or `Infinity`. */
  deadlineAt: number;
  onAbort?: () => void;
  signal?: AbortSignal;
  /** Epoch ms the command was written; the status line's arrival dates from here. */
  writtenAt: number;
  /**
   * Nothing else was in flight when this command was written
   */
  solo: boolean;
  /**
   * Streaming payload: raw chunks are handed here as they arrive (nothing
   * accumulates) and the request resolves with the total payload byte count once
   * the dot terminator is seen. Absent = buffer the whole payload into a slot.
   */
  consumer?: (chunk: Buffer) => boolean;
  /** Full lifecycle consumer used only by the new streamed segment path. */
  bodyConsumer?: BackpressuredBodyConsumer;
  /** Terminator was consumed while waiting for the consumer's drain. */
  payloadEnded?: boolean;
  /** Guards the asynchronous consumer end operation against duplicate starts. */
  consumerEndStarted?: boolean;
  /** Start of the current local backpressure interval. */
  localPauseStartedAt?: number;
}

let CONNECTION_SEQ = 0;

/** Upper bound (bytes) on a pooled article buffer; larger payloads are allocated fresh, not pooled. */
const RAW_POOL_CAP = 1 << 20;

/** Per-connection reused socket-read buffer for the `onread` path (Node fills it, consumed synchronously). */
const READ_BUF_SIZE = NNTP_READ_WINDOW_BYTES;

interface ConnectionTimer {
  cancel(): void;
  unref?(): void;
}

interface ReadCarryChunk {
  readonly buffer: Buffer;
  start: number;
  readonly end: number;
}

export interface NntpReadCarryStats {
  readonly bytes: number;
  readonly chunks: number;
  readonly limitBytes: number;
}

/**
 * A single NNTP connection over TCP or TLS. Supports **pipelining**: multiple
 * `BODY`/`STAT` commands may be in flight at once (bounded by the caller), with
 * responses matched to requests FIFO. A rolling stall timer destroys the socket
 * if the peer stops making progress, and an optional absolute per-request
 * deadline destroys it if a transfer keeps trickling but never finishes; either
 * way any socket/timeout error rejects the whole in-flight queue (the failover
 * layer resubmits), so one hung stream can't wedge the rest. A clean `430`
 * rejects only its own request; the connection stays healthy. GROUP/AUTH/greeting
 * are issued sequentially (never pipelined).
 */
export class NntpConnection {
  readonly id: number;
  readonly label: string;
  private socket: net.Socket | tls.TLSSocket;
  /** Zero-alloc streaming parser, fed the reused read buffer synchronously. */
  private readonly parser = new NntpOnreadParser();
  /** Reused per-connection socket read buffer (Node fills it, no per-read alloc). */
  private readBuf: Buffer;
  /** FIFO queue of in-flight requests; head is the one currently being read. */
  private queue: PipelineRequest[] = [];
  /** Provider-progress/absolute timer, (re)armed while the queue is non-empty. */
  private stallTimer: ConnectionTimer | null = null;
  /** Current head paused by its local consumer; at most one can exist. */
  private pausedHead: PipelineRequest | null = null;
  /**
   * Lazy FIFO of owned transport windows delivered after local pause. Both its
   * bytes and elements are hard-bounded by the download lease reserved before
   * the BODY command goes on wire. No view of Node's reused onread buffer is
   * retained beyond its callback.
   */
  private readonly readCarry: ReadCarryChunk[] = [];
  private readCarryBytes = 0;
  private socketLocallyPaused = false;
  /** Start of the current connection-wide local backpressure interval. */
  private localPauseStartedAt: number | undefined;
  private continuationScheduled = false;
  private destroyed = false;
  private fatalError: Error | null = null;
  /** Set when a protocol desync is detected: the connection must not be reused. */
  private pipeliningUnsafe = false;

  currentGroup: string | null = null;
  /** Epoch ms after which this idle connection is considered stale. */
  staleAt = 0;

  /**
   * Reusable destination buffers for buffered article payloads, recycled in a
   * ring so each segment doesn't allocate a fresh buffer. The ring grows to
   * `inFlight + 1` so a slot is never reused while an earlier resolved-but-not-
   * yet-decoded body still views it (bodies resolve synchronously during an
   * `onRead` pass; their decode runs in later microtasks). Buffers larger than
   * {@link RAW_POOL_CAP} are allocated fresh to bound retained memory.
   */
  private rawSlots: Buffer[] = [];
  private rawNext = 0;

  private constructor(
    socket: net.Socket | tls.TLSSocket,
    label: string,
    private opts: ConnectionOptions,
    readBuf: Buffer
  ) {
    this.id = ++CONNECTION_SEQ;
    this.label = label;
    this.socket = socket;
    this.readBuf = readBuf;
    this.attach();
  }

  get isUsable(): boolean {
    return (
      !this.destroyed &&
      !this.fatalError &&
      !this.pipeliningUnsafe &&
      !this.socket.destroyed
    );
  }

  /** Number of requests currently in flight (written, awaiting a response). */
  get inFlight(): number {
    return this.queue.length;
  }

  /** Actual owned late-read memory; payload bytes are never exposed here. */
  get readCarryStats(): NntpReadCarryStats {
    return {
      bytes: this.readCarryBytes,
      chunks: this.readCarry.length,
      limitBytes: NNTP_READ_CARRY_MAX_BYTES,
    };
  }

  /**
   * Whether another request can be pipelined onto this connection right now: it
   * must be usable and have fewer than `depth` requests already in flight.
   */
  canAccept(depth: number): boolean {
    return (
      this.isUsable &&
      this.pausedHead === null &&
      !this.socketLocallyPaused &&
      !this.processing &&
      !this.continuationScheduled &&
      this.readCarry.length === 0 &&
      this.queue.length < Math.max(1, depth)
    );
  }

  private attach(): void {
    // The socket is consumed via the reused read-buffer callback wired at
    // construction (no `'data'` events fire); we only handle error/close here.
    const fail = (err: Error) => {
      this.fatalError =
        err instanceof NntpError
          ? err
          : new NntpError('connection', err.message, {
              provider: this.label,
              connId: this.id,
              cause: err,
            });
      this.rejectAll(this.fatalError);
      this.destroy();
    };
    this.socket.on('error', fail);
    this.socket.on('close', () => {
      if (!this.destroyed) {
        fail(
          new NntpError('connection', 'socket closed by peer', {
            provider: this.label,
            connId: this.id,
          })
        );
      }
    });
  }

  /** Open a connection and consume the server greeting. */
  static async connect(
    config: ProviderConfig,
    opts: ConnectionOptions
  ): Promise<NntpConnection> {
    const label = config.name ?? config.id;
    // The socket is created WITH a reused read buffer whose callback routes to the
    // (not-yet-constructed) instance. `conn` is assigned in the microtask after
    // connect resolves, before the first onread IO tick fires, so the closure always
    // sees a live instance (the `conn?.` guard is belt-and-braces).
    const readBuf = Buffer.allocUnsafe(READ_BUF_SIZE);
    let conn: NntpConnection | undefined;
    // `onread` is valid on both net and tls sockets at runtime but is missing from
    // @types/node's tls.ConnectionOptions; spread it in (via a variable) to skip the
    // excess-property check.
    const onreadOpt = {
      onread: {
        buffer: readBuf,
        callback: (bytesRead: number, buf: Buffer): boolean => {
          return conn?.onRead(bytesRead, buf) ?? true;
        },
      },
    };
    const socket = await new Promise<net.Socket | tls.TLSSocket>(
      (resolve, reject) => {
        const onError = (err: Error) =>
          reject(
            new NntpError('connection', `dial failed: ${err.message}`, {
              provider: label,
              cause: err,
            })
          );
        let s: net.Socket | tls.TLSSocket;
        const timer = setTimeout(() => {
          s?.destroy();
          reject(new NntpError('timeout', 'dial timeout', { provider: label }));
        }, opts.dialTimeoutMs);
        const onConnect = () => {
          clearTimeout(timer);
          s.setNoDelay(true);
          s.setKeepAlive(true, 30_000);
          resolve(s);
        };
        if (config.tls) {
          s = tls.connect({
            host: config.host,
            port: config.port,
            rejectUnauthorized: !config.tlsSkipVerify,
            servername: net.isIP(config.host) ? undefined : config.host,
            ...onreadOpt,
          });
          s.once('secureConnect', onConnect);
        } else {
          s = net.connect({
            host: config.host,
            port: config.port,
            ...onreadOpt,
          });
          s.once('connect', onConnect);
        }
        s.once('error', onError);
      }
    );

    conn = new NntpConnection(socket, label, opts, readBuf);
    // Greeting: 200 (posting allowed) or 201 (no posting). Unsolicited: the
    // server sends it on connect, so read a line without writing a command.
    const greeting = await conn.readGreeting(opts.dialTimeoutMs);
    const status = parseStatusLine(greeting);
    if (status.code !== 200 && status.code !== 201) {
      conn.destroy();
      // Some providers refuse the connection at greeting time when the account
      // ceiling is hit (502/400/"too many connections"). Treat that as transient
      // capacity backpressure, not a hard protocol error, so the pool throttles.
      const limited = isConnectionLimitResponse(status.code, greeting);
      if (!limited) {
        logger.warn(
          {
            provider: config.name ?? config.id,
            host: config.host,
            code: status.code,
          },
          'unexpected nntp greeting'
        );
      }
      throw new NntpError(
        limited ? 'connection_limit' : 'protocol',
        limited
          ? `connection limit at greeting: ${greeting}`
          : `unexpected greeting: ${greeting}`,
        {
          code: status.code,
          provider: config.name ?? config.id,
          connId: conn.id,
        }
      );
    }

    if (config.username) {
      await conn.authenticate(config.username, config.password ?? '');
    }

    conn.touch();
    logger.debug(
      {
        provider: config.name ?? config.id,
        host: config.host,
        port: config.port,
        tls: config.tls,
        connId: conn.id,
      },
      'nntp connection established'
    );
    return conn;
  }

  private async authenticate(
    username: string,
    password: string
  ): Promise<void> {
    const userResp = await this.command(
      `AUTHINFO USER ${username}`,
      undefined,
      this.opts.dialTimeoutMs
    );
    const userStatus = parseStatusLine(userResp);
    if (userStatus.code === 281) return; // accepted without password
    if (userStatus.code !== 381) {
      // A "too many connections" rejection (TorBox uses 482 here) is capacity
      // backpressure, not bad credentials; classify it as transient so the pool
      // throttles instead of latching the provider dead.
      const limited = isConnectionLimitResponse(userStatus.code, userResp);
      throw new NntpError(
        limited ? 'connection_limit' : 'auth_failed',
        `auth user rejected: ${userResp}`,
        { code: userStatus.code, provider: this.label, connId: this.id }
      );
    }
    const passResp = await this.command(
      `AUTHINFO PASS ${password}`,
      undefined,
      this.opts.dialTimeoutMs
    );
    const passStatus = parseStatusLine(passResp);
    if (passStatus.code !== 281) {
      // 482/502/"too many connections" at AUTHINFO PASS is the account
      // connection ceiling, not a credential failure (this is where TorBox's
      // `482 too many connections for your user` lands). Surface it as transient.
      const limited = isConnectionLimitResponse(passStatus.code, passResp);
      if (!limited) {
        logger.warn(
          { provider: this.label, code: passStatus.code },
          'nntp authentication rejected'
        );
      }
      throw new NntpError(
        limited ? 'connection_limit' : 'auth_failed',
        limited
          ? `connection limit reached: ${passResp}`
          : `auth pass rejected: ${passStatus.code}`,
        { code: passStatus.code, provider: this.label, connId: this.id }
      );
    }
  }

  /** Select a newsgroup. */
  async group(
    name: string,
    signal?: AbortSignal,
    timeoutMs = 30_000
  ): Promise<void> {
    const resp = await this.command(`GROUP ${name}`, signal, timeoutMs);
    const status = parseStatusLine(resp);
    if (status.code !== 211) {
      throw new NntpError(
        classifyNntpStatus(status.code),
        `GROUP failed: ${resp}`,
        {
          code: status.code,
          provider: this.label,
          connId: this.id,
        }
      );
    }
    this.currentGroup = name;
  }

  /**
   * Fetch a raw article body (still dot-stuffed). messageId without <>. May be
   * pipelined: the command is written immediately and the response matched FIFO,
   * so several concurrent `body()` calls share one connection.
   */
  body(
    messageId: string,
    signal: AbortSignal | undefined,
    stallTimeoutMs: number,
    totalTimeoutMs?: number
  ): Promise<Buffer> {
    return this.submit<Buffer>(
      'body',
      `BODY <${messageId}>`,
      signal,
      stallTimeoutMs,
      undefined,
      totalTimeoutMs
    );
  }

  /**
   * Fetch an article body, streaming the raw (dot-stuffed) payload to
   * `onChunk` instead of buffering it: the wire still carries the whole
   * article, but the process never holds it. Resolves with the total payload
   * byte count. Used by import probes that only need the leading bytes.
   */
  bodyStreaming(
    messageId: string,
    onChunk: (chunk: Buffer) => void,
    signal: AbortSignal | undefined,
    stallTimeoutMs: number,
    totalTimeoutMs?: number
  ): Promise<number> {
    return this.submit<number>(
      'body',
      `BODY <${messageId}>`,
      signal,
      stallTimeoutMs,
      (chunk) => {
        onChunk(chunk);
        return true;
      },
      totalTimeoutMs
    );
  }

  /**
   * Stream one complete raw BODY into a lifecycle consumer with socket-level
   * backpressure. The raw article is never accumulated; a false write pauses
   * the entire pipelined connection until the consumer emits one drain.
   * `onEnqueued` runs synchronously only after the command and matching request
   * have both entered the connection FIFO, providing the streaming fetcher's
   * exact on-wire linearization point.
   */
  bodyToConsumer(
    messageId: string,
    consumer: BackpressuredBodyConsumer,
    signal: AbortSignal | undefined,
    stallTimeoutMs: number,
    totalTimeoutMs?: number,
    onEnqueued?: () => void
  ): Promise<number> {
    return this.submit<number>(
      'body',
      `BODY <${messageId}>`,
      signal,
      stallTimeoutMs,
      (chunk) => consumer.write(chunk),
      totalTimeoutMs,
      consumer,
      onEnqueued
    );
  }

  /** STAT returns true if the article exists, false on 430. May be pipelined. */
  async stat(
    messageId: string,
    signal: AbortSignal | undefined,
    timeoutMs: number
  ): Promise<boolean> {
    const resp = await this.command(`STAT <${messageId}>`, signal, timeoutMs);
    const status = parseStatusLine(resp);
    if (status.code === 223) return true;
    if (status.code === 430 || status.code === 423) return false;
    throw new NntpError(
      classifyNntpStatus(status.code),
      `STAT failed: ${resp}`,
      {
        code: status.code,
        provider: this.label,
        connId: this.id,
      }
    );
  }

  /** DATE: cheap health check / keepalive. */
  async date(signal?: AbortSignal, timeoutMs = 15_000): Promise<void> {
    const resp = await this.command('DATE', signal, timeoutMs);
    const status = parseStatusLine(resp);
    if (status.code !== 111) {
      throw new NntpError('protocol', `DATE failed: ${resp}`, {
        code: status.code,
        provider: this.label,
        connId: this.id,
      });
    }
  }

  quit(): void {
    if (this.isUsable) {
      try {
        this.socket.write(Buffer.concat([Buffer.from('QUIT'), CRLF]));
      } catch {
        /* ignore */
      }
    }
    this.destroy();
  }

  destroy(): void {
    if (this.destroyed) return;
    this.destroyed = true;
    this.clearStallTimer();
    // Fail any still-queued requests so callers never hang on a dead socket.
    if (this.queue.length > 0) {
      this.rejectAll(
        this.fatalError ??
          new NntpError('connection', 'connection destroyed', {
            provider: this.label,
            connId: this.id,
          })
      );
    }
    try {
      this.socket.destroy();
    } catch {
      /* ignore */
    }
  }

  /** Refresh the idle stale deadline (called on release). */
  touch(): void {
    this.staleAt = this.now() + this.opts.idleConnectionMs;
  }

  isStale(): boolean {
    return this.staleAt > 0 && this.now() > this.staleAt;
  }

  // ---- internals -----------------------------------------------------------

  /**
   * Issue a single-line-response command (STAT/GROUP/DATE/AUTHINFO) and resolve
   * with the raw status line. Does NOT throw on protocol error codes; the
   * caller classifies them (so e.g. "too many connections" can be told apart
   * from a credential failure). May be pipelined, though in practice these are
   * only issued during sequential setup / health checks.
   */
  private command(
    line: string,
    signal: AbortSignal | undefined,
    timeoutMs: number
  ): Promise<string> {
    return this.submit<string>('line', line, signal, timeoutMs);
  }

  /**
   * Write a command immediately and queue a request for its response, matched to
   * the response stream strictly FIFO. Concurrent calls pipeline onto the one
   * connection (the caller bounds depth via {@link canAccept}).
   */
  private submit<T>(
    kind: 'line' | 'body',
    line: string,
    signal: AbortSignal | undefined,
    stallTimeoutMs: number,
    consumer?: (chunk: Buffer) => boolean,
    totalTimeoutMs?: number,
    bodyConsumer?: BackpressuredBodyConsumer,
    onEnqueued?: () => void
  ): Promise<T> {
    if (!this.isUsable) {
      const error =
        this.fatalError ??
        new NntpError('connection', 'connection not usable', {
          provider: this.label,
          connId: this.id,
        });
      this.notifyConsumerFailure(bodyConsumer, error);
      return Promise.reject(error);
    }
    if (signal?.aborted) {
      const error = new NntpError('connection', 'aborted', {
        provider: this.label,
        connId: this.id,
      });
      this.notifyConsumerFailure(bodyConsumer, error);
      this.fatalError = error;
      this.destroy();
      return Promise.reject(error);
    }
    try {
      this.write(line);
    } catch (cause) {
      const error = new NntpError('connection', 'command write failed', {
        provider: this.label,
        connId: this.id,
        cause,
      });
      this.notifyConsumerFailure(bodyConsumer, error);
      this.fatalError = error;
      this.destroy();
      return Promise.reject(error);
    }
    return this.queueRequest<T>(
      kind,
      signal,
      stallTimeoutMs,
      consumer,
      totalTimeoutMs,
      bodyConsumer,
      onEnqueued
    );
  }

  /**
   * Read the unsolicited server greeting (sent on connect, with no command
   * written first). Single-line, never pipelined.
   */
  private readGreeting(timeoutMs: number): Promise<string> {
    return this.queueRequest<string>('line', undefined, timeoutMs);
  }

  /** Push a response request and resolve when the FIFO machine completes it. */
  private queueRequest<T>(
    kind: 'line' | 'body',
    signal: AbortSignal | undefined,
    stallTimeoutMs: number,
    consumer?: (chunk: Buffer) => boolean,
    totalTimeoutMs?: number,
    bodyConsumer?: BackpressuredBodyConsumer,
    onEnqueued?: () => void
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const now = this.now();
      // A non-positive/absent total budget means "no wall-clock deadline"; only
      // the rolling stall timer bounds the request.
      const total =
        totalTimeoutMs && totalTimeoutMs > 0 ? totalTimeoutMs : Infinity;
      const req: PipelineRequest = {
        kind,
        stage: 'status',
        resolve,
        reject,
        stallTimeoutMs,
        totalTimeoutMs: total,
        deadlineAt: total === Infinity ? Infinity : now + total,
        signal,
        consumer,
        bodyConsumer,
        writtenAt: now,
        // The command was written immediately before this push, so an empty
        // queue here means it went out on an otherwise-silent connection.
        solo: this.queue.length === 0,
      };
      if (signal) {
        // Aborting one request mid-pipeline can't un-send its command, so the
        // safe response is to tear the whole connection down (the failover layer
        // resubmits). In practice the pipelined fetch path runs signal-free.
        req.onAbort = () => {
          this.fatalError = new NntpError('connection', 'aborted', {
            provider: this.label,
            connId: this.id,
          });
          this.destroy();
        };
        signal.addEventListener('abort', req.onAbort, { once: true });
      }
      this.queue.push(req);
      this.armStallTimer();
      try {
        onEnqueued?.();
      } catch (cause) {
        this.fatalError = new NntpError(
          'connection',
          'command enqueue observer failed',
          { provider: this.label, connId: this.id, cause }
        );
        this.destroy();
      }
      // Response bytes only exist during a read callback; the next onRead picks
      // this request up.
    });
  }

  private write(line: string): void {
    this.socket.write(Buffer.concat([Buffer.from(line, 'utf8'), CRLF]));
  }

  /**
   * (Re)arm the connection's sole response timer. It enforces two independent
   * budgets, whichever bites first:
   *  - the rolling stall budget (`stallTimeoutMs`), re-armed on every inbound
   *    byte and whenever the head advances, so it fires after a full window of
   *    SILENCE, to catch a connection that went dead mid-transfer; and
   *  - the earliest finite absolute deadline across every written pipeline
   *    request. Inbound bytes never push those deadlines out, and a later FIFO
   *    request may therefore expire before the current head.
   *
   * While the socket is locally paused, provider-stall timing is suspended but
   * the earliest pipeline deadline remains armed. The queue is caller-bounded,
   * so the linear scan does not introduce an unbounded helper structure.
   */
  private armStallTimer(): void {
    this.stallTimer?.cancel();
    this.stallTimer = null;
    const head = this.queue[0];
    if (!head) return;

    const scheduledAt = this.now();
    const earliestDeadline = this.earliestFiniteDeadline();
    const deadlineDueAt = earliestDeadline?.deadlineAt ?? Infinity;
    const stallDueAt = this.socketLocallyPaused
      ? Infinity
      : scheduledAt + head.stallTimeoutMs;
    const dueAt = Math.min(deadlineDueAt, stallDueAt);
    if (dueAt === Infinity) return;

    this.stallTimer = this.scheduleTimeout(
      () => {
        this.stallTimer = null;
        if (this.destroyed || this.queue.length === 0) return;

        const callbackNow = this.now();
        if (this.failExpiredPipelineDeadline(callbackNow)) return;

        // A custom scheduler may invoke a callback before its requested due time.
        // Never turn an early absolute-deadline callback into a provider stall.
        if (
          this.socketLocallyPaused ||
          callbackNow < stallDueAt ||
          this.queue[0] !== head
        ) {
          this.armStallTimer();
          return;
        }
        this.failProviderStall(head);
      },
      Math.max(0, dueAt - scheduledAt)
    );
    this.stallTimer.unref?.();
  }

  /** Earliest finite deadline, preserving FIFO order when deadlines tie. */
  private earliestFiniteDeadline(): PipelineRequest | undefined {
    let earliest: PipelineRequest | undefined;
    for (const request of this.queue) {
      if (request.deadlineAt === Infinity) continue;
      if (!earliest || request.deadlineAt < earliest.deadlineAt) {
        earliest = request;
      }
    }
    return earliest;
  }

  /**
   * Enforce hard wall-clock deadlines synchronously at every response-progress
   * boundary. This closes the race where the event loop delivers IO, drain, or
   * consumer completion after a timer was due but before its callback ran.
   */
  private failExpiredPipelineDeadline(now = this.now()): boolean {
    const expired = this.earliestFiniteDeadline();
    if (!expired || now < expired.deadlineAt) return false;

    const timeoutSource = this.socketLocallyPaused
      ? 'local_backpressure'
      : 'absolute';
    const localBackpressureMs =
      timeoutSource === 'local_backpressure'
        ? Math.max(0, now - (this.localPauseStartedAt ?? now))
        : undefined;
    logger.warn(
      {
        provider: this.label,
        connId: this.id,
        inFlight: this.queue.length,
        totalTimeoutMs: expired.totalTimeoutMs,
        faultDomain:
          timeoutSource === 'local_backpressure' ? 'local' : 'provider',
        timeoutSource,
        carryBytes: this.readCarryBytes,
        carryChunks: this.readCarry.length,
        carryLimitBytes: NNTP_READ_CARRY_MAX_BYTES,
        ...(timeoutSource === 'local_backpressure'
          ? {
              localBackpressureMs,
            }
          : {}),
      },
      timeoutSource === 'local_backpressure'
        ? 'nntp segment exceeded its total time budget during local backpressure; destroying'
        : 'nntp segment exceeded its total time budget; destroying'
    );
    this.fatalError = new NntpError(
      'timeout',
      timeoutSource === 'local_backpressure'
        ? `segment exceeded total budget of ${expired.totalTimeoutMs}ms during local backpressure`
        : `segment exceeded total budget of ${expired.totalTimeoutMs}ms`,
      {
        provider: this.label,
        timeoutSource,
        connId: this.id,
        localBackpressureMs,
        carryBytes: this.readCarryBytes,
        carryChunks: this.readCarry.length,
        carryLimitBytes: NNTP_READ_CARRY_MAX_BYTES,
      }
    );
    this.destroy();
    return true;
  }

  private failProviderStall(head: PipelineRequest): void {
    logger.warn(
      {
        provider: this.label,
        connId: this.id,
        inFlight: this.queue.length,
        stallTimeoutMs: head.stallTimeoutMs,
      },
      'nntp connection stalled; destroying'
    );
    this.fatalError = new NntpError(
      'timeout',
      `no response progress for ${head.stallTimeoutMs}ms`,
      {
        provider: this.label,
        timeoutSource: 'provider_stall',
        connId: this.id,
      }
    );
    this.destroy();
  }

  private clearStallTimer(): void {
    if (this.stallTimer) {
      this.stallTimer.cancel();
      this.stallTimer = null;
    }
  }

  private now(): number {
    return this.opts.clock?.() ?? Date.now();
  }

  private scheduleTimeout(
    callback: () => void,
    delayMs: number
  ): ConnectionTimer {
    if (this.opts.scheduleTimeout) {
      return this.opts.scheduleTimeout(callback, delayMs);
    }
    const timer = setTimeout(callback, delayMs);
    return {
      cancel: () => clearTimeout(timer),
      unref: () => timer.unref?.(),
    };
  }

  /** Re-entrancy guard for {@link onRead} (a synchronous `resolve` may enqueue). */
  private processing = false;

  /**
   * onread read path: drive the FIFO machine over the reused socket buffer
   * `buf[0..nread]` synchronously. Reads from the live window via
   * {@link NntpOnreadParser}; the window is only valid during this call, so
   * buffered bodies are copied into a pooled slot before resolving.
   */
  private onRead(nread: number, buf: Buffer): boolean {
    const parser = this.parser;
    if (!parser || this.destroyed) return false;
    if (nread <= 0) return true;
    if (this.failExpiredPipelineDeadline()) return false;

    // `socket.pause()` is not a TLS delivery fence: data already decrypted or
    // queued for JS may arrive in later callbacks. Own those bytes immediately
    // because Node reuses `buf` as soon as this callback returns.
    if (
      this.processing ||
      this.pausedHead ||
      this.socketLocallyPaused ||
      this.readCarry.length > 0 ||
      this.continuationScheduled
    ) {
      try {
        this.enqueueLateRead(buf, 0, nread);
        this.ensureSocketLocallyPaused();
        if (!this.pausedHead && !this.processing) {
          this.scheduleReadContinuation();
        }
      } catch (error) {
        this.failFromConsumer(error);
      }
      return false;
    }
    // Any byte from the peer is progress; push the rolling stall deadline out.
    this.armStallTimer();
    this.processing = true;
    let keepReading = false;
    try {
      keepReading = this.processReadWindow(buf, 0, nread, false);
    } catch (error) {
      this.failFromConsumer(error);
    } finally {
      this.processing = false;
    }
    if (!this.destroyed && !this.pausedHead && this.readCarry.length > 0) {
      this.ensureSocketLocallyPaused();
      this.scheduleReadContinuation();
      return false;
    }
    return keepReading && !this.socketLocallyPaused;
  }

  private processReadWindow(
    buf: Buffer,
    initialOffset: number,
    nread: number,
    ownedWindow: boolean
  ): boolean {
    const parser = this.parser;
    let off = initialOffset;
    while (off < nread && this.queue.length > 0) {
      if (this.failExpiredPipelineDeadline()) return false;
      const head = this.queue[0];
      if (head.stage === 'status') {
        const step = parser.feedLine(buf, off, nread);
        if (step.status === 'need-more') return true;
        if (step.status === 'desync') {
          this.onDesync('non-line where a status line was expected');
          return false;
        }
        off = step.off;
        if (!this.onStatusLine(head, step.text)) return false;
        continue;
      }

      const step = parser.feedBody(buf, off, nread);
      off = step.off;
      if (step.backpressured) {
        this.pauseForConsumerDrain(
          head,
          step.ended,
          buf,
          off,
          nread,
          ownedWindow
        );
        return false;
      }
      if (!step.ended) return true;
      if (head.bodyConsumer) {
        this.pauseForConsumerEnd(head, buf, off, nread, ownedWindow);
        return false;
      }
      if (head.consumer) {
        const streamed = parser.streamed;
        if (!this.finishHead(() => head.resolve(streamed))) return false;
      } else {
        // A view of the pooled slot, valid until the ring recycles it (after
        // `inFlight` more bodies); long enough for the synchronous decode that
        // follows the resolve in the next microtask.
        const body = parser.dest!.subarray(0, parser.bodyLen);
        if (!this.finishHead(() => head.resolve(body))) return false;
      }
    }
    return true;
  }

  private pauseForConsumerDrain(
    head: PipelineRequest,
    payloadEnded: boolean,
    buf: Buffer,
    off: number,
    nread: number,
    ownedWindow: boolean
  ): void {
    const consumer = head.bodyConsumer;
    if (!consumer) {
      throw new NntpError(
        'protocol',
        'non-backpressured BODY consumer requested a pause',
        { provider: this.label, connId: this.id }
      );
    }
    head.payloadEnded = payloadEnded;
    this.establishLocalPause(head, buf, off, nread, ownedWindow);
    consumer.onceDrain(() => {
      queueMicrotask(() => {
        try {
          this.onConsumerDrain(head);
        } catch (error) {
          this.failFromConsumer(error);
        }
      });
    });
  }

  private onConsumerDrain(head: PipelineRequest): void {
    if (this.destroyed || this.pausedHead !== head || this.queue[0] !== head) {
      return;
    }
    if (this.failExpiredPipelineDeadline()) return;
    if (head.payloadEnded) {
      this.startConsumerEnd(head);
      return;
    }
    this.releaseLocalPause(head);
    this.scheduleReadContinuation();
  }

  private pauseForConsumerEnd(
    head: PipelineRequest,
    buf: Buffer,
    off: number,
    nread: number,
    ownedWindow: boolean
  ): void {
    head.payloadEnded = true;
    this.establishLocalPause(head, buf, off, nread, ownedWindow);
    this.startConsumerEnd(head);
  }

  private startConsumerEnd(head: PipelineRequest): void {
    const consumer = head.bodyConsumer;
    if (!consumer || head.consumerEndStarted) return;
    head.consumerEndStarted = true;
    const streamed = this.parser.streamed;
    void Promise.resolve()
      .then(() => consumer.end())
      .then(() => {
        if (
          this.destroyed ||
          this.pausedHead !== head ||
          this.queue[0] !== head
        ) {
          return;
        }
        if (this.failExpiredPipelineDeadline()) return;
        this.releaseLocalPause(head);
        if (!this.finishHead(() => head.resolve(streamed))) return;
        this.scheduleReadContinuation();
      })
      .catch((error: unknown) => this.failFromConsumer(error));
  }

  private establishLocalPause(
    head: PipelineRequest,
    buf: Buffer,
    off: number,
    nread: number,
    ownedWindow: boolean
  ): void {
    if (this.pausedHead && this.pausedHead !== head) {
      throw new NntpError('protocol', 'multiple local BODY pauses detected', {
        provider: this.label,
        connId: this.id,
      });
    }
    this.retainCurrentReadRemainder(buf, off, nread, ownedWindow);
    this.pausedHead = head;
    const now = this.now();
    head.localPauseStartedAt ??= now;
    this.ensureSocketLocallyPaused(now);
    this.armStallTimer();
  }

  private releaseLocalPause(head: PipelineRequest): void {
    if (this.pausedHead !== head) return;
    this.pausedHead = null;
    head.localPauseStartedAt = undefined;
    head.payloadEnded = false;
    this.armStallTimer();
  }

  /** Keep the unconsumed current window ahead of all later callbacks. */
  private retainCurrentReadRemainder(
    buf: Buffer,
    off: number,
    nread: number,
    ownedWindow: boolean
  ): void {
    if (ownedWindow) {
      const head = this.readCarry[0];
      if (!head || head.buffer !== buf || head.end !== nread) {
        throw new NntpError(
          'protocol',
          'owned NNTP carry head changed during processing',
          { provider: this.label, connId: this.id }
        );
      }
      const consumed = Math.max(0, Math.min(head.end, off) - head.start);
      head.start += consumed;
      this.readCarryBytes -= consumed;
      if (head.start >= head.end) this.readCarry.shift();
      this.assertReadCarryInvariants();
      return;
    }
    if (off >= nread) return;
    const length = nread - off;
    this.assertReadCarryCapacity(length, 1);
    const owned = Buffer.allocUnsafe(length);
    buf.copy(owned, 0, off, nread);
    this.readCarry.unshift({ buffer: owned, start: 0, end: length });
    this.readCarryBytes += length;
    this.assertReadCarryInvariants();
  }

  /** Append one late callback after all already-owned bytes. */
  private enqueueLateRead(buf: Buffer, off: number, nread: number): void {
    if (off >= nread) return;
    const length = nread - off;
    this.assertReadCarryCapacity(length, 1);
    const owned = Buffer.allocUnsafe(length);
    buf.copy(owned, 0, off, nread);
    this.readCarry.push({ buffer: owned, start: 0, end: length });
    this.readCarryBytes += length;
    this.assertReadCarryInvariants();
    this.opts.onLateRead?.({
      bytes: this.readCarryBytes,
      chunks: this.readCarry.length,
      limitBytes: NNTP_READ_CARRY_MAX_BYTES,
    });
  }

  private assertReadCarryCapacity(bytes: number, chunks: number): void {
    const nextBytes = this.readCarryBytes + bytes;
    const nextChunks = this.readCarry.length + chunks;
    if (
      bytes <= 0 ||
      bytes > READ_BUF_SIZE ||
      nextBytes > NNTP_READ_CARRY_MAX_BYTES ||
      nextChunks > NNTP_READ_CARRY_MAX_CHUNKS
    ) {
      const now = this.now();
      const localBackpressureMs = Math.max(
        0,
        now - (this.localPauseStartedAt ?? now)
      );
      logger.warn(
        {
          provider: this.label,
          connId: this.id,
          faultDomain: 'local',
          carryBytes: nextBytes,
          carryChunks: nextChunks,
          carryLimitBytes: NNTP_READ_CARRY_MAX_BYTES,
          localBackpressureMs,
        },
        'bounded nntp read carry capacity exceeded; destroying connection'
      );
      throw new NntpError(
        'local_backpressure',
        'bounded NNTP read carry capacity exceeded',
        {
          provider: this.label,
          connId: this.id,
          faultDomain: 'local',
          localBackpressureMs,
          carryBytes: nextBytes,
          carryChunks: nextChunks,
          carryLimitBytes: NNTP_READ_CARRY_MAX_BYTES,
        }
      );
    }
  }

  private ensureSocketLocallyPaused(now = this.now()): void {
    if (this.socketLocallyPaused) return;
    this.localPauseStartedAt = now;
    this.socket.pause();
    this.socketLocallyPaused = true;
    logger.trace(
      {
        provider: this.label,
        connId: this.id,
        inFlight: this.queue.length,
        faultDomain: 'local',
        carryBytes: this.readCarryBytes,
        carryChunks: this.readCarry.length,
        carryLimitBytes: NNTP_READ_CARRY_MAX_BYTES,
      },
      'nntp socket paused for local consumer backpressure'
    );
    this.armStallTimer();
    this.opts.onLocalPause?.((chunk) => this.onRead(chunk.length, chunk));
  }

  private scheduleReadContinuation(): void {
    if (this.continuationScheduled) return;
    this.continuationScheduled = true;
    queueMicrotask(() => {
      this.continuationScheduled = false;
      try {
        this.continueAfterLocalPause();
      } catch (error) {
        this.failFromConsumer(error);
      }
    });
  }

  private continueAfterLocalPause(): void {
    if (this.destroyed || this.pausedHead) return;
    if (this.processing) {
      this.scheduleReadContinuation();
      return;
    }
    if (this.failExpiredPipelineDeadline()) return;
    while (this.readCarry.length > 0) {
      const carry = this.readCarry[0];
      this.processing = true;
      let consumed = false;
      try {
        consumed = this.processReadWindow(
          carry.buffer,
          carry.start,
          carry.end,
          true
        );
      } catch (error) {
        this.failFromConsumer(error);
        return;
      } finally {
        this.processing = false;
      }
      if (!consumed || this.destroyed || this.pausedHead) return;
      if (this.readCarry[0] !== carry) {
        this.onDesync('owned NNTP carry head changed without backpressure');
        return;
      }
      this.readCarry.shift();
      this.readCarryBytes -= carry.end - carry.start;
      this.assertReadCarryInvariants();
    }
    if (
      !this.destroyed &&
      !this.pausedHead &&
      !this.processing &&
      this.readCarry.length === 0 &&
      this.socketLocallyPaused
    ) {
      const resumedAt = this.now();
      const localBackpressureMs = Math.max(
        0,
        resumedAt - (this.localPauseStartedAt ?? resumedAt)
      );
      this.socketLocallyPaused = false;
      this.localPauseStartedAt = undefined;
      this.socket.resume();
      logger.trace(
        {
          provider: this.label,
          connId: this.id,
          inFlight: this.queue.length,
          localBackpressureMs,
          carryBytes: 0,
          carryChunks: 0,
          carryLimitBytes: NNTP_READ_CARRY_MAX_BYTES,
        },
        'nntp socket resumed after local consumer backpressure'
      );
      this.armStallTimer();
    }
  }

  private assertReadCarryInvariants(): void {
    let actualBytes = 0;
    for (const chunk of this.readCarry) {
      actualBytes += chunk.end - chunk.start;
    }
    if (
      actualBytes !== this.readCarryBytes ||
      this.readCarryBytes < 0 ||
      this.readCarryBytes > NNTP_READ_CARRY_MAX_BYTES ||
      this.readCarry.length > NNTP_READ_CARRY_MAX_CHUNKS
    ) {
      throw new Error('NNTP read carry invariant violated');
    }
  }

  private failFromConsumer(error: unknown): void {
    if (this.destroyed) return;
    this.fatalError =
      error instanceof Error
        ? error
        : new NntpError('protocol', 'BODY consumer failed', {
            provider: this.label,
            connId: this.id,
            cause: error,
          });
    this.destroy();
  }

  /**
   * Act on a completed status line for the current head (onread mode): resolve a
   * line request, reject a body 4xx (consuming no payload, so the connection
   * stays healthy), or arm the parser for the 2xx payload. Returns false (after
   * tearing the connection down) on a desync.
   */
  private onStatusLine(head: PipelineRequest, line: string): boolean {
    const parser = this.parser!;
    const status = parseStatusLine(line);
    if (status.code === 0) {
      this.onDesync(line);
      return false;
    }
    if (head.kind === 'line') {
      return this.finishHead(() => head.resolve(line));
    }
    // body request: ≥4xx (e.g. 430) → reject ONLY this request, no body to read.
    if (statusClass(status.code) >= 4) {
      const err = new NntpError(
        classifyNntpStatus(status.code),
        `command failed: ${status.code} ${status.message}`,
        { code: status.code, provider: this.label, connId: this.id }
      );
      this.failBodyConsumer(head, err);
      return this.finishHead(() => head.reject(err), false);
    }
    if (head.solo) {
      this.opts.onLatencySample?.(this.now() - head.writtenAt);
    }
    head.stage = 'payload';
    if (head.consumer) {
      parser.beginStreamingBody(head.consumer);
    } else {
      // Fixed-cap pooled slot: size is unknown until the terminator, so the body
      // streams in and the slot's own tail is the terminator carry.
      parser.beginBufferedBody(this.acquireRaw(RAW_POOL_CAP));
    }
    return true;
  }

  /** Pipeline desync: mark unusable, latch the error, destroy. */
  private onDesync(line: string): void {
    this.pipeliningUnsafe = true;
    this.fatalError = new NntpError('protocol', `protocol desync: ${line}`, {
      provider: this.label,
      connId: this.id,
    });
    this.destroy();
  }

  /** Shift the completed head, re-arm the response timer, then deliver it. */
  private finishHead(deliver: () => void, enforceDeadline = true): boolean {
    if (enforceDeadline && this.failExpiredPipelineDeadline()) return false;
    const head = this.queue.shift();
    if (!head) return false;
    if (head.signal && head.onAbort) {
      head.signal.removeEventListener('abort', head.onAbort);
    }
    this.armStallTimer();
    deliver();
    return true;
  }

  /**
   * Return a recycled destination buffer of at least `size` bytes for the next
   * buffered article payload. The ring holds one slot per concurrently in-flight
   * request (+1 margin) so a slot is never overwritten while an earlier body it
   * backs is still awaiting decode. Oversized articles bypass the pool.
   */
  private acquireRaw(size: number): Buffer {
    if (size > RAW_POOL_CAP) return Buffer.allocUnsafe(size);
    const want = Math.max(2, this.queue.length + 1);
    while (this.rawSlots.length < want) {
      this.rawSlots.push(Buffer.allocUnsafe(size));
    }
    if (this.rawNext >= this.rawSlots.length) this.rawNext = 0;
    let buf = this.rawSlots[this.rawNext];
    if (buf.length < size) {
      buf = Buffer.allocUnsafe(size);
      this.rawSlots[this.rawNext] = buf;
    }
    this.rawNext = (this.rawNext + 1) % this.rawSlots.length;
    return buf;
  }

  /** Reject every in-flight request (socket error / timeout / destroy). */
  private rejectAll(err: Error): void {
    const pending = this.queue;
    this.queue = [];
    this.pausedHead = null;
    this.readCarry.splice(0, this.readCarry.length);
    this.readCarryBytes = 0;
    this.socketLocallyPaused = false;
    this.localPauseStartedAt = undefined;
    this.clearStallTimer();
    for (const req of pending) {
      if (req.signal && req.onAbort) {
        req.signal.removeEventListener('abort', req.onAbort);
      }
      this.failBodyConsumer(req, err);
      req.reject(err);
    }
  }

  private failBodyConsumer(req: PipelineRequest, error: Error): void {
    this.notifyConsumerFailure(req.bodyConsumer, error);
  }

  private notifyConsumerFailure(
    consumer: BackpressuredBodyConsumer | undefined,
    error: Error
  ): void {
    if (!consumer) return;
    try {
      consumer.fail(error);
    } catch {
      logger.warn(
        { provider: this.label, connId: this.id },
        'nntp BODY consumer cleanup failed'
      );
    }
  }
}

export { DOT_TERMINATOR };
