import { getDb } from '../db.js';
import { sql, join, raw, SqlFragment } from '../sql.js';

/** Which byte-serving path a session belongs to. */
export type StreamTransport = 'usenet' | 'proxy';

/**
 * Why a session stopped being active. `idle` is the ordinary ending (the watch
 * went quiet for longer than the session idle timeout); the rest are forced.
 */
export type StreamEndReason =
  | 'idle'
  | 'stopped'
  | 'banned'
  | 'limit'
  | 'error'
  | 'stale'
  | 'shutdown';

/** A persisted session row (active when `endedAt` is undefined). */
export interface StreamSessionRow {
  id: string;
  transport: StreamTransport;
  /** Empty string when the caller could not be identified (legacy tokens). */
  username: string;
  clientIp?: string;
  targetKey: string;
  filename?: string;
  /** Log-safe upstream URL; the raw URL is never persisted. */
  displayUrl?: string;
  size: number;
  bytesServed: number;
  requests: number;
  startedAt: number;
  lastSeenAt: number;
  endedAt?: number;
  endReason?: StreamEndReason;
  instanceId: string;
  stopRequestedAt?: number;
}

/** The mutable slice of a session written on every flush. */
export interface StreamSessionUpsert {
  id: string;
  transport: StreamTransport;
  username: string;
  clientIp?: string;
  targetKey: string;
  filename?: string;
  displayUrl?: string;
  size: number;
  bytesServed: number;
  requests: number;
  startedAt: number;
  lastSeenAt: number;
  endedAt?: number;
  endReason?: StreamEndReason;
  instanceId: string;
}

/** Served bytes to fold into an hourly bucket. */
export interface StreamBandwidthDelta {
  username: string;
  transport: StreamTransport;
  bytes: number;
  /** Defaults to now; the bucket is the containing hour. */
  atMs?: number;
}

/** Per-user (and per-transport) totals over a window. */
export interface StreamBandwidthRollup {
  username: string;
  transport: StreamTransport;
  bytes: number;
}

export interface StreamBandwidthBucket {
  bucketMs: number;
  bytes: number;
}

/** A bucket of one user's usage, for the per-user chart. */
export interface StreamBandwidthUserBucket extends StreamBandwidthBucket {
  username: string;
}

export type StreamBanScope = 'user' | 'target';

export interface StreamBan {
  id: string;
  scope: StreamBanScope;
  username: string;
  /** Set only for `scope: 'target'`. */
  targetKey?: string;
  reason?: string;
  createdAt: number;
  createdBy?: string;
  /** Undefined means the ban holds until it is lifted by hand. */
  expiresAt?: number;
}

export interface StreamHistoryQuery {
  limit?: number;
  offset?: number;
  username?: string;
  transport?: StreamTransport;
  /** Case-insensitive substring match against filename / display URL. */
  search?: string;
}

interface SessionDbRow {
  id: string;
  transport: string;
  username: string;
  client_ip: string | null;
  target_key: string;
  filename: string | null;
  display_url: string | null;
  size: number | string;
  bytes_served: number | string;
  requests: number | string;
  started_at: number | string;
  last_seen_at: number | string;
  ended_at: number | string | null;
  end_reason: string | null;
  instance_id: string;
  stop_requested_at: number | string | null;
  [k: string]: unknown;
}

interface BanDbRow {
  id: string;
  scope: string;
  username: string;
  target_key: string | null;
  reason: string | null;
  created_at: number | string;
  created_by: string | null;
  expires_at: number | string | null;
  [k: string]: unknown;
}

const HOUR_MS = 3_600_000;

/**
 * Rows per batched upsert. Well inside both dialects' bound-parameter limits at
 * the widest table here (15 columns).
 */
const UPSERT_CHUNK = 200;

function hourFloor(ts: number): number {
  return ts - (ts % HOUR_MS);
}

/**
 * Floor an hourly row to its chart bucket. Modulo, not division: a bound width
 * arrives as a float, so `(hour_ms / w) * w` would never truncate.
 */
function bucketExpr(bucketMs: number): SqlFragment {
  return sql`(hour_ms - (hour_ms % ${bucketMs}))`;
}

function optionalNumber(v: number | string | null): number | undefined {
  return v == null ? undefined : Number(v);
}

function toSession(r: SessionDbRow): StreamSessionRow {
  return {
    id: r.id,
    transport: r.transport as StreamTransport,
    username: r.username,
    clientIp: r.client_ip ?? undefined,
    targetKey: r.target_key,
    filename: r.filename ?? undefined,
    displayUrl: r.display_url ?? undefined,
    size: Number(r.size ?? 0),
    bytesServed: Number(r.bytes_served ?? 0),
    requests: Number(r.requests ?? 0),
    startedAt: Number(r.started_at),
    lastSeenAt: Number(r.last_seen_at),
    endedAt: optionalNumber(r.ended_at),
    endReason: (r.end_reason as StreamEndReason | null) ?? undefined,
    instanceId: r.instance_id,
    stopRequestedAt: optionalNumber(r.stop_requested_at),
  };
}

function toBan(r: BanDbRow): StreamBan {
  return {
    id: r.id,
    scope: r.scope as StreamBanScope,
    username: r.username,
    targetKey: r.target_key ?? undefined,
    reason: r.reason ?? undefined,
    createdAt: Number(r.created_at),
    createdBy: r.created_by ?? undefined,
    expiresAt: optionalNumber(r.expires_at),
  };
}

const SESSION_COLUMNS = `id, transport, username, client_ip, target_key, filename,
       display_url, size, bytes_served, requests, started_at, last_seen_at,
       ended_at, end_reason, instance_id, stop_requested_at`;

/**
 * Persistence for unified stream sessions, their bandwidth rollups and the
 * temporary ban list. The in-memory registry owns live state; this is its
 * durable mirror, so writes are idempotent upserts driven by a periodic flush
 * rather than one write per HTTP request.
 */
export class StreamSessionRepository {
  /**
   * Insert or refresh a batch of sessions. The registry carries `started_at`
   * and `requests`, so the update takes them verbatim: every flush reports the
   * session's full current state.
   */
  static async upsertMany(rows: StreamSessionUpsert[]): Promise<void> {
    if (rows.length === 0) return;
    // One entry per id: an ON CONFLICT DO UPDATE cannot touch the same row
    // twice within one statement.
    const latest = new Map(rows.map((r) => [r.id, r]));
    const values = [...latest.values()].map(
      (r) => sql`(${r.id}, ${r.transport}, ${r.username}, ${r.clientIp ?? null},
                  ${r.targetKey}, ${r.filename ?? null}, ${r.displayUrl ?? null},
                  ${r.size}, ${r.bytesServed}, ${r.requests}, ${r.startedAt},
                  ${r.lastSeenAt}, ${r.endedAt ?? null}, ${r.endReason ?? null},
                  ${r.instanceId})`
    );
    const db = getDb();
    for (let i = 0; i < values.length; i += UPSERT_CHUNK) {
      await db.exec(
        sql`INSERT INTO stream_sessions
              (id, transport, username, client_ip, target_key, filename, display_url,
               size, bytes_served, requests, started_at, last_seen_at, ended_at,
               end_reason, instance_id)
            VALUES ${join(values.slice(i, i + UPSERT_CHUNK))}
            ON CONFLICT(id) DO UPDATE SET
              -- Rewritten so tightening the IP recording policy also scrubs
              -- rows that are still live.
              client_ip = EXCLUDED.client_ip,
              filename = EXCLUDED.filename,
              display_url = EXCLUDED.display_url,
              size = EXCLUDED.size,
              bytes_served = EXCLUDED.bytes_served,
              requests = EXCLUDED.requests,
              last_seen_at = EXCLUDED.last_seen_at,
              ended_at = EXCLUDED.ended_at,
              end_reason = EXCLUDED.end_reason`
      );
    }
  }

  /** Active (live or idle) sessions across every replica, newest first. */
  static async listActive(): Promise<StreamSessionRow[]> {
    const rows = await getDb().query<SessionDbRow>(
      sql`SELECT ${raw(SESSION_COLUMNS)}
            FROM stream_sessions
           WHERE ended_at IS NULL
           ORDER BY started_at DESC`
    );
    return rows.map(toSession);
  }

  /** Ended sessions, newest first, with the total for pagination. */
  static async listHistory(
    q: StreamHistoryQuery = {}
  ): Promise<{ entries: StreamSessionRow[]; total: number }> {
    const limit = Math.min(Math.max(q.limit ?? 50, 1), 500);
    const offset = Math.max(q.offset ?? 0, 0);
    const filters: SqlFragment[] = [sql`ended_at IS NOT NULL`];
    if (q.username) filters.push(sql`username = ${q.username}`);
    if (q.transport) filters.push(sql`transport = ${q.transport}`);
    if (q.search) {
      const like = `%${q.search.toLowerCase()}%`;
      filters.push(
        sql`(LOWER(COALESCE(filename, '')) LIKE ${like}
             OR LOWER(COALESCE(display_url, '')) LIKE ${like})`
      );
    }
    const where = join(filters, ' AND ');
    const [rows, total] = await Promise.all([
      getDb().query<SessionDbRow>(
        sql`SELECT ${raw(SESSION_COLUMNS)}
              FROM stream_sessions
             WHERE ${where}
             ORDER BY ended_at DESC
             LIMIT ${limit} OFFSET ${offset}`
      ),
      getDb().count(sql`SELECT COUNT(*) FROM stream_sessions WHERE ${where}`),
    ]);
    return { entries: rows.map(toSession), total };
  }

  /** One session by id, active or ended. */
  static async get(id: string): Promise<StreamSessionRow | undefined> {
    const row = await getDb().maybeOne<SessionDbRow>(
      sql`SELECT ${raw(SESSION_COLUMNS)} FROM stream_sessions WHERE id = ${id}`
    );
    return row ? toSession(row) : undefined;
  }

  /**
   * Flag an active session for termination. The replica that owns the live
   * reader picks this up on its next flush; a local stop does not wait for it.
   */
  static async requestStop(id: string, atMs = Date.now()): Promise<boolean> {
    const res = await getDb().exec(
      sql`UPDATE stream_sessions
             SET stop_requested_at = ${atMs}
           WHERE id = ${id} AND ended_at IS NULL`
    );
    return (res.rowCount ?? 0) > 0;
  }

  /** Ids this replica owns that another replica has asked to stop. */
  static async pendingStops(instanceId: string): Promise<string[]> {
    const rows = await getDb().query<{ id: string }>(
      sql`SELECT id FROM stream_sessions
           WHERE ended_at IS NULL
             AND instance_id = ${instanceId}
             AND stop_requested_at IS NOT NULL`
    );
    return rows.map((r) => r.id);
  }

  /**
   * Close out sessions this replica left behind when it died. Without this a
   * hard restart leaves phantom active rows that can never be stopped, because
   * no live process owns them.
   */
  static async endOrphans(
    instanceId: string,
    atMs = Date.now()
  ): Promise<number> {
    const res = await getDb().exec(
      sql`UPDATE stream_sessions
             SET ended_at = ${atMs}, end_reason = 'stale'
           WHERE ended_at IS NULL AND instance_id = ${instanceId}`
    );
    return res.rowCount ?? 0;
  }

  /**
   * Backstop for rows whose owning replica is never coming back (a lost
   * instance id, a removed replica). Any active row untouched since `cutoffMs`
   * is dead by definition: a live session is rewritten on every flush.
   */
  static async endStale(cutoffMs: number, atMs = Date.now()): Promise<number> {
    const res = await getDb().exec(
      sql`UPDATE stream_sessions
             SET ended_at = ${atMs}, end_reason = 'stale'
           WHERE ended_at IS NULL AND last_seen_at < ${cutoffMs}`
    );
    return res.rowCount ?? 0;
  }

  /** Force one active row closed, whoever owns it. */
  static async endById(
    id: string,
    reason: StreamEndReason,
    atMs = Date.now()
  ): Promise<boolean> {
    const res = await getDb().exec(
      sql`UPDATE stream_sessions
             SET ended_at = ${atMs}, end_reason = ${reason}
           WHERE id = ${id} AND ended_at IS NULL`
    );
    return (res.rowCount ?? 0) > 0;
  }

  /**
   * Delete specific history rows. Active sessions are skipped: removing a row
   * under a live reader would orphan its accounting.
   */
  static async deleteHistory(ids: string[]): Promise<number> {
    if (ids.length === 0) return 0;
    const list = join(ids.map((id) => sql`${id}`));
    const res = await getDb().exec(
      sql`DELETE FROM stream_sessions
           WHERE ended_at IS NOT NULL AND id IN (${list})`
    );
    return res.rowCount ?? 0;
  }

  /** Delete every finished session. Bandwidth rollups are untouched. */
  static async clearHistory(): Promise<number> {
    const res = await getDb().exec(
      sql`DELETE FROM stream_sessions WHERE ended_at IS NOT NULL`
    );
    return res.rowCount ?? 0;
  }

  /** Delete ended sessions older than the cutoff. */
  static async pruneOlderThan(cutoffMs: number): Promise<number> {
    const res = await getDb().exec(
      sql`DELETE FROM stream_sessions
           WHERE ended_at IS NOT NULL AND ended_at < ${cutoffMs}`
    );
    return res.rowCount ?? 0;
  }

  // --- bandwidth ----------------------------------------------------------

  /** Fold served-byte deltas into their hourly buckets. */
  static async addBandwidth(deltas: StreamBandwidthDelta[]): Promise<void> {
    // Summed per bucket first: two deltas landing in the same hour would
    // otherwise have one statement update the same row twice.
    const folded = new Map<
      string,
      {
        hour: number;
        username: string;
        transport: StreamTransport;
        bytes: number;
      }
    >();
    for (const d of deltas) {
      if (d.bytes <= 0) continue;
      const hour = hourFloor(d.atMs ?? Date.now());
      const key = JSON.stringify([hour, d.username, d.transport]);
      const seen = folded.get(key);
      if (seen) seen.bytes += d.bytes;
      else {
        folded.set(key, {
          hour,
          username: d.username,
          transport: d.transport,
          bytes: d.bytes,
        });
      }
    }
    if (folded.size === 0) return;
    const values = [...folded.values()].map(
      (d) => sql`(${d.hour}, ${d.username}, ${d.transport}, ${d.bytes})`
    );
    const db = getDb();
    for (let i = 0; i < values.length; i += UPSERT_CHUNK) {
      await db.exec(
        sql`INSERT INTO stream_bandwidth (hour_ms, username, transport, bytes)
            VALUES ${join(values.slice(i, i + UPSERT_CHUNK))}
            ON CONFLICT(hour_ms, username, transport) DO UPDATE SET
              bytes = stream_bandwidth.bytes + EXCLUDED.bytes`
      );
    }
  }

  /** Per-user, per-transport totals over `[sinceMs, now]`. */
  static async bandwidthByUser(
    sinceMs: number
  ): Promise<StreamBandwidthRollup[]> {
    const rows = await getDb().query<{
      username: string;
      transport: string;
      bytes: number | string;
    }>(
      sql`SELECT username, transport, SUM(bytes) AS bytes
            FROM stream_bandwidth
           WHERE hour_ms >= ${sinceMs}
           GROUP BY username, transport`
    );
    return rows.map((r) => ({
      username: r.username,
      transport: r.transport as StreamTransport,
      bytes: Number(r.bytes ?? 0),
    }));
  }

  /** Bucketed series over `[sinceMs, now]` for the bandwidth chart. */
  static async bandwidthSeries(
    sinceMs: number,
    bucketMs: number
  ): Promise<StreamBandwidthBucket[]> {
    const rows = await getDb().query<{
      bucket: number | string;
      bytes: number | string;
    }>(
      sql`SELECT ${bucketExpr(bucketMs)} AS bucket, SUM(bytes) AS bytes
            FROM stream_bandwidth
           WHERE hour_ms >= ${sinceMs}
           GROUP BY bucket
           ORDER BY bucket ASC`
    );
    return rows.map((r) => ({
      bucketMs: Number(r.bucket),
      bytes: Number(r.bytes ?? 0),
    }));
  }

  /** The same buckets split per user, for the per-user chart view. */
  static async bandwidthSeriesByUser(
    sinceMs: number,
    bucketMs: number
  ): Promise<StreamBandwidthUserBucket[]> {
    const rows = await getDb().query<{
      bucket: number | string;
      username: string;
      bytes: number | string;
    }>(
      sql`SELECT ${bucketExpr(bucketMs)} AS bucket, username, SUM(bytes) AS bytes
            FROM stream_bandwidth
           WHERE hour_ms >= ${sinceMs}
           GROUP BY bucket, username
           ORDER BY bucket ASC`
    );
    return rows.map((r) => ({
      bucketMs: Number(r.bucket),
      username: r.username,
      bytes: Number(r.bytes ?? 0),
    }));
  }

  /** Delete bandwidth buckets older than the cutoff. */
  static async pruneBandwidthOlderThan(cutoffMs: number): Promise<number> {
    const res = await getDb().exec(
      sql`DELETE FROM stream_bandwidth WHERE hour_ms < ${cutoffMs}`
    );
    return res.rowCount ?? 0;
  }

  // --- bans ---------------------------------------------------------------

  /** Every ban that has not expired as of `nowMs`. */
  static async listBans(nowMs = Date.now()): Promise<StreamBan[]> {
    const rows = await getDb().query<BanDbRow>(
      sql`SELECT id, scope, username, target_key, reason, created_at, created_by, expires_at
            FROM stream_bans
           WHERE expires_at IS NULL OR expires_at > ${nowMs}
           ORDER BY created_at DESC`
    );
    return rows.map(toBan);
  }

  static async addBan(ban: StreamBan): Promise<void> {
    await getDb().exec(
      sql`INSERT INTO stream_bans
            (id, scope, username, target_key, reason, created_at, created_by, expires_at)
          VALUES
            (${ban.id}, ${ban.scope}, ${ban.username}, ${ban.targetKey ?? null},
             ${ban.reason ?? null}, ${ban.createdAt}, ${ban.createdBy ?? null},
             ${ban.expiresAt ?? null})`
    );
  }

  static async removeBan(id: string): Promise<boolean> {
    const res = await getDb().exec(
      sql`DELETE FROM stream_bans WHERE id = ${id}`
    );
    return (res.rowCount ?? 0) > 0;
  }

  /** Drop bans whose expiry has passed. */
  static async pruneExpiredBans(nowMs = Date.now()): Promise<number> {
    const res = await getDb().exec(
      sql`DELETE FROM stream_bans
           WHERE expires_at IS NOT NULL AND expires_at <= ${nowMs}`
    );
    return res.rowCount ?? 0;
  }
}
