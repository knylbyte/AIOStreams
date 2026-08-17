import React from 'react';
import {
  keepPreviousData,
  useMutation,
  useQuery,
  useQueryClient,
  type QueryClient,
} from '@tanstack/react-query';
import { api } from '@/lib/api';

export type StreamTransport = 'usenet' | 'proxy';

export type StreamEndReason =
  | 'idle'
  | 'stopped'
  | 'banned'
  | 'limit'
  | 'error'
  | 'stale'
  | 'shutdown';

/**
 * What a session is doing. `paused` and `idle` both mean no bytes are moving,
 * split by whether the client still holds its request open.
 */
export type StreamActivity = 'streaming' | 'paused' | 'idle';

/** One in-flight watch: every range read of a playback folded into one row. */
export interface LiveStreamSession {
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
  /** Open reads. A paused player still holds one. */
  activeReads: number;
  activity: StreamActivity;
  /** Time since the last byte reached the client. */
  idleMs: number;
  start: number;
  currentBytes: number;
  bytesPerSec: number;
  instanceId: string;
}

export interface LiveStreams {
  streams: LiveStreamSession[];
  summary: {
    streaming: number;
    paused: number;
    idle: number;
    totalBytesPerSec: number;
    /** Instance-wide concurrent-stream cap; 0 when unlimited. */
    connectionLimit: number;
  };
  /** The server's sampling interval; present only on streamed frames. */
  tickMs?: number;
}

export interface StreamHistoryRow {
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

/**
 * `24h` and `7d` are plain rolling windows; `30d` is the accounting period:
 * the last 30 days, or month-to-date when the period mode is monthly.
 */
export type BandwidthWindow = '24h' | '7d' | '30d';

export interface BandwidthOverview {
  window: BandwidthWindow;
  generatedAt: number;
  sinceMs: number;
  bucketMs: number;
  periodStart: number;
  periodMode: 'rolling' | 'monthly';
  total: number;
  byTransport: Record<StreamTransport, number>;
  byUser: Array<{
    username: string;
    bytes: number;
    limit: number;
    connectionLimit: number;
  }>;
  series: Array<{ bucketMs: number; bytes: number }>;
  seriesByUser: Array<{
    username: string;
    aggregated?: boolean;
    series: Array<{ bucketMs: number; bytes: number }>;
  }>;
  globalLimit: number;
  periodTotal: number;
}

export interface StreamBan {
  id: string;
  scope: 'user' | 'target';
  username: string;
  targetKey?: string;
  reason?: string;
  createdAt: number;
  createdBy?: string;
  expiresAt?: number;
}

const ROOT = ['dashboard', 'streams'] as const;
const LIVE_QUERY_KEY = [...ROOT, 'live'] as const;
const BANS_QUERY_KEY = [...ROOT, 'bans'] as const;

/** Ease window when no live frame has arrived yet. */
export const LIVE_FRAME_FALLBACK_MS = 500;

/** How long the UI should take to ease from the last live frame to this one. */
export function liveFrameMs(d: LiveStreams | undefined): number {
  return d?.tickMs ?? LIVE_FRAME_FALLBACK_MS;
}

/**
 * Browsers cap concurrent connections per host and this page can mount beside
 * the usenet live view, so the EventSource is refcounted and shared.
 */
let liveSource: EventSource | null = null;
let liveRefs = 0;

function subscribeLive(qc: QueryClient): () => void {
  liveRefs++;
  if (!liveSource) {
    liveSource = new EventSource('/api/v1/dashboard/streams/live/stream', {
      withCredentials: true,
    });
    // Browsers auto-reconnect SSE on transient errors; nothing to do on error.
    liveSource.onmessage = (e) => {
      try {
        qc.setQueryData(LIVE_QUERY_KEY, JSON.parse(e.data) as LiveStreams);
      } catch {
        /* ignore a malformed frame */
      }
    };
  }
  return () => {
    if (--liveRefs > 0) return;
    liveSource?.close();
    liveSource = null;
  };
}

export function useLiveStreams(enabled = true) {
  const qc = useQueryClient();
  React.useEffect(() => {
    if (!enabled) return;
    return subscribeLive(qc);
  }, [qc, enabled]);
  return useQuery({
    queryKey: LIVE_QUERY_KEY,
    queryFn: () => api<LiveStreams>('/dashboard/streams/live'),
    staleTime: Infinity,
    enabled,
  });
}

export function useStreamHistory(opts: {
  limit?: number;
  offset?: number;
  username?: string;
  transport?: StreamTransport | '';
  search?: string;
}) {
  const {
    limit = 50,
    offset = 0,
    username = '',
    transport = '',
    search = '',
  } = opts;
  const trimmed = search.trim();
  const qs = new URLSearchParams({
    limit: String(limit),
    offset: String(offset),
  });
  if (username) qs.set('username', username);
  if (transport) qs.set('transport', transport);
  if (trimmed) qs.set('q', trimmed);
  return useQuery({
    queryKey: [...ROOT, 'history', limit, offset, username, transport, trimmed],
    queryFn: () =>
      api<{ entries: StreamHistoryRow[]; total: number }>(
        `/dashboard/streams/history?${qs.toString()}`
      ),
    // Keep the previous page on screen while the next loads so `total` never
    // momentarily collapses.
    placeholderData: keepPreviousData,
    staleTime: 10_000,
  });
}

/**
 * Remove finished sessions: the given ids, or all of them when `ids` is
 * omitted. Bandwidth totals live in their own rollups and are unaffected.
 */
export function useDeleteStreamHistory() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (ids?: string[]) =>
      api<{ deleted: number }>('DELETE /dashboard/streams/history', {
        body: ids ? { ids } : undefined,
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: [...ROOT, 'history'] }),
  });
}

export function useBandwidth(window: BandwidthWindow) {
  return useQuery({
    queryKey: [...ROOT, 'bandwidth', window],
    queryFn: () =>
      api<BandwidthOverview>(`/dashboard/streams/bandwidth?window=${window}`),
    staleTime: 15_000,
    refetchInterval: 30_000,
  });
}

export function useStopStream() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      api(`DELETE /dashboard/streams/sessions/${encodeURIComponent(id)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: LIVE_QUERY_KEY }),
  });
}

export function useStopUserStreams() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (username: string) =>
      api<{ stopped: number }>(
        `POST /dashboard/streams/users/${encodeURIComponent(username)}/kill`
      ),
    onSuccess: () => qc.invalidateQueries({ queryKey: LIVE_QUERY_KEY }),
  });
}

export function useStreamBans() {
  return useQuery({
    queryKey: BANS_QUERY_KEY,
    queryFn: () => api<{ bans: StreamBan[] }>('/dashboard/streams/bans'),
    staleTime: 10_000,
  });
}

export interface CreateBanInput extends Record<string, unknown> {
  scope: 'user' | 'target';
  username: string;
  targetKey?: string;
  reason?: string;
  /** Omit for a block that holds until it is lifted by hand. */
  durationMs?: number;
}

export function useCreateStreamBan() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: CreateBanInput) =>
      api<{ ban: StreamBan; stopped: number }>('POST /dashboard/streams/bans', {
        body: input,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: BANS_QUERY_KEY });
      qc.invalidateQueries({ queryKey: LIVE_QUERY_KEY });
    },
  });
}

export function useLiftStreamBan() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      api(`DELETE /dashboard/streams/bans/${encodeURIComponent(id)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: BANS_QUERY_KEY }),
  });
}

/** Produce a proxified URL for an arbitrary upstream link. */
export function useGenerateProxyLink() {
  return useMutation({
    mutationFn: async (input: {
      url: string;
      filename?: string;
      requestHeaders?: Record<string, string>;
      responseHeaders?: Record<string, string>;
      type?: 'stream' | 'nzb';
      encrypt?: boolean;
    }) => {
      // `/proxy/generate` predates the standard envelope and answers with a
      // bare object, so it can't go through `api()`.
      const res = await fetch('/api/v1/proxy/generate', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(input),
      });
      const json = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(
          (json as any)?.error?.message ??
            (json as any)?.detail ??
            `Request failed (${res.status})`
        );
      }
      return json as { proxified_url: string };
    },
  });
}
