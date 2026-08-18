import type { CommandPriority } from '../types.js';

export type SegmentStreamCleanupCause =
  | 'eof'
  | 'client_close'
  | 'abort'
  | 'idle_reaper'
  | 'engine_close'
  | 'error';

export type UsenetResourceLifecycleEvent =
  | {
      readonly type: 'memory_wait_start';
      readonly kind: 'download' | 'stream';
      readonly bytes: number;
      readonly priority: CommandPriority;
      readonly queueDepth: number;
    }
  | {
      readonly type: 'memory_wait_end';
      readonly kind: 'download' | 'stream';
      readonly bytes: number;
      readonly priority: CommandPriority;
      readonly queueDepth: number;
      readonly waitMs: number;
      readonly outcome: 'granted' | 'aborted' | 'closed';
    }
  | {
      readonly type: 'spool_wait_start';
      readonly kind: 'spool';
      readonly bytes: number;
      readonly priority: CommandPriority;
      readonly queueDepth: number;
    }
  | {
      readonly type: 'spool_wait_end';
      readonly kind: 'spool';
      readonly bytes: number;
      readonly priority: CommandPriority;
      readonly queueDepth: number;
      readonly waitMs: number;
      readonly outcome:
        | 'granted'
        | 'aborted'
        | 'closed'
        | 'capacity_rejected'
        | 'disk_rejected';
    }
  | {
      readonly type: 'disk_safety_warning';
      readonly requestedBytes: number;
      readonly freeBytes: number;
      readonly requiredBytes: number;
      readonly minFreeDiskBytes: number;
    }
  | {
      readonly type: 'promotion_result';
      readonly outcome: 'success' | 'skipped' | 'failed';
    }
  | {
      readonly type: 'stream_cleanup';
      readonly cause: SegmentStreamCleanupCause;
    }
  | {
      readonly type: 'spool_cleanup_error';
      readonly operation: string;
    };

export type UsenetResourceEventObserver = (
  event: UsenetResourceLifecycleEvent
) => void;

const LOG_WINDOW_EVENTS = 256;
const LOG_WINDOW_MS = 10_000;
const LONG_WAIT_MS = 50;

export interface ResourceEventLogSink {
  debugEnabled(): boolean;
  debug(fields: Record<string, unknown>, message: string): void;
  warn(fields: Record<string, unknown>, message: string): void;
  emitted(): void;
  suppressed(): void;
}

interface ResourceEventLogWindow {
  count: number;
  totalBytes: number;
  maxQueueDepth: number;
  totalWaitMs: number;
  maxWaitMs: number;
  waitCount: number;
  granted: number;
  success: number;
  skipped: number;
  streamCleanups: number;
}

function emptyWindow(): ResourceEventLogWindow {
  return {
    count: 0,
    totalBytes: 0,
    maxQueueDepth: 0,
    totalWaitMs: 0,
    maxWaitMs: 0,
    waitCount: 0,
    granted: 0,
    success: 0,
    skipped: 0,
    streamCleanups: 0,
  };
}

/**
 * Fixed-size, event-driven logging window for high-frequency resource events.
 * Raw observers remain lossless; only successful debug serialization is
 * coalesced. There is no per-event history and no timer owned by this class.
 */
export class ResourceEventLogAggregator {
  private window = emptyWindow();
  private windowStartedAt: number;

  constructor(
    private readonly sink: ResourceEventLogSink,
    private readonly clock: () => number = Date.now
  ) {
    this.windowStartedAt = clock();
  }

  observe(event: UsenetResourceLifecycleEvent): void {
    if (this.isImmediate(event)) {
      this.emitImmediate(event);
      return;
    }
    this.sink.suppressed();
    if (!this.sink.debugEnabled()) return;
    this.addToWindow(event);
    const now = this.clock();
    if (
      this.window.count >= LOG_WINDOW_EVENTS ||
      now - this.windowStartedAt >= LOG_WINDOW_MS
    ) {
      this.flushAt(now);
    }
  }

  flush(): void {
    this.flushAt(this.clock());
  }

  private isImmediate(event: UsenetResourceLifecycleEvent): boolean {
    if (
      event.type === 'disk_safety_warning' ||
      event.type === 'spool_cleanup_error'
    ) {
      return true;
    }
    if (event.type === 'promotion_result') return event.outcome === 'failed';
    if (event.type === 'stream_cleanup') return event.cause === 'error';
    if (event.type !== 'memory_wait_end' && event.type !== 'spool_wait_end') {
      return false;
    }
    return event.outcome !== 'granted' || event.waitMs >= LONG_WAIT_MS;
  }

  private emitImmediate(event: UsenetResourceLifecycleEvent): void {
    if (event.type === 'disk_safety_warning') {
      this.sink.warn(event, 'usenet resource disk safety warning');
      this.sink.emitted();
      return;
    }
    if (
      event.type === 'spool_cleanup_error' ||
      (event.type === 'promotion_result' && event.outcome === 'failed') ||
      (event.type === 'stream_cleanup' && event.cause === 'error')
    ) {
      this.sink.warn(event, `usenet resource event: ${event.type}`);
      this.sink.emitted();
      return;
    }
    if (this.sink.debugEnabled()) {
      this.sink.debug(event, `usenet resource event: ${event.type}`);
      this.sink.emitted();
    } else {
      this.sink.suppressed();
    }
  }

  private addToWindow(event: UsenetResourceLifecycleEvent): void {
    this.window.count++;
    if ('bytes' in event) this.window.totalBytes += event.bytes;
    if ('queueDepth' in event) {
      this.window.maxQueueDepth = Math.max(
        this.window.maxQueueDepth,
        event.queueDepth
      );
    }
    if (event.type === 'memory_wait_end' || event.type === 'spool_wait_end') {
      this.window.waitCount++;
      this.window.totalWaitMs += event.waitMs;
      this.window.maxWaitMs = Math.max(this.window.maxWaitMs, event.waitMs);
      if (event.outcome === 'granted') this.window.granted++;
    } else if (event.type === 'promotion_result') {
      if (event.outcome === 'success') this.window.success++;
      if (event.outcome === 'skipped') this.window.skipped++;
    } else if (event.type === 'stream_cleanup') {
      this.window.streamCleanups++;
    }
  }

  private flushAt(now: number): void {
    if (this.window.count === 0) {
      this.windowStartedAt = now;
      return;
    }
    const window = this.window;
    this.window = emptyWindow();
    this.windowStartedAt = now;
    if (!this.sink.debugEnabled()) return;
    this.sink.debug(
      {
        resourceEvents: window.count,
        totalBytes: window.totalBytes,
        maxQueueDepth: window.maxQueueDepth,
        averageWaitMs:
          window.waitCount === 0 ? 0 : window.totalWaitMs / window.waitCount,
        maxWaitMs: window.maxWaitMs,
        granted: window.granted,
        promotionSuccess: window.success,
        promotionSkipped: window.skipped,
        streamCleanups: window.streamCleanups,
      },
      'usenet resource event summary'
    );
    this.sink.emitted();
  }
}
