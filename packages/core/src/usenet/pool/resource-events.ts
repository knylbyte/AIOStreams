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
