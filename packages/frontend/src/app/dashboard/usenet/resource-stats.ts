import type { ResourceStats } from './queries';

export type ResourceSummaryItem =
  | {
      readonly id: 'arena' | 'memory' | 'spool';
      readonly label: string;
      readonly kind: 'bytes';
      readonly value: number;
      readonly max: number;
      readonly peak?: number;
      readonly waiting?: number;
      readonly reserved?: number;
    }
  | {
      readonly id: 'ownership';
      readonly label: string;
      readonly kind: 'count';
      readonly files: number;
      readonly sessions: number;
      readonly openFiles: number;
      readonly waiting: number;
    };

export function streamingModeLabel(
  mode: ResourceStats['streamingMode']
): string {
  return mode === 'segment_spooling' ? 'Segment Spooling' : 'Segment Buffering';
}

/** Compact dashboard model; chunk-level implementation details stay hidden. */
export function resourceSummaryItems(
  resources: ResourceStats
): readonly ResourceSummaryItem[] {
  const items: ResourceSummaryItem[] = [
    {
      id: 'arena',
      label: 'Segment arena',
      kind: 'bytes',
      value: resources.arena.usedBytes,
      max: resources.arena.budgetBytes,
    },
  ];
  if (resources.streamingMode === 'segment_buffering') return items;
  items.push(
    {
      id: 'memory',
      label: 'Transient memory',
      kind: 'bytes',
      value: resources.memory.usedBytes,
      max: resources.memory.maxBytes,
      peak: resources.memory.peakBytes,
      waiting: resources.memory.waiting,
    },
    {
      id: 'spool',
      label: 'Transient spool',
      kind: 'bytes',
      value: resources.spool.actualBytes,
      max: resources.spool.maxBytes,
      peak: resources.spool.peakActualBytes,
      reserved: resources.spool.reservedBytes,
    },
    {
      id: 'ownership',
      label: 'Spool ownership',
      kind: 'count',
      files: resources.spool.files,
      sessions: resources.spool.sessions,
      openFiles: resources.spool.openFiles,
      waiting: resources.spool.waiting,
    }
  );
  return items;
}
