import { describe, expect, it } from 'vitest';
import { resourceSummaryItems, streamingModeLabel } from '../resource-stats';
import type { ResourceStats } from '../queries';

function resources(mode: ResourceStats['streamingMode']): ResourceStats {
  return {
    streamingMode: mode,
    memory: {
      usedBytes: 1,
      maxBytes: 2,
      peakBytes: 3,
      waiting: 4,
      carryBytes: 20,
      carryChunks: 21,
      carryLimitBytes: 22,
    },
    spool: {
      reservedBytes: 5,
      actualBytes: 6,
      maxBytes: 7,
      peakReservedBytes: 8,
      peakActualBytes: 9,
      sessions: 10,
      files: 11,
      openFiles: 12,
      waiting: 13,
      writeBytesPerSec: 17,
      readBytesPerSec: 18,
      cleanupErrors: 19,
    },
    arena: { usedBytes: 14, budgetBytes: 15, exhaustions: 16 },
  };
}

describe('resource dashboard model', () => {
  it('keeps buffering compact and exposes only arena resources', () => {
    const items = resourceSummaryItems(resources('segment_buffering'));
    expect(streamingModeLabel('segment_buffering')).toBe('Segment Buffering');
    expect(items.map((item) => item.id)).toEqual(['arena']);
  });

  it('maps every spooling owner without chunk details', () => {
    const items = resourceSummaryItems(resources('segment_spooling'));
    expect(streamingModeLabel('segment_spooling')).toBe('Segment Spooling');
    expect(items.map((item) => item.id)).toEqual([
      'arena',
      'memory',
      'spool',
      'ownership',
    ]);
    expect(items[1]).toEqual({
      id: 'memory',
      label: 'Transient memory',
      kind: 'bytes',
      value: 1,
      max: 2,
      peak: 3,
      waiting: 4,
      carryBytes: 20,
      carryChunks: 21,
      carryLimitBytes: 22,
    });
    expect(items[3]).toEqual({
      id: 'ownership',
      label: 'Spool ownership',
      kind: 'count',
      files: 11,
      sessions: 10,
      openFiles: 12,
      waiting: 13,
      writeBytesPerSec: 17,
      readBytesPerSec: 18,
      cleanupErrors: 19,
    });
  });
});
