import { describe, expect, it } from 'vitest';
import {
  BUNDLED_LEAVES,
  bundledValuesMatchPreset,
  groupUsenetSettings,
} from '../settings-page-logic';

const setting = (leaf: string) => ({ key: `usenet.${leaf}` });

const spoolingLeaves = [
  'segmentSpoolingMemoryBudgetBytes',
  'segmentSpoolingStreamBufferBytes',
  'segmentSpoolingSpoolBytes',
  'segmentSpoolingMinFreeDiskBytes',
] as const;

const settings = [
  setting('streamingMode'),
  setting('performanceProfile'),
  setting('prefetchSegments'),
  setting('maxConcurrentDownloads'),
  setting('segmentDiskCacheBytes'),
  setting('segmentMemoryCacheBytes'),
  ...spoolingLeaves.map(setting),
];

describe('groupUsenetSettings', () => {
  it('hides Segment Spooling fields in segment_buffering mode', () => {
    const groups = groupUsenetSettings(settings, 'segment_buffering');

    expect(groups.map((group) => group.title)).toEqual([
      'Streaming mode',
      'Performance',
      'Memory cache',
    ]);
    expect(
      groups
        .flatMap((group) => group.keys)
        .filter(({ key }) => key.startsWith('usenet.segmentSpooling'))
    ).toEqual([]);
  });

  it('shows all Segment Spooling fields in segment_spooling mode', () => {
    const groups = groupUsenetSettings(settings, 'segment_spooling');
    const spooling = groups.find((group) => group.title === 'Segment Spooling');

    expect(spooling?.keys).toEqual(spoolingLeaves.map(setting));
    expect(
      groups.find((group) => group.title === 'Memory cache')?.keys
    ).toEqual([setting('segmentMemoryCacheBytes')]);
  });
});

describe('performance profile bundle', () => {
  const preset = {
    prefetchSegments: 32,
    maxConcurrentDownloads: 0,
    segmentDiskCacheBytes: 2_000_000_000,
  };

  it('remains limited to the three established leaves', () => {
    expect(BUNDLED_LEAVES).toEqual([
      'prefetchSegments',
      'maxConcurrentDownloads',
      'segmentDiskCacheBytes',
    ]);
  });

  it('compares the current values by their typed leaf names', () => {
    expect(
      bundledValuesMatchPreset(
        {
          prefetchSegments: '32',
          maxConcurrentDownloads: 0,
          segmentDiskCacheBytes: 2_000_000_000,
        },
        preset
      )
    ).toBe(true);
    expect(
      bundledValuesMatchPreset(
        {
          prefetchSegments: 16,
          maxConcurrentDownloads: 0,
          segmentDiskCacheBytes: 2_000_000_000,
        },
        preset
      )
    ).toBe(false);
  });
});
