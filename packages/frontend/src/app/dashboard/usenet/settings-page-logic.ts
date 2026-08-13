import type { UsenetProfilePreset } from './queries';

export const BUNDLED_LEAVES = [
  'prefetchSegments',
  'maxConcurrentDownloads',
  'segmentDiskCacheBytes',
] as const;

export type BundledLeaf = (typeof BUNDLED_LEAVES)[number];

export const PROFILE_LEAF = 'performanceProfile';
export const STREAMING_MODE_LEAF = 'streamingMode';

export const usenetKey = (leaf: string) => `usenet.${leaf}`;
export const leafOf = (key: string) => key.replace(/^usenet\./, '');

interface SettingsSection {
  readonly id: string;
  readonly title: string;
  readonly leaves: readonly string[];
  readonly note?: string;
  readonly spoolingOnly?: boolean;
}

export interface SettingsSectionGroup<T> {
  readonly id: string;
  readonly title: string;
  readonly note?: string;
  readonly keys: T[];
}

const SECTIONS: readonly SettingsSection[] = [
  {
    id: 'streaming-mode',
    title: 'Streaming mode',
    leaves: [STREAMING_MODE_LEAF],
    note: 'The streaming mode is independent of the performance profile. Because it changes the storage structure, a mode change requires a restart before it takes effect.',
  },
  {
    id: 'performance',
    title: 'Performance',
    leaves: [PROFILE_LEAF, ...BUNDLED_LEAVES],
    note: 'Pick a profile and the values below are filled in for you — that is all most setups need. Editing any of the values switches the profile to **custom**. `segmentDiskCacheBytes` controls the persistent LRU cache; it is separate from the transient Segment Spooling spool.',
  },
  {
    id: 'memory-cache',
    title: 'Memory cache',
    leaves: ['segmentMemoryCacheBytes'],
  },
  {
    id: 'segment-spooling',
    title: 'Segment Spooling',
    leaves: [
      'segmentSpoolingMemoryBudgetBytes',
      'segmentSpoolingStreamBufferBytes',
      'segmentSpoolingSpoolBytes',
      'segmentSpoolingMinFreeDiskBytes',
    ],
    note: '`segmentSpoolingSpoolBytes` controls the transient spool; it is separate from `segmentDiskCacheBytes`, the persistent LRU cache.',
    spoolingOnly: true,
  },
  {
    id: 'connections-timeouts',
    title: 'Connections & timeouts',
    leaves: [
      'streamingPriority',
      'segmentTimeout',
      'segmentStallTimeout',
      'dialTimeout',
      'idleConnection',
      'streamIdleTimeout',
    ],
  },
  {
    id: 'reliability',
    title: 'Reliability',
    leaves: ['circuitBreakerThreshold', 'circuitBreakerCooldown'],
  },
  {
    id: 'archive-handling',
    title: 'Archive handling',
    leaves: ['lazyRarResolution', 'strictArchiveMembership'],
  },
  {
    id: 'verification',
    title: 'Verification',
    leaves: [
      'verifyMode',
      'verifyBudgetMs',
      'damagePolicy',
      'matroskaHoleFill',
      'censusShadowConcurrency',
      'censusMaxLifetime',
    ],
    note:
      'When something is imported, AIOStreams checks that it can actually be downloaded from your providers — so broken or incomplete releases are caught up front instead of failing mid-playback. ' +
      'The checks run alongside the import, so they normally add no waiting time: badly damaged releases are rejected, slightly damaged ones are recorded as “degraded”, the damage policy below decides whether those are still offered as streams. Any checking that did not finish during the import simply continues in the background. ' +
      'A stream that is already playing is never interrupted by these verdicts; they apply from the next playback onwards. ' +
      'Providers that give unreliable answers are detected and ignored automatically.',
  },
  {
    id: 'import-api',
    title: 'Import & API',
    leaves: ['maxNzbSize', 'maxConcurrentInspects', 'sabnzbdApiEnabled'],
  },
];

export function isSegmentSpoolingMode(
  value: unknown
): value is 'segment_spooling' {
  return value === 'segment_spooling';
}

export function groupUsenetSettings<T extends { readonly key: string }>(
  keys: readonly T[],
  streamingMode: unknown
): SettingsSectionGroup<T>[] {
  const byLeaf = new Map(keys.map((key) => [leafOf(key.key), key]));
  const mappedLeaves = new Set(SECTIONS.flatMap((section) => section.leaves));
  const showSpooling = isSegmentSpoolingMode(streamingMode);

  const groups: SettingsSectionGroup<T>[] = [];
  for (const section of SECTIONS) {
    if (section.spoolingOnly && !showSpooling) continue;
    const sectionKeys = section.leaves
      .map((leaf) => byLeaf.get(leaf))
      .filter((key): key is T => key !== undefined);
    if (sectionKeys.length === 0) continue;
    groups.push({
      id: section.id,
      title: section.title,
      note: section.note,
      keys: sectionKeys,
    });
  }

  const leftover = keys.filter((key) => !mappedLeaves.has(leafOf(key.key)));
  if (leftover.length > 0) {
    groups.push({ id: 'other', title: 'Other', keys: leftover });
  }
  return groups;
}

export function bundledValuesMatchPreset(
  current: Readonly<Record<BundledLeaf, unknown>>,
  preset: UsenetProfilePreset
): boolean {
  return BUNDLED_LEAVES.every((leaf) => Number(current[leaf]) === preset[leaf]);
}
