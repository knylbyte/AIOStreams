import assert from 'node:assert/strict';
import test from 'node:test';
import { PERFORMANCE_PROFILES, usenetSchema } from './usenet.js';

const NEW_FIELDS = [
  'streamingMode',
  'segmentMemoryCacheBytes',
  'segmentSpoolingMemoryBudgetBytes',
  'segmentSpoolingStreamBufferBytes',
  'segmentSpoolingSpoolBytes',
  'segmentSpoolingMinFreeDiskBytes',
] as const;

test('Usenet streaming resource fields expose the specified metadata', () => {
  assert.deepEqual(
    NEW_FIELDS.map((name) => ({
      name,
      env: usenetSchema[name].env,
      default: usenetSchema[name].default,
      label: usenetSchema[name].label,
      description: usenetSchema[name].description,
      requiresRestart: usenetSchema[name].requiresRestart,
      secret: usenetSchema[name].secret,
      ui: usenetSchema[name].ui,
    })),
    [
      {
        name: 'streamingMode',
        env: 'USENET_STREAMING_MODE',
        default: 'segment_buffering',
        label: 'Streaming mode',
        description:
          'How decoded Usenet segments are transported to the player. ' +
          '**segment_buffering** uses the existing compatible, RAM-oriented ' +
          'data path. **segment_spooling** uses bounded memory and a transient ' +
          'disk spool. This setting is independent of the performance profile.',
        requiresRestart: true,
        secret: false,
        ui: { hidden: true },
      },
      {
        name: 'segmentMemoryCacheBytes',
        env: 'USENET_SEGMENT_MEMORY_CACHE_BYTES',
        default: 0,
        label: 'Segment memory cache size',
        description:
          'The memory budget for the decoded-segment arena. **0** (the default) ' +
          'sizes it automatically for the selected streaming mode; it does not ' +
          'disable the arena.',
        requiresRestart: true,
        secret: false,
        ui: { hidden: true },
      },
      {
        name: 'segmentSpoolingMemoryBudgetBytes',
        env: 'USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES',
        default: 128_000_000,
        label: 'Segment spooling memory budget',
        description:
          'The global hard memory budget for transient buffers and queues owned ' +
          'by segment spooling. It is used only in **segment_spooling** mode.',
        requiresRestart: true,
        secret: false,
        ui: { hidden: true },
      },
      {
        name: 'segmentSpoolingStreamBufferBytes',
        env: 'USENET_SEGMENT_SPOOLING_STREAM_BUFFER_BYTES',
        default: 8_000_000,
        label: 'Segment spooling stream buffer',
        description:
          'The maximum buffer share reserved for each active HTTP stream in ' +
          '**segment_spooling** mode.',
        requiresRestart: true,
        secret: false,
        ui: { hidden: true },
      },
      {
        name: 'segmentSpoolingSpoolBytes',
        env: 'USENET_SEGMENT_SPOOLING_SPOOL_BYTES',
        default: 2_000_000_000,
        label: 'Segment spooling spool size',
        description:
          'The global hard disk budget for transient segment spool files. This ' +
          'is separate from the persistent segment disk cache.',
        requiresRestart: true,
        secret: false,
        ui: { hidden: true },
      },
      {
        name: 'segmentSpoolingMinFreeDiskBytes',
        env: 'USENET_SEGMENT_SPOOLING_MIN_FREE_DISK_BYTES',
        default: 512_000_000,
        label: 'Segment spooling minimum free disk space',
        description:
          'The free-space safety margin retained on the spool filesystem in ' +
          '**segment_spooling** mode.',
        requiresRestart: true,
        secret: false,
        ui: { hidden: true },
      },
    ]
  );
});

test('streaming mode and byte-size schemas accept explicit overrides', () => {
  assert.equal(
    usenetSchema.streamingMode.schema.parse('segment_buffering'),
    'segment_buffering'
  );
  assert.equal(
    usenetSchema.streamingMode.schema.parse('segment_spooling'),
    'segment_spooling'
  );
  assert.throws(() => usenetSchema.streamingMode.schema.parse('automatic'));

  assert.equal(
    usenetSchema.segmentMemoryCacheBytes.schema.parse('24MB'),
    24_000_000
  );
  assert.equal(
    usenetSchema.segmentSpoolingMemoryBudgetBytes.schema.parse('96MB'),
    96_000_000
  );
  assert.equal(
    usenetSchema.segmentSpoolingStreamBufferBytes.schema.parse('12MB'),
    12_000_000
  );
  assert.equal(
    usenetSchema.segmentSpoolingSpoolBytes.schema.parse('8GB'),
    8_000_000_000
  );
  assert.equal(
    usenetSchema.segmentSpoolingMinFreeDiskBytes.schema.parse('1GB'),
    1_000_000_000
  );
});

test('performance profiles retain only their three existing fields', () => {
  assert.deepEqual(PERFORMANCE_PROFILES, {
    conservative: {
      prefetchSegments: 16,
      maxConcurrentDownloads: 30,
      segmentDiskCacheBytes: 1_000_000_000,
    },
    balanced: {
      prefetchSegments: 32,
      maxConcurrentDownloads: 0,
      segmentDiskCacheBytes: 2_000_000_000,
    },
    high: {
      prefetchSegments: 64,
      maxConcurrentDownloads: 0,
      segmentDiskCacheBytes: 8_000_000_000,
    },
  });
});
