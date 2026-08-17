import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildUsenetEngineOptions,
  type UsenetEngineRuntimeSettings,
} from './engine.js';
import { UsenetResourcePlanConfigError } from '../resource-plan.js';
import { DEFAULT_ENGINE_OPTIONS, type ProviderConfig } from '../types.js';
import { usenetSchema } from '../../config/schema/usenet.js';

const MEBIBYTE_BYTES = 1024 * 1024;

const providers: ProviderConfig[] = [
  {
    id: 'primary',
    host: 'news.example.test',
    port: 563,
    tls: true,
    maxConnections: 2,
    pipelineDepth: 3,
    priority: 0,
  },
];

function runtimeSettings(
  overrides: Partial<UsenetEngineRuntimeSettings> = {}
): UsenetEngineRuntimeSettings {
  return {
    performanceProfile: 'custom',
    maxConcurrentDownloads: 7,
    prefetchSegments: 11,
    streamingMode: 'segment_spooling',
    streamingPriority: 0.75,
    segmentMemoryCacheBytes: 24_000_000,
    segmentSpoolingMemoryBudgetBytes: 96_000_000,
    segmentSpoolingStreamBufferBytes: 8_000_000,
    segmentSpoolingSpoolBytes: 3_000_000_000,
    segmentSpoolingMinFreeDiskBytes: 600_000_000,
    segmentDiskCacheBytes: 4_000_000_000,
    segmentTimeout: 31,
    segmentStallTimeout: 32,
    dialTimeout: 16,
    idleConnection: 61,
    streamIdleTimeout: 3_601,
    circuitBreakerThreshold: 6,
    circuitBreakerCooldown: 33,
    lazyRarResolution: false,
    strictArchiveMembership: true,
    verifyMode: 'none',
    verifyBudgetMs: 123,
    censusShadowConcurrency: 13,
    censusMaxLifetime: 1_801,
    ...overrides,
  };
}

test('buildUsenetEngineOptions maps all explicit resource overrides in bytes', () => {
  const options = buildUsenetEngineOptions(
    providers,
    runtimeSettings(),
    () => '/test/cache'
  );
  assert.deepEqual(options, {
    maxConcurrentDownloads: 7,
    prefetchSegments: 11,
    streamingMode: 'segment_spooling',
    streamingPriority: 0.75,
    segmentMemoryCacheBytes: 24_000_000,
    segmentSpoolingMemoryBudgetBytes: 96_000_000,
    segmentSpoolingStreamBufferBytes: 8_000_000,
    segmentSpoolingSpoolBytes: 3_000_000_000,
    segmentSpoolingMinFreeDiskBytes: 600_000_000,
    segmentDiskCacheBytes: 4_000_000_000,
    segmentDiskCachePath: '/test/cache',
    segmentTimeoutMs: 31_000,
    segmentStallTimeoutMs: 32_000,
    dialTimeoutMs: 16_000,
    idleConnectionMs: 61_000,
    streamIdleTimeoutMs: 3_601_000,
    circuitBreakerThreshold: 6,
    circuitBreakerCooldownMs: 33_000,
    lazyRarResolution: false,
    strictArchiveMembership: true,
    verifyMode: 'none',
    verifyBudgetMs: 123,
    censusShadowConcurrency: 13,
    censusMaxLifetimeMs: 1_801_000,
  });
});

test('buildUsenetEngineOptions maps the runtime schema defaults', () => {
  const options = buildUsenetEngineOptions(
    providers,
    runtimeSettings({
      streamingMode: usenetSchema.streamingMode.default,
      segmentMemoryCacheBytes: usenetSchema.segmentMemoryCacheBytes.default,
      segmentSpoolingMemoryBudgetBytes:
        usenetSchema.segmentSpoolingMemoryBudgetBytes.default,
      segmentSpoolingStreamBufferBytes:
        usenetSchema.segmentSpoolingStreamBufferBytes.default,
      segmentSpoolingSpoolBytes: usenetSchema.segmentSpoolingSpoolBytes.default,
      segmentSpoolingMinFreeDiskBytes:
        usenetSchema.segmentSpoolingMinFreeDiskBytes.default,
    }),
    () => '/test/cache'
  );

  assert.equal(options.streamingMode, 'segment_buffering');
  assert.equal(options.segmentMemoryCacheBytes, 0);
  assert.equal(options.segmentSpoolingMemoryBudgetBytes, 128_000_000);
  assert.equal(options.segmentSpoolingStreamBufferBytes, 8_000_000);
  assert.equal(options.segmentSpoolingSpoolBytes, 2_000_000_000);
  assert.equal(options.segmentSpoolingMinFreeDiskBytes, 512_000_000);
});

test('performance profiles do not override mode or segment-spooling fields', () => {
  const options = buildUsenetEngineOptions(
    providers,
    runtimeSettings({
      performanceProfile: 'balanced',
      maxConcurrentDownloads: 99,
      prefetchSegments: 99,
      streamingMode: 'segment_spooling',
      segmentMemoryCacheBytes: 33_000_000,
      segmentSpoolingMemoryBudgetBytes: 128_000_000,
      segmentSpoolingStreamBufferBytes: 12_000_000,
      segmentSpoolingSpoolBytes: 5_000_000_000,
      segmentSpoolingMinFreeDiskBytes: 700_000_000,
      segmentDiskCacheBytes: 99,
    }),
    () => '/profile/cache'
  );

  assert.equal(options.maxConcurrentDownloads, 6);
  assert.equal(options.prefetchSegments, 32);
  assert.equal(options.segmentDiskCacheBytes, 2_000_000_000);
  assert.equal(options.streamingMode, 'segment_spooling');
  assert.equal(options.segmentMemoryCacheBytes, 33_000_000);
  assert.equal(options.segmentSpoolingMemoryBudgetBytes, 128_000_000);
  assert.equal(options.segmentSpoolingStreamBufferBytes, 12_000_000);
  assert.equal(options.segmentSpoolingSpoolBytes, 5_000_000_000);
  assert.equal(options.segmentSpoolingMinFreeDiskBytes, 700_000_000);
});

test('buildUsenetEngineOptions validates spooling combinations centrally', () => {
  assert.throws(
    () =>
      buildUsenetEngineOptions(
        providers,
        runtimeSettings({
          segmentSpoolingMemoryBudgetBytes: 16 * MEBIBYTE_BYTES,
          segmentSpoolingStreamBufferBytes: 8 * MEBIBYTE_BYTES + 1,
        }),
        () => '/test/cache'
      ),
    UsenetResourcePlanConfigError
  );

  assert.doesNotThrow(() =>
    buildUsenetEngineOptions(
      providers,
      runtimeSettings({
        streamingMode: 'segment_buffering',
        segmentSpoolingMemoryBudgetBytes: 0,
        segmentSpoolingStreamBufferBytes: 0,
        segmentSpoolingSpoolBytes: 0,
      }),
      () => '/test/cache'
    )
  );
});

test('engine live stats expose the effective buffering resource plan', async () => {
  const { UsenetEngine } = await import('../index.js');
  const engine = new UsenetEngine([], {
    ...DEFAULT_ENGINE_OPTIONS,
    streamingMode: 'segment_buffering',
    segmentDiskCacheBytes: 0,
  });
  const snapshot = engine.liveStats();
  assert.equal(snapshot.resources.streamingMode, 'segment_buffering');
  assert.deepEqual(snapshot.resources.memory, {
    usedBytes: 0,
    maxBytes: 0,
    peakBytes: 0,
    waiting: 0,
  });
  assert.equal(
    snapshot.resources.arena.budgetBytes,
    engine.resourcePlan.arenaBytes
  );
  assert.equal(snapshot.resources.arena.usedBytes, 0);
  assert.equal(snapshot.resources.arena.exhaustions, 0);
  await engine.close();
});

test('engine live stats are sourced from spooling memory, disk and file owners', async () => {
  const { UsenetEngine } = await import('../index.js');
  const engine = new UsenetEngine([], {
    ...DEFAULT_ENGINE_OPTIONS,
    streamingMode: 'segment_spooling',
    segmentDiskCacheBytes: 0,
  });
  const plan = engine.resourcePlan.segmentSpooling;
  assert(plan);
  const resources = engine.liveStats().resources;
  assert.equal(resources.streamingMode, 'segment_spooling');
  assert.equal(resources.memory.maxBytes, plan.memoryBudgetBytes);
  assert.equal(resources.spool.maxBytes, plan.spoolBytes);
  assert.deepEqual(
    {
      memoryUsed: resources.memory.usedBytes,
      reserved: resources.spool.reservedBytes,
      actual: resources.spool.actualBytes,
      sessions: resources.spool.sessions,
      files: resources.spool.files,
      openFiles: resources.spool.openFiles,
    },
    {
      memoryUsed: 0,
      reserved: 0,
      actual: 0,
      sessions: 0,
      files: 0,
      openFiles: 0,
    }
  );
  await engine.close();
});

test('registry waits for the previous stable cache writer before replacement', async () => {
  const { UsenetEngineRegistry } = await import('../index.js');
  const registry = new UsenetEngineRegistry(60_000);
  const firstProvider: ProviderConfig = {
    id: 'first',
    host: '127.0.0.1',
    port: 119,
    tls: false,
    maxConnections: 1,
    priority: 0,
  };
  const first = await registry.get([firstProvider], {
    ...DEFAULT_ENGINE_OPTIONS,
    segmentDiskCacheBytes: 0,
  });
  const closeStarted = Promise.withResolvers<void>();
  const permitClose = Promise.withResolvers<void>();
  const originalClose = first.close.bind(first);
  first.close = async () => {
    closeStarted.resolve();
    await permitClose.promise;
    await originalClose();
  };

  let replacementResolved = false;
  const replacement = registry.get(
    [{ ...firstProvider, id: 'second', port: 120 }],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      segmentDiskCacheBytes: 0,
    }
  );
  void replacement.then(() => {
    replacementResolved = true;
  });
  await closeStarted.promise;
  await Promise.resolve();
  assert.equal(replacementResolved, false);

  permitClose.resolve();
  const second = await replacement;
  assert.notEqual(second, first);
  assert.equal(replacementResolved, true);
  await registry.closeAll();
});
