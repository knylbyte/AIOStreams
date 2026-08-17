import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import test from 'node:test';
import {
  buildUsenetEngineOptions,
  type UsenetEngineRuntimeSettings,
} from './engine.js';
import { UsenetResourcePlanConfigError } from '../resource-plan.js';
import { DEFAULT_ENGINE_OPTIONS, type ProviderConfig } from '../types.js';
import { usenetSchema } from '../../config/schema/usenet.js';
import type { SeekableStream } from '../pool/file-stream.js';
import type { Nzb } from '../nzb/model.js';
import type { SharedSegment } from '../pool/segment-arena.js';
import { CensusShadowOwner } from './census-shadow-owner.js';

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

class BarrierReader extends Readable {
  readonly destroyEntered = Promise.withResolvers<void>();

  constructor(
    private readonly gate: Promise<void>,
    private readonly replacementError?: Error
  ) {
    super({ read() {} });
  }

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    this.destroyEntered.resolve();
    void this.gate.then(() => callback(this.replacementError ?? error));
  }
}

function seekableReturning(reader: Readable): SeekableStream {
  return {
    filename: 'barrier.bin',
    size: () => 1,
    open: async () => undefined,
    readAt: async () => Buffer.from([0]),
    createReadStream: () => reader,
  };
}

const barrierNzb: Nzb = {
  hash: 'reader-close-barrier',
  meta: {},
  files: [],
};

interface EngineTestAccess {
  track(nzb: Nzb, stream: SeekableStream): SeekableStream;
  pool: {
    close(): Promise<void>;
    fetchSegmentShared(...args: readonly unknown[]): Promise<SharedSegment>;
  };
  cache: { close(): Promise<void> };
}

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
      writeBytesPerSec: resources.spool.writeBytesPerSec,
      readBytesPerSec: resources.spool.readBytesPerSec,
      cleanupErrors: resources.spool.cleanupErrors,
    },
    {
      memoryUsed: 0,
      reserved: 0,
      actual: 0,
      sessions: 0,
      files: 0,
      openFiles: 0,
      writeBytesPerSec: 0,
      readBytesPerSec: 0,
      cleanupErrors: 0,
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

test('registry replacement waits for the previous engine reader close', async () => {
  const { UsenetEngineRegistry } = await import('../index.js');
  const registry = new UsenetEngineRegistry(60_000);
  const firstProvider: ProviderConfig = {
    id: 'reader-first',
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
  const destroyGate = Promise.withResolvers<void>();
  const reader = new BarrierReader(destroyGate.promise);
  reader.on('error', () => undefined);
  const tracked = (first as unknown as EngineTestAccess).track(
    barrierNzb,
    seekableReturning(reader)
  );
  tracked.createReadStream();

  let replacementResolved = false;
  const replacement = registry.get(
    [{ ...firstProvider, id: 'reader-second', port: 120 }],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      segmentDiskCacheBytes: 0,
    }
  );
  void replacement.then(() => {
    replacementResolved = true;
  });
  await reader.destroyEntered.promise;
  await Promise.resolve();
  assert.equal(reader.destroyed, true);
  assert.equal(reader.closed, false);
  assert.equal(replacementResolved, false);

  destroyGate.resolve();
  const second = await replacement;
  assert.equal(reader.closed, true);
  assert.notEqual(second, first);
  await registry.closeAll();
});

test('registry replacement retires the old census before a same-hash reimport publishes', async () => {
  const { UsenetEngineRegistry } = await import('../index.js');
  const registry = new UsenetEngineRegistry(60_000);
  const firstProvider: ProviderConfig = {
    id: 'shadow-first',
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
  const owner = new CensusShadowOwner<{
    readonly complete: boolean;
    readonly generation: string;
  }>(2);
  const beforePublish = Promise.withResolvers<void>();
  const permitPublish = Promise.withResolvers<void>();
  const censusCancelled = Promise.withResolvers<void>();
  const writes: string[] = [];
  const shadow = owner.spawn({
    nzbHash: 'provider-retirement',
    census: {
      done: Promise.resolve({ complete: true, generation: 'old' }),
      cancel: () => censusCancelled.resolve(),
    },
    apply: async (snapshot, publication) => {
      beforePublish.resolve();
      await permitPublish.promise;
      await publication.step(async () => {
        writes.push(snapshot.generation);
      });
    },
    onError: (error) => {
      throw error;
    },
  });
  assert(shadow);
  assert.equal(first.trackCensusShadow(shadow), true);
  await beforePublish.promise;

  let replacementResolved = false;
  const replacement = registry.get(
    [{ ...firstProvider, id: 'shadow-second', port: 120 }],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      segmentDiskCacheBytes: 0,
    }
  );
  void replacement.then(() => {
    replacementResolved = true;
  });
  await censusCancelled.promise;
  const reimport = owner.spawn({
    nzbHash: 'provider-retirement',
    census: {
      done: Promise.resolve({ complete: true, generation: 'new' }),
      cancel: () => undefined,
    },
    apply: async (snapshot, publication) => {
      await publication.step(async () => {
        writes.push(snapshot.generation);
      });
    },
    onError: (error) => {
      throw error;
    },
  });
  assert(reimport);
  await Promise.resolve();
  assert.equal(replacementResolved, false);
  assert.deepEqual(writes, []);

  permitPublish.resolve();
  const [second] = await Promise.all([replacement, reimport.done]);
  assert.notEqual(second, first);
  assert.deepEqual(writes, ['new']);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
  await owner.close();
  await registry.closeAll();
});

test('registry replacement ignores a completed normal reader failure', async () => {
  const { UsenetEngineRegistry } = await import('../index.js');
  const registry = new UsenetEngineRegistry(60_000);
  const firstProvider: ProviderConfig = {
    id: 'runtime-error-first',
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
  const runtimeError = Object.assign(new Error('provider read failed'), {
    code: 'EIO',
  });
  const reader = new BarrierReader(Promise.resolve());
  const observed: unknown[] = [];
  reader.once('error', (error) => observed.push(error));
  const tracked = (first as unknown as EngineTestAccess).track(
    barrierNzb,
    seekableReturning(reader)
  );
  const publicReader = tracked.createReadStream();
  const closed = new Promise<void>((resolve) =>
    publicReader.once('close', resolve)
  );
  publicReader.destroy(runtimeError);
  await closed;
  assert.deepEqual(observed, [runtimeError]);

  const second = await registry.get(
    [{ ...firstProvider, id: 'runtime-error-second', port: 120 }],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      segmentDiskCacheBytes: 0,
    }
  );
  assert.notEqual(second, first);
  await registry.closeAll();
});

test('engine close accepts a reader already stopped by the stream registry shutdown', async () => {
  const [{ UsenetEngine }, { StreamRegistry }] = await Promise.all([
    import('../index.js'),
    import('../../stream-sessions/registry.js'),
  ]);
  const engine = new UsenetEngine([], {
    ...DEFAULT_ENGINE_OPTIONS,
    segmentDiskCacheBytes: 0,
  });
  const registry = new StreamRegistry(() => ({ ok: true }));
  const opened = registry.open({
    transport: 'usenet',
    username: 'shutdown-user',
    targetKey: 'coordinated-reader',
  });
  assert.equal(opened.ok, true);
  if (!opened.ok) return;

  const destroyGate = Promise.withResolvers<void>();
  const reader = new BarrierReader(destroyGate.promise);
  reader.on('error', () => undefined);
  const access = engine as unknown as EngineTestAccess;
  const tracked = access.track(barrierNzb, seekableReturning(reader));
  const publicReader = tracked.createReadStream();
  opened.handle.attach(publicReader);

  registry.sealAndCloseAll('shutdown');
  await reader.destroyEntered.promise;
  assert.equal(reader.destroyed, true);
  assert.equal(reader.closed, false);

  let settled = false;
  const closing = engine.close().then(() => {
    settled = true;
  });
  await Promise.resolve();
  assert.equal(settled, false);

  destroyGate.resolve();
  await closing;
  assert.equal(reader.closed, true);
  assert.equal(engine.liveStats().tiles.activeStreams, 0);
  assert.equal(engine.liveStats().resources.memory.usedBytes, 0);
  assert.equal(engine.liveStats().resources.spool.files, 0);
});

test('engine close aggregates reader and pool failures after cache cleanup', async () => {
  const { UsenetEngine } = await import('../index.js');
  const engine = new UsenetEngine([], {
    ...DEFAULT_ENGINE_OPTIONS,
    segmentDiskCacheBytes: 0,
  });
  const readerGate = Promise.withResolvers<void>();
  const readerError = new Error('synthetic reader close failure');
  const poolError = new Error('synthetic pool close failure');
  const reader = new BarrierReader(readerGate.promise, readerError);
  reader.on('error', () => undefined);
  const access = engine as unknown as EngineTestAccess;
  access.track(barrierNzb, seekableReturning(reader)).createReadStream();

  const originalPoolClose = access.pool.close.bind(access.pool);
  access.pool.close = async () => {
    await originalPoolClose();
    throw poolError;
  };
  let cacheCloseCalls = 0;
  const originalCacheClose = access.cache.close.bind(access.cache);
  access.cache.close = async () => {
    cacheCloseCalls++;
    await originalCacheClose();
  };

  const closing = engine.close();
  await reader.destroyEntered.promise;
  readerGate.resolve();
  await assert.rejects(closing, (error: unknown) => {
    assert.ok(error instanceof AggregateError);
    assert.ok(error.errors.includes(readerError));
    assert.ok(error.errors.includes(poolError));
    return true;
  });
  assert.equal(cacheCloseCalls, 1);
  assert.equal(reader.closed, true);
  assert.equal(engine.liveStats().tiles.activeStreams, 0);
});

test('an engine open crossing close cannot publish a seekable handle', async () => {
  const { UsenetEngine } = await import('../index.js');
  const engine = new UsenetEngine([], {
    ...DEFAULT_ENGINE_OPTIONS,
    segmentDiskCacheBytes: 0,
  });
  const access = engine as unknown as EngineTestAccess;
  const entered = Promise.withResolvers<void>();
  const proceed = Promise.withResolvers<void>();
  let releases = 0;
  access.pool.fetchSegmentShared = async () => {
    entered.resolve();
    await proceed.promise;
    return {
      data: {
        body: Buffer.from('data'),
        size: 4,
        fileSize: 4,
        name: 'crossing.bin',
      },
      owned: true,
      release: () => {
        releases++;
      },
    };
  };
  const nzb: Nzb = {
    hash: 'crossing-open',
    meta: {},
    files: [
      {
        subject: 'crossing.bin',
        groups: ['alt.binaries.test'],
        encodedSize: 8,
        filename: 'crossing.bin',
        segments: [{ messageId: 'crossing', number: 1, bytes: 8 }],
      },
    ],
  };

  const opening = engine.openFileStream(nzb, { fileIndex: 0 });
  await entered.promise;
  const firstClose = engine.close();
  assert.equal(engine.close(), firstClose);
  await firstClose;
  proceed.resolve();

  await assert.rejects(
    opening,
    (error: unknown) =>
      (error as NodeJS.ErrnoException).code === 'USENET_ENGINE_CLOSED'
  );
  assert.equal(releases, 1);
  assert.equal(engine.liveStats().tiles.activeStreams, 0);
});

test('engine close synchronously fences every public work entry point', async () => {
  const { UsenetEngine } = await import('../index.js');
  const engine = new UsenetEngine([], {
    ...DEFAULT_ENGINE_OPTIONS,
    segmentDiskCacheBytes: 0,
  });
  const nzb: Nzb = { hash: 'closed-work', meta: {}, files: [] };

  const closing = engine.close();
  const outcomes = await Promise.allSettled([
    engine.inspect(nzb),
    engine.openFileStream(nzb, { fileIndex: 0 }),
    engine.selectAndOpen(nzb),
    engine.fetchArticle({ messageId: 'closed', bytes: 1 }, nzb.hash),
  ]);
  const reasons = outcomes.map((outcome) => {
    assert.equal(outcome.status, 'rejected');
    if (outcome.status !== 'rejected') {
      throw new Error('closed engine unexpectedly admitted work');
    }
    assert.equal(
      (outcome.reason as NodeJS.ErrnoException).code,
      'USENET_ENGINE_CLOSED'
    );
    return outcome.reason;
  });
  assert.ok(reasons.every((reason) => reason === reasons[0]));
  await closing;
});

test('registry close fences all getters waiting behind an active retirement', async () => {
  const { UsenetEngineRegistry } = await import('../index.js');
  const registry = new UsenetEngineRegistry(60_000);
  const provider: ProviderConfig = {
    id: 'shutdown-first',
    host: '127.0.0.1',
    port: 119,
    tls: false,
    maxConnections: 1,
    priority: 0,
  };
  const first = await registry.get([provider], {
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

  const replacementProviders = [
    { ...provider, id: 'shutdown-next', port: 120 },
  ];
  const getters = Array.from({ length: 4 }, () =>
    registry.get(replacementProviders, {
      ...DEFAULT_ENGINE_OPTIONS,
      segmentDiskCacheBytes: 0,
    })
  );
  const getterOutcomes = Promise.allSettled(getters);
  await closeStarted.promise;
  const firstClose = registry.closeAll();
  const secondClose = registry.closeAll();
  assert.equal(secondClose, firstClose);
  permitClose.resolve();

  await firstClose;
  for (const outcome of await getterOutcomes) {
    assert.equal(outcome.status, 'rejected');
    if (outcome.status === 'rejected') {
      assert.match(String(outcome.reason), /registry is closed/);
    }
  }
  assert.equal(registry.size, 0);
  await assert.rejects(
    registry.get(replacementProviders, {
      ...DEFAULT_ENGINE_OPTIONS,
      segmentDiskCacheBytes: 0,
    }),
    /registry is closed/
  );
});

test('stream registry shutdown seal destroys active reads and refuses later admission', async () => {
  const { StreamRegistry } = await import('../../stream-sessions/registry.js');
  const registry = new StreamRegistry(() => ({ ok: true }));
  const input = {
    transport: 'usenet' as const,
    username: '',
    targetKey: 'shutdown-test',
  };
  const opened = registry.open(input);
  assert.equal(opened.ok, true);
  if (!opened.ok) return;
  const stream = new Readable({ read() {} });
  stream.on('error', () => undefined);
  let killed = 0;
  opened.handle.attach(stream);
  opened.handle.onKill(() => {
    killed++;
  });

  registry.sealAndCloseAll('stale');

  assert.equal(killed, 1);
  assert.equal(stream.destroyed, true);
  assert.equal(registry.isSealed, true);
  assert.deepEqual(registry.open(input), {
    ok: false,
    verdict: {
      ok: false,
      reason: 'shutdown',
      message: 'Server is shutting down',
    },
  });
  registry.sealAndCloseAll('stale');
  assert.equal(killed, 1);
});
