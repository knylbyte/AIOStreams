import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test, { type TestContext } from 'node:test';
import type { BackpressuredByteSink } from './streaming-yenc-article-decoder.js';
import type {
  SegmentFetcher,
  SegmentHeadFetchOptions,
  SegmentHeadData,
  StatDetail,
  StreamingSegmentAttempt,
  StreamingSegmentResult,
} from '../nntp/segment-fetcher.js';
import { ArticleNotFoundError, NntpError } from '../nntp/errors.js';
import type {
  CreateSpoolArtifactOptions,
  SpoolManagerOptions,
} from '../spool/manager.js';
import { SpoolManager } from '../spool/manager.js';
import type { GrowingSpoolArtifact } from '../spool/growing-artifact.js';
import { UsenetSpoolError } from '../spool/errors.js';
import type { SegmentSpoolingPlan } from '../resource-plan.js';
import { StatsAccumulator } from '../stats/accumulator.js';
import {
  CommandPriority,
  DEFAULT_ENGINE_OPTIONS,
  type EngineOptions,
  type NzbSegmentRef,
  type ProviderPoolInfo,
  type SegmentData,
} from '../types.js';
import { ByteBudget } from './byte-budget.js';
import { SegmentCache } from './segment-cache.js';
import {
  ArenaSegmentArtifact,
  type SegmentArtifact,
  type SegmentArtifactFetchOptions,
} from './segment-artifact.js';
import type { SegmentArtifactCacheLookup } from './segment-artifact.js';
import { SegmentSpoolingRuntime } from './segment-spooling-runtime.js';
import { MultiProviderPool } from './multi-provider-pool.js';
import { SpoolingSegmentsStream } from './spooling-segments-stream.js';
import { YencDecodeError, YencMetadataError } from './yenc.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;

interface FakeBehavior {
  readonly body: Buffer;
  readonly attemptCreated?: PromiseWithResolvers<void>;
  readonly preWireGate?: Promise<void>;
  readonly gate?: Promise<void>;
  readonly started?: PromiseWithResolvers<void>;
  readonly finished?: PromiseWithResolvers<void>;
  readonly firstChunkBytes?: number;
  readonly firstChunkWritten?: PromiseWithResolvers<void>;
  readonly afterFirstChunkGate?: Promise<void>;
  readonly afterSinkEndGate?: Promise<void>;
  readonly afterBodyWriteGate?: Promise<void>;
  readonly bodyWritten?: PromiseWithResolvers<void>;
  readonly errorAfterBody?: Error;
  readonly headerExpectedSize?: number;
  readonly headerByteRange?: readonly [number, number];
  readonly metadataSize?: number;
  readonly metadataByteRange?: readonly [number, number];
  readonly metadataFileSize?: number;
  readonly error?: Error;
}

class FakeSegmentFetcher implements SegmentFetcher {
  readonly behaviors = new Map<string, FakeBehavior>();
  readonly headResults = new Map<string, SegmentHeadData | Error>();
  streamingCalls = 0;
  bufferingCalls = 0;
  headCalls = 0;
  lastHeadOptions: SegmentHeadFetchOptions | undefined;
  closed = false;

  async fetchBody(
    segment: NzbSegmentRef,
    _nzbHash: string,
    _priority: CommandPriority,
    _out?: () => Buffer,
    _signal?: AbortSignal,
    onWireStart?: () => void
  ): Promise<SegmentData> {
    this.bufferingCalls++;
    onWireStart?.();
    const body = Buffer.from(
      this.behaviors.get(segment.messageId)?.body ?? 'buffering-body'
    );
    return { body, size: body.length, name: 'buffering.bin' };
  }

  async fetchBodyToSink<T>(
    segment: NzbSegmentRef,
    _nzbHash: string,
    _priority: CommandPriority,
    createAttempt: () => Promise<StreamingSegmentAttempt<T>>,
    signal?: AbortSignal,
    onWireStart?: () => void
  ): Promise<StreamingSegmentResult<T>> {
    if (signal?.aborted) throw new NntpError('connection', 'aborted');
    this.streamingCalls++;
    const behavior = this.behaviors.get(segment.messageId) ?? {
      body: Buffer.from('streaming-body'),
    };
    let attempt: StreamingSegmentAttempt<T> | undefined;
    try {
      attempt = await createAttempt();
      behavior.attemptCreated?.resolve();
      await behavior.preWireGate;
      if (signal?.aborted) throw new NntpError('connection', 'aborted');
      onWireStart?.();
      behavior.started?.resolve();
      await behavior.gate;
      if (behavior.error) throw behavior.error;
      attempt.onHeader?.({
        byteRange: behavior.headerByteRange ?? [0, behavior.body.length],
        fileSize: behavior.metadataFileSize ?? behavior.body.length,
        totalParts: 1,
        name: 'streaming.bin',
        expectedSize: behavior.headerExpectedSize ?? behavior.body.length,
      });
      const firstChunkBytes = behavior.firstChunkBytes;
      if (
        firstChunkBytes !== undefined &&
        firstChunkBytes > 0 &&
        firstChunkBytes < behavior.body.length
      ) {
        await writeBodyChunks(
          attempt.sink,
          behavior.body.subarray(0, firstChunkBytes)
        );
        behavior.firstChunkWritten?.resolve();
        await behavior.afterFirstChunkGate;
        await writeBodyChunks(
          attempt.sink,
          behavior.body.subarray(firstChunkBytes)
        );
      } else {
        await writeBodyChunks(attempt.sink, behavior.body);
      }
      behavior.bodyWritten?.resolve();
      await behavior.afterBodyWriteGate;
      if (behavior.errorAfterBody) throw behavior.errorAfterBody;
      await attempt.sink.end();
      await behavior.afterSinkEndGate;
      return {
        value: attempt.value,
        metadata: {
          byteRange: behavior.metadataByteRange ?? [0, behavior.body.length],
          fileSize: behavior.metadataFileSize ?? behavior.body.length,
          totalParts: 1,
          name: 'streaming.bin',
          size: behavior.metadataSize ?? behavior.body.length,
        },
      };
    } catch (error) {
      const failure =
        error instanceof Error ? error : new Error('fake segment fetch failed');
      if (attempt) {
        attempt.sink.fail(failure);
        await attempt.dispose(failure);
      }
      throw failure;
    } finally {
      behavior.finished?.resolve();
    }
  }

  async fetchHead(
    segment: NzbSegmentRef,
    _nzbHash: string,
    _priority: CommandPriority,
    want: number,
    onWireStart?: () => void,
    signal?: AbortSignal,
    options?: SegmentHeadFetchOptions
  ): Promise<SegmentHeadData> {
    if (signal?.aborted) throw new NntpError('connection', 'aborted');
    this.headCalls++;
    this.lastHeadOptions = options;
    onWireStart?.();
    const configured = this.headResults.get(segment.messageId);
    if (configured instanceof Error) throw configured;
    if (configured) return configured;
    const body = this.behaviors.get(segment.messageId)?.body ?? Buffer.alloc(0);
    return {
      head: Buffer.from(body.subarray(0, want)),
      byteRange: [0, body.length],
      fileSize: body.length,
      totalParts: 1,
      name: 'metadata.bin',
      size: body.length,
      layout: 'global-range',
    };
  }

  statSegment(): Promise<boolean> {
    return Promise.resolve(true);
  }

  statSegmentDetailed(): Promise<StatDetail> {
    return Promise.resolve({ present: true, answered: true });
  }

  probeBodyOnProvider(): Promise<'ok'> {
    return Promise.resolve('ok');
  }

  providerIds(): string[] {
    return [];
  }

  info(): ProviderPoolInfo[] {
    return [];
  }

  purgeStaleIdles(): void {}

  close(): void {
    this.closed = true;
  }
}

class LateFailoverSegmentFetcher extends FakeSegmentFetcher {
  readonly firstChunkWritten = Promise.withResolvers<void>();
  readonly failFirstAttempt = Promise.withResolvers<void>();
  readonly secondAttemptStarted = Promise.withResolvers<void>();
  readonly completed = Promise.withResolvers<void>();

  constructor(
    private readonly targetMessageId: string,
    private readonly firstPrefix: Buffer,
    private readonly successfulBody: Buffer,
    private readonly byteRangeStart = 0
  ) {
    super();
  }

  override async fetchBodyToSink<T>(
    segment: NzbSegmentRef,
    nzbHash: string,
    priority: CommandPriority,
    createAttempt: () => Promise<StreamingSegmentAttempt<T>>,
    signal?: AbortSignal,
    onWireStart?: () => void
  ): Promise<StreamingSegmentResult<T>> {
    if (segment.messageId !== this.targetMessageId) {
      return super.fetchBodyToSink(
        segment,
        nzbHash,
        priority,
        createAttempt,
        signal,
        onWireStart
      );
    }
    this.streamingCalls++;
    const byteRange: readonly [number, number] = [
      this.byteRangeStart,
      this.byteRangeStart + this.successfulBody.length,
    ];
    const header = {
      byteRange,
      fileSize: byteRange[1],
      totalParts: 1,
      name: 'failover.bin',
      expectedSize: this.successfulBody.length,
    };
    const first = await createAttempt();
    first.onHeader?.(header);
    onWireStart?.();
    await writeBodyChunks(first.sink, this.firstPrefix);
    this.firstChunkWritten.resolve();
    await this.failFirstAttempt.promise;
    const firstFailure = new NntpError(
      'connection',
      'synthetic late provider failure'
    );
    await first.dispose(firstFailure);

    if (signal?.aborted) throw new NntpError('connection', 'aborted');
    const second = await createAttempt();
    second.onHeader?.(header);
    this.secondAttemptStarted.resolve();
    await writeBodyChunks(second.sink, this.successfulBody);
    await second.sink.end();
    this.completed.resolve();
    return {
      value: second.value,
      metadata: {
        byteRange,
        fileSize: byteRange[1],
        totalParts: 1,
        name: 'failover.bin',
        size: this.successfulBody.length,
      },
    };
  }
}

class FailingSpoolManager extends SpoolManager {
  constructor(options: SpoolManagerOptions) {
    super(options);
  }

  override createArtifact(
    _options: CreateSpoolArtifactOptions
  ): Promise<GrowingSpoolArtifact> {
    return Promise.reject(
      new UsenetSpoolError(
        'USENET_SPOOL_IO',
        'Synthetic spool creation failure'
      )
    );
  }
}

class CountingSpoolManager extends SpoolManager {
  disposeCalls = 0;

  override async createArtifact(
    options: CreateSpoolArtifactOptions
  ): Promise<GrowingSpoolArtifact> {
    const artifact = await super.createArtifact(options);
    const dispose = artifact.dispose.bind(artifact);
    artifact.dispose = () => {
      this.disposeCalls++;
      return dispose();
    };
    return artifact;
  }
}

class GatedSpoolManager extends SpoolManager {
  constructor(
    options: SpoolManagerOptions,
    private readonly started: PromiseWithResolvers<void>,
    private readonly gate: Promise<void>
  ) {
    super(options);
  }

  override async createArtifact(
    options: CreateSpoolArtifactOptions
  ): Promise<GrowingSpoolArtifact> {
    this.started.resolve();
    await this.gate;
    return super.createArtifact(options);
  }
}

async function writeBodyChunks(
  sink: BackpressuredByteSink,
  body: Buffer
): Promise<void> {
  const chunkBytes = 16 * KIBIBYTE_BYTES;
  for (let offset = 0; offset < body.length; offset += chunkBytes) {
    const chunk = Buffer.from(body.subarray(offset, offset + chunkBytes));
    if (!sink.write(chunk)) {
      await new Promise<void>((resolve) => sink.onceDrain(resolve));
    }
  }
}

function spoolingPlan(): SegmentSpoolingPlan {
  return {
    memoryBudgetBytes: MEBIBYTE_BYTES,
    perStreamBufferBytes: 512 * KIBIBYTE_BYTES,
    spoolBytes: 16 * MEBIBYTE_BYTES,
    minFreeDiskBytes: 0,
    decoderChunkBytes: 64 * KIBIBYTE_BYTES,
    writerQueueBytes: 128 * KIBIBYTE_BYTES,
    readerHighWaterMarkBytes: 64 * KIBIBYTE_BYTES,
    perDownloadBaseLeaseBytes: 128 * KIBIBYTE_BYTES,
    maxOpenSpoolFiles: 16,
    orphanTtlMs: 60_000,
  };
}

interface HarnessOptions {
  readonly maxConcurrentDownloads?: number;
  readonly memoryBudget?: ByteBudget;
  readonly spoolManager?: SpoolManager;
  readonly artifactCache?: SegmentArtifactCacheLookup;
}

async function createHarness(
  context: TestContext,
  fetcher: FakeSegmentFetcher,
  options: HarnessOptions = {}
): Promise<{
  readonly pool: MultiProviderPool;
  readonly runtime: SegmentSpoolingRuntime;
  readonly cache: SegmentCache;
  readonly cacheRoot: string;
}> {
  const cacheRoot = await mkdtemp(path.join(tmpdir(), 'artifact-fetch-'));
  const plan = spoolingPlan();
  const runtime = new SegmentSpoolingRuntime({
    plan,
    engineId: 'artifact-fetch-test',
    cacheRoot,
    memoryBudget: options.memoryBudget,
    spoolManager: options.spoolManager,
    artifactCache: options.artifactCache,
  });
  const cache = new SegmentCache({ arenaBytes: 2 * MEBIBYTE_BYTES });
  const engineOptions: EngineOptions = {
    ...DEFAULT_ENGINE_OPTIONS,
    streamingMode: 'segment_spooling',
    maxConcurrentDownloads: options.maxConcurrentDownloads ?? 4,
  };
  const pool = new MultiProviderPool(
    [],
    engineOptions,
    cache,
    new StatsAccumulator(),
    { fetcher, spooling: runtime }
  );
  context.after(async () => {
    pool.close();
    await Promise.allSettled([runtime.close(), cache.close()]);
    await rm(cacheRoot, { recursive: true, force: true });
  });
  return { pool, runtime, cache, cacheRoot };
}

async function readArtifact(artifact: SegmentArtifact): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of artifact.createReadStream()) {
    assert(Buffer.isBuffer(chunk));
    chunks.push(chunk);
  }
  await artifact.release();
  return Buffer.concat(chunks);
}

test('fetchSegmentArtifact single-flights one network fetch into independently releasable handles', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const started = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  const body = Buffer.alloc(70 * KIBIBYTE_BYTES);
  for (let index = 0; index < body.length; index++) body[index] = index % 251;
  fetcher.behaviors.set('shared', { body, gate: gate.promise, started });
  const { pool, runtime, cache } = await createHarness(context, fetcher);

  const firstPromise = pool.fetchSegmentArtifact(
    { messageId: 'shared', bytes: body.length },
    'nzb',
    undefined
  );
  const secondPromise = pool.fetchSegmentArtifact(
    { messageId: 'shared', bytes: body.length },
    'nzb',
    undefined
  );
  await started.promise;
  assert.equal(fetcher.streamingCalls, 1);
  gate.resolve();

  const [first, second] = await Promise.all([firstPromise, secondPromise]);
  assert.notEqual(first, second);
  assert.equal(first.storage, 'spool');
  assert.deepEqual(
    await Promise.all([readArtifact(first), readArtifact(second)]),
    [body, body]
  );
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.spoolManager.stats().budget.reservedBytes, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
  assert.equal(cache.stats().misses, 0, 'spooling path must not call getAsync');
  assert.equal(fetcher.bufferingCalls, 0);
});

test('network single-flight validates each waiter expected length independently', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const started = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  const body = Buffer.from('data');
  fetcher.behaviors.set('per-waiter-length', {
    body,
    started,
    gate: gate.promise,
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  const matching = pool.fetchSegmentArtifact(
    { messageId: 'per-waiter-length', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: 4 }
  );
  const mismatching = pool.fetchSegmentArtifact(
    { messageId: 'per-waiter-length', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: 5 }
  );
  await started.promise;
  gate.resolve();

  const artifact = await matching;
  await assert.rejects(mismatching, (error: unknown) => {
    assert(error instanceof UsenetSpoolError);
    assert.equal(error.code, 'USENET_SPOOL_METADATA_MISMATCH');
    return true;
  });
  assert.deepEqual(await readArtifact(artifact), body);
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('locator range mismatch is typed and never enters the body miss cache', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const body = Buffer.from('data');
  fetcher.behaviors.set('range-mismatch', {
    body,
    headerByteRange: [8, 12],
    metadataByteRange: [8, 12],
    metadataFileSize: 12,
  });
  const managerRoot = await mkdtemp(path.join(tmpdir(), 'range-mismatch-'));
  const manager = new CountingSpoolManager({
    plan: spoolingPlan(),
    engineId: 'range-mismatch',
    cacheRoot: managerRoot,
  });
  context.after(async () => {
    await Promise.allSettled([manager.close()]);
    await rm(managerRoot, { recursive: true, force: true });
  });
  const { pool, runtime } = await createHarness(context, fetcher, {
    spoolManager: manager,
  });

  await assert.rejects(
    pool.fetchSegmentArtifact(
      { messageId: 'range-mismatch', bytes: body.length },
      'nzb',
      undefined,
      CommandPriority.High,
      { expectedLength: 4, expectedByteRange: [4, 8] }
    ),
    (error: unknown) => {
      assert(error instanceof UsenetSpoolError);
      assert.equal(error.code, 'USENET_SPOOL_METADATA_MISMATCH');
      return true;
    }
  );
  assert.equal(manager.disposeCalls, 1);
  const valid = await pool.fetchSegmentArtifact(
    { messageId: 'range-mismatch', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: 4, expectedByteRange: [8, 12] }
  );
  assert.deepEqual(await readArtifact(valid), body);
  assert.equal(fetcher.streamingCalls, 2);
  assert.equal(manager.disposeCalls, 2);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
});

test('one network flight validates equal lengths against each waiter range', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const started = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  const body = Buffer.from('data');
  fetcher.behaviors.set('per-waiter-range', {
    body,
    started,
    gate: gate.promise,
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  const matching = pool.fetchSegmentArtifact(
    { messageId: 'per-waiter-range', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: 4, expectedByteRange: [0, 4] }
  );
  const mismatching = pool.fetchSegmentArtifact(
    { messageId: 'per-waiter-range', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: 4, expectedByteRange: [4, 8] }
  );
  await started.promise;
  gate.resolve();

  const artifact = await matching;
  await assert.rejects(mismatching, (error: unknown) => {
    assert(error instanceof UsenetSpoolError);
    assert.equal(error.code, 'USENET_SPOOL_METADATA_MISMATCH');
    return true;
  });
  assert.deepEqual(await readArtifact(artifact), body);
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
});

test('growing handles validate header and final ranges independently', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const afterFirstChunkGate = Promise.withResolvers<void>();
  const body = Buffer.from('data');
  fetcher.behaviors.set('growing-range-validation', {
    body,
    firstChunkBytes: 2,
    afterFirstChunkGate: afterFirstChunkGate.promise,
    headerByteRange: [0, 4],
    metadataByteRange: [4, 8],
    metadataFileSize: 8,
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  const headerWaiter = await pool.fetchSegmentArtifact(
    { messageId: 'growing-range-validation', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    {
      expectedLength: 4,
      expectedByteRange: [0, 4],
      allowGrowing: true,
    }
  );
  const finalWaiter = pool.fetchSegmentArtifact(
    { messageId: 'growing-range-validation', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    {
      expectedLength: 4,
      expectedByteRange: [4, 8],
      allowGrowing: true,
    }
  );
  let finalWaiterSettled = false;
  void finalWaiter.finally(() => {
    finalWaiterSettled = true;
  });
  const headerReader = headerWaiter.createReadStream();
  const headerResult = new Promise<void>((resolve, reject) => {
    headerReader.once('end', resolve);
    headerReader.once('error', reject);
    headerReader.resume();
  });
  await Promise.resolve();
  assert.equal(finalWaiterSettled, false);
  afterFirstChunkGate.resolve();

  await assert.rejects(headerResult, (error: unknown) => {
    assert(error instanceof UsenetSpoolError);
    assert.equal(error.code, 'USENET_SPOOL_METADATA_MISMATCH');
    return true;
  });
  const finalArtifact = await finalWaiter;
  assert.deepEqual(await readArtifact(finalArtifact), body);
  await headerWaiter.release();
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
});

test('arena and file-backed hits enforce the same byte-range expectations', async (context) => {
  await context.test('arena', async (subtest) => {
    const fetcher = new FakeSegmentFetcher();
    const { pool, cache } = await createHarness(subtest, fetcher);
    const lease = cache.arena.checkout(MEBIBYTE_BYTES);
    assert(lease);
    Buffer.from('data').copy(lease.slot);
    cache.arena.commit(lease, 'arena-range', {
      body: lease.slot.subarray(0, 4),
      byteRange: [0, 4],
      fileSize: 8,
      size: 4,
    });
    await assert.rejects(
      pool.fetchSegmentArtifact(
        { messageId: 'arena-range' },
        'nzb',
        undefined,
        CommandPriority.High,
        { expectedLength: 4, expectedByteRange: [4, 8] }
      ),
      (error: unknown) => {
        assert(error instanceof UsenetSpoolError);
        assert.equal(error.code, 'USENET_SPOOL_METADATA_MISMATCH');
        return true;
      }
    );
    assert.equal(cache.arena.stats().pinned, 0);
    assert.equal(fetcher.streamingCalls, 0);
  });

  await context.test('file-backed extension point', async (subtest) => {
    const fetcher = new FakeSegmentFetcher();
    let releases = 0;
    const artifactCache: SegmentArtifactCacheLookup = {
      acquire: () =>
        Promise.resolve(
          new ArenaSegmentArtifact({
            data: {
              body: Buffer.from('data'),
              byteRange: [0, 4],
              fileSize: 8,
              size: 4,
            },
            owned: true,
            release: () => {
              releases++;
            },
          })
        ),
    };
    const { pool } = await createHarness(subtest, fetcher, { artifactCache });
    await assert.rejects(
      pool.fetchSegmentArtifact(
        { messageId: 'l2-range' },
        'nzb',
        undefined,
        CommandPriority.High,
        { expectedLength: 4, expectedByteRange: [4, 8] }
      ),
      (error: unknown) => {
        assert(error instanceof UsenetSpoolError);
        assert.equal(error.code, 'USENET_SPOOL_METADATA_MISMATCH');
        return true;
      }
    );
    assert.equal(releases, 1);
    assert.equal(fetcher.streamingCalls, 0);
  });
});

test('undefined and exact network waiters receive independent valid handles', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const started = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  const body = Buffer.from('data');
  fetcher.behaviors.set('mixed-length-waiters', {
    body,
    started,
    gate: gate.promise,
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  const unspecified = pool.fetchSegmentArtifact(
    { messageId: 'mixed-length-waiters', bytes: body.length },
    'nzb',
    undefined
  );
  const exact = pool.fetchSegmentArtifact(
    { messageId: 'mixed-length-waiters', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: body.length }
  );
  await started.promise;
  gate.resolve();
  const [first, second] = await Promise.all([unspecified, exact]);
  assert.notEqual(first, second);
  assert.equal(first.metadata.size, body.length);
  assert.equal(second.metadata.size, body.length);
  assert.deepEqual(
    await Promise.all([readArtifact(first), readArtifact(second)]),
    [body, body]
  );
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('a late contradictory waiter cannot join an already published growing owner', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const afterFirstChunkGate = Promise.withResolvers<void>();
  const body = Buffer.from('data');
  fetcher.behaviors.set('late-contradiction', {
    body,
    firstChunkBytes: 2,
    afterFirstChunkGate: afterFirstChunkGate.promise,
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  const correct = await pool.fetchSegmentArtifact(
    { messageId: 'late-contradiction', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: body.length, allowGrowing: true }
  );
  const correctRead = readArtifact(correct);
  const contradictory = pool.fetchSegmentArtifact(
    { messageId: 'late-contradiction', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: body.length + 1, allowGrowing: true }
  );
  let contradictorySettled = false;
  void contradictory.then(
    () => {
      contradictorySettled = true;
    },
    () => {
      contradictorySettled = true;
    }
  );
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(contradictorySettled, false);

  afterFirstChunkGate.resolve();
  assert.deepEqual(await correctRead, body);
  await assert.rejects(contradictory, (error: unknown) => {
    assert(error instanceof UsenetSpoolError);
    assert.equal(error.code, 'USENET_SPOOL_METADATA_MISMATCH');
    return true;
  });
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('an exact-length artifact becomes readable after its first committed spool chunk', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const firstChunkWritten = Promise.withResolvers<void>();
  const afterFirstChunkGate = Promise.withResolvers<void>();
  const finished = Promise.withResolvers<void>();
  const body = Buffer.alloc(64 * KIBIBYTE_BYTES, 0x5a);
  fetcher.behaviors.set('growing', {
    body,
    firstChunkBytes: 16 * KIBIBYTE_BYTES,
    firstChunkWritten,
    afterFirstChunkGate: afterFirstChunkGate.promise,
    finished,
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  let producerFinished = false;
  void finished.promise.then(() => {
    producerFinished = true;
  });

  const artifactPromise = pool.fetchSegmentArtifact(
    { messageId: 'growing', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: body.length, allowGrowing: true }
  );
  await firstChunkWritten.promise;
  const artifact = await artifactPromise;
  assert.equal(producerFinished, false);
  const reader = artifact.createReadStream();
  const chunks: Buffer[] = [];
  const firstData = Promise.withResolvers<void>();
  reader.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    firstData.resolve();
  });
  const readerEnded = new Promise<void>((resolve, reject) => {
    reader.once('end', resolve);
    reader.once('error', reject);
  });
  await firstData.promise;
  assert.equal(producerFinished, false);

  afterFirstChunkGate.resolve();
  await Promise.all([finished.promise, readerEnded]);
  await artifact.release();
  assert.deepEqual(Buffer.concat(chunks), body);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('a growing reader withholds EOF until producer validation completes', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const validationGate = Promise.withResolvers<void>();
  const body = Buffer.alloc(24 * KIBIBYTE_BYTES, 0x31);
  fetcher.behaviors.set('validation-gate', {
    body,
    firstChunkBytes: 8 * KIBIBYTE_BYTES,
    afterSinkEndGate: validationGate.promise,
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  const artifact = await pool.fetchSegmentArtifact(
    { messageId: 'validation-gate', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: body.length, allowGrowing: true }
  );
  const reader = artifact.createReadStream();
  const chunks: Buffer[] = [];
  let received = 0;
  let ended = false;
  const allBytes = Promise.withResolvers<void>();
  reader.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    received += chunk.length;
    if (received === body.length) allBytes.resolve();
  });
  const readerDone = new Promise<void>((resolve, reject) => {
    reader.once('end', () => {
      ended = true;
      resolve();
    });
    reader.once('error', reject);
  });

  await allBytes.promise;
  assert.equal(ended, false);
  validationGate.resolve();
  await readerDone;
  assert.deepEqual(Buffer.concat(chunks), body);
  await artifact.release();
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('a decoder failure after expected payload bytes cannot become successful EOF', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const body = Buffer.from('complete-payload-before-missing-yend');
  const failGate = Promise.withResolvers<void>();
  const finished = Promise.withResolvers<void>();
  fetcher.behaviors.set('missing-yend', {
    body,
    afterBodyWriteGate: failGate.promise,
    finished,
    errorAfterBody: new YencDecodeError(
      'no_end_found',
      'synthetic missing yend after payload',
      { terminal: true }
    ),
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  const artifact = await pool.fetchSegmentArtifact(
    { messageId: 'missing-yend', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: body.length, allowGrowing: true }
  );
  const reader = artifact.createReadStream();
  const chunks: Buffer[] = [];
  let received = 0;
  const allBytes = Promise.withResolvers<void>();
  reader.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    received += chunk.length;
    if (received === body.length) allBytes.resolve();
  });
  const readerResult = new Promise<void>((resolve, reject) => {
    reader.once('end', resolve);
    reader.once('error', reject);
  });
  await allBytes.promise;
  const expectedFailure = assert.rejects(readerResult, (error: unknown) => {
    assert(error instanceof UsenetSpoolError);
    assert.equal(error.code, 'USENET_SPOOL_IO');
    return true;
  });
  failGate.resolve();
  await Promise.all([expectedFailure, finished.promise]);
  assert.deepEqual(Buffer.concat(chunks), body);
  await artifact.release();
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('growing readers reject both larger and smaller final decoded lengths', async (context) => {
  for (const scenario of [
    { id: 'larger', body: Buffer.from('12345') },
    { id: 'smaller', body: Buffer.from('123') },
  ]) {
    await context.test(scenario.id, async (subtest) => {
      const fetcher = new FakeSegmentFetcher();
      const validationGate = Promise.withResolvers<void>();
      fetcher.behaviors.set(scenario.id, {
        body: scenario.body,
        headerExpectedSize: 4,
        metadataSize: scenario.body.length,
        afterSinkEndGate: validationGate.promise,
      });
      const { pool, runtime } = await createHarness(subtest, fetcher);
      const artifact = await pool.fetchSegmentArtifact(
        { messageId: scenario.id, bytes: scenario.body.length },
        'nzb',
        undefined,
        CommandPriority.High,
        { expectedLength: 4, allowGrowing: true }
      );
      const reader = artifact.createReadStream();
      const readerResult = new Promise<void>((resolve, reject) => {
        reader.once('end', resolve);
        reader.once('error', reject);
        reader.resume();
      });
      validationGate.resolve();
      await assert.rejects(readerResult, (error: unknown) => {
        assert(error instanceof UsenetSpoolError);
        assert.equal(error.code, 'USENET_SPOOL_METADATA_MISMATCH');
        return true;
      });
      assert.equal(artifact.metadata.size, scenario.body.length);
      assert.equal(artifact.length, scenario.body.length);
      await artifact.release();
      assert.equal(runtime.spoolManager.stats().artifacts, 0);
      assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
    });
  }
});

test('a published growing attempt fails without mixing backup-provider bytes', async (context) => {
  const fetcher = new LateFailoverSegmentFetcher(
    'late-growing-failure',
    Buffer.from('abc'),
    Buffer.from('abcdef')
  );
  const { pool, runtime } = await createHarness(context, fetcher);
  const artifact = await pool.fetchSegmentArtifact(
    { messageId: 'late-growing-failure', bytes: 6 },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: 6, allowGrowing: true }
  );
  const reader = artifact.createReadStream();
  const chunks: Buffer[] = [];
  const firstData = Promise.withResolvers<void>();
  reader.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    firstData.resolve();
  });
  const readerResult = new Promise<void>((resolve, reject) => {
    reader.once('end', resolve);
    reader.once('error', reject);
  });
  await firstData.promise;
  const expectedFailure = assert.rejects(readerResult, UsenetSpoolError);
  fetcher.failFirstAttempt.resolve();
  await fetcher.completed.promise;
  await expectedFailure;
  assert.deepEqual(Buffer.concat(chunks), Buffer.from('abc'));
  await artifact.release();
  const allMemory = await runtime.memoryBudget.acquire(
    spoolingPlan().memoryBudgetBytes
  );
  allMemory.release();
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('a non-growing prefetched waiter retains complete provider failover', async (context) => {
  const fetcher = new LateFailoverSegmentFetcher(
    'future-failover',
    Buffer.from('abc'),
    Buffer.from('abcdef')
  );
  const { pool, runtime } = await createHarness(context, fetcher);
  const currentPromise = pool.fetchSegmentArtifact(
    { messageId: 'future-failover', bytes: 6 },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: 6, allowGrowing: true }
  );
  const futurePromise = pool.fetchSegmentArtifact(
    { messageId: 'future-failover', bytes: 6 },
    'nzb',
    undefined,
    CommandPriority.High,
    { expectedLength: 6, allowGrowing: false }
  );
  const current = await currentPromise;
  const currentReader = current.createReadStream();
  const currentResult = new Promise<void>((resolve, reject) => {
    currentReader.once('end', resolve);
    currentReader.once('error', reject);
    currentReader.resume();
  });
  const expectedCurrentFailure = assert.rejects(
    currentResult,
    UsenetSpoolError
  );
  fetcher.failFirstAttempt.resolve();
  const future = await futurePromise;

  await expectedCurrentFailure;
  assert.deepEqual(await readArtifact(future), Buffer.from('abcdef'));
  await current.release();
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('a future SpoolingSegmentsStream task waits for complete provider failover', async (context) => {
  const fetcher = new LateFailoverSegmentFetcher(
    'future-stream-segment',
    Buffer.from('x'),
    Buffer.from('1111'),
    4
  );
  const currentGate = Promise.withResolvers<void>();
  fetcher.behaviors.set('current-stream-segment', {
    body: Buffer.from('0000'),
    firstChunkBytes: 2,
    afterFirstChunkGate: currentGate.promise,
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  const stream = new SpoolingSegmentsStream({
    pool,
    segments: [
      { messageId: 'current-stream-segment', bytes: 4 },
      { messageId: 'future-stream-segment', bytes: 4 },
    ],
    nzbHash: 'future-stream-failover',
    maxPrefetchSegments: 2,
    readerHighWaterMarkBytes: runtime.plan.readerHighWaterMarkBytes,
    priority: CommandPriority.High,
    sizeForSegment: () => 4,
  });
  const output = (async (): Promise<Buffer> => {
    const chunks: Buffer[] = [];
    for await (const chunk of stream) {
      assert(Buffer.isBuffer(chunk));
      chunks.push(chunk);
    }
    return Buffer.concat(chunks);
  })();

  await fetcher.firstChunkWritten.promise;
  fetcher.failFirstAttempt.resolve();
  await fetcher.completed.promise;
  assert.equal(stream.readableEnded, false);
  currentGate.resolve();

  assert.deepEqual(await output, Buffer.from('00001111'));
  assert.equal(fetcher.streamingCalls, 2);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('stream memory atomically reserves the requested bounded queue bytes', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const { pool, runtime } = await createHarness(context, fetcher);
  const bytes = 2 * runtime.plan.readerHighWaterMarkBytes;
  const lease = await pool.acquireSegmentStreamMemory(
    bytes,
    CommandPriority.High
  );
  assert.equal(lease.bytes, bytes);
  assert.equal(runtime.memoryBudget.stats().usedBytes, lease.bytes);
  lease.release();
  lease.release();
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('range metadata probes retain only scalar fields and honor abort', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const body = Buffer.from('metadata-only');
  fetcher.behaviors.set('metadata-only', { body });
  const { pool, cache } = await createHarness(context, fetcher);

  const metadata = await pool.fetchSegmentRangeMetadata(
    { messageId: 'metadata-only', bytes: body.length },
    'nzb',
    undefined,
    CommandPriority.High
  );
  assert.deepEqual(metadata, {
    byteRange: [0, body.length],
    fileSize: body.length,
    totalParts: 1,
    name: 'metadata.bin',
    decodedSize: body.length,
    layout: 'global-range',
  });
  assert.equal(fetcher.headCalls, 1);
  assert.deepEqual(fetcher.lastHeadOptions, {
    strictYencMetadata: true,
    requireByteRange: undefined,
    allowStandalonePart: undefined,
  });
  assert.equal(fetcher.streamingCalls, 0);
  assert.equal(fetcher.bufferingCalls, 0);
  assert.equal(cache.stats().misses, 0);

  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    pool.fetchSegmentRangeMetadata(
      { messageId: 'aborted-metadata' },
      'nzb',
      controller.signal,
      CommandPriority.High
    ),
    NntpError
  );
  assert.equal(fetcher.headCalls, 1);
});

test('locator-only metadata failure never poisons the body miss cache', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const body = Buffer.from('valid standalone body');
  fetcher.headResults.set(
    'metadata-does-not-poison-body',
    new YencMetadataError(
      'invalid_header',
      'synthetic locator-only metadata failure'
    )
  );
  fetcher.behaviors.set('metadata-does-not-poison-body', { body });
  const { pool } = await createHarness(context, fetcher);

  await assert.rejects(
    pool.fetchSegmentRangeMetadata(
      { messageId: 'metadata-does-not-poison-body' },
      'nzb',
      undefined,
      CommandPriority.High,
      { requireByteRange: true, allowStandalonePart: true }
    ),
    YencMetadataError
  );

  const artifact = await pool.fetchSegmentArtifact(
    { messageId: 'metadata-does-not-poison-body', bytes: body.length },
    'nzb',
    undefined
  );
  assert.deepEqual(await readArtifact(artifact), body);
  assert.equal(fetcher.headCalls, 1);
  assert.equal(fetcher.streamingCalls, 1);
});

test('artifact range expectations reject invalid scalar constraints before I/O', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const { pool } = await createHarness(context, fetcher);
  const invalid: readonly SegmentArtifactFetchOptions[] = [
    { expectedByteRange: [-1, 3] },
    { expectedByteRange: [3, 3] },
    { expectedByteRange: [0, Number.NaN] },
    { expectedByteRange: [0, 1.5] },
    { expectedLength: 3, expectedByteRange: [0, 4] },
  ];

  for (const options of invalid) {
    await assert.rejects(
      pool.fetchSegmentArtifact(
        { messageId: 'invalid-artifact-expectation' },
        'nzb',
        undefined,
        CommandPriority.High,
        options
      ),
      (error: unknown) => {
        assert(error instanceof UsenetSpoolError);
        assert.equal(error.code, 'USENET_SPOOL_INVALID_ARGUMENT');
        return true;
      }
    );
  }
  assert.equal(fetcher.streamingCalls, 0);
});

test('bounded stream admission preserves download-memory progress', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const { pool, runtime } = await createHarness(context, fetcher);
  const active = await Promise.all(
    Array.from({ length: 4 }, () =>
      pool.acquireSegmentStreamMemory(
        2 * runtime.plan.readerHighWaterMarkBytes,
        CommandPriority.High
      )
    )
  );
  let fifthGranted = false;
  const fifthPromise = pool
    .acquireSegmentStreamMemory(
      2 * runtime.plan.readerHighWaterMarkBytes,
      CommandPriority.High
    )
    .then((lease) => {
      fifthGranted = true;
      return lease;
    });
  await Promise.resolve();
  assert.equal(fifthGranted, false);

  const download = await runtime.acquireDownloadMemory(CommandPriority.High);
  active[0].release();
  const fifth = await fifthPromise;
  download.release();
  fifth.release();
  for (const lease of active) lease.release();
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('one artifact waiter may abort while another receives the completed fetch', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const started = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  const body = Buffer.from('surviving waiter');
  fetcher.behaviors.set('partial-abort', {
    body,
    gate: gate.promise,
    started,
  });
  const { pool } = await createHarness(context, fetcher);
  const aborter = new AbortController();

  const abandoned = pool.fetchSegmentArtifact(
    { messageId: 'partial-abort' },
    'nzb',
    aborter.signal
  );
  const surviving = pool.fetchSegmentArtifact(
    { messageId: 'partial-abort' },
    'nzb',
    undefined
  );
  await started.promise;
  aborter.abort();
  await assert.rejects(abandoned, (error: unknown) => {
    assert(error instanceof NntpError);
    assert.equal(error.message, 'aborted');
    return true;
  });
  assert.equal(getEventListeners(aborter.signal, 'abort').length, 0);
  gate.resolve();
  assert.deepEqual(await readArtifact(await surviving), body);
  assert.equal(fetcher.streamingCalls, 1);
});

test('an on-wire flight with no remaining waiters completes and disposes consistently', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const started = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  fetcher.behaviors.set('on-wire-abort', {
    body: Buffer.from('completed without a waiter'),
    gate: gate.promise,
    started,
  });
  const { pool, runtime } = await createHarness(context, fetcher);
  const controller = new AbortController();
  const abandoned = pool.fetchSegmentArtifact(
    { messageId: 'on-wire-abort' },
    'nzb',
    controller.signal
  );
  await started.promise;
  controller.abort();
  await assert.rejects(abandoned, { name: 'NntpError' });
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  gate.resolve();

  const completeBudget = await runtime.memoryBudget.acquire(
    spoolingPlan().memoryBudgetBytes
  );
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.spoolManager.stats().budget.reservedBytes, 0);
  completeBudget.release();
});

test('the last abort cancels artifact work before its global semaphore grant', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const blockerStarted = Promise.withResolvers<void>();
  const blockerGate = Promise.withResolvers<void>();
  fetcher.behaviors.set('blocker', {
    body: Buffer.from('blocker'),
    gate: blockerGate.promise,
    started: blockerStarted,
  });
  fetcher.behaviors.set('queued', { body: Buffer.from('must not fetch') });
  const { pool, runtime } = await createHarness(context, fetcher, {
    maxConcurrentDownloads: 1,
  });

  const blocker = pool.fetchSegmentArtifact(
    { messageId: 'blocker' },
    'nzb',
    undefined
  );
  await blockerStarted.promise;
  const firstAbort = new AbortController();
  const secondAbort = new AbortController();
  const first = pool.fetchSegmentArtifact(
    { messageId: 'queued' },
    'nzb',
    firstAbort.signal
  );
  const second = pool.fetchSegmentArtifact(
    { messageId: 'queued' },
    'nzb',
    secondAbort.signal
  );
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(pool.poolInfo().globalDownloadsWaiting, 1);
  firstAbort.abort();
  secondAbort.abort();
  await Promise.all([
    assert.rejects(first, { name: 'NntpError' }),
    assert.rejects(second, { name: 'NntpError' }),
  ]);
  assert.equal(getEventListeners(firstAbort.signal, 'abort').length, 0);
  assert.equal(getEventListeners(secondAbort.signal, 'abort').length, 0);
  const remainingMemory = await runtime.memoryBudget.acquire(
    spoolingPlan().memoryBudgetBytes - spoolingPlan().perDownloadBaseLeaseBytes
  );
  remainingMemory.release();
  assert.equal(pool.poolInfo().globalDownloadsWaiting, 0);
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 128 * KIBIBYTE_BYTES);

  blockerGate.resolve();
  await (await blocker).release();
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('the last pre-wire waiter abort disposes a created spool attempt and every permit once', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const attemptCreated = Promise.withResolvers<void>();
  const preWireGate = Promise.withResolvers<void>();
  const finished = Promise.withResolvers<void>();
  fetcher.behaviors.set('prepared-abort', {
    body: Buffer.from('must not reach the wire'),
    attemptCreated,
    preWireGate: preWireGate.promise,
    finished,
  });
  const cacheRoot = await mkdtemp(path.join(tmpdir(), 'prepared-abort-'));
  const manager = new CountingSpoolManager({
    plan: spoolingPlan(),
    engineId: 'prepared-abort',
    cacheRoot,
  });
  context.after(async () => {
    await Promise.allSettled([manager.close()]);
    await rm(cacheRoot, { recursive: true, force: true });
  });
  const { pool, runtime } = await createHarness(context, fetcher, {
    maxConcurrentDownloads: 1,
    spoolManager: manager,
  });
  const controller = new AbortController();
  const pending = pool.fetchSegmentArtifact(
    { messageId: 'prepared-abort' },
    'nzb',
    controller.signal
  );

  await attemptCreated.promise;
  assert.equal(pool.poolInfo().globalDownloadsInUse, 1);
  assert.equal(pool.poolInfo().globalDownloadsOnWire, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 128 * KIBIBYTE_BYTES);
  assert.equal(runtime.spoolManager.stats().artifacts, 1);
  controller.abort();
  await assert.rejects(pending, { name: 'NntpError' });
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  preWireGate.resolve();
  await finished.promise;
  const allMemory = await runtime.memoryBudget.acquire(
    spoolingPlan().memoryBudgetBytes
  );

  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(pool.poolInfo().globalDownloadsInUse, 0);
  assert.equal(pool.poolInfo().globalDownloadsOnWire, 0);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.spoolManager.stats().budget.reservedBytes, 0);
  assert.equal(runtime.spoolManager.stats().files.openFiles, 0);
  assert.equal(manager.disposeCalls, 1);
  allMemory.release();
});

test('abort during spool attempt creation remains pre-wire and releases partial ownership', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const creationStarted = Promise.withResolvers<void>();
  const creationGate = Promise.withResolvers<void>();
  const finished = Promise.withResolvers<void>();
  fetcher.behaviors.set('creation-abort', {
    body: Buffer.from('must not reach the wire'),
    finished,
  });
  const cacheRoot = await mkdtemp(path.join(tmpdir(), 'creation-abort-'));
  const manager = new GatedSpoolManager(
    {
      plan: spoolingPlan(),
      engineId: 'creation-abort',
      cacheRoot,
    },
    creationStarted,
    creationGate.promise
  );
  context.after(async () => {
    await Promise.allSettled([manager.close()]);
    await rm(cacheRoot, { recursive: true, force: true });
  });
  const { pool, runtime } = await createHarness(context, fetcher, {
    maxConcurrentDownloads: 1,
    spoolManager: manager,
  });
  const controller = new AbortController();
  const pending = pool.fetchSegmentArtifact(
    { messageId: 'creation-abort' },
    'nzb',
    controller.signal
  );

  await creationStarted.promise;
  assert.equal(pool.poolInfo().globalDownloadsInUse, 1);
  assert.equal(pool.poolInfo().globalDownloadsOnWire, 0);
  controller.abort();
  await assert.rejects(pending, { name: 'NntpError' });
  creationGate.resolve();
  await finished.promise;
  const allMemory = await runtime.memoryBudget.acquire(
    spoolingPlan().memoryBudgetBytes
  );

  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  assert.equal(fetcher.streamingCalls, 1);
  assert.equal(pool.poolInfo().globalDownloadsInUse, 0);
  assert.equal(pool.poolInfo().globalDownloadsOnWire, 0);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.spoolManager.stats().budget.reservedBytes, 0);
  assert.equal(runtime.spoolManager.stats().files.openFiles, 0);
  allMemory.release();
});

test('definitive misses and decode failures dispose partial spools and populate the negative cache', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  fetcher.behaviors.set('missing', {
    body: Buffer.alloc(0),
    error: new ArticleNotFoundError('missing everywhere', {
      messageId: 'missing',
      allProviders: true,
    }),
  });
  fetcher.behaviors.set('undecodable', {
    body: Buffer.alloc(0),
    error: new YencDecodeError(
      'no_end_found',
      'synthetic terminal decode failure',
      { terminal: true }
    ),
  });
  const { pool, runtime } = await createHarness(context, fetcher);

  await assert.rejects(
    pool.fetchSegmentArtifact({ messageId: 'missing' }, 'nzb', undefined),
    ArticleNotFoundError
  );
  await assert.rejects(
    pool.fetchSegmentArtifact({ messageId: 'missing' }, 'nzb', undefined),
    ArticleNotFoundError
  );
  await assert.rejects(
    pool.fetchSegmentArtifact({ messageId: 'undecodable' }, 'nzb', undefined),
    YencDecodeError
  );
  await assert.rejects(
    pool.fetchSegmentArtifact({ messageId: 'undecodable' }, 'nzb', undefined),
    YencDecodeError
  );

  assert.equal(fetcher.streamingCalls, 2);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.spoolManager.stats().budget.reservedBytes, 0);
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});

test('spool and memory-budget failures leave every resource owner empty', async (context) => {
  await context.test('spool creation failure', async (subtest) => {
    const fetcher = new FakeSegmentFetcher();
    fetcher.behaviors.set('spool-failure', { body: Buffer.from('body') });
    const cacheRoot = await mkdtemp(path.join(tmpdir(), 'failing-spool-'));
    const failing = new FailingSpoolManager({
      plan: spoolingPlan(),
      engineId: 'failing-spool',
      cacheRoot,
    });
    subtest.after(async () => {
      await Promise.allSettled([failing.close()]);
      await rm(cacheRoot, { recursive: true, force: true });
    });
    const { pool, runtime } = await createHarness(subtest, fetcher, {
      spoolManager: failing,
    });
    await assert.rejects(
      pool.fetchSegmentArtifact(
        { messageId: 'spool-failure' },
        'nzb',
        undefined
      ),
      (error: unknown) => {
        assert(error instanceof UsenetSpoolError);
        assert.equal(error.code, 'USENET_SPOOL_IO');
        return true;
      }
    );
    assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
    assert.equal(runtime.spoolManager.stats().artifacts, 0);
  });

  await context.test('oversized base memory lease', async (subtest) => {
    const fetcher = new FakeSegmentFetcher();
    const { pool, runtime } = await createHarness(subtest, fetcher, {
      memoryBudget: new ByteBudget(1),
    });
    await assert.rejects(
      pool.fetchSegmentArtifact(
        { messageId: 'budget-failure' },
        'nzb',
        undefined
      ),
      (error: unknown) => {
        assert(error instanceof UsenetSpoolError);
        assert.equal(error.code, 'USENET_MEMORY_BUDGET');
        return true;
      }
    );
    assert.equal(fetcher.streamingCalls, 0);
    assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
    assert.equal(runtime.spoolManager.stats().artifacts, 0);
  });

  await context.test('oversized initial disk reservation', async (subtest) => {
    const fetcher = new FakeSegmentFetcher();
    const { pool, runtime } = await createHarness(subtest, fetcher);
    await assert.rejects(
      pool.fetchSegmentArtifact(
        {
          messageId: 'disk-budget-failure',
          bytes: spoolingPlan().spoolBytes + 1,
        },
        'nzb',
        undefined
      ),
      (error: unknown) => {
        assert(error instanceof UsenetSpoolError);
        assert.equal(error.code, 'USENET_SPOOL_CAPACITY');
        return true;
      }
    );
    assert.equal(fetcher.streamingCalls, 1);
    assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
    assert.equal(runtime.spoolManager.stats().artifacts, 0);
    assert.equal(runtime.spoolManager.stats().budget.reservedBytes, 0);
  });
});

test('the final independent release disposes a shared spool exactly once', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  fetcher.behaviors.set('release-once', { body: Buffer.from('release-once') });
  const cacheRoot = await mkdtemp(path.join(tmpdir(), 'counting-spool-'));
  const manager = new CountingSpoolManager({
    plan: spoolingPlan(),
    engineId: 'counting-spool',
    cacheRoot,
  });
  context.after(async () => {
    await Promise.allSettled([manager.close()]);
    await rm(cacheRoot, { recursive: true, force: true });
  });
  const { pool, runtime } = await createHarness(context, fetcher, {
    spoolManager: manager,
  });
  const [first, second] = await Promise.all([
    pool.fetchSegmentArtifact({ messageId: 'release-once' }, 'nzb', undefined),
    pool.fetchSegmentArtifact({ messageId: 'release-once' }, 'nzb', undefined),
  ]);
  assert.equal(runtime.spoolManager.stats().artifacts, 1);
  await first.release();
  await first.release();
  assert.equal(runtime.spoolManager.stats().artifacts, 1);
  await second.release();
  await second.release();
  assert.equal(manager.disposeCalls, 1);
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.spoolManager.stats().budget.reservedBytes, 0);
});

test('arena artifacts and all existing buffering APIs remain on their original paths', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  fetcher.behaviors.set('buffered', { body: Buffer.from('buffered-result') });
  const { pool, cache } = await createHarness(context, fetcher);
  const lease = cache.arena.checkout(MEBIBYTE_BYTES);
  assert(lease);
  const resident = Buffer.from('arena-result');
  resident.copy(lease.slot);
  cache.arena.commit(lease, 'arena-hit', {
    body: lease.slot.subarray(0, resident.length),
    size: resident.length,
  });

  const arena = await pool.fetchSegmentArtifact(
    { messageId: 'arena-hit' },
    'nzb',
    undefined
  );
  assert.equal(arena.storage, 'arena');
  assert.deepEqual(await readArtifact(arena), resident);
  const buffered = await pool.fetchSegment(
    { messageId: 'buffered' },
    'nzb',
    undefined
  );
  assert.deepEqual(buffered.body, Buffer.from('buffered-result'));
  assert.equal(fetcher.streamingCalls, 0);
  assert.equal(fetcher.bufferingCalls, 1);
});

test(
  'best-effort promotion protects the spool file without blocking final playback release',
  { timeout: 5_000 },
  async (context) => {
    const fetcher = new FakeSegmentFetcher();
    const body = Buffer.from('promotion-does-not-block-playback');
    fetcher.behaviors.set('promoted-artifact', { body });
    const started = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    const copied = Promise.withResolvers<void>();
    const artifactCache: SegmentArtifactCacheLookup = {
      promotionEnabled: true,
      acquire: () => Promise.resolve(undefined),
      promote: async (_messageId, metadata, sourcePath) => {
        assert.equal(metadata.size, body.length);
        started.resolve();
        await gate.promise;
        assert.deepEqual(await readFile(sourcePath), body);
        copied.resolve();
        return true;
      },
    };
    const { pool, runtime } = await createHarness(context, fetcher, {
      artifactCache,
    });
    const artifact = await pool.fetchSegmentArtifact(
      { messageId: 'promoted-artifact', bytes: body.length },
      'nzb',
      undefined
    );
    await started.promise;
    assert.deepEqual(await readArtifact(artifact), body);
    assert.equal(runtime.spoolManager.stats().artifacts, 1);

    gate.resolve();
    await copied.promise;
    await runtime.close();
    assert.equal(runtime.spoolManager.stats().artifacts, 0);
    assert.equal(runtime.spoolManager.stats().budget.reservedBytes, 0);
    assert.equal(runtime.spoolManager.stats().files.openFiles, 0);
  }
);

test('promotion failure never changes artifact delivery or leaks spool ownership', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const body = Buffer.from('promotion-failure-is-best-effort');
  fetcher.behaviors.set('promotion-failure', { body });
  let promotions = 0;
  const artifactCache: SegmentArtifactCacheLookup = {
    promotionEnabled: true,
    acquire: () => Promise.resolve(undefined),
    promote: () => {
      promotions++;
      return Promise.reject(new Error('synthetic promotion failure'));
    },
  };
  const { pool, runtime } = await createHarness(context, fetcher, {
    artifactCache,
  });
  const artifact = await pool.fetchSegmentArtifact(
    { messageId: 'promotion-failure', bytes: body.length },
    'nzb',
    undefined
  );
  assert.deepEqual(await readArtifact(artifact), body);
  assert.equal(promotions, 1);
  await runtime.close();
  assert.equal(runtime.spoolManager.stats().artifacts, 0);
  assert.equal(runtime.spoolManager.stats().budget.reservedBytes, 0);
});

test('a completed spool promotion becomes a file-backed hit without another network fetch', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const body = Buffer.from('persistent-promotion-hit');
  fetcher.behaviors.set('persistent-promotion', { body });
  const diskRoot = await mkdtemp(path.join(tmpdir(), 'artifact-promotion-l2-'));
  const persistent = new SegmentCache({
    arenaBytes: 0,
    diskBytes: MEBIBYTE_BYTES,
    diskPath: diskRoot,
    namespace: 'segments',
  });
  const promoted = Promise.withResolvers<boolean>();
  const artifactCache: SegmentArtifactCacheLookup = {
    get promotionEnabled() {
      return persistent.promotionEnabled;
    },
    acquire: (messageId, signal) => persistent.acquire(messageId, signal),
    promote: async (messageId, metadata, sourcePath, tryAcquireMemory) => {
      const result = await persistent.promote(
        messageId,
        metadata,
        sourcePath,
        tryAcquireMemory
      );
      promoted.resolve(result);
      return result;
    },
  };
  context.after(async () => {
    await persistent.close();
    await rm(diskRoot, { recursive: true, force: true });
  });
  const { pool } = await createHarness(context, fetcher, { artifactCache });
  const first = await pool.fetchSegmentArtifact(
    { messageId: 'persistent-promotion', bytes: body.length },
    'nzb',
    undefined
  );
  assert.deepEqual(await readArtifact(first), body);
  assert.equal(await promoted.promise, true);

  const second = await pool.fetchSegmentArtifact(
    { messageId: 'persistent-promotion', bytes: body.length },
    'nzb',
    undefined
  );
  assert.equal(second.storage, 'disk-cache');
  assert.deepEqual(await readArtifact(second), body);
  assert.equal(fetcher.streamingCalls, 1);
});

test('an explicitly disabled persistent cache skips promotion entirely', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const body = Buffer.from('no-promotion');
  fetcher.behaviors.set('promotion-disabled', { body });
  let promotions = 0;
  const artifactCache: SegmentArtifactCacheLookup = {
    promotionEnabled: false,
    acquire: () => Promise.resolve(undefined),
    promote: () => {
      promotions++;
      return Promise.resolve(true);
    },
  };
  const { pool } = await createHarness(context, fetcher, { artifactCache });
  const artifact = await pool.fetchSegmentArtifact(
    { messageId: 'promotion-disabled', bytes: body.length },
    'nzb',
    undefined
  );
  assert.deepEqual(await readArtifact(artifact), body);
  assert.equal(promotions, 0);
});

test('best-effort promotion memory never bypasses a queued stream request', async (context) => {
  const fetcher = new FakeSegmentFetcher();
  const { runtime } = await createHarness(context, fetcher);
  const admissionBytes = Math.floor(spoolingPlan().memoryBudgetBytes / 2);
  const first = await runtime.acquireStreamMemory(
    admissionBytes,
    CommandPriority.High
  );
  const queued = runtime.acquireStreamMemory(1, CommandPriority.High);

  assert.equal(runtime.tryAcquirePromotionMemory(1), undefined);
  first.release();
  const second = await queued;
  second.release();
  assert.equal(runtime.memoryBudget.stats().usedBytes, 0);
});
