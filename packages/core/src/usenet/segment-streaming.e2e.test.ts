import assert from 'node:assert/strict';
import { promises as fs } from 'node:fs';
import net from 'node:net';
import path from 'node:path';
import { tmpdir } from 'node:os';
import test, { type TestContext } from 'node:test';
import yencode from 'yencode';
import '../config/index.js';
import { MultiProviderPool } from './pool/multi-provider-pool.js';
import { SegmentCache } from './pool/segment-cache.js';
import { SegmentSpoolingRuntime } from './pool/segment-spooling-runtime.js';
import { FileStream } from './pool/file-stream.js';
import { StatsAccumulator } from './stats/accumulator.js';
import { SpoolManager } from './spool/manager.js';
import type { SpoolFileHandle, SpoolFileSystem } from './spool/types.js';
import type { SegmentSpoolingPlan } from './resource-plan.js';
import type { Nzb } from './nzb/model.js';
import {
  DEFAULT_ENGINE_OPTIONS,
  type EngineOptions,
  type NzbSegmentRef,
  type ProviderConfig,
} from './types.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;

interface FakeArticleReply {
  readonly response?: Buffer;
  readonly gate?: Promise<void>;
  readonly fragmentBytes?: number;
  readonly status?: 430;
  readonly onWritten?: () => void;
}

type FakeArticleResolver = (
  messageId: string,
  occurrence: number
) => FakeArticleReply;

class FakeNntpServer {
  private readonly server = net.createServer();
  private readonly clients = new Set<net.Socket>();
  private readonly commandWaiters: Array<{
    readonly count: number;
    readonly resolve: () => void;
  }> = [];
  private occurrences = new Map<string, number>();
  readonly bodyCommands: string[] = [];

  private constructor(private readonly resolveArticle: FakeArticleResolver) {
    this.server.on('connection', (socket) => {
      this.clients.add(socket);
      socket.on('error', () => undefined);
      socket.on('close', () => this.clients.delete(socket));
      socket.write('200 fake nntp ready\r\n', 'latin1');
      let pending = '';
      let responses = Promise.resolve();
      socket.on('data', (chunk: Buffer) => {
        pending += chunk.toString('latin1');
        for (;;) {
          const end = pending.indexOf('\r\n');
          if (end < 0) return;
          const command = pending.slice(0, end);
          pending = pending.slice(end + 2);
          if (command.startsWith('BODY ')) {
            const messageId = command.slice(6, -1);
            this.bodyCommands.push(messageId);
            this.resolveWaiters();
            responses = responses.then(() =>
              this.respondBody(socket, messageId)
            );
          } else if (command.startsWith('STAT ')) {
            responses = responses.then(() =>
              this.write(socket, Buffer.from('223 1 article exists\r\n'))
            );
          } else if (command === 'DATE') {
            responses = responses.then(() =>
              this.write(socket, Buffer.from('111 20260817120000\r\n'))
            );
          } else {
            responses = responses.then(() =>
              this.write(socket, Buffer.from('500 unsupported\r\n'))
            );
          }
        }
      });
    });
  }

  static async create(
    context: TestContext,
    resolver: FakeArticleResolver
  ): Promise<FakeNntpServer> {
    const fake = new FakeNntpServer(resolver);
    await new Promise<void>((resolve, reject) => {
      fake.server.once('error', reject);
      fake.server.listen(0, '127.0.0.1', () => {
        fake.server.removeListener('error', reject);
        resolve();
      });
    });
    context.after(() => fake.close());
    return fake;
  }

  get port(): number {
    const address = this.server.address();
    assert(address && typeof address !== 'string');
    return address.port;
  }

  waitForBodyCount(count: number): Promise<void> {
    if (this.bodyCommands.length >= count) return Promise.resolve();
    const waiter = Promise.withResolvers<void>();
    this.commandWaiters.push({ count, resolve: waiter.resolve });
    return waiter.promise;
  }

  private resolveWaiters(): void {
    for (let index = this.commandWaiters.length - 1; index >= 0; index--) {
      const waiter = this.commandWaiters[index];
      if (this.bodyCommands.length < waiter.count) continue;
      this.commandWaiters.splice(index, 1);
      waiter.resolve();
    }
  }

  private async respondBody(
    socket: net.Socket,
    messageId: string
  ): Promise<void> {
    const occurrence = (this.occurrences.get(messageId) ?? 0) + 1;
    this.occurrences.set(messageId, occurrence);
    const reply = this.resolveArticle(messageId, occurrence);
    await reply.gate;
    if (reply.status === 430) {
      await this.write(socket, Buffer.from('430 no such article\r\n'));
    } else {
      assert(reply.response);
      await this.write(socket, reply.response, reply.fragmentBytes);
    }
    reply.onWritten?.();
  }

  private async write(
    socket: net.Socket,
    buffer: Buffer,
    fragmentBytes = buffer.length
  ): Promise<void> {
    for (let offset = 0; offset < buffer.length; offset += fragmentBytes) {
      const chunk = buffer.subarray(offset, offset + fragmentBytes);
      await new Promise<void>((resolve) => socket.write(chunk, resolve));
      // An event-loop boundary models a fragmented/slow provider without a
      // wall-clock assertion or sleep.
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
  }

  private async close(): Promise<void> {
    for (const socket of this.clients) socket.destroy();
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

function articleResponse(
  body: Buffer,
  part: number,
  totalParts: number,
  begin: number,
  fileSize: number
): Buffer {
  const encoded = yencode.post('stream.bin', body, 128);
  const firstLineEnd = encoded.indexOf('\r\n');
  assert(firstLineEnd >= 0);
  return Buffer.concat([
    Buffer.from(
      [
        '222 article follows',
        `=ybegin part=${part} total=${totalParts} line=128 size=${fileSize} name=stream.bin`,
        `=ypart begin=${begin + 1} end=${begin + body.length}`,
        '',
      ].join('\r\n'),
      'latin1'
    ),
    encoded.subarray(firstLineEnd + 2),
    Buffer.from('\r\n.\r\n', 'latin1'),
  ]);
}

function testPlan(
  overrides: Partial<SegmentSpoolingPlan> = {}
): SegmentSpoolingPlan {
  return {
    memoryBudgetBytes: 2 * MEBIBYTE_BYTES,
    perStreamBufferBytes: 512 * KIBIBYTE_BYTES,
    spoolBytes: 16 * MEBIBYTE_BYTES,
    minFreeDiskBytes: 0,
    decoderChunkBytes: 64 * KIBIBYTE_BYTES,
    writerQueueBytes: 128 * KIBIBYTE_BYTES,
    readerHighWaterMarkBytes: 64 * KIBIBYTE_BYTES,
    perDownloadBaseLeaseBytes: 128 * KIBIBYTE_BYTES,
    maxOpenSpoolFiles: 16,
    orphanTtlMs: 60_000,
    ...overrides,
  };
}

function provider(
  id: string,
  port: number,
  options: Partial<ProviderConfig> = {}
): ProviderConfig {
  return {
    id,
    host: '127.0.0.1',
    port,
    tls: false,
    maxConnections: 2,
    pipelineDepth: 1,
    priority: 0,
    ...options,
  };
}

interface E2eHarness {
  readonly pool: MultiProviderPool;
  readonly runtime?: SegmentSpoolingRuntime;
  readonly options: EngineOptions;
  readonly plan?: SegmentSpoolingPlan;
  readonly waitForPoolIdle: () => Promise<void>;
}

/** Single-waiter barrier fed by the pool's bounded operation-owner count. */
class PoolIdleBarrier {
  private active = 0;
  private waiter: PromiseWithResolvers<void> | undefined;

  readonly observe = (active: number): void => {
    this.active = active;
    if (active !== 0 || !this.waiter) return;
    this.waiter.resolve();
    this.waiter = undefined;
  };

  wait(): Promise<void> {
    if (this.active === 0) return Promise.resolve();
    this.waiter ??= Promise.withResolvers<void>();
    return this.waiter.promise;
  }
}

async function createHarness(
  context: TestContext,
  providers: ProviderConfig[],
  mode: 'segment_buffering' | 'segment_spooling',
  options: {
    readonly plan?: SegmentSpoolingPlan;
    readonly fileSystem?: Partial<SpoolFileSystem>;
  } = {}
): Promise<E2eHarness> {
  const cacheRoot = await fs.mkdtemp(path.join(tmpdir(), 'spooling-e2e-'));
  const engineOptions: EngineOptions = {
    ...DEFAULT_ENGINE_OPTIONS,
    streamingMode: mode,
    maxConcurrentDownloads: 6,
    prefetchSegments: 4,
    segmentDiskCacheBytes: 0,
    segmentTimeoutMs: 10_000,
    segmentStallTimeoutMs: 2_000,
  };
  const cache = new SegmentCache({ arenaBytes: 4 * MEBIBYTE_BYTES });
  const idle = new PoolIdleBarrier();
  const plan =
    mode === 'segment_spooling' ? (options.plan ?? testPlan()) : undefined;
  const runtime = plan
    ? new SegmentSpoolingRuntime({
        plan,
        engineId: 'block-9-e2e',
        cacheRoot,
        artifactCache: cache,
        spoolManager: new SpoolManager({
          plan,
          engineId: 'block-9-e2e',
          cacheRoot,
          fileSystem: options.fileSystem,
        }),
      })
    : undefined;
  const pool = new MultiProviderPool(
    providers,
    engineOptions,
    cache,
    new StatsAccumulator(),
    {
      spooling: runtime,
      onActiveOperationCountChanged: idle.observe,
    }
  );
  context.after(async () => {
    await Promise.allSettled([pool.close(), cache.close()]);
    await fs.rm(cacheRoot, { recursive: true, force: true });
  });
  return {
    pool,
    runtime,
    options: engineOptions,
    plan,
    waitForPoolIdle: () => idle.wait(),
  };
}

function fileStream(
  harness: E2eHarness,
  bodies: readonly Buffer[],
  knownSize = bodies.reduce((sum, body) => sum + body.length, 0)
): FileStream {
  let offset = 0;
  const segments: NzbSegmentRef[] = bodies.map((body, index) => {
    offset += body.length;
    return {
      messageId: `segment-${index}`,
      bytes: body.length + 512,
    };
  });
  return new FileStream(
    harness.pool,
    { segments, knownSize, filename: 'stream.bin' },
    'e2e-nzb',
    harness.options,
    undefined,
    undefined,
    harness.plan
      ? {
          mode: 'segment_spooling',
          arenaBytes: 4 * MEBIBYTE_BYTES,
          segmentSpooling: harness.plan,
        }
      : { mode: 'segment_buffering', arenaBytes: 4 * MEBIBYTE_BYTES }
  );
}

async function collect(stream: NodeJS.ReadableStream): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    assert(Buffer.isBuffer(chunk));
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function collectWithControlledPlayerPause(stream: NodeJS.ReadableStream): {
  readonly paused: Promise<void>;
  readonly result: Promise<Buffer>;
  readonly resume: () => void;
} {
  const paused = Promise.withResolvers<void>();
  const resume = Promise.withResolvers<void>();
  const chunks: Buffer[] = [];
  let pausedOnce = false;
  const result = new Promise<Buffer>((resolve, reject) => {
    stream.on('data', (chunk: Buffer) => {
      assert(Buffer.isBuffer(chunk));
      chunks.push(chunk);
      if (pausedOnce) return;
      pausedOnce = true;
      stream.pause();
      paused.resolve();
      void resume.promise.then(() => stream.resume());
    });
    stream.once('end', () => {
      if (!pausedOnce) paused.reject(new Error('player received no data'));
      resolve(Buffer.concat(chunks));
    });
    stream.once('error', (error) => {
      if (!pausedOnce) paused.reject(error);
      reject(error);
    });
  });
  return { paused: paused.promise, result, resume: resume.resolve };
}

test('fragmented pipelined yEnc streams preserve range, slow-player and buffering bytes', async (context) => {
  const bodies = [
    Buffer.from('alpha-'),
    Buffer.from('bravo-'),
    Buffer.from('charlie'),
  ];
  const expected = Buffer.concat(bodies);
  const firstGate = Promise.withResolvers<void>();
  let begin = 0;
  const responses = new Map(
    bodies.map((body, index) => {
      const response = articleResponse(
        body,
        index + 1,
        bodies.length,
        begin,
        expected.length
      );
      begin += body.length;
      return [`segment-${index}`, response] as const;
    })
  );
  const server = await FakeNntpServer.create(context, (messageId) => ({
    response: responses.get(messageId),
    gate: messageId === 'segment-0' ? firstGate.promise : undefined,
    fragmentBytes: 3,
  }));
  const spooling = await createHarness(
    context,
    [
      provider('pipeline', server.port, {
        maxConnections: 1,
        pipelineDepth: 3,
      }),
    ],
    'segment_spooling'
  );
  const file = fileStream(spooling, bodies);
  await file.open();
  const output = file.createReadStream();
  const player = collectWithControlledPlayerPause(output);
  await server.waitForBodyCount(3);
  assert.deepEqual(server.bodyCommands.slice(0, 3), [
    'segment-0',
    'segment-1',
    'segment-2',
  ]);
  assert(spooling.runtime);
  assert(spooling.runtime.stats().memory.usedBytes > 0);
  firstGate.resolve();
  await player.paused;
  // The consumer is explicitly paused while provider and spool work continue;
  // the combined Readable ownership must remain admitted until it resumes.
  assert(spooling.runtime.stats().memory.usedBytes > 0);
  player.resume();
  assert.deepEqual(await player.result, expected);
  assert.equal(spooling.runtime.stats().memory.usedBytes, 0);

  const range = file.createReadStream({ start: 7, end: 17 });
  assert.deepEqual(await collect(range), expected.subarray(7, 17));

  const [parallelA, parallelB] = await Promise.all([
    collect(file.createReadStream({ start: 0, end: 5 })),
    collect(file.createReadStream({ start: 12, end: expected.length })),
  ]);
  assert.deepEqual(parallelA, expected.subarray(0, 5));
  assert.deepEqual(parallelB, expected.subarray(12));

  const buffering = await createHarness(
    context,
    [
      provider('buffering', server.port, {
        maxConnections: 1,
        pipelineDepth: 2,
      }),
    ],
    'segment_buffering'
  );
  const bufferingFile = fileStream(buffering, bodies);
  await bufferingFile.open();
  assert.deepEqual(await collect(bufferingFile.createReadStream()), expected);
});

test('two active clients share one flight while one aborts independently', async (context) => {
  const body = Buffer.alloc(96 * KIBIBYTE_BYTES, 0x5a);
  const response = articleResponse(body, 1, 1, 0, body.length);
  const providerGate = Promise.withResolvers<void>();
  const providerFinished = Promise.withResolvers<void>();
  const server = await FakeNntpServer.create(context, () => ({
    response,
    gate: providerGate.promise,
    fragmentBytes: 1024,
    onWritten: providerFinished.resolve,
  }));
  const harness = await createHarness(
    context,
    [provider('parallel-clients', server.port)],
    'segment_spooling'
  );
  const file = fileStream(harness, [body]);
  await file.open();

  const first = file.createReadStream();
  const second = file.createReadStream();
  const firstResult = collect(first);
  const secondResult = collect(second);
  await server.waitForBodyCount(1);
  assert.equal(server.bodyCommands.length, 1, 'single-flight network fetch');

  first.destroy(new Error('client closed'));
  providerGate.resolve();
  await assert.rejects(firstResult, /client closed/);
  assert.deepEqual(await secondResult, body);
  await providerFinished.promise;
  await harness.waitForPoolIdle();

  assert.equal(server.bodyCommands.length, 1);
  assert(harness.runtime);
  assert.equal(harness.runtime.stats().memory.usedBytes, 0);
  assert.equal(harness.runtime.stats().spool.budget.reservedBytes, 0);
  assert.equal(harness.runtime.stats().spool.files.openFiles, 0);
  assert.equal(harness.runtime.stats().spool.artifacts, 0);
});

test('engine close terminates an active provider/read pipeline and reaches zero owners', async (context) => {
  // Load through the integration entry first; importing index.ts as a fresh
  // root would traverse the legacy integration/index cycle in the opposite
  // direction before UsenetEngineRegistry has initialized.
  await import('./integration/engine.js');
  const { UsenetEngine } = await import('./index.js');
  const body = Buffer.alloc(48 * KIBIBYTE_BYTES, 0x39);
  const response = articleResponse(body, 1, 1, 0, body.length);
  const playbackGate = Promise.withResolvers<void>();
  const server = await FakeNntpServer.create(
    context,
    (_messageId, occurrence) => ({
      response,
      gate: occurrence === 1 ? undefined : playbackGate.promise,
      fragmentBytes: 512,
    })
  );
  const engine = new UsenetEngine([provider('engine-close', server.port)], {
    ...DEFAULT_ENGINE_OPTIONS,
    streamingMode: 'segment_spooling',
    maxConcurrentDownloads: 2,
    prefetchSegments: 2,
    segmentDiskCacheBytes: 0,
    segmentSpoolingMemoryBudgetBytes: 16 * MEBIBYTE_BYTES,
    segmentSpoolingStreamBufferBytes: 2 * MEBIBYTE_BYTES,
    segmentSpoolingSpoolBytes: 64 * MEBIBYTE_BYTES,
    segmentSpoolingMinFreeDiskBytes: 0,
  });
  context.after(() => engine.close());
  const nzb: Nzb = {
    hash: 'engine-close-e2e',
    meta: {},
    files: [
      {
        subject: '"stream.bin" yEnc',
        groups: ['alt.binaries.test'],
        encodedSize: body.length + 512,
        filename: 'stream.bin',
        segments: [
          {
            number: 1,
            bytes: body.length + 512,
            messageId: 'engine-close-segment',
          },
        ],
      },
    ],
  };
  const file = await engine.openFileStream(nzb, { fileIndex: 0 });
  const reader = file.createReadStream();
  reader.on('error', () => undefined);
  reader.resume();
  await server.waitForBodyCount(2);
  const readerClosed = new Promise<void>((resolve) =>
    reader.once('close', resolve)
  );

  const closing = engine.close();
  await Promise.all([closing, readerClosed]);
  playbackGate.resolve();
  await engine.close();

  const resources = engine.liveStats().resources;
  assert.equal(resources.memory.usedBytes, 0);
  assert.equal(resources.memory.waiting, 0);
  assert.equal(resources.spool.reservedBytes, 0);
  assert.equal(resources.spool.actualBytes, 0);
  assert.equal(resources.spool.openFiles, 0);
  assert.equal(resources.spool.files, 0);
  assert.equal(resources.spool.sessions, 0);
  assert.equal(engine.poolInfo().globalDownloadsInUse, 0);
  assert.equal(engine.poolInfo().globalDownloadsWaiting, 0);
  assert.equal(engine.poolInfo().globalDownloadsOnWire, 0);
});

test('out-of-order completed spools remain ordered and provider 430 fails over', async (context) => {
  const bodies = [Buffer.from('first-'), Buffer.from('second')];
  const expected = Buffer.concat(bodies);
  const releaseFirst = Promise.withResolvers<void>();
  const secondWritten = Promise.withResolvers<void>();
  const responses = new Map([
    ['segment-0', articleResponse(bodies[0], 1, 2, 0, expected.length)],
    [
      'segment-1',
      articleResponse(bodies[1], 2, 2, bodies[0].length, expected.length),
    ],
  ]);
  const server = await FakeNntpServer.create(context, (messageId) => ({
    response: responses.get(messageId),
    gate: messageId === 'segment-0' ? releaseFirst.promise : undefined,
    onWritten: messageId === 'segment-1' ? secondWritten.resolve : undefined,
  }));
  const harness = await createHarness(
    context,
    [provider('out-of-order', server.port, { maxConnections: 2 })],
    'segment_spooling'
  );
  const file = fileStream(harness, bodies);
  await file.open();
  const result = collect(file.createReadStream());
  await server.waitForBodyCount(2);
  await secondWritten.promise;
  releaseFirst.resolve();
  assert.deepEqual(await result, expected);

  const missing = await FakeNntpServer.create(context, () => ({ status: 430 }));
  const backup = await FakeNntpServer.create(context, (messageId) => ({
    response: responses.get(messageId),
  }));
  const failover = await createHarness(
    context,
    [
      provider('primary', missing.port, { priority: 0 }),
      provider('backup', backup.port, { priority: 1, isBackup: true }),
    ],
    'segment_spooling'
  );
  const failoverFile = fileStream(failover, bodies);
  await failoverFile.open();
  assert.deepEqual(await collect(failoverFile.createReadStream()), expected);
  assert(missing.bodyCommands.length > 0);
  assert(backup.bodyCommands.length > 0);
});

test('slow disk, ENOSPC, EACCES and client abort leave no spool ownership', async (context) => {
  const body = Buffer.alloc(32 * KIBIBYTE_BYTES, 0x61);
  const response = articleResponse(body, 1, 1, 0, body.length);
  const server = await FakeNntpServer.create(context, () => ({ response }));

  const writeStarted = Promise.withResolvers<void>();
  const allowWrite = Promise.withResolvers<void>();
  let gatedWrite = true;
  const slowOpen: SpoolFileSystem['open'] = async (filePath, flags, mode) => {
    const handle = await fs.open(filePath, flags, mode);
    const wrapped: SpoolFileHandle = {
      read: async (buffer, offset, length, position) => {
        const result = await handle.read(buffer, offset, length, position);
        return { bytesRead: result.bytesRead };
      },
      write: async (buffer, offset, length, position) => {
        if (flags === 'wx+' && gatedWrite) {
          gatedWrite = false;
          writeStarted.resolve();
          await allowWrite.promise;
        }
        const result = await handle.write(buffer, offset, length, position);
        return { bytesWritten: result.bytesWritten };
      },
      close: () => handle.close(),
    };
    return wrapped;
  };
  const slowDisk = await createHarness(
    context,
    [provider('slow-disk', server.port)],
    'segment_spooling',
    { fileSystem: { open: slowOpen } }
  );
  const slowFile = fileStream(slowDisk, [body]);
  await slowFile.open();
  const slowResult = collect(slowFile.createReadStream());
  await writeStarted.promise;
  assert(slowDisk.runtime);
  assert(slowDisk.runtime.stats().spool.budget.reservedBytes > 0);
  allowWrite.resolve();
  assert.deepEqual(await slowResult, body);

  for (const code of ['ENOSPC', 'EACCES'] as const) {
    const failingOpen: SpoolFileSystem['open'] = async (
      filePath,
      flags,
      mode
    ) => {
      if (flags === 'wx+') {
        throw Object.assign(new Error('injected spool failure'), { code });
      }
      const handle = await fs.open(filePath, flags, mode);
      return {
        read: async (buffer, offset, length, position) => {
          const result = await handle.read(buffer, offset, length, position);
          return { bytesRead: result.bytesRead };
        },
        write: async (buffer, offset, length, position) => {
          const result = await handle.write(buffer, offset, length, position);
          return { bytesWritten: result.bytesWritten };
        },
        close: () => handle.close(),
      };
    };
    const failed = await createHarness(
      context,
      [provider(`failed-${code}`, server.port)],
      'segment_spooling',
      { fileSystem: { open: failingOpen } }
    );
    const failedFile = fileStream(failed, [body]);
    await failedFile.open();
    await assert.rejects(collect(failedFile.createReadStream()), (error) => {
      assert(error instanceof Error && 'code' in error);
      return code === 'ENOSPC'
        ? error.code === 'USENET_SPOOL_DISK_FULL'
        : error.code === 'USENET_SPOOL_UNAVAILABLE';
    });
    assert(failed.runtime);
    assert.equal(failed.runtime.stats().memory.usedBytes, 0);
    assert.equal(failed.runtime.stats().spool.budget.reservedBytes, 0);
  }

  const providerGate = Promise.withResolvers<void>();
  const providerFinished = Promise.withResolvers<void>();
  const abortServer = await FakeNntpServer.create(context, () => ({
    response,
    gate: providerGate.promise,
    onWritten: providerFinished.resolve,
  }));
  const abortHarness = await createHarness(
    context,
    [provider('abort', abortServer.port)],
    'segment_spooling'
  );
  const abortFile = fileStream(abortHarness, [body]);
  await abortFile.open();
  const aborted = abortFile.createReadStream();
  aborted.on('error', () => undefined);
  aborted.resume();
  await abortServer.waitForBodyCount(1);
  const closed = new Promise<void>((resolve) => aborted.once('close', resolve));
  aborted.destroy(new Error('client closed'));
  providerGate.resolve();
  await closed;
  await providerFinished.promise;
  await abortHarness.waitForPoolIdle();
  assert(abortHarness.runtime);
  // The on-wire BODY may finish consistently after the last reader leaves, but
  // client abort alone must retire the flight and every owner. Pool shutdown is
  // deliberately not used as a cleanup crutch in this assertion.
  assert.equal(abortHarness.runtime.stats().memory.usedBytes, 0);
  assert.equal(abortHarness.runtime.stats().spool.budget.reservedBytes, 0);
  assert.equal(abortHarness.runtime.stats().spool.files.openFiles, 0);
  assert.equal(abortHarness.runtime.stats().spool.artifacts, 0);
});
