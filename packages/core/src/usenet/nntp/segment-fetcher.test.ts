import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import net from 'node:net';
import test, { type TestContext } from 'node:test';
import yencode from 'yencode';
import '../../config/index.js';
import type {
  BackpressuredByteSink,
  DecodedSegmentMetadata,
} from '../pool/streaming-yenc-article-decoder.js';
import {
  CommandPriority,
  DEFAULT_ENGINE_OPTIONS,
  type ProviderConfig,
} from '../types.js';
import type { StatsEvent } from '../stats/types.js';
import { UsenetSpoolError } from '../spool/errors.js';
import {
  NntpError,
  classifyNntpFailure,
  isProviderUnavailableError,
} from './errors.js';
import { YencDecodeError, YencMetadataError } from '../pool/yenc.js';
import {
  ProviderWorkerPool,
  type WorkerPoolOptions,
  type WorkerPoolScheduledTask,
  type WorkerPoolScheduler,
} from './provider-worker-pool.js';
import {
  LocalSegmentFetcher,
  type StatsSink,
  type StreamingSegmentAttempt,
} from './segment-fetcher.js';

class AutomaticNntpServer {
  private readonly server = net.createServer();
  private readonly clients = new Set<net.Socket>();
  private readonly pendingResponses: net.Socket[] = [];
  private readonly commandWaiters: {
    readonly count: number;
    readonly resolve: () => void;
  }[] = [];
  readonly commands: string[] = [];
  maxPendingResponses = 0;

  private constructor(private readonly response: Buffer | undefined) {
    this.server.on('connection', (socket) => {
      this.clients.add(socket);
      socket.on('error', () => undefined);
      socket.on('close', () => this.clients.delete(socket));
      let pending = '';
      socket.on('data', (chunk: Buffer) => {
        pending += chunk.toString('latin1');
        for (;;) {
          const end = pending.indexOf('\r\n');
          if (end < 0) return;
          const command = pending.slice(0, end);
          pending = pending.slice(end + 2);
          this.commands.push(command);
          if (this.response) socket.write(this.response);
          else {
            this.pendingResponses.push(socket);
            this.maxPendingResponses = Math.max(
              this.maxPendingResponses,
              this.pendingResponses.length
            );
          }
          this.resolveCommandWaiters();
        }
      });
      socket.write('200 local test server ready\r\n');
    });
  }

  static async create(
    context: TestContext,
    response?: Buffer
  ): Promise<AutomaticNntpServer> {
    const instance = new AutomaticNntpServer(response);
    await new Promise<void>((resolve, reject) => {
      instance.server.once('error', reject);
      instance.server.listen(0, '127.0.0.1', () => {
        instance.server.removeListener('error', reject);
        resolve();
      });
    });
    context.after(() => instance.close());
    return instance;
  }

  get port(): number {
    const address = this.server.address();
    assert(address && typeof address !== 'string');
    return address.port;
  }

  waitForCommandCount(count: number): Promise<void> {
    if (this.commands.length >= count) return Promise.resolve();
    const deferred = Promise.withResolvers<void>();
    this.commandWaiters.push({ count, resolve: deferred.resolve });
    return deferred.promise;
  }

  respondNext(response: Buffer): void {
    const socket = this.pendingResponses.shift();
    assert(socket, 'a command must be pending before its response is released');
    socket.write(response);
  }

  failNext(): void {
    const socket = this.pendingResponses.shift();
    assert(socket, 'a command must be pending before its socket is failed');
    socket.destroy();
  }

  private resolveCommandWaiters(): void {
    for (let index = this.commandWaiters.length - 1; index >= 0; index--) {
      const waiter = this.commandWaiters[index];
      if (this.commands.length < waiter.count) continue;
      this.commandWaiters.splice(index, 1);
      waiter.resolve();
    }
  }

  async close(): Promise<void> {
    for (const socket of this.clients) socket.destroy();
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

class ControlledWorkerPoolScheduler implements WorkerPoolScheduler {
  private callback: (() => void) | undefined;
  private active = false;

  every(_intervalMs: number, callback: () => void): WorkerPoolScheduledTask {
    assert.equal(this.callback, undefined);
    this.callback = callback;
    this.active = true;
    return {
      cancel: () => {
        this.active = false;
      },
    };
  }

  run(): void {
    assert.equal(this.active, true);
    assert(this.callback);
    this.callback();
  }
}

class NoopStats implements StatsSink {
  fetchStarted(_providerId: string): void {}
  fetchEnded(_providerId: string): void {}
  record(_event: StatsEvent): void {}
}

class CollectingSink implements BackpressuredByteSink {
  readonly chunks: Buffer[] = [];
  failure: Error | undefined;
  endCalls = 0;

  write(chunk: Buffer): boolean {
    this.chunks.push(chunk);
    return true;
  }

  onceDrain(_listener: () => void): void {
    assert.fail('the collecting sink never applies backpressure');
  }

  end(): Promise<void> {
    this.endCalls++;
    return Promise.resolve();
  }

  fail(error: Error): void {
    this.failure ??= error;
  }

  body(): Buffer {
    return Buffer.concat(this.chunks);
  }
}

class FailingSink implements BackpressuredByteSink {
  failure: Error | undefined;

  write(_chunk: Buffer): boolean {
    throw new UsenetSpoolError(
      'USENET_SPOOL_IO',
      'Synthetic local spool write failure'
    );
  }

  onceDrain(_listener: () => void): void {
    assert.fail('a failing sink never registers drain');
  }

  end(): Promise<void> {
    return Promise.resolve();
  }

  fail(error: Error): void {
    this.failure ??= error;
  }
}

function provider(
  id: string,
  port: number,
  priority: number,
  pipelineDepth?: number
): ProviderConfig {
  return {
    id,
    host: '127.0.0.1',
    port,
    tls: false,
    maxConnections: 1,
    priority,
    pipelineDepth,
  };
}

function workerPoolOptions(pipelineDepth: number): WorkerPoolOptions {
  return {
    dialTimeoutMs: 1000,
    idleConnectionMs: 1000,
    circuitBreakerThreshold: 2,
    circuitBreakerCooldownMs: 1000,
    pipelineDepth,
    streamingPriority: 1,
  };
}

async function warmWorkerPool(
  pool: ProviderWorkerPool,
  server: AutomaticNntpServer
): Promise<void> {
  const warm = pool.submit<boolean>({
    priority: CommandPriority.High,
    run: async (conn) => ({
      value: await conn.stat('warm', undefined, 1000),
      bytes: 0,
    }),
  });
  await server.waitForCommandCount(1);
  server.respondNext(Buffer.from('223 1 <warm> article exists\r\n', 'latin1'));
  await warm;
}

function submitPreparedBody(
  pool: ProviderWorkerPool,
  messageId: string,
  started: PromiseWithResolvers<void>,
  gate: PromiseWithResolvers<void>
): Promise<unknown> {
  return pool.submitPrepared<Buffer, undefined>({
    priority: CommandPriority.High,
    prepare: async () => {
      started.resolve();
      await gate.promise;
      return undefined;
    },
    run: async (conn, _prepared, markTransferStarted) => {
      const pending = conn.body(messageId, undefined, 1000, 5000);
      markTransferStarted();
      const value = await pending;
      return { value, bytes: value.length };
    },
    dispose: async () => undefined,
  });
}

function articleResponse(name: string, body: Buffer): Buffer {
  return Buffer.concat([
    Buffer.from('222 article follows\r\n', 'latin1'),
    yencode.post(name, body, 128),
    Buffer.from('\r\n.\r\n', 'latin1'),
  ]);
}

function multipartArticleResponse(body: Buffer): Buffer {
  const single = yencode.post('ignored.bin', body, 128);
  const firstLineEnd = single.indexOf('\r\n');
  assert(firstLineEnd >= 0);
  return Buffer.concat([
    Buffer.from(
      [
        '222 article follows',
        '=ybegin part=2 total=3 line=128 size=1234 name=range.bin',
        `=ypart begin=101 end=${100 + body.length}`,
        '',
      ].join('\r\n'),
      'latin1'
    ),
    single.subarray(firstLineEnd + 2),
    Buffer.from('\r\n.\r\n', 'latin1'),
  ]);
}

function collectingAttempt(disposals: {
  count: number;
}): StreamingSegmentAttempt<CollectingSink> {
  const sink = new CollectingSink();
  return {
    sink,
    value: sink,
    dispose: async (error) => {
      disposals.count++;
      sink.fail(error);
    },
  };
}

test('classifies local, client, content and provider NNTP faults for the circuit breaker', () => {
  const cases: readonly [NntpError, boolean][] = [
    [new NntpError('local_backpressure', 'carry full'), false],
    [
      new NntpError('timeout', 'local deadline', {
        timeoutSource: 'local_backpressure',
      }),
      false,
    ],
    [new NntpError('connection', 'aborted'), false],
    [new NntpError('article_not_found', 'missing', { code: 430 }), false],
    [new NntpError('connection_limit', 'account capacity'), false],
    [
      new NntpError('timeout', 'provider silent', {
        timeoutSource: 'provider_stall',
      }),
      true,
    ],
    [new NntpError('protocol', 'invalid framing'), true],
    [new NntpError('connection', 'socket closed by peer'), true],
  ];
  for (const [error, expected] of cases) {
    assert.equal(
      classifyNntpFailure(error).countsTowardCircuitBreaker,
      expected,
      `${error.kind}/${error.timeoutSource ?? 'none'}`
    );
  }
  assert.equal(
    isProviderUnavailableError(
      new NntpError('timeout', 'local deadline', {
        timeoutSource: 'local_backpressure',
      })
    ),
    false
  );
  assert.equal(
    isProviderUnavailableError(new NntpError('connection', 'aborted')),
    false
  );
  assert.equal(
    isProviderUnavailableError(
      new NntpError('timeout', 'provider silent', {
        timeoutSource: 'provider_stall',
      })
    ),
    true
  );
});

test('depth-one keepalive reserves the only logical pipeline slot', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const scheduler = new ControlledWorkerPoolScheduler();
  const pool = new ProviderWorkerPool(
    provider('keepalive-depth-one', server.port, 0, 1),
    workerPoolOptions(1),
    scheduler
  );
  context.after(() => pool.close());
  await warmWorkerPool(pool, server);

  scheduler.run();
  await server.waitForCommandCount(2);
  assert.equal(server.commands[1], 'DATE');

  const started = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  let preparationStarted = false;
  void started.promise.then(() => {
    preparationStarted = true;
  });
  const fetch = submitPreparedBody(pool, 'after-depth-one-date', started, gate);
  await Promise.resolve();

  assert.equal(preparationStarted, false);
  assert.equal(pool.inFlight, 1);
  assert.deepEqual(
    {
      freeSlots: pool.info().freeSlots,
      acquired: pool.info().acquired,
      idle: pool.info().idle,
      queued: pool.info().queued,
    },
    { freeSlots: 0, acquired: 1, idle: 0, queued: 1 }
  );

  server.respondNext(Buffer.from('111 20260814120000\r\n', 'latin1'));
  await started.promise;
  gate.resolve();
  await server.waitForCommandCount(3);
  assert.equal(server.commands[2], 'BODY <after-depth-one-date>');
  server.respondNext(
    Buffer.from('222 article follows\r\ndepth-one\r\n.\r\n', 'latin1')
  );
  await fetch;

  assert.equal(pool.inFlight, 0);
  assert.equal(pool.info().freeSlots, 1);
  assert.equal(pool.info().queued, 0);
});

test('depth-two keepalive admits only one provider assignment beside DATE', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const scheduler = new ControlledWorkerPoolScheduler();
  const pool = new ProviderWorkerPool(
    provider('keepalive-depth-two', server.port, 0, 2),
    workerPoolOptions(2),
    scheduler
  );
  context.after(() => pool.close());
  await warmWorkerPool(pool, server);

  scheduler.run();
  await server.waitForCommandCount(2);
  const starts: string[] = [];
  const firstStarted = Promise.withResolvers<void>();
  const secondStarted = Promise.withResolvers<void>();
  const firstGate = Promise.withResolvers<void>();
  const secondGate = Promise.withResolvers<void>();
  void firstStarted.promise.then(() => starts.push('first'));
  void secondStarted.promise.then(() => starts.push('second'));
  const first = submitPreparedBody(
    pool,
    'keepalive-first',
    firstStarted,
    firstGate
  );
  const second = submitPreparedBody(
    pool,
    'keepalive-second',
    secondStarted,
    secondGate
  );

  await firstStarted.promise;
  await Promise.resolve();
  assert.deepEqual(starts, ['first']);
  assert.equal(pool.info().freeSlots, 0);
  assert.equal(pool.info().queued, 1);
  firstGate.resolve();
  await server.waitForCommandCount(3);
  assert.deepEqual(server.commands.slice(1), [
    'DATE',
    'BODY <keepalive-first>',
  ]);
  assert.equal(server.maxPendingResponses, 2);

  server.respondNext(Buffer.from('111 20260814120000\r\n', 'latin1'));
  await secondStarted.promise;
  assert.deepEqual(starts, ['first', 'second']);
  assert.equal(pool.inFlight, 2);
  assert.equal(pool.info().freeSlots, 0);
  secondGate.resolve();
  await server.waitForCommandCount(4);
  assert.equal(server.maxPendingResponses, 2);
  server.respondNext(
    Buffer.from('222 article follows\r\nfirst\r\n.\r\n', 'latin1')
  );
  server.respondNext(
    Buffer.from('222 article follows\r\nsecond\r\n.\r\n', 'latin1')
  );
  await Promise.all([first, second]);

  assert.equal(pool.inFlight, 0);
  assert.equal(pool.info().freeSlots, 2);
});

test('keepalive failure releases maintenance occupancy and reconnects queued work', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const scheduler = new ControlledWorkerPoolScheduler();
  const pool = new ProviderWorkerPool(
    provider('keepalive-failure', server.port, 0, 1),
    workerPoolOptions(1),
    scheduler
  );
  context.after(() => pool.close());
  await warmWorkerPool(pool, server);

  scheduler.run();
  await server.waitForCommandCount(2);
  const started = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  const fetch = submitPreparedBody(pool, 'after-date-failure', started, gate);
  assert.equal(pool.inFlight, 1);
  server.failNext();

  await started.promise;
  gate.resolve();
  await server.waitForCommandCount(3);
  assert.equal(server.commands[2], 'BODY <after-date-failure>');
  server.respondNext(
    Buffer.from('222 article follows\r\nreconnected\r\n.\r\n', 'latin1')
  );
  await fetch;

  assert.equal(pool.inFlight, 0);
  assert.equal(pool.info().freeSlots, 1);
  assert.equal(pool.info().tripped, false);
});

test('pool close releases an active keepalive reservation exactly once', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const scheduler = new ControlledWorkerPoolScheduler();
  const pool = new ProviderWorkerPool(
    provider('keepalive-close', server.port, 0, 1),
    workerPoolOptions(1),
    scheduler
  );
  await warmWorkerPool(pool, server);

  scheduler.run();
  await server.waitForCommandCount(2);
  assert.equal(pool.inFlight, 1);
  pool.close();
  assert.equal(pool.inFlight, 0);
  pool.close();
  assert.equal(pool.inFlight, 0);
});

test('keepalive occupancy is visible without changing provider service metrics', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const scheduler = new ControlledWorkerPoolScheduler();
  const pool = new ProviderWorkerPool(
    provider('keepalive-stats', server.port, 0, 1),
    workerPoolOptions(1),
    scheduler
  );
  context.after(() => pool.close());
  await warmWorkerPool(pool, server);
  pool.recordServiceTime(17);
  pool.recordThroughput(1000, 10);
  pool.recordOutcome(true);
  const baseline = {
    service: pool.avgServiceTimeMs,
    throughput: pool.throughput,
    missRate: pool.missRate,
  };

  scheduler.run();
  await server.waitForCommandCount(2);
  assert.equal(pool.info().acquired, 1);
  assert.equal(pool.info().idle, 0);
  assert.equal(pool.info().freeSlots, 0);
  const afterDate = pool.submit<boolean>({
    priority: CommandPriority.High,
    run: async (conn) => ({
      value: await conn.stat('after-date', undefined, 1000),
      bytes: 0,
    }),
  });
  server.respondNext(Buffer.from('111 20260814120000\r\n', 'latin1'));
  await server.waitForCommandCount(3);

  assert.deepEqual(
    {
      service: pool.avgServiceTimeMs,
      throughput: pool.throughput,
      missRate: pool.missRate,
    },
    baseline
  );
  assert.equal(pool.info().queued, 0);
  assert.equal(pool.info().acquired, 1);
  server.respondNext(
    Buffer.from('223 2 <after-date> article exists\r\n', 'latin1')
  );
  await afterDate;
  assert.equal(pool.inFlight, 0);
});

test('streaming SegmentFetcher preserves provider order and 430 failover with fresh attempt ownership', async (context) => {
  const decoded = Buffer.from([0, 1, 2, 3, 42, 61, 127, 128, 200, 254, 255]);
  const encoded = yencode.post('failover.bin', decoded, 128);
  const missing = await AutomaticNntpServer.create(
    context,
    Buffer.from('430 article not found\r\n', 'latin1')
  );
  const serving = await AutomaticNntpServer.create(
    context,
    Buffer.concat([
      Buffer.from('222 article follows\r\n', 'latin1'),
      encoded,
      Buffer.from('\r\n.\r\n', 'latin1'),
    ])
  );
  const fetcher = new LocalSegmentFetcher(
    [provider('first', missing.port, 0), provider('second', serving.port, 1)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const attempts: {
    readonly sink: CollectingSink;
    disposed: number;
  }[] = [];

  const result = await fetcher.fetchBodyToSink(
    { messageId: 'provider-failover' },
    'nzb-hash',
    CommandPriority.High,
    async (): Promise<StreamingSegmentAttempt<CollectingSink>> => {
      const record = { sink: new CollectingSink(), disposed: 0 };
      attempts.push(record);
      return {
        sink: record.sink,
        value: record.sink,
        dispose: async (error) => {
          record.disposed++;
          record.sink.fail(error);
        },
      };
    }
  );

  assert.equal(missing.commands.length, 1);
  assert.equal(missing.commands[0], 'BODY <provider-failover>');
  assert.equal(serving.commands.length, 1);
  assert.equal(serving.commands[0], 'BODY <provider-failover>');
  assert.equal(attempts.length, 2);
  assert.equal(attempts[0].disposed, 1);
  assert.equal(attempts[0].sink.failure?.name, 'NntpError');
  assert.equal(attempts[1].disposed, 0);
  assert.equal(attempts[1].sink.endCalls, 1);
  assert.deepEqual(result.value.body(), decoded);
  assert.deepEqual(result.metadata, {
    byteRange: undefined,
    fileSize: decoded.length,
    totalParts: undefined,
    name: 'failover.bin',
    size: decoded.length,
  } satisfies DecodedSegmentMetadata);
});

test('streaming SegmentFetcher disposes an undecodable provider attempt before failover', async (context) => {
  const decoded = Buffer.from('valid copy from the second provider');
  const invalid = Buffer.from(
    '222 article follows\r\n=ybegin line=128 size=1 name=bad.bin\r\n+\r\n.\r\n',
    'latin1'
  );
  const encoded = yencode.post('good.bin', decoded, 128);
  const corrupt = await AutomaticNntpServer.create(context, invalid);
  const serving = await AutomaticNntpServer.create(
    context,
    Buffer.concat([
      Buffer.from('222 article follows\r\n', 'latin1'),
      encoded,
      Buffer.from('\r\n.\r\n', 'latin1'),
    ])
  );
  const fetcher = new LocalSegmentFetcher(
    [provider('corrupt', corrupt.port, 0), provider('good', serving.port, 1)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const attempts: {
    readonly sink: CollectingSink;
    disposed: number;
  }[] = [];

  const result = await fetcher.fetchBodyToSink(
    { messageId: 'decode-failover' },
    'nzb-hash',
    CommandPriority.High,
    async (): Promise<StreamingSegmentAttempt<CollectingSink>> => {
      const record = { sink: new CollectingSink(), disposed: 0 };
      attempts.push(record);
      return {
        sink: record.sink,
        value: record.sink,
        dispose: async (error) => {
          record.disposed++;
          record.sink.fail(error);
        },
      };
    }
  );

  assert.deepEqual(corrupt.commands, ['BODY <decode-failover>']);
  assert.deepEqual(serving.commands, ['BODY <decode-failover>']);
  assert.equal(attempts.length, 2);
  assert.equal(attempts[0].disposed, 1);
  assert.equal(attempts[0].sink.failure?.name, 'YencDecodeError');
  assert.equal(attempts[1].disposed, 0);
  assert.deepEqual(result.value.body(), decoded);
  assert.equal(result.metadata.name, 'good.bin');
});

test('a local spool failure does not trip the provider circuit', async (context) => {
  const decoded = Buffer.from('provider remains healthy');
  const encoded = yencode.post('healthy.bin', decoded, 128);
  const server = await AutomaticNntpServer.create(
    context,
    Buffer.concat([
      Buffer.from('222 article follows\r\n', 'latin1'),
      encoded,
      Buffer.from('\r\n.\r\n', 'latin1'),
    ])
  );
  const fetcher = new LocalSegmentFetcher(
    [provider('healthy', server.port, 0)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      circuitBreakerThreshold: 1,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const failing = new FailingSink();
  let disposals = 0;

  await assert.rejects(
    fetcher.fetchBodyToSink(
      { messageId: 'local-spool-failure' },
      'nzb-hash',
      CommandPriority.High,
      async (): Promise<StreamingSegmentAttempt<FailingSink>> => ({
        sink: failing,
        value: failing,
        dispose: async () => {
          disposals++;
        },
      })
    ),
    (error: unknown) => {
      assert(error instanceof UsenetSpoolError);
      assert.equal(error.code, 'USENET_SPOOL_IO');
      return true;
    }
  );
  assert.equal(disposals, 1);
  assert.equal(fetcher.info()[0].tripped, false);

  const recovered = await fetcher.fetchBodyToSink(
    { messageId: 'after-local-spool-failure' },
    'nzb-hash',
    CommandPriority.High,
    async (): Promise<StreamingSegmentAttempt<CollectingSink>> => {
      const sink = new CollectingSink();
      return { sink, value: sink, dispose: async () => undefined };
    }
  );
  assert.deepEqual(recovered.value.body(), decoded);
  assert.equal(fetcher.info()[0].tripped, false);
  assert.deepEqual(server.commands, [
    'BODY <local-spool-failure>',
    'BODY <after-local-spool-failure>',
  ]);
});

test('provider worker circuit excludes local backpressure and client cancellation', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const pool = new ProviderWorkerPool(provider('matrix', server.port, 0), {
    ...workerPoolOptions(1),
    circuitBreakerThreshold: 1,
  });
  context.after(() => pool.close());
  await warmWorkerPool(pool, server);

  for (const error of [
    new NntpError('local_backpressure', 'bounded carry exceeded'),
    new NntpError('timeout', 'local deadline', {
      timeoutSource: 'local_backpressure',
    }),
    new NntpError('connection', 'aborted'),
  ]) {
    await assert.rejects(
      pool.submit({
        priority: CommandPriority.High,
        run: async () => {
          throw error;
        },
      }),
      (actual: unknown) => actual === error
    );
    assert.equal(pool.info().tripped, false);
  }

  const providerStall = new NntpError('timeout', 'provider silent', {
    timeoutSource: 'provider_stall',
  });
  await assert.rejects(
    pool.submit({
      priority: CommandPriority.High,
      run: async () => {
        throw providerStall;
      },
    }),
    (actual: unknown) => actual === providerStall
  );
  assert.equal(pool.info().tripped, true);
});

test('streaming attempts occupy a depth-one provider slot before async preparation completes', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const fetcher = new LocalSegmentFetcher(
    [provider('depth-one', server.port, 0, 1)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const response = articleResponse('depth-one.bin', Buffer.from('depth-one'));
  const preparationGates = Array.from({ length: 3 }, () =>
    Promise.withResolvers<void>()
  );
  const preparationStarted = Array.from({ length: 3 }, () =>
    Promise.withResolvers<void>()
  );
  const starts: string[] = [];
  const onWire: string[] = [];
  const disposals = { count: 0 };

  const fetches = preparationGates.map((gate, index) => {
    const id = `depth-one-${index}`;
    return fetcher.fetchBodyToSink(
      { messageId: id },
      'nzb-depth-one',
      CommandPriority.High,
      async () => {
        starts.push(id);
        preparationStarted[index].resolve();
        await gate.promise;
        return collectingAttempt(disposals);
      },
      undefined,
      () => onWire.push(id)
    );
  });

  await preparationStarted[0].promise;
  assert.deepEqual(starts, ['depth-one-0']);
  const occupied = fetcher.info()[0];
  assert.equal(occupied.freeSlots, 0);
  assert.equal(occupied.queued, 2);
  assert.equal(occupied.acquired, 1);
  assert.deepEqual(onWire, []);

  for (let index = 0; index < fetches.length; index++) {
    preparationGates[index].resolve();
    await server.waitForCommandCount(index + 1);
    assert.equal(server.commands.length, index + 1);
    assert.deepEqual(
      onWire,
      fetches
        .slice(0, index + 1)
        .map((_, wireIndex) => `depth-one-${wireIndex}`)
    );
    server.respondNext(response);
    if (index + 1 < fetches.length) {
      await preparationStarted[index + 1].promise;
      assert.equal(starts.length, index + 2);
      assert.equal(fetcher.info()[0].freeSlots, 0);
    }
  }

  const results = await Promise.all(fetches);
  assert.deepEqual(
    server.commands,
    fetches.map((_, index) => `BODY <depth-one-${index}>`)
  );
  for (const result of results) {
    assert.deepEqual(result.value.body(), Buffer.from('depth-one'));
  }
  assert.equal(disposals.count, 0);
  assert.equal(fetcher.info()[0].freeSlots, 1);
  assert.equal(fetcher.info()[0].queued, 0);
});

test('streaming assignments respect depth two and admit exactly one successor', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const fetcher = new LocalSegmentFetcher(
    [provider('depth-two', server.port, 0, 2)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const response = articleResponse('depth-two.bin', Buffer.from('depth-two'));
  const preparationGates = Array.from({ length: 3 }, () =>
    Promise.withResolvers<void>()
  );
  const preparationStarted = Array.from({ length: 3 }, () =>
    Promise.withResolvers<void>()
  );
  const starts: string[] = [];
  const disposals = { count: 0 };

  const fetches = preparationGates.map((gate, index) => {
    const id = `depth-two-${index}`;
    return fetcher.fetchBodyToSink(
      { messageId: id },
      'nzb-depth-two',
      CommandPriority.High,
      async () => {
        starts.push(id);
        preparationStarted[index].resolve();
        await gate.promise;
        return collectingAttempt(disposals);
      }
    );
  });

  await Promise.all([
    preparationStarted[0].promise,
    preparationStarted[1].promise,
  ]);
  assert.deepEqual(starts, ['depth-two-0', 'depth-two-1']);
  assert.equal(fetcher.info()[0].freeSlots, 0);
  assert.equal(fetcher.info()[0].queued, 1);

  preparationGates[1].resolve();
  await Promise.resolve();
  assert.equal(server.commands.length, 0);
  preparationGates[0].resolve();
  await server.waitForCommandCount(2);
  assert.deepEqual(server.commands, [
    'BODY <depth-two-0>',
    'BODY <depth-two-1>',
  ]);

  server.respondNext(response);
  await preparationStarted[2].promise;
  assert.equal(starts.length, 3);
  assert.equal(fetcher.info()[0].freeSlots, 0);
  assert.equal(fetcher.info()[0].queued, 0);
  preparationGates[2].resolve();
  await server.waitForCommandCount(3);
  assert.deepEqual(server.commands, [
    'BODY <depth-two-0>',
    'BODY <depth-two-1>',
    'BODY <depth-two-2>',
  ]);
  server.respondNext(response);
  server.respondNext(response);

  await Promise.all(fetches);
  assert.equal(disposals.count, 0);
  assert.equal(fetcher.info()[0].freeSlots, 2);
});

test('out-of-order streaming preparation cannot invert the provider command FIFO', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const fetcher = new LocalSegmentFetcher(
    [provider('fifo', server.port, 0, 2)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const response = articleResponse('fifo.bin', Buffer.from('fifo'));
  const slowGate = Promise.withResolvers<void>();
  const slowStarted = Promise.withResolvers<void>();
  const fastStarted = Promise.withResolvers<void>();
  const disposals = { count: 0 };

  const slow = fetcher.fetchBodyToSink(
    { messageId: 'slow-first' },
    'nzb-fifo',
    CommandPriority.High,
    async () => {
      slowStarted.resolve();
      await slowGate.promise;
      return collectingAttempt(disposals);
    }
  );
  const fast = fetcher.fetchBodyToSink(
    { messageId: 'fast-second' },
    'nzb-fifo',
    CommandPriority.High,
    async () => {
      fastStarted.resolve();
      return collectingAttempt(disposals);
    }
  );

  await Promise.all([slowStarted.promise, fastStarted.promise]);
  await Promise.resolve();
  assert.equal(server.commands.length, 0);
  slowGate.resolve();
  await server.waitForCommandCount(2);
  assert.deepEqual(server.commands, [
    'BODY <slow-first>',
    'BODY <fast-second>',
  ]);
  server.respondNext(response);
  server.respondNext(response);
  await Promise.all([slow, fast]);
  assert.equal(disposals.count, 0);
});

test('abort after streaming attempt creation remains pre-wire and disposes once', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const fetcher = new LocalSegmentFetcher(
    [provider('pre-wire-abort', server.port, 0, 1)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      circuitBreakerThreshold: 1,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const controller = new AbortController();
  const disposals = { count: 0 };
  let onWireCalls = 0;

  const fetch = fetcher.fetchBodyToSink(
    { messageId: 'pre-wire-abort' },
    'nzb-abort',
    CommandPriority.High,
    async () => {
      const attempt = collectingAttempt(disposals);
      controller.abort();
      return attempt;
    },
    controller.signal,
    () => {
      onWireCalls++;
    }
  );

  await assert.rejects(fetch, (error: unknown) => {
    assert(error instanceof NntpError);
    assert.equal(error.message, 'aborted');
    return true;
  });
  assert.equal(server.commands.length, 0);
  assert.equal(onWireCalls, 0);
  assert.equal(disposals.count, 1);
  assert.equal(fetcher.info()[0].freeSlots, 1);
  assert.equal(fetcher.info()[0].queued, 0);
  assert.equal(fetcher.info()[0].tripped, false);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
});

test('abort during streaming attempt preparation never sends BODY or leaks the assignment', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const fetcher = new LocalSegmentFetcher(
    [provider('preparation-abort', server.port, 0, 1)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      circuitBreakerThreshold: 1,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const controller = new AbortController();
  const started = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  const disposals = { count: 0 };
  let onWireCalls = 0;

  const fetch = fetcher.fetchBodyToSink(
    { messageId: 'preparation-abort' },
    'nzb-abort',
    CommandPriority.High,
    async () => {
      const attempt = collectingAttempt(disposals);
      started.resolve();
      await gate.promise;
      return attempt;
    },
    controller.signal,
    () => {
      onWireCalls++;
    }
  );
  await started.promise;
  assert.equal(fetcher.info()[0].freeSlots, 0);
  controller.abort();
  gate.resolve();

  await assert.rejects(fetch, (error: unknown) => {
    assert(error instanceof NntpError);
    assert.equal(error.message, 'aborted');
    return true;
  });
  assert.equal(server.commands.length, 0);
  assert.equal(onWireCalls, 0);
  assert.equal(disposals.count, 1);
  assert.equal(fetcher.info()[0].freeSlots, 1);
  assert.equal(fetcher.info()[0].queued, 0);
  assert.equal(fetcher.info()[0].tripped, false);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
});

test('assigned direct BODY aborts immediately behind slow prepared work', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const fetcher = new LocalSegmentFetcher(
    [provider('direct-pre-wire-abort', server.port, 0, 2)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      circuitBreakerThreshold: 1,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const slowStarted = Promise.withResolvers<void>();
  const slowGate = Promise.withResolvers<void>();
  const disposals = { count: 0 };
  const slow = fetcher.fetchBodyToSink(
    { messageId: 'slow-prepared-first' },
    'nzb-direct-abort',
    CommandPriority.High,
    async () => {
      slowStarted.resolve();
      await slowGate.promise;
      return collectingAttempt(disposals);
    }
  );
  await slowStarted.promise;

  const controller = new AbortController();
  let directOnWire = 0;
  const direct = fetcher.fetchBody(
    { messageId: 'aborted-direct-second' },
    'nzb-direct-abort',
    CommandPriority.High,
    undefined,
    controller.signal,
    () => {
      directOnWire++;
    }
  );
  assert.equal(fetcher.info()[0].freeSlots, 0);
  controller.abort();

  await assert.rejects(direct, (error: unknown) => {
    assert(error instanceof NntpError);
    assert.equal(error.kind, 'connection');
    assert.equal(error.message, 'aborted');
    return true;
  });
  assert.equal(server.commands.length, 0);
  assert.equal(directOnWire, 0);
  assert.equal(fetcher.info()[0].freeSlots, 1);
  assert.equal(fetcher.info()[0].tripped, false);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);

  slowGate.resolve();
  await server.waitForCommandCount(1);
  assert.deepEqual(server.commands, ['BODY <slow-prepared-first>']);
  server.respondNext(articleResponse('slow.bin', Buffer.from('slow')));
  await slow;
  assert.equal(disposals.count, 0);
  assert.deepEqual(server.commands, ['BODY <slow-prepared-first>']);
});

test('aborted middle assignment cannot block the third command turn', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const fetcher = new LocalSegmentFetcher(
    [provider('middle-turn-abort', server.port, 0, 3)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      circuitBreakerThreshold: 1,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const firstStarted = Promise.withResolvers<void>();
  const firstGate = Promise.withResolvers<void>();
  const disposals = { count: 0 };
  const first = fetcher.fetchBodyToSink(
    { messageId: 'turn-first' },
    'nzb-middle-abort',
    CommandPriority.High,
    async () => {
      firstStarted.resolve();
      await firstGate.promise;
      return collectingAttempt(disposals);
    }
  );
  await firstStarted.promise;

  const middleController = new AbortController();
  let middleOnWire = 0;
  const middle = fetcher.fetchBody(
    { messageId: 'turn-second-aborted' },
    'nzb-middle-abort',
    CommandPriority.High,
    undefined,
    middleController.signal,
    () => {
      middleOnWire++;
    }
  );
  const third = fetcher.fetchBody(
    { messageId: 'turn-third' },
    'nzb-middle-abort',
    CommandPriority.High
  );
  assert.equal(fetcher.info()[0].freeSlots, 0);
  middleController.abort();
  await assert.rejects(middle, (error: unknown) => {
    assert(error instanceof NntpError);
    assert.equal(error.message, 'aborted');
    return true;
  });
  assert.equal(fetcher.info()[0].freeSlots, 1);
  assert.equal(middleOnWire, 0);
  assert.equal(getEventListeners(middleController.signal, 'abort').length, 0);

  firstGate.resolve();
  await server.waitForCommandCount(2);
  assert.deepEqual(server.commands, ['BODY <turn-first>', 'BODY <turn-third>']);
  const response = articleResponse('turn.bin', Buffer.from('turn'));
  server.respondNext(response);
  server.respondNext(response);
  await Promise.all([first, third]);
  assert.equal(disposals.count, 0);
  assert.equal(fetcher.info()[0].freeSlots, 3);
});

test('assigned direct STAT aborts pre-wire behind a prepared BODY', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const fetcher = new LocalSegmentFetcher(
    [provider('stat-pre-wire-abort', server.port, 0, 2)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      circuitBreakerThreshold: 1,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const slowStarted = Promise.withResolvers<void>();
  const slowGate = Promise.withResolvers<void>();
  const disposals = { count: 0 };
  const slow = fetcher.fetchBodyToSink(
    { messageId: 'stat-blocking-body' },
    'nzb-stat-abort',
    CommandPriority.High,
    async () => {
      slowStarted.resolve();
      await slowGate.promise;
      return collectingAttempt(disposals);
    }
  );
  await slowStarted.promise;

  const controller = new AbortController();
  const stat = fetcher.statSegmentDetailed(
    'aborted-stat',
    'nzb-stat-abort',
    CommandPriority.High,
    controller.signal
  );
  assert.equal(fetcher.info()[0].freeSlots, 0);
  controller.abort();
  await assert.rejects(stat, (error: unknown) => {
    assert(error instanceof NntpError);
    assert.equal(error.message, 'aborted');
    return true;
  });
  await Promise.resolve();

  assert.equal(fetcher.info()[0].freeSlots, 1);
  assert.equal(fetcher.info()[0].tripped, false);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  assert.equal(server.commands.length, 0);
  slowGate.resolve();
  await server.waitForCommandCount(1);
  assert.deepEqual(server.commands, ['BODY <stat-blocking-body>']);
  server.respondNext(articleResponse('stat.bin', Buffer.from('stat')));
  await slow;
  assert.equal(disposals.count, 0);
});

test('pool close cancels every deferred command turn and releases listeners', async (context) => {
  const server = await AutomaticNntpServer.create(context);
  const fetcher = new LocalSegmentFetcher(
    [provider('close-deferred-turns', server.port, 0, 2)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());
  const preparedController = new AbortController();
  const directController = new AbortController();
  const preparationStarted = Promise.withResolvers<void>();
  const preparationGate = Promise.withResolvers<void>();
  const disposed = Promise.withResolvers<void>();
  let disposeCalls = 0;
  let directOnWire = 0;
  const prepared = fetcher.fetchBodyToSink(
    { messageId: 'close-prepared-first' },
    'nzb-close-turns',
    CommandPriority.High,
    async () => {
      preparationStarted.resolve();
      await preparationGate.promise;
      return {
        sink: new CollectingSink(),
        value: undefined,
        dispose: async () => {
          disposeCalls++;
          disposed.resolve();
        },
      };
    },
    preparedController.signal
  );
  await preparationStarted.promise;
  const direct = fetcher.fetchBody(
    { messageId: 'close-direct-second' },
    'nzb-close-turns',
    CommandPriority.High,
    undefined,
    directController.signal,
    () => {
      directOnWire++;
    }
  );
  assert.equal(fetcher.info()[0].freeSlots, 0);

  fetcher.close();
  await Promise.all([
    assert.rejects(prepared, NntpError),
    assert.rejects(direct, NntpError),
  ]);
  assert.equal(directOnWire, 0);
  assert.equal(fetcher.info()[0].freeSlots, 2);
  assert.equal(fetcher.info()[0].acquired, 0);
  assert.equal(fetcher.info()[0].queued, 0);
  assert.equal(getEventListeners(preparedController.signal, 'abort').length, 0);
  assert.equal(getEventListeners(directController.signal, 'abort').length, 0);
  assert.equal(
    server.commands.filter((command) => command.startsWith('BODY ')).length,
    0
  );

  preparationGate.resolve();
  await disposed.promise;
  assert.equal(disposeCalls, 1);
  fetcher.close();
});

test('strict range metadata accepts an exact standalone yEnc article', async (context) => {
  const server = await AutomaticNntpServer.create(
    context,
    articleResponse('standalone.bin', Buffer.from('abc'))
  );
  const fetcher = new LocalSegmentFetcher(
    [provider('standalone-metadata', server.port, 0)],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());

  const metadata = await fetcher.fetchHead(
    { messageId: 'strict-standalone' },
    'strict-standalone-nzb',
    CommandPriority.High,
    0,
    undefined,
    undefined,
    {
      strictYencMetadata: true,
      requireByteRange: true,
      allowStandalonePart: true,
    }
  );

  assert.equal(metadata.layout, 'standalone-part');
  assert.equal(metadata.byteRange, undefined);
  assert.equal(metadata.fileSize, 3);
  assert.equal(metadata.size, 3);
  assert.deepEqual(server.commands, ['BODY <strict-standalone>']);
});

test('strict range metadata fails over from malformed yEnc to a valid provider', async (context) => {
  const malformed = await AutomaticNntpServer.create(
    context,
    Buffer.from('222 article follows\r\nnot-yenc\r\n.\r\n', 'latin1')
  );
  const valid = await AutomaticNntpServer.create(
    context,
    multipartArticleResponse(Buffer.from('range'))
  );
  const fetcher = new LocalSegmentFetcher(
    [
      provider('malformed-metadata', malformed.port, 0),
      provider('valid-metadata', valid.port, 1),
    ],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      circuitBreakerThreshold: 1,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());

  const metadata = await fetcher.fetchHead(
    { messageId: 'strict-range-failover' },
    'strict-range-nzb',
    CommandPriority.High,
    0,
    undefined,
    undefined,
    {
      strictYencMetadata: true,
      requireByteRange: true,
      allowStandalonePart: true,
    }
  );

  assert.deepEqual(metadata.byteRange, [100, 105]);
  assert.equal(metadata.layout, 'global-range');
  assert.equal(metadata.fileSize, 1234);
  assert.equal(metadata.size, 5);
  assert.deepEqual(malformed.commands, ['BODY <strict-range-failover>']);
  assert.deepEqual(valid.commands, ['BODY <strict-range-failover>']);
  assert(fetcher.info().every((entry) => entry.tripped === false));
});

test('strict range metadata rejects when every provider response is unusable', async (context) => {
  const first = await AutomaticNntpServer.create(
    context,
    Buffer.from('222 article follows\r\nplain body\r\n.\r\n', 'latin1')
  );
  const second = await AutomaticNntpServer.create(
    context,
    Buffer.from(
      [
        '222 article follows',
        '=ybegin part=1 total=2 line=128 size=12 name=broken.bin',
        'payload-without-ypart',
        '=yend size=12',
        '.',
        '',
      ].join('\r\n'),
      'latin1'
    )
  );
  const fetcher = new LocalSegmentFetcher(
    [
      provider('missing-ybegin', first.port, 0),
      provider('missing-ypart', second.port, 1),
    ],
    {
      ...DEFAULT_ENGINE_OPTIONS,
      circuitBreakerThreshold: 1,
      dialTimeoutMs: 1000,
      segmentStallTimeoutMs: 1000,
      segmentTimeoutMs: 5000,
    },
    new NoopStats()
  );
  context.after(() => fetcher.close());

  await assert.rejects(
    fetcher.fetchHead(
      { messageId: 'strict-range-invalid' },
      'strict-invalid-nzb',
      CommandPriority.High,
      0,
      undefined,
      undefined,
      {
        strictYencMetadata: true,
        requireByteRange: true,
        allowStandalonePart: true,
      }
    ),
    (error: unknown) => {
      assert(error instanceof YencMetadataError);
      assert.match(error.message, /metadata unusable on all providers/);
      return true;
    }
  );
  assert.deepEqual(first.commands, ['BODY <strict-range-invalid>']);
  assert.deepEqual(second.commands, ['BODY <strict-range-invalid>']);
  assert(fetcher.info().every((entry) => entry.tripped === false));
});
