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
import { NntpError } from './errors.js';
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
          else this.pendingResponses.push(socket);
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

function articleResponse(name: string, body: Buffer): Buffer {
  return Buffer.concat([
    Buffer.from('222 article follows\r\n', 'latin1'),
    yencode.post(name, body, 128),
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
