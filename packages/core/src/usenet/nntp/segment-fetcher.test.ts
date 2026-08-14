import assert from 'node:assert/strict';
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
  LocalSegmentFetcher,
  type StatsSink,
  type StreamingSegmentAttempt,
} from './segment-fetcher.js';

class AutomaticNntpServer {
  private readonly server = net.createServer();
  private readonly clients = new Set<net.Socket>();
  readonly commands: string[] = [];

  private constructor(private readonly response: Buffer) {
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
          socket.write(this.response);
        }
      });
      socket.write('200 local test server ready\r\n');
    });
  }

  static async create(
    context: TestContext,
    response: Buffer
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

function provider(id: string, port: number, priority: number): ProviderConfig {
  return {
    id,
    host: '127.0.0.1',
    port,
    tls: false,
    maxConnections: 1,
    priority,
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
