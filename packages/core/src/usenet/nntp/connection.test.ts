import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import { readFile } from 'node:fs/promises';
import net from 'node:net';
import tls from 'node:tls';
import test, { type TestContext } from 'node:test';
// The production entry point initializes config before constructing module
// loggers. Preserve that ordering in this isolated connection test as well.
import '../../config/index.js';
import type { ProviderConfig } from '../types.js';
import {
  NntpConnection,
  type BackpressuredBodyConsumer,
  type ConnectionOptions,
} from './connection.js';
import { NntpError } from './errors.js';
import {
  allocateNntpReadCarryBuffer,
  NNTP_READ_CARRY_MAX_BYTES,
  NNTP_READ_CARRY_MAX_CHUNKS,
  NNTP_READ_WINDOW_BYTES,
} from './read-carry.js';

class ScriptedNntpServer {
  private readonly server = net.createServer();
  private readonly clientReady = Promise.withResolvers<net.Socket>();
  private readonly clients = new Set<net.Socket>();
  private readonly commands: string[] = [];
  private commandWaiter: PromiseWithResolvers<string> | undefined;
  private closed = false;

  private constructor() {
    this.server.on('connection', (socket) => {
      this.clients.add(socket);
      socket.on('error', () => undefined);
      socket.on('close', () => this.clients.delete(socket));
      let carry = '';
      socket.on('data', (chunk: Buffer) => {
        carry += chunk.toString('latin1');
        for (;;) {
          const end = carry.indexOf('\r\n');
          if (end < 0) return;
          const command = carry.slice(0, end);
          carry = carry.slice(end + 2);
          const waiter = this.commandWaiter;
          if (waiter) {
            this.commandWaiter = undefined;
            waiter.resolve(command);
          } else {
            this.commands.push(command);
          }
        }
      });
      this.clientReady.resolve(socket);
      socket.write('200 test server ready\r\n');
    });
  }

  static async create(context: TestContext): Promise<ScriptedNntpServer> {
    const scripted = new ScriptedNntpServer();
    await new Promise<void>((resolve, reject) => {
      scripted.server.once('error', reject);
      scripted.server.listen(0, '127.0.0.1', () => {
        scripted.server.removeListener('error', reject);
        resolve();
      });
    });
    context.after(() => scripted.close());
    return scripted;
  }

  get port(): number {
    const address = this.server.address();
    assert(address && typeof address !== 'string');
    return address.port;
  }

  nextCommand(): Promise<string> {
    const command = this.commands.shift();
    if (command !== undefined) return Promise.resolve(command);
    assert.equal(
      this.commandWaiter,
      undefined,
      'only one command waiter allowed'
    );
    this.commandWaiter = Promise.withResolvers<string>();
    return this.commandWaiter.promise;
  }

  async send(value: string | Buffer): Promise<void> {
    const socket = await this.clientReady.promise;
    await new Promise<void>((resolve, reject) => {
      socket.write(value, (error) => (error ? reject(error) : resolve()));
    });
  }

  async closeClient(): Promise<void> {
    const socket = await this.clientReady.promise;
    socket.destroy();
    if (socket.closed) return;
    await new Promise<void>((resolve) => socket.once('close', resolve));
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.commandWaiter?.reject(new Error('test server closed'));
    this.commandWaiter = undefined;
    for (const socket of this.clients) socket.destroy();
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

class TlsScriptedNntpServer {
  private readonly clientReady = Promise.withResolvers<tls.TLSSocket>();
  private readonly clients = new Set<tls.TLSSocket>();
  private readonly commands: string[] = [];
  private commandWaiter: PromiseWithResolvers<string> | undefined;
  private closed = false;

  private constructor(private readonly server: tls.Server) {
    server.on('secureConnection', (socket) => {
      this.clients.add(socket);
      socket.setMaxSendFragment(16 * 1024);
      socket.on('error', () => undefined);
      socket.on('close', () => this.clients.delete(socket));
      let carry = '';
      socket.on('data', (chunk: Buffer) => {
        carry += chunk.toString('latin1');
        for (;;) {
          const end = carry.indexOf('\r\n');
          if (end < 0) return;
          const command = carry.slice(0, end);
          carry = carry.slice(end + 2);
          const waiter = this.commandWaiter;
          if (waiter) {
            this.commandWaiter = undefined;
            waiter.resolve(command);
          } else {
            this.commands.push(command);
          }
        }
      });
      this.clientReady.resolve(socket);
      socket.write('200 tls test server ready\r\n');
    });
  }

  static async create(context: TestContext): Promise<TlsScriptedNntpServer> {
    const [key, cert] = await Promise.all([
      readFile(
        new URL('../../../test/fixtures/nntp-test-key.pem', import.meta.url)
      ),
      readFile(
        new URL('../../../test/fixtures/nntp-test-cert.pem', import.meta.url)
      ),
    ]);
    const instance = new TlsScriptedNntpServer(tls.createServer({ key, cert }));
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

  nextCommand(): Promise<string> {
    const command = this.commands.shift();
    if (command !== undefined) return Promise.resolve(command);
    assert.equal(this.commandWaiter, undefined);
    this.commandWaiter = Promise.withResolvers<string>();
    return this.commandWaiter.promise;
  }

  async send(value: Buffer): Promise<void> {
    const socket = await this.clientReady.promise;
    await new Promise<void>((resolve, reject) => {
      socket.write(value, (error) => (error ? reject(error) : resolve()));
    });
  }

  async sendRecords(values: readonly Buffer[]): Promise<void> {
    const socket = await this.clientReady.promise;
    await new Promise<void>((resolve, reject) => {
      for (let index = 0; index < values.length; index++) {
        socket.write(values[index], (error) => {
          if (error) reject(error);
          else if (index === values.length - 1) resolve();
        });
      }
    });
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.commandWaiter?.reject(new Error('tls test server closed'));
    this.commandWaiter = undefined;
    for (const socket of this.clients) socket.destroy();
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

class ControlledConsumer implements BackpressuredBodyConsumer {
  readonly chunks: Buffer[] = [];
  readonly endStarted = Promise.withResolvers<void>();
  endCalls = 0;
  failCalls = 0;
  failure: Error | undefined;
  private readonly writesChanged = new Set<() => void>();
  private drainListener: (() => void) | undefined;

  constructor(
    private readonly writeResults: boolean[] = [],
    private readonly endGate?: Promise<void>
  ) {}

  write(chunk: Buffer): boolean {
    // The onread view is copied synchronously; no alias escapes the callback.
    this.chunks.push(Buffer.from(chunk));
    for (const changed of this.writesChanged) changed();
    this.writesChanged.clear();
    return this.writeResults.shift() ?? true;
  }

  onceDrain(listener: () => void): void {
    assert.equal(this.drainListener, undefined);
    this.drainListener = listener;
  }

  emitDrain(): void {
    const listener = this.drainListener;
    assert(listener, 'expected one registered drain listener');
    this.drainListener = undefined;
    listener();
  }

  get hasDrainListener(): boolean {
    return this.drainListener !== undefined;
  }

  async end(): Promise<void> {
    this.endCalls++;
    this.endStarted.resolve();
    await this.endGate;
  }

  fail(error: Error): void {
    this.failCalls++;
    this.failure ??= error;
    this.drainListener = undefined;
  }

  async waitForWrites(count: number): Promise<void> {
    while (this.chunks.length < count) {
      const changed = Promise.withResolvers<void>();
      this.writesChanged.add(changed.resolve);
      await changed.promise;
    }
  }

  body(): Buffer {
    return Buffer.concat(this.chunks);
  }
}

class AutoDrainingConsumer implements BackpressuredBodyConsumer {
  readonly chunks: Buffer[] = [];
  failure: Error | undefined;

  write(chunk: Buffer): boolean {
    this.chunks.push(Buffer.from(chunk));
    return false;
  }

  onceDrain(listener: () => void): void {
    queueMicrotask(listener);
  }

  end(): Promise<void> {
    return Promise.resolve();
  }

  fail(error: Error): void {
    this.failure ??= error;
  }

  body(): Buffer {
    return Buffer.concat(this.chunks);
  }
}

interface ScheduledTimer {
  readonly callback: () => void;
  readonly delayMs: number;
}

class ControlledTimerScheduler {
  private pending: ScheduledTimer | undefined;

  readonly schedule: NonNullable<ConnectionOptions['scheduleTimeout']> = (
    callback,
    delayMs
  ) => {
    const scheduled = { callback, delayMs };
    this.pending = scheduled;
    return {
      cancel: () => {
        if (this.pending === scheduled) this.pending = undefined;
      },
      unref: () => undefined,
    };
  };

  get delayMs(): number | undefined {
    return this.pending?.delayMs;
  }

  get pendingCount(): number {
    return this.pending ? 1 : 0;
  }

  run(): void {
    const scheduled = this.pending;
    assert(scheduled, 'expected one response timer');
    this.pending = undefined;
    scheduled.callback();
  }
}

function provider(port: number): ProviderConfig {
  return {
    id: 'local-test-provider',
    host: '127.0.0.1',
    port,
    tls: false,
    maxConnections: 1,
    priority: 0,
  };
}

async function connectTest(
  context: TestContext,
  overrides: Partial<ConnectionOptions> = {}
): Promise<{
  readonly connection: NntpConnection;
  readonly server: ScriptedNntpServer;
}> {
  const server = await ScriptedNntpServer.create(context);
  const connection = await NntpConnection.connect(provider(server.port), {
    dialTimeoutMs: 1000,
    idleConnectionMs: 60_000,
    ...overrides,
  });
  context.after(() => connection.destroy());
  return { connection, server };
}

async function nextTurn(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
    assert.fail('expected rejection');
  } catch (error) {
    return error;
  }
}

test('pauses the whole socket for multiple consumer drain cycles', async (context) => {
  const { connection, server } = await connectTest(context);
  const consumer = new ControlledConsumer([false, false, true]);
  const bodyPromise = connection.bodyToConsumer(
    'multi-drain',
    consumer,
    undefined,
    1000
  );
  assert.equal(await server.nextCommand(), 'BODY <multi-drain>');

  const first = 'first-network-window';
  const second = '-second-network-window';
  const third = '-third-and-final';
  await server.send(`222 0 article follows\r\n${first}`);
  await consumer.waitForWrites(1);
  assert.equal(consumer.hasDrainListener, true);

  await server.send(second);
  await nextTurn();
  assert.equal(consumer.chunks.length, 1, 'socket must remain paused');
  consumer.emitDrain();
  await consumer.waitForWrites(2);
  assert.equal(consumer.hasDrainListener, true);

  await server.send(`${third}\r\n.\r\n`);
  await nextTurn();
  assert.equal(consumer.chunks.length, 2, 'second pause must remain active');
  consumer.emitDrain();

  assert.equal(await bodyPromise, Buffer.byteLength(first + second + third));
  assert.deepEqual(consumer.body(), Buffer.from(first + second + third));
  assert.equal(consumer.endCalls, 1);
  assert.equal(consumer.hasDrainListener, false);
  assert.equal(consumer.failure, undefined);
});

test('owns late read callbacks during local pause and drains them FIFO before resume', async (context) => {
  const firstLate = Buffer.from('second-');
  const secondLate = Buffer.from('third\r\n.\r\n');
  let injected = false;
  const { connection, server } = await connectTest(context, {
    onLocalPause: (deliverLateRead) => {
      if (injected) return;
      injected = true;
      assert.equal(deliverLateRead(firstLate), false);
      assert.equal(deliverLateRead(secondLate), false);
      firstLate.fill(0x78);
      secondLate.fill(0x79);
    },
  });
  const consumer = new ControlledConsumer([false, true, true]);
  const body = connection.bodyToConsumer(
    'late-reads',
    consumer,
    undefined,
    1000
  );
  assert.equal(await server.nextCommand(), 'BODY <late-reads>');
  await server.send('222 article\r\nfirst-');
  await consumer.waitForWrites(1);
  assert.deepEqual(connection.readCarryStats, {
    bytes: Buffer.byteLength('second-third\r\n.\r\n'),
    readableBytes: Buffer.byteLength('second-third\r\n.\r\n'),
    chunks: 2,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });
  consumer.emitDrain();

  assert.equal(await body, Buffer.byteLength('first-second-third'));
  assert.deepEqual(consumer.body(), Buffer.from('first-second-third'));
  assert.equal(connection.isUsable, true);
  assert.deepEqual(connection.readCarryStats, {
    bytes: 0,
    readableBytes: 0,
    chunks: 0,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });

  const next = connection.stat('next-command', undefined, 1000);
  assert.equal(await server.nextCommand(), 'STAT <next-command>');
  await server.send('223 1 article exists\r\n');
  assert.equal(await next, true);
});

test('keeps the carry head ordered across repeated drain cycles', async (context) => {
  const lateOne = Buffer.from('second-');
  const lateTwo = Buffer.from('third\r\n.\r\n');
  let injected = false;
  const { connection, server } = await connectTest(context, {
    onLocalPause: (deliverLateRead) => {
      if (injected) return;
      injected = true;
      assert.equal(deliverLateRead(lateOne), false);
      assert.equal(deliverLateRead(lateTwo), false);
    },
  });
  const consumer = new ControlledConsumer([false, false, false]);
  const body = connection.bodyToConsumer(
    'carry-drain-cycles',
    consumer,
    undefined,
    1000
  );
  assert.equal(await server.nextCommand(), 'BODY <carry-drain-cycles>');
  await server.send('222 article\r\nfirst-');

  await consumer.waitForWrites(1);
  assert.deepEqual(connection.readCarryStats, {
    bytes: lateOne.length + lateTwo.length,
    readableBytes: lateOne.length + lateTwo.length,
    chunks: 2,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });
  consumer.emitDrain();
  await consumer.waitForWrites(2);
  assert.deepEqual(connection.readCarryStats, {
    bytes: lateTwo.length,
    readableBytes: lateTwo.length,
    chunks: 1,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });
  assert(
    Buffer.from('first-second-third')
      .subarray(0, consumer.body().length)
      .equals(consumer.body()),
    'a parser-safe terminator tail may split one carry window, but not reorder it'
  );
  consumer.emitDrain();
  await consumer.waitForWrites(3);
  assert.deepEqual(consumer.body(), Buffer.from('first-second-third'));
  assert.equal(connection.readCarryStats.bytes, 0);
  assert.equal(connection.readCarryStats.readableBytes, 0);
  consumer.emitDrain();

  assert.equal(await body, Buffer.byteLength('first-second-third'));
  assert.equal(consumer.hasDrainListener, false);
  assert.equal(connection.isUsable, true);
});

test('drains the current read remainder before later callbacks in a pipeline', async (context) => {
  const lateOne = Buffer.from('second-');
  const lateTwo = Buffer.from('tail\r\n.\r\n');
  let injected = false;
  const { connection, server } = await connectTest(context, {
    onLocalPause: (deliverLateRead) => {
      if (injected) return;
      injected = true;
      deliverLateRead(lateOne);
      deliverLateRead(lateTwo);
    },
  });
  const consumer = new ControlledConsumer([false]);
  const first = connection.bodyToConsumer(
    'first-fifo',
    consumer,
    undefined,
    1000
  );
  const second = connection.body('second-fifo', undefined, 1000);
  assert.equal(await server.nextCommand(), 'BODY <first-fifo>');
  assert.equal(await server.nextCommand(), 'BODY <second-fifo>');
  await server.send('222 first\r\nfirst-body\r\n.\r\n222 second\r\ncurrent-');
  await consumer.waitForWrites(1);
  consumer.emitDrain();

  assert.equal(await first, Buffer.byteLength('first-body'));
  assert.deepEqual(
    await second,
    Buffer.from('current-second-tail'),
    'the in-window remainder must stay ahead of both late callbacks'
  );
  assert.equal(connection.isUsable, true);
});

test('owns tiny late callbacks with exact unpooled backing allocations', async (context) => {
  const owners = new Set<ArrayBufferLike>();
  let injected = false;
  const { connection, server } = await connectTest(context, {
    allocateReadCarryBuffer: (bytes) => {
      const buffer = allocateNntpReadCarryBuffer(bytes);
      owners.add(buffer.buffer);
      return buffer;
    },
    onLocalPause: (deliverLateRead) => {
      if (injected) return;
      injected = true;
      for (let index = 0; index < 200; index++) {
        assert.equal(deliverLateRead(Buffer.from([index & 0xff])), false);
      }
    },
  });
  const consumer = new ControlledConsumer([false]);
  const pending = connection.bodyToConsumer(
    'tiny-carry-owners',
    consumer,
    undefined,
    1000
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <tiny-carry-owners>');
  await server.send('222 article\r\nfirst-window');
  await consumer.waitForWrites(1);

  assert.equal(owners.size, 200);
  assert.equal(
    [...owners].reduce((bytes, owner) => bytes + owner.byteLength, 0),
    200
  );
  assert.deepEqual(connection.readCarryStats, {
    bytes: 200,
    readableBytes: 200,
    chunks: 200,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });

  connection.destroy();
  await failed;
  assert.deepEqual(connection.readCarryStats, {
    bytes: 0,
    readableBytes: 0,
    chunks: 0,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });
});

test('bounds owned late callback bytes and reports a local backpressure fault', async (context) => {
  assert.equal(NNTP_READ_CARRY_MAX_CHUNKS, 256);
  assert.equal(NNTP_READ_CARRY_MAX_BYTES, 4 * NNTP_READ_WINDOW_BYTES);
  let injected = false;
  const { connection, server } = await connectTest(context, {
    onLocalPause: (deliverLateRead) => {
      if (injected) return;
      injected = true;
      for (let index = 0; index < 4; index++) {
        deliverLateRead(Buffer.alloc(NNTP_READ_WINDOW_BYTES, index));
      }
      deliverLateRead(Buffer.from([0xff]));
    },
  });
  const consumer = new ControlledConsumer([false]);
  const pending = connection.bodyToConsumer(
    'carry-overflow',
    consumer,
    undefined,
    1000
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <carry-overflow>');
  await server.send('222 article\r\nfirst-window');

  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'local_backpressure');
  assert.equal(error.faultDomain, 'local');
  assert.equal(error.carryChunks, 5);
  assert.equal(error.carryBytes, NNTP_READ_CARRY_MAX_BYTES + 1);
  assert.equal(error.carryLimitBytes, NNTP_READ_CARRY_MAX_BYTES);
  assert.equal(consumer.failure, error);
  assert.equal(connection.isUsable, false);
  assert.equal(connection.inFlight, 0);
  assert.deepEqual(connection.readCarryStats, {
    bytes: 0,
    readableBytes: 0,
    chunks: 0,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });
});

test('bounds the number of tiny late callbacks independently of their bytes', async (context) => {
  const owners = new Set<ArrayBufferLike>();
  let injected = false;
  const { connection, server } = await connectTest(context, {
    allocateReadCarryBuffer: (bytes) => {
      const buffer = allocateNntpReadCarryBuffer(bytes);
      owners.add(buffer.buffer);
      return buffer;
    },
    onLocalPause: (deliverLateRead) => {
      if (injected) return;
      injected = true;
      for (let index = 0; index < NNTP_READ_CARRY_MAX_CHUNKS + 1; index++) {
        deliverLateRead(Buffer.from([index & 0xff]));
      }
    },
  });
  const consumer = new ControlledConsumer([false]);
  const pending = connection.bodyToConsumer(
    'carry-chunk-overflow',
    consumer,
    undefined,
    1000
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <carry-chunk-overflow>');
  await server.send('222 article\r\nfirst-window');

  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'local_backpressure');
  assert.equal(error.carryChunks, NNTP_READ_CARRY_MAX_CHUNKS + 1);
  assert.equal(error.carryBytes, NNTP_READ_CARRY_MAX_CHUNKS + 1);
  assert.equal(owners.size, NNTP_READ_CARRY_MAX_CHUNKS);
  assert.equal(
    [...owners].reduce((bytes, owner) => bytes + owner.byteLength, 0),
    NNTP_READ_CARRY_MAX_CHUNKS
  );
  assert.equal(connection.isUsable, false);
});

test('keeps a partially consumed carry allocation retained for capacity', async (context) => {
  const endGate = Promise.withResolvers<void>();
  let deliverLateRead: ((chunk: Buffer) => boolean) | undefined;
  let injected = false;
  const { connection, server } = await connectTest(context, {
    onLocalPause: (deliver) => {
      deliverLateRead = deliver;
      if (injected) return;
      injected = true;
      for (let index = 0; index < 4; index++) {
        const chunk = Buffer.alloc(NNTP_READ_WINDOW_BYTES, 0x61 + index);
        if (index === 0) Buffer.from('\r\n.\r\n').copy(chunk);
        assert.equal(deliver(chunk), false);
      }
    },
  });
  const consumer = new ControlledConsumer([false, true], endGate.promise);
  const pending = connection.bodyToConsumer(
    'partial-carry-owner',
    consumer,
    undefined,
    1000
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <partial-carry-owner>');
  await server.send('222 article\r\nfirst-window');
  await consumer.waitForWrites(1);
  assert.equal(connection.readCarryStats.bytes, NNTP_READ_CARRY_MAX_BYTES);

  consumer.emitDrain();
  await consumer.endStarted.promise;
  assert.equal(
    connection.readCarryStats.bytes,
    NNTP_READ_CARRY_MAX_BYTES,
    'the partially consumed head still retains its complete allocation'
  );
  assert(
    connection.readCarryStats.readableBytes < connection.readCarryStats.bytes
  );

  assert(deliverLateRead);
  assert.equal(deliverLateRead(Buffer.from([0x7a])), false);
  const error = await failed;
  endGate.resolve();
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'local_backpressure');
  assert.equal(error.carryBytes, NNTP_READ_CARRY_MAX_BYTES + 1);
  assert.deepEqual(connection.readCarryStats, {
    bytes: 0,
    readableBytes: 0,
    chunks: 0,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });
});

test('accepts the exact four-window carry boundary', async (context) => {
  let finalCarryBytes = 0;
  let finalReadableBytes = 0;
  let injected = false;
  const { connection, server } = await connectTest(context, {
    onLateRead: (stats) => {
      finalCarryBytes = stats.bytes;
      finalReadableBytes = stats.readableBytes;
    },
    onLocalPause: (deliverLateRead) => {
      if (injected) return;
      injected = true;
      const fullWindows = NNTP_READ_CARRY_MAX_BYTES / NNTP_READ_WINDOW_BYTES;
      for (let index = 0; index < fullWindows; index++) {
        const chunk = Buffer.alloc(NNTP_READ_WINDOW_BYTES, 0x61 + index);
        if (index === fullWindows - 1) {
          Buffer.from('\r\n.\r\n').copy(
            chunk,
            chunk.length - Buffer.byteLength('\r\n.\r\n')
          );
        }
        assert.equal(deliverLateRead(chunk), false);
      }
    },
  });
  const consumer = new ControlledConsumer([false]);
  const pending = connection.bodyToConsumer(
    'carry-boundary',
    consumer,
    undefined,
    1000
  );
  assert.equal(await server.nextCommand(), 'BODY <carry-boundary>');
  await server.send('222 article\r\nfirst-');
  await consumer.waitForWrites(1);
  assert.equal(finalCarryBytes, NNTP_READ_CARRY_MAX_BYTES);
  assert.equal(finalReadableBytes, NNTP_READ_CARRY_MAX_BYTES);
  consumer.emitDrain();

  assert.equal(
    await pending,
    Buffer.byteLength('first-') +
      NNTP_READ_CARRY_MAX_BYTES -
      Buffer.byteLength('\r\n.\r\n')
  );
  assert.equal(connection.isUsable, true);
  assert.equal(consumer.failure, undefined);
  assert.equal(connection.readCarryStats.bytes, 0);
  assert.equal(connection.readCarryStats.readableBytes, 0);
});

test('streams a complete fragmented TLS BODY through repeated local pauses and reuses the connection', async (context) => {
  const server = await TlsScriptedNntpServer.create(context);
  const payload = Buffer.alloc(3 * NNTP_READ_WINDOW_BYTES, 0x5a);
  const config = {
    ...provider(server.port),
    tls: true,
    tlsSkipVerify: true,
  };
  const connection = await NntpConnection.connect(config, {
    dialTimeoutMs: 1000,
    idleConnectionMs: 60_000,
  });
  context.after(() => connection.destroy());
  const consumer = new AutoDrainingConsumer();
  const body = connection.bodyToConsumer(
    'tls-late-read',
    consumer,
    undefined,
    5000
  );
  assert.equal(await server.nextCommand(), 'BODY <tls-late-read>');
  await server.sendRecords([
    Buffer.from('222 article follows\r\n'),
    payload.subarray(0, NNTP_READ_WINDOW_BYTES),
    payload.subarray(NNTP_READ_WINDOW_BYTES, 2 * NNTP_READ_WINDOW_BYTES),
    payload.subarray(2 * NNTP_READ_WINDOW_BYTES),
    Buffer.from('\r\n.\r\n'),
  ]);

  assert.equal(await body, payload.length);
  assert.deepEqual(consumer.body(), payload);
  assert.equal(consumer.failure, undefined);
  assert.equal(connection.isUsable, true);

  const next = connection.stat('tls-next', undefined, 1000);
  assert.equal(await server.nextCommand(), 'STAT <tls-next>');
  await server.send(Buffer.from('223 1 article exists\r\n'));
  assert.equal(await next, true);
});

test('owns deterministic already-decrypted callbacks on a real TLS connection', async (context) => {
  const server = await TlsScriptedNntpServer.create(context);
  const payload = Buffer.alloc(3 * NNTP_READ_WINDOW_BYTES, 0x4c);
  const wirePrefixBytes = 8 * 1024;
  const alreadyDecrypted = Buffer.concat([
    payload.subarray(wirePrefixBytes),
    Buffer.from('\r\n.\r\n'),
  ]);
  let injected = false;
  let lateReadCallbacks = 0;
  const connection = await NntpConnection.connect(
    {
      ...provider(server.port),
      tls: true,
      tlsSkipVerify: true,
    },
    {
      dialTimeoutMs: 1000,
      idleConnectionMs: 60_000,
      onLateRead: () => {
        lateReadCallbacks++;
      },
      // Native TLS callback batching is platform-dependent. This seam models
      // callbacks that TLS has already decrypted at the pause linearization
      // point; the preceding bytes and connection lifecycle remain real TLS.
      onLocalPause: (deliverLateRead) => {
        if (injected) return;
        injected = true;
        for (
          let offset = 0;
          offset < alreadyDecrypted.length;
          offset += NNTP_READ_WINDOW_BYTES
        ) {
          assert.equal(
            deliverLateRead(
              alreadyDecrypted.subarray(offset, offset + NNTP_READ_WINDOW_BYTES)
            ),
            false
          );
        }
      },
    }
  );
  context.after(() => connection.destroy());
  const consumer = new AutoDrainingConsumer();
  const body = connection.bodyToConsumer(
    'tls-deterministic-late-read',
    consumer,
    undefined,
    5000
  );
  assert.equal(
    await server.nextCommand(),
    'BODY <tls-deterministic-late-read>'
  );
  await server.sendRecords([
    Buffer.from('222 article follows\r\n'),
    payload.subarray(0, wirePrefixBytes),
  ]);

  assert.equal(await body, payload.length);
  assert.deepEqual(consumer.body(), payload);
  assert(lateReadCallbacks > 0);
  assert.equal(consumer.failure, undefined);
  assert.equal(connection.isUsable, true);
});

test('keeps FIFO aligned when a paused head shares a read with a pipelined BODY', async (context) => {
  const { connection, server } = await connectTest(context);
  const consumer = new ControlledConsumer([false]);
  const firstPromise = connection.bodyToConsumer(
    'first',
    consumer,
    undefined,
    1000
  );
  const secondPromise = connection.body('second', undefined, 1000);
  let secondSettled = false;
  void secondPromise.then(
    () => (secondSettled = true),
    () => (secondSettled = true)
  );

  assert.equal(await server.nextCommand(), 'BODY <first>');
  assert.equal(await server.nextCommand(), 'BODY <second>');
  await server.send(
    '222 first\r\nfirst-body\r\n.\r\n222 second\r\nsecond-body\r\n.\r\n'
  );
  await consumer.waitForWrites(1);
  await nextTurn();
  assert.equal(secondSettled, false);
  assert.equal(consumer.hasDrainListener, true);

  consumer.emitDrain();
  assert.equal(await firstPromise, Buffer.byteLength('first-body'));
  assert.deepEqual(await secondPromise, Buffer.from('second-body'));
  assert.deepEqual(consumer.body(), Buffer.from('first-body'));
  assert.equal(connection.isUsable, true);
});

test('aborting a locally paused BODY fails consumer and connection consistently', async (context) => {
  let injected = false;
  const { connection, server } = await connectTest(context, {
    onLocalPause: (deliverLateRead) => {
      if (injected) return;
      injected = true;
      deliverLateRead(Buffer.from('owned-after-pause'));
    },
  });
  const controller = new AbortController();
  const consumer = new ControlledConsumer([false]);
  const pending = connection.bodyToConsumer(
    'abort-me',
    consumer,
    controller.signal,
    1000
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <abort-me>');
  await server.send('222 article\r\npartial-body-with-room');
  await consumer.waitForWrites(1);
  assert.equal(consumer.hasDrainListener, true);
  assert(connection.readCarryStats.bytes > 0);

  controller.abort();
  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'connection');
  assert.match(error.message, /aborted/);
  assert.equal(consumer.failure, error);
  assert.equal(consumer.hasDrainListener, false);
  assert.equal(connection.isUsable, false);
  assert.equal(connection.inFlight, 0);
  assert.deepEqual(connection.readCarryStats, {
    bytes: 0,
    readableBytes: 0,
    chunks: 0,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });
});

test('suspends provider-stall timing during local pressure but retains absolute timeout', async (context) => {
  let now = 0;
  const timers = new ControlledTimerScheduler();
  const { connection, server } = await connectTest(context, {
    clock: () => now,
    scheduleTimeout: timers.schedule,
  });
  const consumer = new ControlledConsumer([false]);
  const pending = connection.bodyToConsumer(
    'local-timeout',
    consumer,
    undefined,
    10,
    100
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <local-timeout>');
  assert.equal(timers.delayMs, 10);

  await server.send('222 article\r\npartial-body-with-room');
  await consumer.waitForWrites(1);
  assert.equal(consumer.hasDrainListener, true);
  assert.equal(
    timers.delayMs,
    100,
    'local pause must replace the provider-stall window with total budget'
  );

  now = 100;
  timers.run();
  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'timeout');
  assert.equal(error.timeoutSource, 'local_backpressure');
  assert.match(error.message, /local backpressure/);
  assert.equal(consumer.failure, error);
});

test('classifies provider silence separately from local backpressure', async (context) => {
  let now = 0;
  const timers = new ControlledTimerScheduler();
  const { connection, server } = await connectTest(context, {
    clock: () => now,
    scheduleTimeout: timers.schedule,
  });
  const pending = connection.body('provider-stall', undefined, 10, 100);
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <provider-stall>');
  assert.equal(timers.delayMs, 10);

  now = 10;
  timers.run();
  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'timeout');
  assert.equal(error.timeoutSource, 'provider_stall');
});

test('classifies an ordinary absolute segment deadline separately', async (context) => {
  let now = 0;
  const timers = new ControlledTimerScheduler();
  const { connection, server } = await connectTest(context, {
    clock: () => now,
    scheduleTimeout: timers.schedule,
  });
  const pending = connection.body('absolute-timeout', undefined, 100, 10);
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <absolute-timeout>');
  assert.equal(timers.delayMs, 10);

  now = 10;
  timers.run();
  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'timeout');
  assert.equal(error.timeoutSource, 'absolute');
  assert.doesNotMatch(error.message, /local backpressure/);
});

test('uses a non-head absolute deadline while the pipeline head is locally paused', async (context) => {
  let now = 0;
  const timers = new ControlledTimerScheduler();
  const { connection, server } = await connectTest(context, {
    clock: () => now,
    scheduleTimeout: timers.schedule,
  });
  const firstController = new AbortController();
  const secondController = new AbortController();
  const firstConsumer = new ControlledConsumer([false]);
  const first = connection.bodyToConsumer(
    'paused-head',
    firstConsumer,
    firstController.signal,
    100,
    1000
  );
  const second = connection.body(
    'earlier-deadline',
    secondController.signal,
    100,
    10
  );
  let firstResolved = false;
  let secondResolved = false;
  void first.then(
    () => (firstResolved = true),
    () => undefined
  );
  void second.then(
    () => (secondResolved = true),
    () => undefined
  );
  const firstFailed = rejection(first);
  const secondFailed = rejection(second);
  assert.equal(await server.nextCommand(), 'BODY <paused-head>');
  assert.equal(await server.nextCommand(), 'BODY <earlier-deadline>');
  assert.equal(getEventListeners(firstController.signal, 'abort').length, 1);
  assert.equal(getEventListeners(secondController.signal, 'abort').length, 1);

  await server.send(
    '222 first\r\nfirst-body\r\n.\r\n222 second\r\nsecond-body\r\n.\r\n'
  );
  await firstConsumer.waitForWrites(1);
  assert.equal(firstConsumer.hasDrainListener, true);
  assert.equal(timers.delayMs, 10);

  now = 10;
  timers.run();
  const firstError = await firstFailed;
  const secondError = await secondFailed;
  assert(firstError instanceof NntpError);
  assert.equal(firstError, secondError);
  assert.equal(firstError.kind, 'timeout');
  assert.equal(firstError.timeoutSource, 'local_backpressure');
  assert.match(firstError.message, /10ms/);
  assert.equal(firstConsumer.failure, firstError);
  assert.equal(firstConsumer.failCalls, 1);
  assert.equal(firstConsumer.hasDrainListener, false);
  await nextTurn();
  assert.equal(firstResolved, false);
  assert.equal(secondResolved, false);
  assert.equal(getEventListeners(firstController.signal, 'abort').length, 0);
  assert.equal(getEventListeners(secondController.signal, 'abort').length, 0);
  assert.equal(timers.pendingCount, 0);
  assert.equal(connection.inFlight, 0);
  assert.equal(connection.isUsable, false);
});

test('enforces a non-head deadline synchronously before releasing a paused head', async (context) => {
  let now = 0;
  const timers = new ControlledTimerScheduler();
  const { connection, server } = await connectTest(context, {
    clock: () => now,
    scheduleTimeout: timers.schedule,
  });
  const firstConsumer = new ControlledConsumer([false]);
  const first = connection.bodyToConsumer(
    'drain-head',
    firstConsumer,
    undefined,
    100,
    1000
  );
  const second = connection.body('drain-deadline', undefined, 100, 10);
  const firstFailed = rejection(first);
  const secondFailed = rejection(second);
  assert.equal(await server.nextCommand(), 'BODY <drain-head>');
  assert.equal(await server.nextCommand(), 'BODY <drain-deadline>');
  await server.send(
    '222 first\r\nfirst-body\r\n.\r\n222 second\r\nsecond-body\r\n.\r\n'
  );
  await firstConsumer.waitForWrites(1);

  now = 11;
  firstConsumer.emitDrain();
  const firstError = await firstFailed;
  const secondError = await secondFailed;
  assert(firstError instanceof NntpError);
  assert.equal(firstError, secondError);
  assert.equal(firstError.timeoutSource, 'local_backpressure');
  assert.match(firstError.message, /10ms/);
  assert.equal(firstConsumer.endCalls, 0);
  assert.equal(timers.pendingCount, 0);
  assert.equal(connection.inFlight, 0);
});

test('does not end a locally paused head after its own absolute deadline', async (context) => {
  let now = 0;
  const timers = new ControlledTimerScheduler();
  const { connection, server } = await connectTest(context, {
    clock: () => now,
    scheduleTimeout: timers.schedule,
  });
  const consumer = new ControlledConsumer([false]);
  const pending = connection.bodyToConsumer(
    'expired-before-drain',
    consumer,
    undefined,
    100,
    10
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <expired-before-drain>');
  await server.send('222 article\r\ncomplete-body\r\n.\r\n');
  await consumer.waitForWrites(1);

  now = 10;
  consumer.emitDrain();
  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.timeoutSource, 'local_backpressure');
  assert.equal(consumer.failure, error);
  assert.equal(consumer.failCalls, 1);
  assert.equal(consumer.endCalls, 0);
  assert.equal(consumer.hasDrainListener, false);
  assert.equal(timers.pendingCount, 0);
  assert.equal(connection.inFlight, 0);
});

test('rejects an async consumer end that completes after the absolute deadline', async (context) => {
  let now = 0;
  const timers = new ControlledTimerScheduler();
  const endGate = Promise.withResolvers<void>();
  const { connection, server } = await connectTest(context, {
    clock: () => now,
    scheduleTimeout: timers.schedule,
  });
  const firstConsumer = new ControlledConsumer([true], endGate.promise);
  const first = connection.bodyToConsumer(
    'late-end',
    firstConsumer,
    undefined,
    100,
    10
  );
  const second = connection.body('after-late-end', undefined, 100, 100);
  let secondResolved = false;
  void second.then(
    () => (secondResolved = true),
    () => undefined
  );
  const firstFailed = rejection(first);
  const secondFailed = rejection(second);
  assert.equal(await server.nextCommand(), 'BODY <late-end>');
  assert.equal(await server.nextCommand(), 'BODY <after-late-end>');
  await server.send(
    '222 first\r\nfirst-body\r\n.\r\n222 second\r\nsecond-body\r\n.\r\n'
  );
  await firstConsumer.endStarted.promise;

  now = 11;
  endGate.resolve();
  const firstError = await firstFailed;
  const secondError = await secondFailed;
  assert(firstError instanceof NntpError);
  assert.equal(firstError, secondError);
  assert.equal(firstError.timeoutSource, 'local_backpressure');
  assert.equal(firstConsumer.failure, firstError);
  assert.equal(firstConsumer.failCalls, 1);
  assert.equal(secondResolved, false);
  assert.equal(timers.pendingCount, 0);
  assert.equal(connection.inFlight, 0);
});

test('rejects buffered response progress after a delayed absolute deadline callback', async (context) => {
  let now = 0;
  const timers = new ControlledTimerScheduler();
  const { connection, server } = await connectTest(context, {
    clock: () => now,
    scheduleTimeout: timers.schedule,
  });
  const pending = connection.body('late-buffered', undefined, 100, 10);
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <late-buffered>');
  assert.equal(timers.delayMs, 10);

  now = 11;
  await server.send('222 article\r\nlate-body\r\n.\r\n');
  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'timeout');
  assert.equal(error.timeoutSource, 'absolute');
  assert.equal(connection.inFlight, 0);
  assert.equal(connection.isUsable, false);
  assert.equal(timers.pendingCount, 0);
});

test('keeps a non-expired controlled pipeline successful', async (context) => {
  let now = 0;
  const timers = new ControlledTimerScheduler();
  const { connection, server } = await connectTest(context, {
    clock: () => now,
    scheduleTimeout: timers.schedule,
  });
  const consumer = new ControlledConsumer([false]);
  const first = connection.bodyToConsumer(
    'timely-first',
    consumer,
    undefined,
    50,
    100
  );
  const second = connection.body('timely-second', undefined, 50, 80);
  assert.equal(await server.nextCommand(), 'BODY <timely-first>');
  assert.equal(await server.nextCommand(), 'BODY <timely-second>');
  assert.equal(timers.delayMs, 50);

  now = 10;
  await server.send(
    '222 first\r\nfirst-body\r\n.\r\n222 second\r\nsecond-body\r\n.\r\n'
  );
  await consumer.waitForWrites(1);
  assert.equal(timers.delayMs, 70);

  now = 20;
  consumer.emitDrain();
  assert.equal(await first, Buffer.byteLength('first-body'));
  assert.deepEqual(await second, Buffer.from('second-body'));
  assert.deepEqual(consumer.body(), Buffer.from('first-body'));
  assert.equal(consumer.failure, undefined);
  assert.equal(connection.isUsable, true);
  assert.equal(connection.inFlight, 0);
  assert.equal(timers.pendingCount, 0);
});

test('does not create an absolute timer for unbounded requests during local pause', async (context) => {
  for (const totalTimeoutMs of [undefined, 0]) {
    let now = 0;
    const timers = new ControlledTimerScheduler();
    const { connection, server } = await connectTest(context, {
      clock: () => now,
      scheduleTimeout: timers.schedule,
    });
    const consumer = new ControlledConsumer([false]);
    const messageId = `stall-only-pause-${String(totalTimeoutMs)}`;
    const pending = connection.bodyToConsumer(
      messageId,
      consumer,
      undefined,
      10,
      totalTimeoutMs
    );
    assert.equal(await server.nextCommand(), `BODY <${messageId}>`);
    assert.equal(timers.delayMs, 10);
    await server.send('222 article\r\ncomplete-body\r\n.\r\n');
    await consumer.waitForWrites(1);
    assert.equal(timers.pendingCount, 0);

    now = 1000;
    consumer.emitDrain();
    assert.equal(await pending, Buffer.byteLength('complete-body'));
    assert.equal(consumer.failure, undefined);
    assert.equal(connection.isUsable, true);
    assert.equal(timers.pendingCount, 0);
  }
});

test('does not expose the next pipeline response until async consumer end settles', async (context) => {
  const endGate = Promise.withResolvers<void>();
  const { connection, server } = await connectTest(context);
  const consumer = new ControlledConsumer([true], endGate.promise);
  const first = connection.bodyToConsumer(
    'flush-first',
    consumer,
    undefined,
    1000
  );
  const second = connection.body('after-flush', undefined, 1000);
  let secondSettled = false;
  void second.then(
    () => (secondSettled = true),
    () => (secondSettled = true)
  );
  assert.equal(await server.nextCommand(), 'BODY <flush-first>');
  assert.equal(await server.nextCommand(), 'BODY <after-flush>');

  await server.send(
    '222 first\r\nflush-body\r\n.\r\n222 second\r\nafter-body\r\n.\r\n'
  );
  await consumer.endStarted.promise;
  await nextTurn();
  assert.equal(secondSettled, false);

  endGate.resolve();
  assert.equal(await first, Buffer.byteLength('flush-body'));
  assert.deepEqual(await second, Buffer.from('after-body'));
  assert.equal(consumer.endCalls, 1);
});

test('430 fails only its consumer and leaves the connection reusable', async (context) => {
  const { connection, server } = await connectTest(context);
  const consumer = new ControlledConsumer();
  const missing = connection.bodyToConsumer(
    'missing',
    consumer,
    undefined,
    1000
  );
  const failed = rejection(missing);
  assert.equal(await server.nextCommand(), 'BODY <missing>');
  await server.send('430 no such article\r\n');

  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'article_not_found');
  assert.equal(error.code, 430);
  assert.equal(consumer.failure, error);
  assert.equal(consumer.endCalls, 0);
  assert.equal(connection.isUsable, true);

  const stat = connection.stat('present', undefined, 1000);
  assert.equal(await server.nextCommand(), 'STAT <present>');
  await server.send('223 0 <present>\r\n');
  assert.equal(await stat, true);
});

test('provider socket failure rejects the BODY and fails its consumer', async (context) => {
  const { connection, server } = await connectTest(context);
  const consumer = new ControlledConsumer();
  const pending = connection.bodyToConsumer(
    'socket-failure',
    consumer,
    undefined,
    1000
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <socket-failure>');
  await server.closeClient();

  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'connection');
  assert.equal(consumer.failure, error);
  assert.equal(connection.inFlight, 0);
  assert.equal(connection.isUsable, false);
});

test('connection destroy releases every retained late-read owner', async (context) => {
  let injected = false;
  const { connection, server } = await connectTest(context, {
    onLocalPause: (deliverLateRead) => {
      if (injected) return;
      injected = true;
      deliverLateRead(Buffer.from('owned-provider-tail'));
    },
  });
  const consumer = new ControlledConsumer([false]);
  const pending = connection.bodyToConsumer(
    'socket-failure-with-carry',
    consumer,
    undefined,
    1000
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <socket-failure-with-carry>');
  await server.send('222 article\r\npartial-body');
  await consumer.waitForWrites(1);
  assert(connection.readCarryStats.bytes > 0);

  connection.destroy();
  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'connection');
  assert.deepEqual(connection.readCarryStats, {
    bytes: 0,
    readableBytes: 0,
    chunks: 0,
    limitBytes: NNTP_READ_CARRY_MAX_BYTES,
  });
});

test('consumer write failure tears down request, listener and timer state', async (context) => {
  const { connection, server } = await connectTest(context);
  const writeFailure = new Error('controlled consumer write failure');
  let notified: Error | undefined;
  const consumer: BackpressuredBodyConsumer = {
    write: () => {
      throw writeFailure;
    },
    onceDrain: () => assert.fail('a throwing write must not register drain'),
    end: async () => assert.fail('a throwing write must not end'),
    fail: (error) => {
      notified ??= error;
    },
  };
  const pending = connection.bodyToConsumer(
    'consumer-failure',
    consumer,
    undefined,
    1000
  );
  const failed = rejection(pending);
  assert.equal(await server.nextCommand(), 'BODY <consumer-failure>');
  await server.send('222 article\r\nbody-large-enough-to-emit');

  assert.equal(await failed, writeFailure);
  assert.equal(notified, writeFailure);
  assert.equal(connection.inFlight, 0);
  assert.equal(connection.isUsable, false);
});

test('keeps buffered body and streaming probe behavior unchanged', async (context) => {
  const { connection, server } = await connectTest(context);
  const buffered = connection.body('buffered', undefined, 1000);
  assert.equal(await server.nextCommand(), 'BODY <buffered>');
  await server.send('222 article\r\nbuffered\r\n..body\r\n.\r\n');
  assert.deepEqual(await buffered, Buffer.from('buffered\r\n..body'));

  const chunks: Buffer[] = [];
  const streamed = connection.bodyStreaming(
    'probe',
    (chunk) => chunks.push(Buffer.from(chunk)),
    undefined,
    1000
  );
  assert.equal(await server.nextCommand(), 'BODY <probe>');
  await server.send('222 article\r\nprobe-prefix');
  await server.send('-and-tail\r\n.\r\n');
  assert.equal(await streamed, Buffer.byteLength('probe-prefix-and-tail'));
  assert.deepEqual(Buffer.concat(chunks), Buffer.from('probe-prefix-and-tail'));
  assert.equal(connection.isUsable, true);
});
