import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import net from 'node:net';
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
  const { connection, server } = await connectTest(context);
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

  controller.abort();
  const error = await failed;
  assert(error instanceof NntpError);
  assert.equal(error.kind, 'connection');
  assert.match(error.message, /aborted/);
  assert.equal(consumer.failure, error);
  assert.equal(consumer.hasDrainListener, false);
  assert.equal(connection.isUsable, false);
  assert.equal(connection.inFlight, 0);
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
