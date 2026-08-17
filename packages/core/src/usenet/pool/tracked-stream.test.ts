import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import test from 'node:test';
import '../../config/index.js';
import {
  StreamRegistry,
  StreamStoppedError,
} from '../../stream-sessions/registry.js';
import { StatsAccumulator } from '../stats/accumulator.js';
import type { SeekableStream } from './file-stream.js';
import {
  destroyTrackedReaders,
  TrackedReaderOwner,
  trackSeekableStream,
  UsenetEngineClosedError,
  UsenetStreamReapedError,
} from './tracked-stream.js';

class ControlledSeekableStream implements SeekableStream {
  readonly filename = 'controlled.bin';
  openCalls = 0;
  readAtCalls = 0;
  readAtIntoCalls = 0;
  createReadStreamCalls = 0;
  openGate: Promise<void> | undefined;
  readGate: Promise<void> | undefined;

  size(): number {
    return 4;
  }

  async open(): Promise<void> {
    this.openCalls++;
    await this.openGate;
  }

  async readAt(_offset: number, length: number): Promise<Buffer> {
    this.readAtCalls++;
    await this.readGate;
    return Buffer.alloc(length, 0x61);
  }

  async readAtInto(
    dst: Buffer,
    dstOffset: number,
    _offset: number,
    length: number
  ): Promise<number> {
    this.readAtIntoCalls++;
    await this.readGate;
    dst.fill(0x62, dstOffset, dstOffset + length);
    return length;
  }

  createReadStream(): Readable {
    this.createReadStreamCalls++;
    return Readable.from([Buffer.from('data')]);
  }
}

function controlledWrapper(inner: ControlledSeekableStream): {
  stream: SeekableStream;
  close: () => UsenetEngineClosedError;
} {
  let closedError: UsenetEngineClosedError | undefined;
  return {
    stream: trackSeekableStream(
      inner,
      new StatsAccumulator(),
      'tracked-test',
      new Map(),
      () => {
        if (closedError) throw closedError;
      }
    ),
    close: () => {
      closedError ??= new UsenetEngineClosedError();
      return closedError;
    },
  };
}

test('issued seekable wrappers reject every work API after engine close', async () => {
  const inner = new ControlledSeekableStream();
  const { stream, close } = controlledWrapper(inner);
  const closedError = close();

  assert.throws(
    () => stream.createReadStream(),
    (error) => error === closedError
  );
  await assert.rejects(stream.open(), (error) => error === closedError);
  await assert.rejects(stream.readAt(0, 1), (error) => error === closedError);
  await assert.rejects(
    stream.readAtInto?.(Buffer.alloc(1), 0, 0, 1),
    (error) => error === closedError
  );

  assert.equal(inner.createReadStreamCalls, 0);
  assert.equal(inner.openCalls, 0);
  assert.equal(inner.readAtCalls, 0);
  assert.equal(inner.readAtIntoCalls, 0);
});

test('an issued wrapper cannot publish an open crossing engine close', async () => {
  const inner = new ControlledSeekableStream();
  const entered = Promise.withResolvers<void>();
  const proceed = Promise.withResolvers<void>();
  inner.openGate = (async () => {
    entered.resolve();
    await proceed.promise;
  })();
  const { stream, close } = controlledWrapper(inner);

  const opening = stream.open();
  await entered.promise;
  const closedError = close();
  proceed.resolve();

  await assert.rejects(opening, (error) => error === closedError);
  assert.equal(inner.openCalls, 1);
  assert.throws(
    () => stream.createReadStream(),
    (error) => error === closedError
  );
});

class DelayedDestroyReadable extends Readable {
  readonly destroyEntered = Promise.withResolvers<void>();

  constructor(
    private readonly destroyGate: Promise<void>,
    private readonly replacementError?: Error
  ) {
    super({ read() {} });
  }

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    this.destroyEntered.resolve();
    void this.destroyGate.then(() => callback(this.replacementError ?? error));
  }
}

class SynchronousDestroyReadable extends Readable {
  constructor(private readonly replacementError?: Error) {
    super({ read() {} });
  }

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    callback(this.replacementError ?? error);
  }
}

function waitForClose(stream: Readable): Promise<void> {
  if (stream.closed) return Promise.resolve();
  return new Promise<void>((resolve) => stream.once('close', resolve));
}

test('a normal reader failure is not retained for later engine close', async () => {
  const runtimeError = Object.assign(new Error('provider read failed'), {
    code: 'EIO',
  });
  const reader = new SynchronousDestroyReadable();
  const owner = new TrackedReaderOwner();
  const observed: unknown[] = [];
  owner.register(1, reader);
  reader.once('error', (error) => observed.push(error));

  const closed = waitForClose(reader);
  reader.destroy(runtimeError);
  await closed;

  assert.deepEqual(observed, [runtimeError]);
  assert.equal(owner.size, 0);
  assert.deepEqual(await owner.close(new UsenetEngineClosedError()), []);
  assert.equal(reader.listenerCount('error'), 0);
});

test('reader owner retains a synchronous cleanup replacement before engine close', async () => {
  const cleanupError = Object.assign(new Error('reader cleanup failed'), {
    code: 'EIO',
  });
  const reader = new SynchronousDestroyReadable(cleanupError);
  const owner = new TrackedReaderOwner();
  owner.register(1, reader);

  const closed = waitForClose(reader);
  reader.destroy(new StreamStoppedError('shutdown'));
  await closed;
  assert.equal(owner.size, 0);

  assert.deepEqual(
    await destroyTrackedReaders(owner, new UsenetEngineClosedError()),
    [cleanupError]
  );
  assert.deepEqual(
    await destroyTrackedReaders(owner, new UsenetEngineClosedError()),
    []
  );
});

test('registry shutdown preserves a synchronous reader cleanup error for engine close', async () => {
  const cleanupError = Object.assign(new Error('reader cleanup failed'), {
    code: 'EIO',
  });
  const inner: SeekableStream = {
    filename: 'registry.bin',
    size: () => 1,
    open: async () => undefined,
    readAt: async () => Buffer.from('x'),
    createReadStream: () => new SynchronousDestroyReadable(cleanupError),
  };
  const stats = new StatsAccumulator();
  const owner = new TrackedReaderOwner();
  const tracked = trackSeekableStream(
    inner,
    stats,
    'registry-cleanup',
    owner,
    () => undefined
  );
  const registry = new StreamRegistry(() => ({ ok: true }));
  const admitted = registry.open({
    transport: 'usenet',
    username: 'reader-owner-test',
    targetKey: 'registry-cleanup',
  });
  assert.equal(admitted.ok, true);
  if (!admitted.ok) return;
  const reader = tracked.createReadStream();
  const closed = waitForClose(reader);
  admitted.handle.attach(reader);

  registry.sealAndCloseAll('shutdown');
  await closed;
  assert.equal(owner.size, 0);
  assert.equal(stats.activeStreams, 0);
  assert.deepEqual(await owner.close(new UsenetEngineClosedError()), [
    cleanupError,
  ]);
  assert.deepEqual(registry.snapshot(), []);
});

test('reader owner discards synchronous expected lifecycle termination', async () => {
  const reader = new SynchronousDestroyReadable();
  const owner = new TrackedReaderOwner();
  owner.register(1, reader);

  const closed = waitForClose(reader);
  reader.destroy(new StreamStoppedError('shutdown'));
  await closed;

  assert.equal(owner.size, 0);
  assert.deepEqual(await owner.close(new UsenetEngineClosedError()), []);
});

test('reader owner promptly removes an old normally closed reader', async () => {
  const reader = new SynchronousDestroyReadable();
  const owner = new TrackedReaderOwner();
  owner.register(1, reader);

  const closed = waitForClose(reader);
  reader.destroy();
  await closed;

  assert.equal(owner.size, 0);
  assert.deepEqual(await owner.close(new UsenetEngineClosedError()), []);
});

test('reader cleanup error handoff remains bounded and removes every listener', async () => {
  const owner = new TrackedReaderOwner();
  const readers = Array.from({ length: 66 }, (_, index) => {
    const cleanupError = Object.assign(
      new Error(`reader cleanup failed ${index}`),
      { code: 'EIO' }
    );
    const reader = new SynchronousDestroyReadable(cleanupError);
    owner.register(index, reader);
    return reader;
  });

  const closed = readers.map(waitForClose);
  for (const reader of readers) {
    reader.destroy(new StreamStoppedError('shutdown'));
  }
  await Promise.all(closed);

  const errors = await owner.close(new UsenetEngineClosedError());
  assert.equal(errors.length, 64);
  assert.match(
    String(errors.at(-1)),
    /Additional usenet reader cleanup errors were suppressed/
  );
  assert.equal(owner.size, 0);
  for (const reader of readers) assert.equal(reader.listenerCount('error'), 0);
  assert.deepEqual(await owner.close(new UsenetEngineClosedError()), []);
});

test('reader teardown waits for asynchronous close after destroy', async () => {
  const gate = Promise.withResolvers<void>();
  const reader = new DelayedDestroyReadable(gate.promise);
  const readers = new Map([[1, reader as Readable]]);
  reader.once('close', () => readers.delete(1));
  const closeError = new UsenetEngineClosedError();

  let settled = false;
  const closing = destroyTrackedReaders(readers, closeError).then((errors) => {
    settled = true;
    return errors;
  });
  await reader.destroyEntered.promise;
  await Promise.resolve();
  assert.equal(reader.destroyed, true);
  assert.equal(reader.closed, false);
  assert.equal(settled, false);

  gate.resolve();
  assert.deepEqual(await closing, []);
  assert.equal(reader.closed, true);
  assert.equal(readers.size, 0);
});

test('reader teardown reports cleanup errors after every close barrier settles', async () => {
  const firstGate = Promise.withResolvers<void>();
  const secondGate = Promise.withResolvers<void>();
  const cleanupError = new Error('synthetic reader cleanup failure');
  const first = new DelayedDestroyReadable(firstGate.promise, cleanupError);
  const second = new DelayedDestroyReadable(secondGate.promise);
  const readers = new Map<number, Readable>([
    [1, first],
    [2, second],
  ]);
  first.once('close', () => readers.delete(1));
  second.once('close', () => readers.delete(2));

  let settled = false;
  const closing = destroyTrackedReaders(
    readers,
    new UsenetEngineClosedError()
  ).then((errors) => {
    settled = true;
    return errors;
  });
  await Promise.all([
    first.destroyEntered.promise,
    second.destroyEntered.promise,
  ]);
  firstGate.resolve();
  await new Promise<void>((resolve) => first.once('close', resolve));
  assert.equal(settled, false);

  secondGate.resolve();
  assert.deepEqual(await closing, [cleanupError]);
  assert.equal(readers.size, 0);
});

test('reader teardown observes readers already terminalized by lifecycle owners', async (t) => {
  const cases: Array<{ name: string; error: Error }> = [
    {
      name: 'stream registry shutdown',
      error: new StreamStoppedError('shutdown'),
    },
    {
      name: 'idle reaper',
      error: new UsenetStreamReapedError('idle stream'),
    },
    {
      name: 'abort',
      error: new DOMException('client aborted', 'AbortError'),
    },
    { name: 'client close', error: new Error('client closed') },
  ];

  for (const entry of cases) {
    await t.test(entry.name, async () => {
      const gate = Promise.withResolvers<void>();
      const reader = new DelayedDestroyReadable(gate.promise);
      const readers = new Map([[1, reader as Readable]]);
      reader.once('close', () => readers.delete(1));
      reader.destroy(entry.error);
      await reader.destroyEntered.promise;

      let settled = false;
      const closing = destroyTrackedReaders(
        readers,
        new UsenetEngineClosedError()
      ).then((errors) => {
        settled = true;
        return errors;
      });
      await Promise.resolve();
      assert.equal(settled, false);
      assert.equal(reader.closed, false);

      gate.resolve();
      assert.deepEqual(await closing, []);
      assert.equal(reader.closed, true);
      assert.equal(readers.size, 0);
    });
  }
});

test('reader teardown retains a real cleanup error after prior lifecycle termination', async () => {
  const gate = Promise.withResolvers<void>();
  const cleanupError = Object.assign(new Error('reader cleanup failed'), {
    code: 'EIO',
  });
  const reader = new DelayedDestroyReadable(gate.promise, cleanupError);
  const readers = new Map([[1, reader as Readable]]);
  reader.once('close', () => readers.delete(1));
  reader.destroy(new StreamStoppedError('shutdown'));
  await reader.destroyEntered.promise;

  const closing = destroyTrackedReaders(readers, new UsenetEngineClosedError());
  gate.resolve();
  assert.deepEqual(await closing, [cleanupError]);
  assert.equal(reader.closed, true);
  assert.equal(readers.size, 0);
});
