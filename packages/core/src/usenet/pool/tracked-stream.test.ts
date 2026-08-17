import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import test from 'node:test';
import '../../config/index.js';
import { StatsAccumulator } from '../stats/accumulator.js';
import type { SeekableStream } from './file-stream.js';
import {
  destroyTrackedReaders,
  trackSeekableStream,
  UsenetEngineClosedError,
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
