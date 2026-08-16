import assert from 'node:assert/strict';
import { once } from 'node:events';
import test from 'node:test';
import type { Readable } from 'node:stream';
import { GrowingFileReader } from './growing-readable.js';
import type { GrowingReadableSource, ManagedSpoolFile } from './types.js';
import {
  resolveSegmentStreamQueuePlan,
  SEGMENT_STREAM_MAX_CHUNK_BYTES,
} from '../stream-queue-budget.js';
import { UsenetSpoolError } from './errors.js';

async function waitForReadableLength(
  stream: Readable,
  minimumBytes: number
): Promise<void> {
  if (stream.readableLength >= minimumBytes) return;
  await new Promise<void>((resolve, reject) => {
    const check = (): void => {
      if (stream.readableLength < minimumBytes) return;
      cleanup();
      resolve();
    };
    const fail = (error: Error): void => {
      cleanup();
      reject(error);
    };
    const cleanup = (): void => {
      stream.removeListener('readable', check);
      stream.removeListener('error', fail);
    };
    stream.on('readable', check);
    stream.once('error', fail);
    check();
  });
}

async function collect(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    assert(Buffer.isBuffer(chunk));
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

test('GrowingFileReader caps chunks and refill bytes at the hard queue capacity', async () => {
  const highWaterMark = 500_000;
  const queuePlan = resolveSegmentStreamQueuePlan(highWaterMark);
  const body = Buffer.alloc(
    queuePlan.capacityBytes + 2 * SEGMENT_STREAM_MAX_CHUNK_BYTES,
    0x5a
  );
  const readSizes: number[] = [];
  let closed = false;
  let onClosedCalls = 0;
  const managedFile: ManagedSpoolFile = {
    handle: {
      read: (target, offset, length, position) => {
        readSizes.push(length);
        const bytesRead = Math.min(length, body.length - position);
        body.copy(target, offset, position, position + bytesRead);
        return Promise.resolve({ bytesRead });
      },
      write: () => Promise.reject(new Error('reader fixture cannot write')),
      close: () => Promise.resolve(),
    },
    close: () => {
      closed = true;
      return Promise.resolve();
    },
  };
  const source: GrowingReadableSource = {
    snapshot: () => ({
      state: 'complete',
      committedBytes: body.length,
    }),
    waitForChange: () =>
      Promise.reject(new Error('complete reader must not wait for changes')),
    openReadableFile: () => Promise.resolve(managedFile),
  };
  const reader = new GrowingFileReader({
    source,
    start: 0,
    highWaterMark,
    onClosed: () => {
      onClosedCalls++;
    },
  });

  reader.read(0);
  const initiallyQueued =
    Math.ceil(highWaterMark / SEGMENT_STREAM_MAX_CHUNK_BYTES) *
    SEGMENT_STREAM_MAX_CHUNK_BYTES;
  await waitForReadableLength(reader, initiallyQueued);
  const consumeBytes = reader.readableLength - (highWaterMark - 1);
  const prefix = reader.read(consumeBytes);
  assert(Buffer.isBuffer(prefix));

  await waitForReadableLength(reader, queuePlan.capacityBytes);
  assert.equal(reader.readableLength, queuePlan.capacityBytes);
  assert(readSizes.every((bytes) => bytes <= SEGMENT_STREAM_MAX_CHUNK_BYTES));

  const suffix = await collect(reader);
  assert.deepEqual(Buffer.concat([prefix, suffix]), body);
  assert.equal(closed, true);
  assert.equal(onClosedCalls, 1);
});

test('GrowingFileReader fills one owned output buffer across one-byte short reads', async () => {
  const body = Buffer.alloc(1_000, 0x41);
  const targets: Buffer[] = [];
  const offsets: number[] = [];
  const positions: number[] = [];
  let closeCalls = 0;
  let onClosedCalls = 0;
  const source: GrowingReadableSource = {
    snapshot: () => ({ state: 'complete', committedBytes: body.length }),
    waitForChange: () =>
      Promise.reject(new Error('complete reader must not wait for changes')),
    openReadableFile: () =>
      Promise.resolve({
        handle: {
          read: (target, offset, _length, position) => {
            targets.push(target);
            offsets.push(offset);
            positions.push(position);
            body.copy(target, offset, position, position + 1);
            return Promise.resolve({ bytesRead: 1 });
          },
          write: () => Promise.reject(new Error('reader fixture cannot write')),
          close: () => Promise.resolve(),
        },
        close: () => {
          closeCalls++;
          return Promise.resolve();
        },
      }),
  };
  const reader = new GrowingFileReader({
    source,
    start: 0,
    highWaterMark: body.length,
    onClosed: () => {
      onClosedCalls++;
    },
  });
  const chunks: Buffer[] = [];
  reader.on('data', (chunk: Buffer) => chunks.push(chunk));
  const closed = once(reader, 'close');

  await once(reader, 'end');

  assert.equal(chunks.length, 1);
  assert.equal(chunks[0].length, body.length);
  assert.equal(chunks[0].buffer.byteLength, body.length);
  assert.equal(chunks[0].byteOffset, 0);
  assert.deepEqual(chunks[0], body);
  assert.equal(targets.length, body.length);
  assert(targets.every((target) => target === targets[0]));
  assert.equal(targets[0].buffer.byteLength, body.length);
  assert.deepEqual(
    offsets,
    Array.from({ length: body.length }, (_, i) => i)
  );
  assert.deepEqual(
    positions,
    Array.from({ length: body.length }, (_, i) => i)
  );
  await closed;
  assert.equal(closeCalls, 1);
  assert.equal(onClosedCalls, 1);
});

test('GrowingFileReader keeps variable short reads within exact chunk allocations', async () => {
  const body = Buffer.alloc(SEGMENT_STREAM_MAX_CHUNK_BYTES + 5_000, 0x5b);
  const shortReads = [1, 7, 4_096] as const;
  const requestedBuffers: Buffer[] = [];
  let readIndex = 0;
  const source: GrowingReadableSource = {
    snapshot: () => ({ state: 'complete', committedBytes: body.length }),
    waitForChange: () =>
      Promise.reject(new Error('complete reader must not wait for changes')),
    openReadableFile: () =>
      Promise.resolve({
        handle: {
          read: (target, offset, length, position) => {
            requestedBuffers.push(target);
            const bytesRead = Math.min(
              length,
              shortReads[readIndex++ % shortReads.length],
              body.length - position
            );
            body.copy(target, offset, position, position + bytesRead);
            return Promise.resolve({ bytesRead });
          },
          write: () => Promise.reject(new Error('reader fixture cannot write')),
          close: () => Promise.resolve(),
        },
        close: () => Promise.resolve(),
      }),
  };
  const reader = new GrowingFileReader({
    source,
    start: 0,
    highWaterMark: 2_000,
    onClosed: () => undefined,
  });
  const chunks: Buffer[] = [];
  for await (const chunk of reader) {
    assert(Buffer.isBuffer(chunk));
    chunks.push(chunk);
  }

  assert.deepEqual(Buffer.concat(chunks), body);
  assert(
    chunks.every((chunk) => chunk.length <= SEGMENT_STREAM_MAX_CHUNK_BYTES)
  );
  const distinctBuffers = new Set(requestedBuffers);
  assert.equal(distinctBuffers.size, chunks.length);
  assert.equal(
    [...distinctBuffers].reduce(
      (total, buffer) => total + buffer.buffer.byteLength,
      0
    ),
    body.length
  );
  assert(chunks.every((chunk) => chunk.buffer.byteLength === chunk.length));
  assert.equal(chunks[0].length, SEGMENT_STREAM_MAX_CHUNK_BYTES);
  assert.equal(chunks.at(-1)?.length, 5_000);
});

test('GrowingFileReader rejects a null read before committed EOF and closes once', async () => {
  let closeCalls = 0;
  let onClosedCalls = 0;
  const source: GrowingReadableSource = {
    snapshot: () => ({ state: 'complete', committedBytes: 8 }),
    waitForChange: () =>
      Promise.reject(new Error('complete reader must not wait for changes')),
    openReadableFile: () =>
      Promise.resolve({
        handle: {
          read: () => Promise.resolve({ bytesRead: 0 }),
          write: () => Promise.reject(new Error('reader fixture cannot write')),
          close: () => Promise.resolve(),
        },
        close: () => {
          closeCalls++;
          return Promise.resolve();
        },
      }),
  };
  const reader = new GrowingFileReader({
    source,
    start: 0,
    highWaterMark: 8,
    onClosed: () => {
      onClosedCalls++;
    },
  });

  await assert.rejects(collect(reader), (error: unknown) => {
    assert(error instanceof UsenetSpoolError);
    assert.equal(error.code, 'USENET_SPOOL_IO');
    return true;
  });
  assert.equal(closeCalls, 1);
  assert.equal(onClosedCalls, 1);
});

test('GrowingFileReader aborts between short reads without pushing a partial chunk', async () => {
  const controller = new AbortController();
  let readCalls = 0;
  let closeCalls = 0;
  let dataCalls = 0;
  const source: GrowingReadableSource = {
    snapshot: () => ({ state: 'complete', committedBytes: 8 }),
    waitForChange: () =>
      Promise.reject(new Error('complete reader must not wait for changes')),
    openReadableFile: () =>
      Promise.resolve({
        handle: {
          read: (target, offset) => {
            readCalls++;
            target[offset] = 0x61;
            if (readCalls === 1) queueMicrotask(() => controller.abort('stop'));
            return Promise.resolve({ bytesRead: 1 });
          },
          write: () => Promise.reject(new Error('reader fixture cannot write')),
          close: () => Promise.resolve(),
        },
        close: () => {
          closeCalls++;
          return Promise.resolve();
        },
      }),
  };
  const reader = new GrowingFileReader({
    source,
    start: 0,
    highWaterMark: 8,
    signal: controller.signal,
    onClosed: () => undefined,
  });
  reader.on('data', () => dataCalls++);

  const error = Promise.withResolvers<Error>();
  const closed = new Promise<void>((resolve) => reader.once('close', resolve));
  reader.once('error', error.resolve);
  reader.resume();
  await error.promise;
  await closed;

  assert.equal(dataCalls, 0);
  assert.equal(readCalls, 1);
  assert.equal(closeCalls, 1);
});
