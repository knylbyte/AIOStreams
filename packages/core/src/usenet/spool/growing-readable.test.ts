import assert from 'node:assert/strict';
import test from 'node:test';
import type { Readable } from 'node:stream';
import { GrowingFileReader } from './growing-readable.js';
import type { GrowingReadableSource, ManagedSpoolFile } from './types.js';
import {
  resolveSegmentStreamQueuePlan,
  SEGMENT_STREAM_MAX_CHUNK_BYTES,
} from '../stream-queue-budget.js';

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
