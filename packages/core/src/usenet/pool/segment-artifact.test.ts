import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import type { Readable } from 'node:stream';
import type { SegmentSpoolingPlan } from '../resource-plan.js';
import { SpoolManager } from '../spool/manager.js';
import { SegmentArena, type SharedSegment } from './segment-arena.js';
import {
  ArenaSegmentArtifact,
  GrowingSpoolArtifactAdapter,
  ZeroSegmentArtifact,
} from './segment-artifact.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;

function plan(): SegmentSpoolingPlan {
  return {
    memoryBudgetBytes: MEBIBYTE_BYTES,
    perStreamBufferBytes: 512 * KIBIBYTE_BYTES,
    spoolBytes: 4 * MEBIBYTE_BYTES,
    minFreeDiskBytes: 0,
    decoderChunkBytes: 64 * KIBIBYTE_BYTES,
    writerQueueBytes: 128 * KIBIBYTE_BYTES,
    readerHighWaterMarkBytes: 64 * KIBIBYTE_BYTES,
    perDownloadBaseLeaseBytes: 128 * KIBIBYTE_BYTES,
    maxOpenSpoolFiles: 8,
    orphanTtlMs: 60_000,
  };
}

async function collect(readable: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of readable) {
    assert(Buffer.isBuffer(chunk));
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function collectEmittedChunks(readable: Readable): Promise<Buffer[]> {
  const chunks: Buffer[] = [];
  readable.on('data', (chunk: Buffer) => chunks.push(chunk));
  await once(readable, 'end');
  return chunks;
}

test('ArenaSegmentArtifact holds its pin through range reading and releases once', async () => {
  let releases = 0;
  const shared: SharedSegment = {
    data: {
      body: Buffer.from('0123456789'),
      byteRange: [10, 20],
      fileSize: 100,
      totalParts: 10,
      name: 'part.bin',
      size: 10,
    },
    owned: false,
    release: () => {
      releases++;
    },
  };
  const artifact = new ArenaSegmentArtifact(shared);

  assert.equal(artifact.storage, 'arena');
  assert.equal(artifact.length, 10);
  assert.deepEqual(artifact.metadata.byteRange, [10, 20]);
  assert.equal(
    (
      await collect(artifact.createReadStream({ start: 2, endExclusive: 7 }))
    ).toString(),
    '23456'
  );
  await artifact.release();
  await artifact.release();
  assert.equal(releases, 1);
});

test('GrowingSpoolArtifactAdapter exposes a complete spool range and disposes once', async (context) => {
  const cacheRoot = await mkdtemp(path.join(tmpdir(), 'artifact-adapter-'));
  const manager = new SpoolManager({
    plan: plan(),
    engineId: 'artifact-adapter-test',
    cacheRoot,
    idGenerator: () => 'artifact-adapter-id',
  });
  context.after(async () => {
    await Promise.allSettled([manager.close()]);
    await rm(cacheRoot, { recursive: true, force: true });
  });
  const source = Buffer.from('growing-spool-body');
  const spool = await manager.createArtifact({
    sessionId: 'session',
    segmentId: 'segment',
    initialReservationBytes: MEBIBYTE_BYTES,
  });
  let chunkReleases = 0;
  spool.write(source, {
    bytes: source.length,
    release: () => {
      chunkReleases++;
    },
  });
  await spool.complete();
  let referenceReleases = 0;
  const artifact = new GrowingSpoolArtifactAdapter(
    spool,
    { size: source.length, name: 'segment.bin' },
    async () => {
      referenceReleases++;
      await spool.dispose();
    }
  );

  assert.equal(
    (
      await collect(
        artifact.createReadStream({ start: 8, endExclusive: source.length })
      )
    ).toString(),
    'spool-body'
  );
  await artifact.release();
  await artifact.release();
  assert.equal(chunkReleases, 1);
  assert.equal(referenceReleases, 1);
  assert.equal(manager.stats().artifacts, 0);
  assert.equal(manager.stats().budget.reservedBytes, 0);
});

test('ZeroSegmentArtifact emits a large logical hole through bounded chunks', async () => {
  const length = 64 * MEBIBYTE_BYTES;
  const artifact = new ZeroSegmentArtifact(length);
  let bytes = 0;
  let maxChunkBytes = 0;
  for await (const chunk of artifact.createReadStream()) {
    assert(Buffer.isBuffer(chunk));
    bytes += chunk.length;
    maxChunkBytes = Math.max(maxChunkBytes, chunk.length);
    assert.equal(
      chunk.every((byte) => byte === 0),
      true
    );
  }

  assert.equal(bytes, length);
  assert(maxChunkBytes <= 64 * KIBIBYTE_BYTES);
  assert.equal(artifact.storage, 'zero');
  await artifact.release();
});

test('ZeroSegmentArtifact applies range and abort contracts without large allocation', async () => {
  const artifact = new ZeroSegmentArtifact(4 * 1024 * MEBIBYTE_BYTES);
  const controller = new AbortController();
  const stream = artifact.createReadStream({
    start: 123,
    endExclusive: 123 + 4 * KIBIBYTE_BYTES,
    highWaterMark: 1024,
    signal: controller.signal,
  });
  assert.equal((await collect(stream)).length, 4 * KIBIBYTE_BYTES);
  assert.throws(
    () => new ZeroSegmentArtifact(10).createReadStream({ endExclusive: 11 }),
    RangeError
  );

  const aborted = new AbortController();
  aborted.abort();
  const abortedStream = new ZeroSegmentArtifact(10).createReadStream({
    signal: aborted.signal,
  });
  await assert.rejects(collect(abortedStream), { name: 'AbortError' });
});

test('ArenaSegmentArtifact chunks remain owned after the real arena slot is recycled', async () => {
  const arena = new SegmentArena({ budgetBytes: MEBIBYTE_BYTES });
  const lease = arena.checkout(MEBIBYTE_BYTES);
  assert(lease);
  const length = 2 * 64 * KIBIBYTE_BYTES;
  lease.slot.fill(0x11, 0, length);
  arena.commit(lease, 'first', {
    body: lease.slot.subarray(0, length),
    size: length,
  });
  const shared = arena.acquire('first');
  assert(shared);
  const artifact = new ArenaSegmentArtifact(shared);
  assert.equal(arena.stats().pinned, 1);

  const chunks = await collectEmittedChunks(
    artifact.createReadStream({ highWaterMark: 64 * KIBIBYTE_BYTES })
  );
  await artifact.release();
  assert.equal(arena.stats().pinned, 0);
  assert.equal(chunks.length, 2);
  assert.notEqual(chunks[0].buffer, lease.slot.buffer);

  const recycled = arena.checkout(MEBIBYTE_BYTES);
  assert(recycled);
  assert.equal(recycled.slot, lease.slot);
  recycled.slot.fill(0x22, 0, length);
  assert.equal(
    chunks[0].every((byte) => byte === 0x11),
    true
  );
  assert.equal(
    chunks[1].every((byte) => byte === 0x11),
    true
  );
  arena.abandon(recycled);
});

test('aborting a slow ArenaSegmentArtifact reader releases one pin without mutating retained bytes', async () => {
  const arena = new SegmentArena({ budgetBytes: MEBIBYTE_BYTES });
  const lease = arena.checkout(MEBIBYTE_BYTES);
  assert(lease);
  const length = 4 * 64 * KIBIBYTE_BYTES;
  lease.slot.fill(0x33, 0, length);
  arena.commit(lease, 'slow', {
    body: lease.slot.subarray(0, length),
    size: length,
  });
  const shared = arena.acquire('slow');
  assert(shared);
  const artifact = new ArenaSegmentArtifact(shared);
  const controller = new AbortController();
  const firstChunk = Promise.withResolvers<Buffer>();
  const reader = artifact.createReadStream({
    highWaterMark: 64 * KIBIBYTE_BYTES,
    signal: controller.signal,
  });
  const closed = new Promise<void>((resolve) => reader.once('close', resolve));
  reader.once('error', () => undefined);
  reader.once('data', (chunk: Buffer) => {
    reader.pause();
    firstChunk.resolve(chunk);
  });
  const retained = await firstChunk.promise;
  assert.equal(arena.stats().pinned, 1);
  controller.abort();
  await closed;
  await artifact.release();
  assert.equal(arena.stats().pinned, 0);

  const recycled = arena.checkout(MEBIBYTE_BYTES);
  assert(recycled);
  assert.equal(recycled.slot, lease.slot);
  recycled.slot.fill(0x44, 0, length);
  assert.equal(
    retained.every((byte) => byte === 0x33),
    true
  );
  arena.abandon(recycled);
});

test('ZeroSegmentArtifact emits independently owned zero chunks', async () => {
  const artifact = new ZeroSegmentArtifact(3 * 64 * KIBIBYTE_BYTES);
  const chunks = await collectEmittedChunks(
    artifact.createReadStream({ highWaterMark: 64 * KIBIBYTE_BYTES })
  );
  assert.equal(chunks.length, 3);
  assert.notEqual(chunks[0].buffer, chunks[1].buffer);
  chunks[0].fill(0x7f);
  assert.equal(
    chunks[1].every((byte) => byte === 0),
    true
  );
  assert.equal(
    chunks[2].every((byte) => byte === 0),
    true
  );
  await artifact.release();
});

test('SegmentArtifact reader highWaterMark has a safe cap compatible with the resource plan', async () => {
  const plannedMaximum = 2 * MEBIBYTE_BYTES;
  const accepted = new ZeroSegmentArtifact(64 * KIBIBYTE_BYTES);
  const chunks = await collectEmittedChunks(
    accepted.createReadStream({ highWaterMark: plannedMaximum })
  );
  assert.equal(
    chunks.reduce((sum, chunk) => sum + chunk.length, 0),
    64 * KIBIBYTE_BYTES
  );
  assert(chunks.every((chunk) => chunk.length <= 64 * KIBIBYTE_BYTES));
  await accepted.release();

  assert.throws(
    () =>
      new ZeroSegmentArtifact(1).createReadStream({
        highWaterMark: plannedMaximum + 1,
      }),
    RangeError
  );
  assert.throws(
    () =>
      new ZeroSegmentArtifact(1).createReadStream({
        highWaterMark: Number.MAX_SAFE_INTEGER + 1,
      }),
    RangeError
  );
});
