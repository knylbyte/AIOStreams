import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';
import type { Readable } from 'node:stream';
import type { SegmentSpoolingPlan } from '../resource-plan.js';
import { SpoolManager } from '../spool/manager.js';
import type { SharedSegment } from './segment-arena.js';
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
