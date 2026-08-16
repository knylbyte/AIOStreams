import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, open, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import type { Readable } from 'node:stream';
import test, { type TestContext } from 'node:test';
// Initialise the repository config/logger cycle in production order.
import '../../config/index.js';
import { SegmentCache } from './segment-cache.js';

const MEBIBYTE_BYTES = 1024 * 1024;

async function collect(readable: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of readable) {
    assert(Buffer.isBuffer(chunk));
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function serializedSegment(
  body: Buffer,
  metadata: {
    readonly byteRange?: readonly [number, number];
    readonly fileSize?: number;
    readonly totalParts?: number;
    readonly name?: string;
    readonly includeSize?: boolean;
  }
): Buffer {
  const encoded = Buffer.from(
    JSON.stringify({
      byteRange: metadata.byteRange,
      fileSize: metadata.fileSize,
      totalParts: metadata.totalParts,
      name: metadata.name,
      size: metadata.includeSize === false ? undefined : body.length,
    })
  );
  const prefix = Buffer.allocUnsafe(4);
  prefix.writeUInt32LE(encoded.length, 0);
  return Buffer.concat([prefix, encoded, body]);
}

async function sourceFile(
  root: string,
  name: string,
  body: Buffer
): Promise<string> {
  const source = path.join(root, name);
  await writeFile(source, body, { mode: 0o600 });
  return source;
}

function createCache(
  context: TestContext,
  root: string,
  diskBytes: number,
  maxPromotions = 4
): SegmentCache {
  const cache = new SegmentCache({
    arenaBytes: 0,
    diskBytes,
    diskPath: root,
    namespace: 'segments',
    maxPromotions,
  });
  context.after(() => cache.close());
  return cache;
}

test('persistent segment hits parse only metadata and stream the requested body range', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-hit-'));
  const cache = createCache(context, root, 8 * MEBIBYTE_BYTES);
  context.after(() => rm(root, { recursive: true, force: true }));
  const body = Buffer.alloc(2 * MEBIBYTE_BYTES, 0x31);
  body.write('range-data', body.length - 10);
  const source = await sourceFile(root, 'source.ready', body);

  assert.equal(
    await cache.promote(
      'file-backed-hit',
      {
        byteRange: [10, 10 + body.length],
        fileSize: 10 + body.length,
        totalParts: 3,
        name: 'payload.bin',
        size: body.length,
      },
      source
    ),
    true
  );
  const artifact = await cache.acquire('file-backed-hit');
  assert(artifact);
  assert.equal(artifact.storage, 'disk-cache');
  assert.equal(artifact.length, body.length);
  assert.deepEqual(artifact.metadata.byteRange, [10, 10 + body.length]);
  assert.equal(artifact.metadata.fileSize, 10 + body.length);
  assert.equal(artifact.metadata.name, 'payload.bin');
  // Mutating body bytes after acquire proves the hit parsed only the small
  // header and left payload ownership with the leased cache file.
  const fileKey = createHash('sha1').update('file-backed-hit').digest('hex');
  const cachedFile = await open(path.join(root, 'segments', fileKey), 'r+');
  try {
    const cachedStats = await cachedFile.stat();
    await cachedFile.write(
      Buffer.from('late-bytes'),
      0,
      10,
      cachedStats.size - 10
    );
  } finally {
    await cachedFile.close();
  }
  const reader = artifact.createReadStream({
    start: body.length - 10,
    endExclusive: body.length,
  });
  assert.equal(reader.readableLength, 0);
  assert.equal((await collect(reader)).toString(), 'late-bytes');
  await artifact.release();
  assert.equal(cache.stats().diskHits, 1);
});

test('legacy cache files and index sizes remain readable across restart', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-legacy-'));
  context.after(() => rm(root, { recursive: true, force: true }));
  const messageId = 'legacy-message';
  const fileKey = createHash('sha1').update(messageId).digest('hex');
  const directory = path.join(root, 'segments');
  await mkdir(directory, { recursive: true });
  const body = Buffer.from('legacy-body');
  const serialized = serializedSegment(body, {
    byteRange: [20, 31],
    fileSize: 31,
    totalParts: 2,
    name: 'legacy.bin',
    includeSize: false,
  });
  await writeFile(path.join(directory, fileKey), serialized, { mode: 0o600 });
  // Older indexes accounted decoded bytes rather than serialized bytes.
  await writeFile(
    path.join(root, 'segments.index.json'),
    JSON.stringify({ [fileKey]: { size: body.length } })
  );

  const cache = createCache(context, root, MEBIBYTE_BYTES);
  const artifact = await cache.acquire(messageId);
  assert(artifact);
  assert.equal(
    (await collect(artifact.createReadStream())).toString(),
    'legacy-body'
  );
  await artifact.release();
  assert.equal(cache.stats().diskBytes, serialized.length);

  const buffered = await cache.getAsync(messageId);
  assert(buffered);
  assert.deepEqual(buffered.body, body);
  assert.deepEqual(buffered.byteRange, [20, 31]);
});

test('a promoted entry survives index flush and cache restart', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-restart-'));
  const body = Buffer.from('restart-promotion');
  const source = await sourceFile(root, 'restart.ready', body);
  const first = new SegmentCache({
    arenaBytes: 0,
    diskBytes: MEBIBYTE_BYTES,
    diskPath: root,
    namespace: 'segments',
  });
  let second: SegmentCache | undefined;
  context.after(async () => {
    await Promise.allSettled([first.close(), second?.close()]);
    await rm(root, { recursive: true, force: true });
  });
  assert.equal(
    await first.promote(
      'restart-message',
      { size: body.length, name: 'restart.bin' },
      source
    ),
    true
  );
  await first.close();

  second = new SegmentCache({
    arenaBytes: 0,
    diskBytes: MEBIBYTE_BYTES,
    diskPath: root,
    namespace: 'segments',
  });
  const artifact = await second.acquire('restart-message');
  assert(artifact);
  assert.deepEqual(await collect(artifact.createReadStream()), body);
  await artifact.release();
  assert.equal(second.stats().diskCount, 1);
});

test('promotion failures are best effort and the bounded admission has no queue', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-promote-'));
  const cache = createCache(context, root, 8 * MEBIBYTE_BYTES, 1);
  context.after(() => rm(root, { recursive: true, force: true }));
  const body = Buffer.alloc(MEBIBYTE_BYTES, 0x42);
  const source = await sourceFile(root, 'promotion.ready', body);
  const metadata = { size: body.length, name: 'promotion.bin' };

  const first = cache.promote('first', metadata, source);
  const saturated = cache.promote('second', metadata, source);
  assert.equal(await saturated, false);
  assert.equal(await first, true);
  assert.equal(
    await cache.promote('missing', metadata, path.join(root, 'missing.ready')),
    false
  );
  assert.equal(cache.stats().diskCount, 1);
  assert(cache.stats().diskBytes > body.length);
});

test('zero-byte disk cache disables promotion and leaves no persistent entry', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-disabled-'));
  const cache = createCache(context, root, 0);
  context.after(() => rm(root, { recursive: true, force: true }));
  const source = await sourceFile(root, 'disabled.ready', Buffer.from('data'));
  assert.equal(cache.promotionEnabled, false);
  assert.equal(
    await cache.promote('disabled', { size: 4, name: 'disabled.bin' }, source),
    false
  );
  assert.equal(await cache.acquire('disabled'), undefined);
  assert.equal(cache.stats().diskBytes, 0);
  assert.equal(cache.stats().diskCount, 0);
});
