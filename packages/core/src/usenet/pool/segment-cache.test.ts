import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { createReadStream, createWriteStream } from 'node:fs';
import {
  access,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readdir,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import type { Readable } from 'node:stream';
import test, { type TestContext } from 'node:test';
// Initialise the repository config/logger cycle in production order.
import '../../config/index.js';
import { ByteBudget, type ByteLease } from './byte-budget.js';
import {
  SEGMENT_CACHE_PROMOTION_MEMORY_BYTES,
  SegmentCache,
} from './segment-cache.js';

const MEBIBYTE_BYTES = 1024 * 1024;

function acquirePromotionMemory(bytes: number): ByteLease {
  return { bytes, release: () => undefined };
}

function budgetAdmission(
  budget: ByteBudget
): (bytes: number) => ByteLease | undefined {
  return (bytes) => budget.tryAcquire(bytes) ?? undefined;
}

function codedError(code: string): Error & { code: string } {
  return Object.assign(new Error(code), { code });
}

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
      source,
      acquirePromotionMemory
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
      source,
      acquirePromotionMemory
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

  const first = cache.promote(
    'first',
    metadata,
    source,
    acquirePromotionMemory
  );
  const saturated = cache.promote(
    'second',
    metadata,
    source,
    acquirePromotionMemory
  );
  assert.equal(await saturated, false);
  assert.equal(await first, true);
  assert.equal(
    await cache.promote(
      'missing',
      metadata,
      path.join(root, 'missing.ready'),
      acquirePromotionMemory
    ),
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
    await cache.promote(
      'disabled',
      { size: 4, name: 'disabled.bin' },
      source,
      acquirePromotionMemory
    ),
    false
  );
  assert.equal(await cache.acquire('disabled'), undefined);
  assert.equal(cache.stats().diskBytes, 0);
  assert.equal(cache.stats().diskCount, 0);
});

test('corrupt file-backed metadata is one miss, never a provisional hit', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-corrupt-'));
  context.after(() => rm(root, { recursive: true, force: true }));
  const messageId = 'corrupt-entry';
  const key = createHash('sha1').update(messageId).digest('hex');
  const directory = path.join(root, 'segments');
  await mkdir(directory, { recursive: true });
  const corrupt = Buffer.alloc(4);
  corrupt.writeUInt32LE(128 * 1024, 0);
  await writeFile(path.join(directory, key), corrupt, { mode: 0o600 });
  await writeFile(
    path.join(root, 'segments.index.json'),
    JSON.stringify({ [key]: { size: corrupt.length } })
  );
  const cache = createCache(context, root, MEBIBYTE_BYTES);

  assert.equal(await cache.acquire(messageId), undefined);
  assert.deepEqual(
    {
      hits: cache.stats().hits,
      misses: cache.stats().misses,
      diskHits: cache.stats().diskHits,
      diskCount: cache.stats().diskCount,
    },
    { hits: 0, misses: 1, diskHits: 0, diskCount: 0 }
  );
});

test('transient metadata-open failure preserves the persistent entry and stats', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-open-io-'));
  let failReadOpen = false;
  const cache = new SegmentCache({
    arenaBytes: 0,
    diskBytes: MEBIBYTE_BYTES,
    diskPath: root,
    namespace: 'segments',
    openFile: async (target, flags, mode) => {
      if (flags === 'r' && failReadOpen) {
        failReadOpen = false;
        throw codedError('EMFILE');
      }
      return open(target, flags, mode);
    },
  });
  context.after(async () => {
    await cache.close();
    await rm(root, { recursive: true, force: true });
  });
  const body = Buffer.from('persistent-body');
  const source = await sourceFile(root, 'open-io.ready', body);
  assert.equal(
    await cache.promote(
      'open-io',
      { size: body.length },
      source,
      acquirePromotionMemory
    ),
    true
  );

  failReadOpen = true;
  assert.equal(await cache.acquire('open-io'), undefined);
  assert.deepEqual(
    {
      hits: cache.stats().hits,
      misses: cache.stats().misses,
      diskHits: cache.stats().diskHits,
      diskCount: cache.stats().diskCount,
    },
    { hits: 0, misses: 0, diskHits: 0, diskCount: 1 }
  );
  const artifact = await cache.acquire('open-io');
  assert(artifact);
  assert.deepEqual(await collect(artifact.createReadStream()), body);
  await artifact.release();
  assert.equal(cache.stats().diskHits, 1);
});

test('abort while metadata open is pending preserves entry and lookup counters', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-open-abort-'));
  const openEntered = Promise.withResolvers<void>();
  const continueOpen = Promise.withResolvers<void>();
  let pauseReadOpen = false;
  const cache = new SegmentCache({
    arenaBytes: 0,
    diskBytes: MEBIBYTE_BYTES,
    diskPath: root,
    namespace: 'segments',
    openFile: async (target, flags, mode) => {
      const handle = await open(target, flags, mode);
      if (flags === 'r' && pauseReadOpen) {
        openEntered.resolve();
        await continueOpen.promise;
      }
      return handle;
    },
  });
  context.after(async () => {
    await cache.close();
    await rm(root, { recursive: true, force: true });
  });
  const body = Buffer.from('abort-body');
  const source = await sourceFile(root, 'abort.ready', body);
  await cache.promote(
    'abort-metadata',
    { size: body.length },
    source,
    acquirePromotionMemory
  );

  pauseReadOpen = true;
  const controller = new AbortController();
  const aborted = new Error('metadata lookup aborted');
  const lookup = cache.acquire('abort-metadata', controller.signal);
  await openEntered.promise;
  controller.abort(aborted);
  continueOpen.resolve();
  await assert.rejects(lookup, (error: unknown) => error === aborted);
  assert.deepEqual(
    {
      hits: cache.stats().hits,
      misses: cache.stats().misses,
      diskHits: cache.stats().diskHits,
      diskCount: cache.stats().diskCount,
    },
    { hits: 0, misses: 0, diskHits: 0, diskCount: 1 }
  );
  pauseReadOpen = false;
  const artifact = await cache.acquire('abort-metadata');
  assert(artifact);
  await artifact.release();
});

test('clear invalidates a promotion paused before prepared install', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-clear-'));
  const installEntered = Promise.withResolvers<void>();
  const continueInstall = Promise.withResolvers<void>();
  const cache = new SegmentCache({
    arenaBytes: 0,
    diskBytes: MEBIBYTE_BYTES,
    diskPath: root,
    namespace: 'segments',
    beforePreparedInstall: async () => {
      installEntered.resolve();
      await continueInstall.promise;
    },
  });
  context.after(async () => {
    await cache.close();
    await rm(root, { recursive: true, force: true });
  });
  const body = Buffer.from('clear-promotion');
  const source = await sourceFile(root, 'clear.ready', body);
  const promotion = cache.promote(
    'clear-promotion',
    { size: body.length },
    source,
    acquirePromotionMemory
  );
  await installEntered.promise;
  await cache.clear();
  continueInstall.resolve();

  assert.equal(await promotion, false);
  assert.equal(cache.stats().diskCount, 0);
  assert.equal(cache.stats().diskBytes, 0);
  const entries = await readdir(path.join(root, 'segments'));
  assert.equal(
    entries.some((name) => name.startsWith('.prepared-')),
    false
  );
  await assert.rejects(access(path.join(root, 'segments.index.json')), {
    code: 'ENOENT',
  });
});

test('clear during pre-copy source I/O cannot recreate a stale prepared path', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'segment-cache-clear-source-')
  );
  const sourceStatEntered = Promise.withResolvers<void>();
  const continueSourceStat = Promise.withResolvers<void>();
  const cache = new SegmentCache({
    arenaBytes: 0,
    diskBytes: MEBIBYTE_BYTES,
    diskPath: root,
    namespace: 'segments',
    lstatFile: async (target) => {
      sourceStatEntered.resolve();
      await continueSourceStat.promise;
      return lstat(target);
    },
  });
  context.after(async () => {
    await cache.close();
    await rm(root, { recursive: true, force: true });
  });
  const body = Buffer.from('clear-before-copy');
  const source = await sourceFile(root, 'clear-source.ready', body);
  const promotion = cache.promote(
    'clear-source',
    { size: body.length },
    source,
    acquirePromotionMemory
  );
  await sourceStatEntered.promise;
  await cache.clear();
  continueSourceStat.resolve();

  assert.equal(await promotion, false);
  const entries = await readdir(path.join(root, 'segments'));
  assert.equal(
    entries.some((name) => name.startsWith('.prepared-')),
    false
  );
  assert.equal(cache.stats().diskCount, 0);
});

test('promotion skips before prepared creation when memory has no free window', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-memory-full-'));
  const cache = createCache(context, root, MEBIBYTE_BYTES);
  context.after(() => rm(root, { recursive: true, force: true }));
  const source = await sourceFile(root, 'full.ready', Buffer.from('data'));
  const budget = new ByteBudget(SEGMENT_CACHE_PROMOTION_MEMORY_BYTES);
  const held = budget.tryAcquire(SEGMENT_CACHE_PROMOTION_MEMORY_BYTES);
  assert(held);

  assert.equal(
    await cache.promote(
      'memory-full',
      { size: 4 },
      source,
      budgetAdmission(budget)
    ),
    false
  );
  assert.equal(budget.stats().usedBytes, SEGMENT_CACHE_PROMOTION_MEMORY_BYTES);
  const entries = await readdir(path.join(root, 'segments'));
  assert.equal(
    entries.some((name) => name.startsWith('.prepared-')),
    false
  );
  held.release();
  assert.equal(budget.stats().usedBytes, 0);
});

test('promotion memory lease covers pipeline and install then releases once', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-memory-'));
  const installEntered = Promise.withResolvers<void>();
  const continueInstall = Promise.withResolvers<void>();
  const cache = new SegmentCache({
    arenaBytes: 0,
    diskBytes: MEBIBYTE_BYTES,
    diskPath: root,
    namespace: 'segments',
    beforePreparedInstall: async () => {
      installEntered.resolve();
      await continueInstall.promise;
    },
  });
  context.after(async () => {
    await cache.close();
    await rm(root, { recursive: true, force: true });
  });
  const body = Buffer.alloc(128 * 1024, 0x61);
  const source = await sourceFile(root, 'memory.ready', body);
  const budget = new ByteBudget(SEGMENT_CACHE_PROMOTION_MEMORY_BYTES);
  const promotion = cache.promote(
    'memory-window',
    { size: body.length },
    source,
    budgetAdmission(budget)
  );
  await installEntered.promise;
  assert.equal(budget.stats().usedBytes, SEGMENT_CACHE_PROMOTION_MEMORY_BYTES);
  assert(budget.stats().usedBytes <= budget.stats().maxBytes);
  continueInstall.resolve();
  assert.equal(await promotion, true);
  assert.equal(budget.stats().usedBytes, 0);
});

test('promotion I/O and EXDEV failures always release their memory window', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'segment-cache-memory-errors-')
  );
  context.after(() => rm(root, { recursive: true, force: true }));
  const body = Buffer.from('promotion-errors');
  const source = await sourceFile(root, 'errors.ready', body);

  const cases: Array<{
    readonly name: string;
    readonly options: ConstructorParameters<typeof SegmentCache>[0];
  }> = [
    {
      name: 'source-read',
      options: {
        createPromotionReadStream: (_target, options) =>
          createReadStream(path.join(root, 'missing-source'), options),
      },
    },
    {
      name: 'destination-write',
      options: {
        createPromotionWriteStream: (_target, options) =>
          createWriteStream(
            path.join(root, 'missing-directory', 'destination'),
            options
          ),
      },
    },
    {
      name: 'header-write',
      options: {
        openFile: async (target, flags, mode) => {
          if (flags === 'r+') throw codedError('EACCES');
          return open(target, flags, mode);
        },
      },
    },
    {
      name: 'install',
      options: {
        diskCacheRenameFile: async () => {
          throw codedError('EIO');
        },
      },
    },
    {
      name: 'exdev-copy',
      options: {
        diskCacheRenameFile: async () => {
          throw codedError('EXDEV');
        },
        diskCacheFileSystem: {
          lstat: async (target) => {
            if (String(target).includes('.install-')) throw codedError('EIO');
            const handle = await open(target, 'r');
            try {
              return await handle.stat();
            } finally {
              await handle.close();
            }
          },
        },
      },
    },
  ];

  for (const item of cases) {
    const budget = new ByteBudget(SEGMENT_CACHE_PROMOTION_MEMORY_BYTES);
    const cache = new SegmentCache({
      arenaBytes: 0,
      diskBytes: MEBIBYTE_BYTES,
      diskPath: root,
      namespace: `segments-${item.name}`,
      ...item.options,
    });
    assert.equal(
      await cache.promote(
        item.name,
        { size: body.length },
        source,
        budgetAdmission(budget)
      ),
      false
    );
    assert.equal(budget.stats().usedBytes, 0);
    await cache.close();
  }
});

test('four promotions own four leases and a fifth never enters either budget', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'segment-cache-four-memory-'));
  const allEntered = Promise.withResolvers<void>();
  const continueInstalls = Promise.withResolvers<void>();
  let entered = 0;
  const cache = new SegmentCache({
    arenaBytes: 0,
    diskBytes: 8 * MEBIBYTE_BYTES,
    diskPath: root,
    namespace: 'segments',
    maxPromotions: 4,
    beforePreparedInstall: async () => {
      entered++;
      if (entered === 4) allEntered.resolve();
      await continueInstalls.promise;
    },
  });
  context.after(async () => {
    await cache.close();
    await rm(root, { recursive: true, force: true });
  });
  const body = Buffer.alloc(64 * 1024, 0x62);
  const source = await sourceFile(root, 'four.ready', body);
  const budget = new ByteBudget(4 * SEGMENT_CACHE_PROMOTION_MEMORY_BYTES);
  const promotions = Array.from({ length: 4 }, (_, index) =>
    cache.promote(
      `parallel-${index}`,
      { size: body.length },
      source,
      budgetAdmission(budget)
    )
  );
  const fifth = cache.promote(
    'parallel-fifth',
    { size: body.length },
    source,
    budgetAdmission(budget)
  );
  assert.equal(await fifth, false);
  await allEntered.promise;
  assert.equal(
    budget.stats().usedBytes,
    4 * SEGMENT_CACHE_PROMOTION_MEMORY_BYTES
  );
  assert(budget.stats().usedBytes <= budget.stats().maxBytes);
  continueInstalls.resolve();
  assert.deepEqual(await Promise.all(promotions), [true, true, true, true]);
  assert.equal(budget.stats().usedBytes, 0);
});
