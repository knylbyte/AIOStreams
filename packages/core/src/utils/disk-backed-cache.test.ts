import assert from 'node:assert/strict';
import {
  access,
  mkdtemp,
  readFile,
  rename,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test, { type TestContext } from 'node:test';
// Initialise the repository config/logger cycle in production order.
import '../config/index.js';
import { DiskBackedCache, type DiskPreparedFile } from './disk-backed-cache.js';

function createCache(
  context: TestContext,
  root: string,
  options: {
    readonly maxDiskBytes: number;
    readonly renameFile?: (
      source: string,
      destination: string
    ) => Promise<void>;
  }
): DiskBackedCache<Buffer> {
  const cache = new DiskBackedCache<Buffer>({
    name: 'test-cache',
    dir: root,
    maxMemBytes: 0,
    maxDiskBytes: options.maxDiskBytes,
    serialize: (value) => Buffer.from(value),
    deserialize: (value) => Buffer.from(value),
    sizeOf: (value) => value.length,
    renameFile: options.renameFile,
  });
  context.after(async () => {
    await cache.close();
  });
  return cache;
}

async function prepared(
  cache: DiskBackedCache<Buffer>,
  body: Buffer
): Promise<DiskPreparedFile> {
  const file = await cache.createPreparedFile();
  await writeFile(file.path, body, { flag: 'w', mode: 0o600 });
  return file;
}

test('prepared installs are atomic, touch LRU, and account serialized bytes', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-lru-'));
  const cache = createCache(context, root, { maxDiskBytes: 8 });
  context.after(() => rm(root, { recursive: true, force: true }));

  assert.equal(
    await cache.installPreparedFile(
      'a',
      await prepared(cache, Buffer.from('aaaa')),
      4
    ),
    true
  );
  assert.equal(
    await cache.installPreparedFile(
      'b',
      await prepared(cache, Buffer.from('bbbb')),
      4
    ),
    true
  );
  const touched = await cache.acquireDiskFile('a');
  assert(touched);
  await touched.release();

  assert.equal(
    await cache.installPreparedFile(
      'c',
      await prepared(cache, Buffer.from('cccc')),
      4
    ),
    true
  );
  assert.equal(await cache.acquireDiskFile('b'), undefined);
  const a = await cache.acquireDiskFile('a');
  const c = await cache.acquireDiskFile('c');
  assert(a);
  assert(c);
  assert.equal((await readFile(a.path)).toString(), 'aaaa');
  assert.equal((await readFile(c.path)).toString(), 'cccc');
  await Promise.all([a.release(), c.release()]);
  assert.deepEqual(
    { diskBytes: cache.stats().diskBytes, diskCount: cache.stats().diskCount },
    { diskBytes: 8, diskCount: 2 }
  );
});

test('the existing set/getAsync buffering path remains compatible', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-buffering-'));
  const cache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  cache.set('buffered', Buffer.from('body'));
  await cache.flush();
  assert.deepEqual(await cache.getAsync('buffered'), Buffer.from('body'));
  assert.equal(cache.stats().diskBytes, 4);
  assert.equal(cache.stats().diskCount, 1);
});

test('an active file lease defers physical eviction until its final release', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-lease-'));
  const cache = createCache(context, root, { maxDiskBytes: 4 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.installPreparedFile(
    'old',
    await prepared(cache, Buffer.from('old!')),
    4
  );
  const lease = await cache.acquireDiskFile('old');
  assert(lease);

  await cache.installPreparedFile(
    'new',
    await prepared(cache, Buffer.from('new!')),
    4
  );
  assert.equal(cache.stats().diskCount, 1);
  assert.equal(cache.stats().diskBytes, 4);
  assert.equal((await readFile(lease.path)).toString(), 'old!');
  await lease.release();
  await lease.release();
  await assert.rejects(access(lease.path), { code: 'ENOENT' });
  assert.equal(cache.stats().diskBytes, 4);
});

test('delete is logical immediately and Windows-friendly physical cleanup is lease-bound', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-delete-'));
  const cache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.installPreparedFile(
    'leased',
    await prepared(cache, Buffer.from('payload')),
    7
  );
  const lease = await cache.acquireDiskFile('leased');
  assert(lease);
  assert.equal(await cache.delete('leased'), true);
  assert.equal(await cache.acquireDiskFile('leased'), undefined);
  assert.equal((await readFile(lease.path)).toString(), 'payload');
  await lease.release();
  await assert.rejects(access(lease.path), { code: 'ENOENT' });
  assert.equal(cache.stats().diskBytes, 0);
  assert.equal(cache.stats().diskCount, 0);
});

test('prepared install falls back to a bounded streamed copy on EXDEV', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-exdev-'));
  let renameCalls = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 1024,
    renameFile: async () => {
      renameCalls++;
      const error = new Error('cross-device');
      Object.assign(error, { code: 'EXDEV' });
      throw error;
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  const body = Buffer.alloc(257, 0x5a);
  const source = await prepared(cache, body);
  assert.equal(
    await cache.installPreparedFile('cross-device', source, 257),
    true
  );
  assert.equal(renameCalls, 1);
  const lease = await cache.acquireDiskFile('cross-device');
  assert(lease);
  assert.deepEqual(await readFile(lease.path), body);
  await lease.release();
  await assert.rejects(access(source.path), { code: 'ENOENT' });
});

test('failed prepared installs and idempotent releases leave accounting unchanged', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-failure-'));
  const cache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  const source = await prepared(cache, Buffer.from('four'));
  await assert.rejects(cache.installPreparedFile('bad', source, 5));
  await source.release();
  await source.release();
  assert.equal(cache.stats().diskBytes, 0);
  assert.equal(cache.stats().diskCount, 0);
});

test('a failed atomic install releases pending accounting for the next write', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-rollback-'));
  let failRename = true;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    renameFile: async (source, destination) => {
      if (failRename) {
        failRename = false;
        const error = new Error('synthetic install failure');
        Object.assign(error, { code: 'EIO' });
        throw error;
      }
      await rename(source, destination);
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  await assert.rejects(
    cache.installPreparedFile(
      'first',
      await prepared(cache, Buffer.from('fail')),
      4
    )
  );
  assert.equal(cache.stats().diskBytes, 0);
  assert.equal(cache.stats().diskCount, 0);
  assert.equal(
    await cache.installPreparedFile(
      'second',
      await prepared(cache, Buffer.from('pass')),
      4
    ),
    true
  );
  assert.equal(cache.stats().diskBytes, 4);
  assert.equal(cache.stats().diskCount, 1);
});

test('concurrent prepared installs for one key count exactly one entry', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-same-key-'));
  const cache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  const [firstPrepared, secondPrepared] = await Promise.all([
    prepared(cache, Buffer.from('one!')),
    prepared(cache, Buffer.from('two!')),
  ]);
  const [first, second] = await Promise.all([
    cache.installPreparedFile('same-key', firstPrepared, 4),
    cache.installPreparedFile('same-key', secondPrepared, 4),
  ]);
  assert.deepEqual([first, second].sort(), [false, true]);
  assert.equal(cache.stats().diskBytes, 4);
  assert.equal(cache.stats().diskCount, 1);
});
