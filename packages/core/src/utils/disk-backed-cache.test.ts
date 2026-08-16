import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import {
  access,
  open,
  mkdtemp,
  readFile,
  readdir,
  rename,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test, { type TestContext } from 'node:test';
// Initialise the repository config/logger cycle in production order.
import '../config/index.js';
import {
  DiskBackedCache,
  DiskBackedCacheError,
  type DiskBackedCacheFileSystem,
  type DiskPreparedFile,
} from './disk-backed-cache.js';

function deferred<T = void>(): PromiseWithResolvers<T> {
  return Promise.withResolvers<T>();
}

function fileKey(key: string): string {
  return createHash('sha1').update(key).digest('hex');
}

function codedError(code: string, message = code): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

function createCache(
  context: TestContext,
  root: string,
  options: {
    readonly maxDiskBytes: number;
    readonly renameFile?: (
      source: string,
      destination: string
    ) => Promise<void>;
    readonly fileSystem?: Partial<DiskBackedCacheFileSystem>;
    readonly name?: string;
  }
): DiskBackedCache<Buffer> {
  const cache = new DiskBackedCache<Buffer>({
    name: options.name ?? 'test-cache',
    dir: root,
    maxMemBytes: 0,
    maxDiskBytes: options.maxDiskBytes,
    serialize: (value) => Buffer.from(value),
    deserialize: (value) => Buffer.from(value),
    sizeOf: (value) => value.length,
    renameFile: options.renameFile,
    fileSystem: options.fileSystem,
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
    renameFile: async (source, destination) => {
      renameCalls++;
      if (renameCalls === 1) {
        const error = new Error('cross-device');
        Object.assign(error, { code: 'EXDEV' });
        throw error;
      }
      await rename(source, destination);
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  const body = Buffer.alloc(257, 0x5a);
  const source = await prepared(cache, body);
  assert.equal(
    await cache.installPreparedFile('cross-device', source, 257),
    true
  );
  assert.equal(renameCalls, 2);
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

test('clear waits an older index snapshot and cannot resurrect it', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-index-clear-'));
  const indexWriteEntered = deferred();
  const continueIndexWrite = deferred();
  let pauseIndexWrite = false;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      writeFile: async (target, data, options) => {
        if (pauseIndexWrite && String(target).endsWith('.index.json')) {
          indexWriteEntered.resolve();
          await continueIndexWrite.promise;
        }
        return writeFile(target, data, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.installPreparedFile(
    'old-index',
    await prepared(cache, Buffer.from('old!')),
    4
  );

  pauseIndexWrite = true;
  const flush = cache.flushIndex();
  await indexWriteEntered.promise;
  let clearSettled = false;
  const clear = cache.clear().finally(() => {
    clearSettled = true;
  });
  await Promise.resolve();
  assert.equal(clearSettled, false);
  continueIndexWrite.resolve();
  await Promise.all([flush, clear]);

  await assert.rejects(access(path.join(root, 'test-cache.index.json')), {
    code: 'ENOENT',
  });
  assert.equal(cache.stats().diskCount, 0);
  assert.equal(cache.stats().diskBytes, 0);
});

test('clear with a file lease removes the index before deferred physical cleanup', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-clear-lease-'));
  const indexWriteEntered = deferred();
  const continueIndexWrite = deferred();
  let pauseIndexWrite = false;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      writeFile: async (target, data, options) => {
        if (pauseIndexWrite && String(target).endsWith('.index.json')) {
          indexWriteEntered.resolve();
          await continueIndexWrite.promise;
        }
        return writeFile(target, data, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.installPreparedFile(
    'leased-clear',
    await prepared(cache, Buffer.from('data')),
    4
  );
  const lease = await cache.acquireDiskFile('leased-clear');
  assert(lease);
  lease.confirmHit();

  pauseIndexWrite = true;
  const flush = cache.flushIndex();
  await indexWriteEntered.promise;
  const clear = cache.clear();
  continueIndexWrite.resolve();
  await Promise.all([flush, clear]);
  await access(lease.path);
  await assert.rejects(access(path.join(root, 'test-cache.index.json')), {
    code: 'ENOENT',
  });
  const restarted = createCache(context, root, { maxDiskBytes: 16 });
  await restarted.whenReady();
  assert.equal(await restarted.acquireDiskFile('leased-clear'), undefined);
  assert.equal(restarted.stats().diskCount, 0);
  // Reconciliation in the replacement instance must not physically delete a
  // path still leased by the retired instance.
  await access(lease.path);
  await lease.release();
  await assert.rejects(access(lease.path), { code: 'ENOENT' });
});

test('prepared creation reserves at most 64 slots before async I/O', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-slots-'));
  const cache = createCache(context, root, { maxDiskBytes: 1024 });
  context.after(() => rm(root, { recursive: true, force: true }));

  const results = await Promise.allSettled(
    Array.from({ length: 100 }, () => cache.createPreparedFile())
  );
  const handles = results.flatMap((result) =>
    result.status === 'fulfilled' ? [result.value] : []
  );
  const errors = results.flatMap((result) =>
    result.status === 'rejected' ? [result.reason] : []
  );
  assert.equal(handles.length, 64);
  assert.equal(errors.length, 36);
  for (const error of errors) {
    assert(error instanceof DiskBackedCacheError);
    assert.equal(error.code, 'DISK_CACHE_PREPARED_LIMIT');
  }
  await Promise.all(handles.map((handle) => handle.release()));
  const entries = await readdir(path.join(root, 'test-cache'));
  assert.equal(
    entries.filter((name) => name.startsWith('.prepared-')).length,
    0
  );
});

test('close crossing prepared-file creation returns no post-close handle', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-create-close-'));
  const openEntered = deferred();
  const continueOpen = deferred();
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      open: async (target, flags, mode) => {
        const handle = await open(target, flags, mode);
        if (String(target).includes('.prepared-')) {
          openEntered.resolve();
          await continueOpen.promise;
        }
        return handle;
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  const creation = cache.createPreparedFile();
  await openEntered.promise;
  const closing = cache.close();
  continueOpen.resolve();
  await assert.rejects(creation, (error: unknown) => {
    assert(error instanceof DiskBackedCacheError);
    return error.code === 'DISK_CACHE_CLOSED';
  });
  await closing;
  const entries = await readdir(path.join(root, 'test-cache'));
  assert.equal(
    entries.filter((name) => name.startsWith('.prepared-')).length,
    0
  );
  await assert.rejects(cache.createPreparedFile(), {
    code: 'DISK_CACHE_CLOSED',
  });
});

test('close invalidates an acquire waiting behind a prepared install', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-acquire-close-'));
  const renameEntered = deferred();
  const continueRename = deferred();
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    renameFile: async (source, destination) => {
      renameEntered.resolve();
      await continueRename.promise;
      await rename(source, destination);
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  const install = cache.installPreparedFile(
    'pending-close',
    await prepared(cache, Buffer.from('data')),
    4
  );
  await renameEntered.promise;
  const acquire = cache.acquireDiskFile('pending-close');
  const closing = cache.close();
  continueRename.resolve();

  assert.equal(await install, false);
  assert.equal(await acquire, undefined);
  await closing;
  assert.deepEqual(
    {
      hits: cache.stats().hits,
      misses: cache.stats().misses,
      diskHits: cache.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );
  assert.equal(cache.stats().diskCount, 0);
});

test('a Windows-style stale destination is replaced exactly once and accounted', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-win-replace-'));
  let renameCalls = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    renameFile: async (source, destination) => {
      renameCalls++;
      if (renameCalls === 1) throw codedError('EPERM');
      await rename(source, destination);
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.whenReady();
  const destination = path.join(root, 'test-cache', fileKey('windows-key'));
  await writeFile(destination, 'orphan', { mode: 0o600 });

  assert.equal(
    await cache.installPreparedFile(
      'windows-key',
      await prepared(cache, Buffer.from('fresh')),
      5
    ),
    true
  );
  assert.equal(renameCalls, 2);
  const lease = await cache.acquireDiskFile('windows-key');
  assert(lease);
  lease.confirmHit();
  assert.equal((await readFile(lease.path)).toString(), 'fresh');
  await lease.release();
  assert.equal(cache.stats().diskBytes, 5);
  assert.equal(cache.stats().diskCount, 1);
});

test('an active lease prevents prepared replacement for the same key', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-owned-dest-'));
  const cache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.installPreparedFile(
    'owned',
    await prepared(cache, Buffer.from('old!')),
    4
  );
  const lease = await cache.acquireDiskFile('owned');
  assert(lease);
  lease.confirmHit();
  assert.equal(
    await cache.installPreparedFile(
      'owned',
      await prepared(cache, Buffer.from('new!')),
      4
    ),
    false
  );
  assert.equal((await readFile(lease.path)).toString(), 'old!');
  await lease.release();
});

test('a failed deferred delete remains retryable by a later flush', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-delete-retry-'));
  const failedAttempt = deferred();
  let failDelete = true;
  const target = path.join(root, 'test-cache', fileKey('retry-delete'));
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === target && failDelete) {
          failDelete = false;
          failedAttempt.resolve();
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.installPreparedFile(
    'retry-delete',
    await prepared(cache, Buffer.from('data')),
    4
  );
  assert.equal(await cache.delete('retry-delete'), true);
  await failedAttempt.promise;
  await cache.flush();
  await assert.rejects(access(target), { code: 'ENOENT' });
  assert.equal(cache.stats().diskBytes, 0);
  assert.equal(cache.stats().diskCount, 0);
});

test('EXDEV copy performs one safe Windows-style destination replacement', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-exdev-replace-'));
  let renameCalls = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 32,
    renameFile: async (source, destination) => {
      renameCalls++;
      if (renameCalls === 1) throw codedError('EXDEV');
      if (renameCalls === 2) throw codedError('EPERM');
      await rename(source, destination);
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.whenReady();
  const destination = path.join(root, 'test-cache', fileKey('cross-replace'));
  await writeFile(destination, 'orphan', { mode: 0o600 });

  assert.equal(
    await cache.installPreparedFile(
      'cross-replace',
      await prepared(cache, Buffer.from('replacement')),
      11
    ),
    true
  );
  assert.equal(renameCalls, 3);
  assert.equal((await readFile(destination)).toString(), 'replacement');
  const entries = await readdir(path.join(root, 'test-cache'));
  assert.equal(
    entries.some((name) => name.startsWith('.install-')),
    false
  );
  assert.equal(
    entries.some((name) => name.startsWith('.prepared-')),
    false
  );
});

test('transient lstat errors preserve entries and do not finalize lookup stats', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-lstat-io-'));
  let failLstat = false;
  const target = path.join(root, 'test-cache', fileKey('lstat-io'));
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        if (String(candidate) === target && failLstat) {
          failLstat = false;
          throw codedError('EIO');
        }
        const handle = await open(candidate, 'r');
        try {
          return await handle.stat();
        } finally {
          await handle.close();
        }
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.installPreparedFile(
    'lstat-io',
    await prepared(cache, Buffer.from('data')),
    4
  );
  failLstat = true;
  await assert.rejects(cache.acquireDiskFile('lstat-io'), { code: 'EIO' });
  assert.deepEqual(
    {
      hits: cache.stats().hits,
      misses: cache.stats().misses,
      diskHits: cache.stats().diskHits,
      diskCount: cache.stats().diskCount,
    },
    { hits: 0, misses: 0, diskHits: 0, diskCount: 1 }
  );
  const lease = await cache.acquireDiskFile('lstat-io');
  assert(lease);
  lease.confirmHit();
  await lease.release();
  assert.equal(cache.stats().diskHits, 1);
});

test('buffering deserialize corruption records one miss with no hit rollback drift', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-deserialize-'));
  const cache = new DiskBackedCache<Buffer>({
    name: 'deserialize-cache',
    dir: root,
    maxMemBytes: 0,
    maxDiskBytes: 16,
    serialize: (value) => Buffer.from(value),
    deserialize: () => {
      throw new Error('corrupt serialized value');
    },
    sizeOf: (value) => value.length,
  });
  context.after(async () => {
    await cache.close();
    await rm(root, { recursive: true, force: true });
  });
  await cache.installPreparedFile(
    'corrupt-buffering',
    await prepared(cache, Buffer.from('data')),
    4
  );

  assert.equal(await cache.getAsync('corrupt-buffering'), undefined);
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

test('load persists a reconciled index after dropping a missing entry', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-reconcile-'));
  const missingKey = fileKey('missing-index-entry');
  await writeFile(
    path.join(root, 'test-cache.index.json'),
    JSON.stringify({ [missingKey]: { size: 4 } })
  );
  const cache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  await cache.flush();
  assert.deepEqual(
    JSON.parse(
      await readFile(path.join(root, 'test-cache.index.json'), 'utf8')
    ),
    {}
  );
  assert.equal(cache.stats().diskCount, 0);
});
