import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import type { Stats } from 'node:fs';
import {
  access,
  lstat,
  mkdir,
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
  DISK_CACHE_DELETE_PARTICIPANT_LIMIT,
  DiskBackedCache,
  DiskBackedCacheError,
  flushAllDiskCaches,
  type DiskBackedCacheFileSystem,
  type DiskFileLease,
  type DiskPreparedFile,
} from './disk-backed-cache.js';

test('global disk cache flush attempts every cache and aggregates failures', async (t) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-flush-all-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  let armed = false;
  let goodIndexWrites = 0;
  let failedIndexWrites = 0;
  const countedWriteFile =
    (kind: 'good' | 'failed'): DiskBackedCacheFileSystem['writeFile'] =>
    async (filePath, data, options) => {
      if (armed && String(filePath).endsWith('.index.json')) {
        if (kind === 'good') goodIndexWrites++;
        else {
          failedIndexWrites++;
          throw Object.assign(new Error('synthetic index failure'), {
            code: 'EIO',
          });
        }
      }
      return writeFile(filePath, data, options);
    };
  const good = createCache(t, root, {
    name: 'flush-good',
    maxDiskBytes: 64,
    fileSystem: { writeFile: countedWriteFile('good') },
  });
  const failed = createCache(t, root, {
    name: 'flush-failed',
    maxDiskBytes: 64,
    fileSystem: { writeFile: countedWriteFile('failed') },
  });
  await Promise.all([good.whenReady(), failed.whenReady()]);
  good.set('good', Buffer.from('good'));
  failed.set('failed', Buffer.from('failed'));
  armed = true;

  await assert.rejects(
    flushAllDiskCaches(),
    (error) =>
      error instanceof AggregateError &&
      error.errors.some(
        (candidate: unknown) =>
          candidate instanceof Error &&
          candidate.message.includes('flush-failed') &&
          candidate.cause instanceof DiskBackedCacheError &&
          candidate.cause.code === 'DISK_CACHE_INDEX_IO'
      )
  );
  assert.equal(goodIndexWrites, 1);
  assert.equal(failedIndexWrites, 1);

  armed = false;
  await Promise.all([good.flush(), failed.flush()]);
});

function deferred<T = void>(): PromiseWithResolvers<T> {
  return Promise.withResolvers<T>();
}

function fileKey(key: string): string {
  return createHash('sha1').update(key).digest('hex');
}

function codedError(code: string, message = code): Error & { code: string } {
  return Object.assign(new Error(message), { code });
}

function reportAsNonRegular(stats: Stats): Stats {
  return new Proxy(stats, {
    get: (target, property, receiver) =>
      property === 'isFile'
        ? () => false
        : Reflect.get(target, property, receiver),
  });
}

function createCache(
  context: TestContext,
  root: string,
  options: {
    readonly maxDiskBytes: number;
    readonly maxMemBytes?: number;
    readonly renameFile?: (
      source: string,
      destination: string
    ) => Promise<void>;
    readonly fileSystem?: Partial<DiskBackedCacheFileSystem>;
    readonly name?: string;
    readonly deserialize?: (value: Buffer) => Buffer;
    readonly beforeParticipantBlockedRetirement?: () => Promise<void>;
  }
): DiskBackedCache<Buffer> {
  const cache = new DiskBackedCache<Buffer>({
    name: options.name ?? 'test-cache',
    dir: root,
    maxMemBytes: options.maxMemBytes ?? 0,
    maxDiskBytes: options.maxDiskBytes,
    serialize: (value) => Buffer.from(value),
    deserialize: options.deserialize ?? ((value) => Buffer.from(value)),
    sizeOf: (value) => value.length,
    renameFile: options.renameFile,
    fileSystem: options.fileSystem,
    beforeParticipantBlockedRetirement:
      options.beforeParticipantBlockedRetirement,
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

async function writePersistentEntry(
  root: string,
  key: string,
  body: Buffer
): Promise<{ readonly dataPath: string; readonly indexPath: string }> {
  const directory = path.join(root, 'test-cache');
  const dataPath = path.join(directory, fileKey(key));
  const indexPath = path.join(root, 'test-cache.index.json');
  await mkdir(directory, { recursive: true });
  await writeFile(dataPath, body, { mode: 0o600 });
  await writeFile(
    indexPath,
    JSON.stringify({ [fileKey(key)]: { size: body.length } })
  );
  return { dataPath, indexPath };
}

function hasParticipantLimitError(error: unknown): boolean {
  if (
    error instanceof DiskBackedCacheError &&
    error.code === 'DISK_CACHE_DELETE_PARTICIPANT_LIMIT'
  ) {
    return true;
  }
  return (
    error instanceof AggregateError &&
    error.errors.some((candidate: unknown) =>
      hasParticipantLimitError(candidate)
    )
  );
}

interface SaturatedDeletePath {
  readonly owner: DiskBackedCache<Buffer>;
  readonly lease: DiskFileLease;
  readonly dataPath: string;
  readonly deleteCalls: () => number;
  readonly participants: readonly DiskBackedCache<Buffer>[];
}

interface SaturatedDeleteOptions {
  readonly failFirstDelete?: boolean;
  readonly beforeParticipantBlockedRetirement?: () => Promise<void>;
}

async function saturateDeleteParticipants(
  context: TestContext,
  root: string,
  key: string,
  maxDiskBytes = 16,
  options: SaturatedDeleteOptions = {}
): Promise<SaturatedDeletePath> {
  const owner = createCache(context, root, {
    maxDiskBytes,
    beforeParticipantBlockedRetirement:
      options.beforeParticipantBlockedRetirement,
  });
  await owner.whenReady();
  assert.equal(
    await owner.installPreparedFile(
      key,
      await prepared(owner, Buffer.from('old!')),
      4
    ),
    true
  );
  await owner.flush();
  const lease = await owner.acquireDiskFile(key);
  assert(lease);
  const dataPath = lease.path;
  await rm(path.join(root, 'test-cache.index.json'), { force: true });

  let deleteCalls = 0;
  const guardedRm: DiskBackedCacheFileSystem['rm'] = async (
    candidate,
    rmOptions
  ) => {
    if (String(candidate) === dataPath) {
      deleteCalls++;
      if (options.failFirstDelete && deleteCalls === 1) {
        throw codedError('EBUSY');
      }
    }
    return rm(candidate, rmOptions);
  };
  const participants: DiskBackedCache<Buffer>[] = [];
  for (let index = 0; index < DISK_CACHE_DELETE_PARTICIPANT_LIMIT; index++) {
    const participant = createCache(context, root, {
      maxDiskBytes,
      fileSystem: { rm: guardedRm },
    });
    participants.push(participant);
    await participant.whenReady();
  }
  assert.equal(deleteCalls, 0);
  return {
    owner,
    lease,
    dataPath,
    deleteCalls: () => deleteCalls,
    participants,
  };
}

async function saturateObservedDeleteParticipants(
  context: TestContext,
  root: string,
  options: {
    readonly name: string;
    readonly dataPath: string;
    readonly maxDiskBytes: number;
  }
): Promise<void> {
  await rm(path.join(root, `${options.name}.index.json`), { force: true });
  for (let index = 0; index < DISK_CACHE_DELETE_PARTICIPANT_LIMIT; index++) {
    const participant = createCache(context, root, {
      name: options.name,
      maxDiskBytes: options.maxDiskBytes,
    });
    await participant.whenReady();
  }
  await access(options.dataPath);
}

async function indexedEntryCount(root: string, name = 'test-cache') {
  const encoded = await readFile(path.join(root, `${name}.index.json`), 'utf8');
  const parsed: unknown = JSON.parse(encoded);
  assert(
    parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed)
  );
  return Object.keys(parsed).length;
}

async function persistentEntryCount(root: string, name = 'test-cache') {
  const entries = await readdir(path.join(root, name));
  return entries.filter((entry) => /^[a-f0-9]{40}$/.test(entry)).length;
}

async function exactBudgetParticipantBlock(
  context: TestContext,
  root: string,
  options: SaturatedDeleteOptions = {}
): Promise<SaturatedDeletePath> {
  const saturated = await saturateDeleteParticipants(
    context,
    root,
    'blocked-old',
    8,
    options
  );
  assert.equal(
    await saturated.owner.installPreparedFile(
      'evicted-middle',
      await prepared(saturated.owner, Buffer.from('mid!')),
      4
    ),
    true
  );
  assert.equal(
    await saturated.owner.installPreparedFile(
      'survivor',
      await prepared(saturated.owner, Buffer.from('keep')),
      4
    ),
    true
  );
  await saturated.owner.flush();
  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
    },
    { diskBytes: 8, diskCount: 2 }
  );
  return saturated;
}

interface LeasedSuccessorPath {
  readonly owner: DiskBackedCache<Buffer>;
  readonly observer: DiskBackedCache<Buffer>;
  readonly lease: DiskFileLease;
  readonly dataPath: string;
}

async function createLeasedSuccessorPath(
  context: TestContext,
  root: string,
  key: string,
  currentDelete: DiskBackedCacheFileSystem['rm']
): Promise<LeasedSuccessorPath> {
  const directory = path.join(root, 'test-cache');
  const dataPath = path.join(directory, fileKey(key));
  await mkdir(directory, { recursive: true });
  let deletePhase = false;
  const owner = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath && deletePhase) {
          return currentDelete(candidate, options);
        }
        return rm(candidate, options);
      },
    },
  });
  await owner.whenReady();
  await writeFile(dataPath, 'old!', { mode: 0o600 });

  const candidateObserved = deferred();
  const continueObservation = deferred();
  let targetLstatCalls = 0;
  const scanner = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        if (String(candidate) === dataPath && ++targetLstatCalls === 1) {
          candidateObserved.resolve();
          await continueObservation.promise;
        }
        return stats;
      },
    },
  });
  await candidateObserved.promise;
  assert.equal(
    await owner.installPreparedFile(
      key,
      await prepared(owner, Buffer.from('new!')),
      4
    ),
    true
  );
  await owner.flush();
  const observer = createCache(context, root, { maxDiskBytes: 16 });
  await observer.whenReady();
  const lease = await owner.acquireDiskFile(key);
  assert(lease);
  continueObservation.resolve();
  await scanner.whenReady();
  deletePhase = true;
  assert.equal(await owner.delete(key), true);
  return { owner, observer, lease, dataPath };
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

test('getAsync crossing clear cannot publish an older disk generation', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-read-clear-'));
  const readEntered = deferred();
  const continueRead = deferred();
  const target = path.join(root, 'test-cache', fileKey('cross-clear'));
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    maxMemBytes: 16,
    fileSystem: {
      readFile: async (candidate, options) => {
        const result = await readFile(candidate, options);
        if (String(candidate) === target) {
          readEntered.resolve();
          await continueRead.promise;
        }
        return result;
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  assert.equal(
    await cache.installPreparedFile(
      'cross-clear',
      await prepared(cache, Buffer.from('data')),
      4
    ),
    true
  );

  const lookup = cache.getAsync('cross-clear');
  await readEntered.promise;
  await cache.clear();
  assert.deepEqual(cache.stats(), {
    memBytes: 0,
    memCount: 0,
    diskBytes: 0,
    diskCount: 0,
    hits: 0,
    misses: 0,
    diskHits: 0,
    hitRate: 0,
  });
  continueRead.resolve();

  assert.equal(await lookup, undefined);
  assert.deepEqual(cache.stats(), {
    memBytes: 0,
    memCount: 0,
    diskBytes: 0,
    diskCount: 0,
    hits: 0,
    misses: 0,
    diskHits: 0,
    hitRate: 0,
  });
  assert.equal(cache.get('cross-clear'), undefined);
});

test('an old getAsync cannot overwrite a same-key value installed after clear', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-read-replace-'));
  const readEntered = deferred();
  const continueRead = deferred();
  const target = path.join(root, 'test-cache', fileKey('same-key'));
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    maxMemBytes: 16,
    fileSystem: {
      readFile: async (candidate, options) => {
        const result = await readFile(candidate, options);
        if (String(candidate) === target) {
          readEntered.resolve();
          await continueRead.promise;
        }
        return result;
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  assert.equal(
    await cache.installPreparedFile(
      'same-key',
      await prepared(cache, Buffer.from('old!')),
      4
    ),
    true
  );

  const oldLookup = cache.getAsync('same-key');
  await readEntered.promise;
  await cache.clear();
  cache.set('same-key', Buffer.from('new!'), { skipDisk: true });
  continueRead.resolve();

  assert.equal(await oldLookup, undefined);
  assert.deepEqual(
    {
      memBytes: cache.stats().memBytes,
      memCount: cache.stats().memCount,
      hits: cache.stats().hits,
      diskHits: cache.stats().diskHits,
      misses: cache.stats().misses,
    },
    { memBytes: 4, memCount: 1, hits: 0, diskHits: 0, misses: 0 }
  );
  assert.deepEqual(cache.get('same-key'), Buffer.from('new!'));
});

test('getAsync crossing close releases without publishing or finalizing stats', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-read-close-'));
  const readEntered = deferred();
  const continueRead = deferred();
  const target = path.join(root, 'test-cache', fileKey('cross-close'));
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    maxMemBytes: 16,
    fileSystem: {
      readFile: async (candidate, options) => {
        const result = await readFile(candidate, options);
        if (String(candidate) === target) {
          readEntered.resolve();
          await continueRead.promise;
        }
        return result;
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  assert.equal(
    await cache.installPreparedFile(
      'cross-close',
      await prepared(cache, Buffer.from('data')),
      4
    ),
    true
  );

  const lookup = cache.getAsync('cross-close');
  await readEntered.promise;
  await cache.close();
  continueRead.resolve();

  assert.equal(await lookup, undefined);
  assert.deepEqual(
    {
      memBytes: cache.stats().memBytes,
      memCount: cache.stats().memCount,
      hits: cache.stats().hits,
      diskHits: cache.stats().diskHits,
      misses: cache.stats().misses,
    },
    { memBytes: 0, memCount: 0, hits: 0, diskHits: 0, misses: 0 }
  );
});

test('ordinary getAsync still promotes one byte-identical hit into L1', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-read-normal-'));
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    maxMemBytes: 16,
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  const body = Buffer.from('data');
  assert.equal(
    await cache.installPreparedFile(
      'normal-buffering',
      await prepared(cache, body),
      body.length
    ),
    true
  );

  assert.deepEqual(await cache.getAsync('normal-buffering'), body);
  assert.deepEqual(
    {
      memBytes: cache.stats().memBytes,
      memCount: cache.stats().memCount,
      hits: cache.stats().hits,
      diskHits: cache.stats().diskHits,
      misses: cache.stats().misses,
    },
    { memBytes: 4, memCount: 1, hits: 1, diskHits: 1, misses: 0 }
  );
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

test('a foreign lease wins atomically and wakes exactly one deferred delete', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-fence-lease-'));
  const { dataPath } = await writePersistentEntry(
    root,
    'foreign-lease',
    Buffer.from('data')
  );
  let deleteCalls = 0;
  const clearingCache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  const readingCache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([clearingCache.whenReady(), readingCache.whenReady()]);

  const lease = await readingCache.acquireDiskFile('foreign-lease');
  assert(lease);
  // Both logical generations may request the same physical cleanup; the
  // process-wide pending slot still wakes one rm after the final lease.
  await Promise.all([clearingCache.clear(), readingCache.clear()]);
  await access(dataPath);
  assert.equal(deleteCalls, 0);

  await lease.release();
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  assert.equal(deleteCalls, 1);
  assert.deepEqual(
    {
      diskBytes: clearingCache.stats().diskBytes,
      diskCount: clearingCache.stats().diskCount,
    },
    { diskBytes: 0, diskCount: 0 }
  );
});

test('a delete claim prevents a cross-instance lease without lookup stats', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-fence-delete-'));
  const { dataPath } = await writePersistentEntry(
    root,
    'claimed-delete',
    Buffer.from('data')
  );
  const deleteEntered = deferred();
  const continueDelete = deferred();
  let deleteCalls = 0;
  const guardedRm: DiskBackedCacheFileSystem['rm'] = async (
    candidate,
    options
  ) => {
    if (String(candidate) === dataPath) {
      deleteCalls++;
      deleteEntered.resolve();
      await continueDelete.promise;
    }
    return rm(candidate, options);
  };
  const clearingCache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: { rm: guardedRm },
  });
  const readingCache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: { rm: guardedRm },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([clearingCache.whenReady(), readingCache.whenReady()]);

  const clearing = clearingCache.clear();
  await deleteEntered.promise;
  assert.equal(await readingCache.acquireDiskFile('claimed-delete'), undefined);
  assert.deepEqual(
    {
      hits: readingCache.stats().hits,
      misses: readingCache.stats().misses,
      diskHits: readingCache.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );
  // A second logical delete joins the already active process claim. Its
  // explicit cleanup barrier must observe that foreign attempt rather than
  // reporting success while the physical removal is still unresolved.
  let joinedClearSettled = false;
  const joinedClear = readingCache.clear().then(() => {
    joinedClearSettled = true;
  });
  await Promise.resolve();
  assert.equal(joinedClearSettled, false);
  assert.equal(deleteCalls, 1);

  continueDelete.resolve();
  await Promise.all([clearing, joinedClear]);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  assert.equal(deleteCalls, 1);
});

test('a joined cleanup waits a failing shared attempt and one controlled retry', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-shared-retry-'));
  const { dataPath } = await writePersistentEntry(
    root,
    'shared-retry',
    Buffer.from('data')
  );
  const deleteEntered = deferred();
  const continueDelete = deferred();
  let deleteCalls = 0;
  const guardedRm: DiskBackedCacheFileSystem['rm'] = async (
    candidate,
    options
  ) => {
    if (String(candidate) !== dataPath) return rm(candidate, options);
    deleteCalls++;
    if (deleteCalls === 1) {
      deleteEntered.resolve();
      await continueDelete.promise;
      throw codedError('EBUSY');
    }
    return rm(candidate, options);
  };
  const first = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: { rm: guardedRm },
  });
  const second = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: { rm: guardedRm },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([first.whenReady(), second.whenReady()]);

  const firstClear = first.clear();
  await deleteEntered.promise;
  let secondSettled = false;
  const secondClear = second.clear().then(() => {
    secondSettled = true;
  });
  await Promise.resolve();
  assert.equal(secondSettled, false);
  assert.equal(deleteCalls, 1);

  continueDelete.resolve();
  await Promise.all([firstClear, secondClear]);
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await Promise.all([first.flush(), second.flush()]);
  assert.equal(deleteCalls, 2);
});

test('a foreign lease rejects prepared replacement before Windows fallback', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-fence-install-'));
  let renameCalls = 0;
  const installingCache = createCache(context, root, {
    maxDiskBytes: 16,
    renameFile: async () => {
      renameCalls++;
      throw codedError('EPERM');
    },
  });
  await installingCache.whenReady();
  const { dataPath } = await writePersistentEntry(
    root,
    'foreign-install',
    Buffer.from('old!')
  );
  const readingCache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await readingCache.whenReady();
  const lease = await readingCache.acquireDiskFile('foreign-install');
  assert(lease);

  assert.equal(
    await installingCache.installPreparedFile(
      'foreign-install',
      await prepared(installingCache, Buffer.from('new!')),
      4
    ),
    false
  );
  assert.equal(renameCalls, 0);
  assert.deepEqual(await readFile(lease.path), Buffer.from('old!'));
  assert.deepEqual(await readFile(dataPath), Buffer.from('old!'));
  assert.equal(
    (await readdir(path.join(root, 'test-cache'))).some((entry) =>
      entry.startsWith('.prepared-')
    ),
    false
  );
  await lease.release();
  await readingCache.clear();
});

test('a Windows replacement claim excludes a lease acquired at the rm boundary', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-fence-win-rm-'));
  const replaceEntered = deferred();
  const continueReplace = deferred();
  let renameCalls = 0;
  const installingCache = createCache(context, root, {
    maxDiskBytes: 16,
    renameFile: async (source, destination) => {
      renameCalls++;
      if (renameCalls === 1) throw codedError('EPERM');
      await rename(source, destination);
    },
    fileSystem: {
      rm: async (candidate, options) => {
        if (
          String(candidate) ===
          path.join(root, 'test-cache', fileKey('windows-race'))
        ) {
          replaceEntered.resolve();
          await continueReplace.promise;
        }
        return rm(candidate, options);
      },
    },
  });
  await installingCache.whenReady();
  const { dataPath } = await writePersistentEntry(
    root,
    'windows-race',
    Buffer.from('old!')
  );
  const readingCache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await readingCache.whenReady();

  const install = installingCache.installPreparedFile(
    'windows-race',
    await prepared(installingCache, Buffer.from('new!')),
    4
  );
  await replaceEntered.promise;
  assert.equal(await readingCache.acquireDiskFile('windows-race'), undefined);
  assert.deepEqual(
    {
      hits: readingCache.stats().hits,
      misses: readingCache.stats().misses,
      diskHits: readingCache.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );

  continueReplace.resolve();
  assert.equal(await install, true);
  assert.equal(renameCalls, 2);
  assert.deepEqual(await readFile(dataPath), Buffer.from('new!'));
  await readingCache.clear();
});

test('startup orphan cleanup defers to an already acquired foreign lease', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-lease-'));
  const { dataPath, indexPath } = await writePersistentEntry(
    root,
    'startup-lease',
    Buffer.from('data')
  );
  const readingCache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await readingCache.whenReady();
  const lease = await readingCache.acquireDiskFile('startup-lease');
  assert(lease);
  await rm(indexPath, { force: true });

  let deleteCalls = 0;
  const recoveringCache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  await recoveringCache.whenReady();
  await access(dataPath);
  assert.equal(deleteCalls, 0);
  assert.equal(recoveringCache.stats().diskCount, 0);

  await lease.release();
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await readingCache.clear();
});

test('startup orphan cleanup claim rejects a simultaneous foreign lease', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-claim-'));
  const { dataPath, indexPath } = await writePersistentEntry(
    root,
    'startup-claim',
    Buffer.from('data')
  );
  const readingCache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await readingCache.whenReady();
  await rm(indexPath, { force: true });
  const deleteEntered = deferred();
  const continueDelete = deferred();
  const recoveringCache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) {
          deleteEntered.resolve();
          await continueDelete.promise;
        }
        return rm(candidate, options);
      },
    },
  });

  await deleteEntered.promise;
  assert.equal(await readingCache.acquireDiskFile('startup-claim'), undefined);
  assert.deepEqual(
    {
      hits: readingCache.stats().hits,
      misses: readingCache.stats().misses,
      diskHits: readingCache.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );
  continueDelete.resolve();
  await recoveringCache.whenReady();
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await readingCache.clear();
});

test('a failed foreign-lease wakeup remains retryable without double accounting', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-fence-retry-'));
  const { dataPath } = await writePersistentEntry(
    root,
    'foreign-retry',
    Buffer.from('data')
  );
  let deleteCalls = 0;
  const clearingCache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath && ++deleteCalls === 1) {
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  const readingCache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([clearingCache.whenReady(), readingCache.whenReady()]);
  const lease = await readingCache.acquireDiskFile('foreign-retry');
  assert(lease);
  await clearingCache.clear();

  await assert.rejects(lease.release(), { code: 'EBUSY' });
  await access(dataPath);
  assert.equal(deleteCalls, 1);
  await clearingCache.flush();
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  assert.deepEqual(
    {
      diskBytes: clearingCache.stats().diskBytes,
      diskCount: clearingCache.stats().diskCount,
    },
    { diskBytes: 0, diskCount: 0 }
  );
  await readingCache.clear();
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

test('stale ENOENT invalidation fences a pre-admitted background replace before lease release', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-stale-enoent-'));
  const key = 'stale-enoent';
  const dataPath = path.join(root, 'test-cache', fileKey(key));
  const backgroundWriteEntered = deferred();
  const continueBackgroundWrite = deferred();
  let pauseBackgroundWrite = true;
  const writer = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      writeFile: async (candidate, data, options) => {
        await writeFile(candidate, data, options);
        if (
          pauseBackgroundWrite &&
          path.basename(String(candidate)).startsWith('.write-')
        ) {
          pauseBackgroundWrite = false;
          backgroundWriteEntered.resolve();
          await continueBackgroundWrite.promise;
        }
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await writer.whenReady();
  await writePersistentEntry(root, key, Buffer.from('old!'));

  const staleLstatEntered = deferred();
  const continueStaleLstat = deferred();
  let reportMissing = false;
  const stale = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        if (reportMissing && String(candidate) === dataPath) {
          staleLstatEntered.resolve();
          await continueStaleLstat.promise;
          queueMicrotask(() => {
            queueMicrotask(() => continueBackgroundWrite.resolve());
          });
          throw codedError('ENOENT');
        }
        return lstat(candidate);
      },
    },
  });
  await stale.whenReady();
  await rm(dataPath, { force: true });
  reportMissing = true;

  const lookup = stale.acquireDiskFile(key);
  await staleLstatEntered.promise;
  writer.set(key, Buffer.from('lost'));
  await backgroundWriteEntered.promise;
  continueStaleLstat.resolve();

  assert.equal(await lookup, undefined);
  await writer.flush();
  assert.deepEqual(
    {
      staleMisses: stale.stats().misses,
      writerDiskBytes: writer.stats().diskBytes,
      writerDiskCount: writer.stats().diskCount,
    },
    { staleMisses: 1, writerDiskBytes: 0, writerDiskCount: 0 }
  );
  await assert.rejects(access(dataPath), { code: 'ENOENT' });

  writer.set(key, Buffer.from('new!'));
  await writer.flush();
  const fresh = await writer.acquireDiskFile(key);
  assert(fresh);
  assert.equal((await readFile(fresh.path)).toString(), 'new!');
  await fresh.release();
  await stale.close();
  await writer.close();
});

test('stale ENOENT invalidation rejects a cross-instance prepared install in the release window', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-stale-prepared-'));
  const key = 'stale-prepared-window';
  const dataPath = path.join(root, 'test-cache', fileKey(key));
  const preparedLstatEntered = deferred();
  const continuePreparedLstat = deferred();
  let pausedPreparedPath: string | undefined;
  let pausePreparedLstat = true;
  const writer = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        if (pausePreparedLstat && String(candidate) === pausedPreparedPath) {
          pausePreparedLstat = false;
          preparedLstatEntered.resolve();
          await continuePreparedLstat.promise;
        }
        return stats;
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await writer.whenReady();
  await writePersistentEntry(root, key, Buffer.from('old!'));

  const staleLstatEntered = deferred();
  const continueStaleLstat = deferred();
  let reportMissing = false;
  const stale = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        if (reportMissing && String(candidate) === dataPath) {
          staleLstatEntered.resolve();
          await continueStaleLstat.promise;
          queueMicrotask(() => {
            queueMicrotask(() => continuePreparedLstat.resolve());
          });
          throw codedError('ENOENT');
        }
        return lstat(candidate);
      },
    },
  });
  await stale.whenReady();
  const staged = await prepared(writer, Buffer.from('lost'));
  pausedPreparedPath = staged.path;
  await rm(dataPath, { force: true });
  reportMissing = true;

  const lookup = stale.acquireDiskFile(key);
  await staleLstatEntered.promise;
  const install = writer.installPreparedFile(key, staged, 4);
  await preparedLstatEntered.promise;
  continueStaleLstat.resolve();

  assert.equal(await lookup, undefined);
  assert.equal(await install, false);
  assert.equal(writer.stats().diskCount, 0);
  assert.equal(
    await writer.installPreparedFile(
      key,
      await prepared(writer, Buffer.from('new!')),
      4
    ),
    true
  );
  const fresh = await writer.acquireDiskFile(key);
  assert(fresh);
  assert.equal((await readFile(fresh.path)).toString(), 'new!');
  await fresh.release();
  await stale.close();
  await writer.close();
});

test('unsafe lookup invalidation fences a pre-admitted replacement before lease release', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-stale-unsafe-'));
  const key = 'stale-unsafe';
  const dataPath = path.join(root, 'test-cache', fileKey(key));
  const backgroundWriteEntered = deferred();
  const continueBackgroundWrite = deferred();
  let pauseBackgroundWrite = true;
  const writer = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      writeFile: async (candidate, data, options) => {
        await writeFile(candidate, data, options);
        if (
          pauseBackgroundWrite &&
          path.basename(String(candidate)).startsWith('.write-')
        ) {
          pauseBackgroundWrite = false;
          backgroundWriteEntered.resolve();
          await continueBackgroundWrite.promise;
        }
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await writer.whenReady();
  await writePersistentEntry(root, key, Buffer.from('old!'));

  const staleLstatEntered = deferred();
  const continueStaleLstat = deferred();
  let reportUnsafe = false;
  const stale = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        if (reportUnsafe && String(candidate) === dataPath) {
          staleLstatEntered.resolve();
          await continueStaleLstat.promise;
          queueMicrotask(() => {
            queueMicrotask(() => continueBackgroundWrite.resolve());
          });
          return reportAsNonRegular(stats);
        }
        return stats;
      },
    },
  });
  await stale.whenReady();
  reportUnsafe = true;

  const lookup = stale.acquireDiskFile(key);
  await staleLstatEntered.promise;
  writer.set(key, Buffer.from('lost'));
  await backgroundWriteEntered.promise;
  continueStaleLstat.resolve();

  assert.equal(await lookup, undefined);
  await writer.flush();
  assert.equal(writer.stats().diskCount, 0);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  writer.set(key, Buffer.from('safe'));
  await writer.flush();
  const fresh = await writer.acquireDiskFile(key);
  assert(fresh);
  assert.equal((await readFile(fresh.path)).toString(), 'safe');
  await fresh.release();
  await stale.close();
  await writer.close();
});

test('stale lookup cleanup failure keeps its intent fenced until explicit retry', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-stale-retry-'));
  const key = 'stale-cleanup-retry';
  const dataPath = path.join(root, 'test-cache', fileKey(key));
  const writer = createCache(context, root, { maxDiskBytes: 32 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await writer.whenReady();
  await writePersistentEntry(root, key, Buffer.from('old!'));

  let reportUnsafe = false;
  let deleteCalls = 0;
  const stale = createCache(context, root, {
    maxDiskBytes: 32,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        return reportUnsafe && String(candidate) === dataPath
          ? reportAsNonRegular(stats)
          : stats;
      },
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath && ++deleteCalls === 1) {
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  await stale.whenReady();
  reportUnsafe = true;

  assert.equal(await stale.acquireDiskFile(key), undefined);
  assert.equal(stale.stats().misses, 1);
  assert.equal(deleteCalls, 1);
  await access(dataPath);
  assert.equal(
    await writer.installPreparedFile(
      key,
      await prepared(writer, Buffer.from('blocked')),
      7
    ),
    false
  );

  await stale.flush();
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  assert.equal(
    await writer.installPreparedFile(
      key,
      await prepared(writer, Buffer.from('fresh')),
      5
    ),
    true
  );
  const fresh = await writer.acquireDiskFile(key);
  assert(fresh);
  assert.equal((await readFile(fresh.path)).toString(), 'fresh');
  await fresh.release();
  await stale.close();
  await writer.close();
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

test('close drains a previously admitted background write into the restart index', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-close-write-'));
  const renamed = deferred();
  const continueRename = deferred();
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    renameFile: async (source, destination) => {
      await rename(source, destination);
      if (path.basename(source).startsWith('.write-')) {
        renamed.resolve();
        await continueRename.promise;
      }
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  cache.set('admitted-before-close', Buffer.from('new!'));
  await renamed.promise;
  let closeSettled = false;
  const closing = cache.close().finally(() => {
    closeSettled = true;
  });
  await Promise.resolve();
  assert.equal(closeSettled, false);
  continueRename.resolve();
  await closing;

  const restarted = createCache(context, root, { maxDiskBytes: 16 });
  await restarted.whenReady();
  assert.deepEqual(
    await restarted.getAsync('admitted-before-close'),
    Buffer.from('new!')
  );
  await restarted.close();
});

test('close preserves an admitted rewrite after its destination rename', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-close-rewrite-'));
  const renamed = deferred();
  const continueRename = deferred();
  let pauseRewrite = false;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    renameFile: async (source, destination) => {
      await rename(source, destination);
      if (pauseRewrite && path.basename(source).startsWith('.write-')) {
        renamed.resolve();
        await continueRename.promise;
      }
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  cache.set('rewrite-before-close', Buffer.from('old!'));
  await cache.flush();

  pauseRewrite = true;
  cache.set('rewrite-before-close', Buffer.from('new!'));
  await renamed.promise;
  const closing = cache.close();
  continueRename.resolve();
  await closing;
  assert.equal(cache.stats().diskCount, 1);
  assert.equal(cache.stats().diskBytes, 4);

  const restarted = createCache(context, root, { maxDiskBytes: 16 });
  await restarted.whenReady();
  assert.deepEqual(
    await restarted.getAsync('rewrite-before-close'),
    Buffer.from('new!')
  );
  await restarted.close();
});

test('close propagates a typed final index durability failure', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-close-index-'));
  const cache = new DiskBackedCache<Buffer>({
    name: 'test-cache',
    dir: root,
    maxMemBytes: 0,
    maxDiskBytes: 16,
    serialize: (value) => Buffer.from(value),
    deserialize: (value) => Buffer.from(value),
    sizeOf: (value) => value.length,
    fileSystem: {
      writeFile: async (target, data, options) => {
        if (String(target).endsWith('.index.json')) throw codedError('EIO');
        return writeFile(target, data, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  cache.set('index-failure', Buffer.from('data'));

  await assert.rejects(cache.close(), (error: unknown) => {
    assert(error instanceof DiskBackedCacheError);
    assert.equal(error.code, 'DISK_CACHE_INDEX_IO');
    assert(error.cause instanceof Error);
    assert('code' in error.cause);
    assert.equal(error.cause.code, 'EIO');
    return true;
  });
});

test('explicit flush propagates and can retry a typed index durability failure', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-flush-index-'));
  let failIndexWrite = true;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      writeFile: async (target, data, options) => {
        if (failIndexWrite && String(target).endsWith('.index.json')) {
          throw codedError('EIO');
        }
        return writeFile(target, data, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  cache.set('flush-index-failure', Buffer.from('data'));

  await assert.rejects(cache.flush(), (error: unknown) => {
    assert(error instanceof DiskBackedCacheError);
    return error.code === 'DISK_CACHE_INDEX_IO';
  });
  failIndexWrite = false;
  await cache.flush();
  await cache.close();

  const restarted = createCache(context, root, { maxDiskBytes: 16 });
  await restarted.whenReady();
  assert.deepEqual(
    await restarted.getAsync('flush-index-failure'),
    Buffer.from('data')
  );
  await restarted.close();
});

test('close retries a current snapshot after an older index flush fails', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-close-retry-'));
  const oldFlushEntered = deferred();
  const continueOldFlush = deferred();
  let indexWrites = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      writeFile: async (target, data, options) => {
        if (String(target).endsWith('.index.json')) {
          indexWrites++;
          if (indexWrites === 1) {
            oldFlushEntered.resolve();
            await continueOldFlush.promise;
            throw codedError('EIO');
          }
        }
        return writeFile(target, data, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.installPreparedFile(
    'retry-current-snapshot',
    await prepared(cache, Buffer.from('data')),
    4
  );

  const oldFlushResult = cache.flushIndex().then(
    () => undefined,
    (error: unknown) => error
  );
  await oldFlushEntered.promise;
  const closing = cache.close();
  continueOldFlush.resolve();
  const oldError = await oldFlushResult;
  assert(oldError instanceof DiskBackedCacheError);
  assert.equal(oldError.code, 'DISK_CACHE_INDEX_IO');
  await closing;
  assert.equal(indexWrites, 2);

  const restarted = createCache(context, root, { maxDiskBytes: 16 });
  await restarted.whenReady();
  assert.deepEqual(
    await restarted.getAsync('retry-current-snapshot'),
    Buffer.from('data')
  );
  await restarted.close();
});

test('transient startup index-read failure preserves the complete disk namespace', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-load-index-'));
  const body = Buffer.from('safe');
  const { dataPath, indexPath } = await writePersistentEntry(
    root,
    'startup-index',
    body
  );
  const originalIndex = await readFile(indexPath, 'utf8');
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      readFile: async () => {
        throw codedError('EIO');
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  cache.set('must-not-write', Buffer.from('nope'));
  await cache.flush();
  assert.equal(await cache.getAsync('startup-index'), undefined);
  await access(dataPath);
  assert.equal(await readFile(indexPath, 'utf8'), originalIndex);
  assert.equal(cache.stats().diskCount, 0);
  assert.equal(cache.stats().misses, 0);

  const restarted = createCache(context, root, { maxDiskBytes: 16 });
  await restarted.whenReady();
  assert.deepEqual(await restarted.getAsync('startup-index'), body);
  await restarted.close();
});

test('transient startup readdir failure preserves the complete disk namespace', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-load-readdir-'));
  const body = Buffer.from('safe');
  const { dataPath, indexPath } = await writePersistentEntry(
    root,
    'startup-readdir',
    body
  );
  const originalIndex = await readFile(indexPath, 'utf8');
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      readdir: async () => {
        throw codedError('EIO');
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  await cache.flush();
  await access(dataPath);
  assert.equal(await readFile(indexPath, 'utf8'), originalIndex);
  assert.equal(cache.stats().diskCount, 0);

  const restarted = createCache(context, root, { maxDiskBytes: 16 });
  await restarted.whenReady();
  assert.deepEqual(await restarted.getAsync('startup-readdir'), body);
  await restarted.close();
});

test('transient startup entry-lstat failure preserves the complete disk namespace', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-load-lstat-'));
  const body = Buffer.from('safe');
  const { dataPath, indexPath } = await writePersistentEntry(
    root,
    'startup-lstat',
    body
  );
  const originalIndex = await readFile(indexPath, 'utf8');
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async () => {
        throw codedError('EIO');
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  await cache.flush();
  await access(dataPath);
  assert.equal(await readFile(indexPath, 'utf8'), originalIndex);
  assert.equal(cache.stats().diskCount, 0);

  const restarted = createCache(context, root, { maxDiskBytes: 16 });
  await restarted.whenReady();
  assert.deepEqual(await restarted.getAsync('startup-lstat'), body);
  await restarted.close();
});

test('startup reconciliation discards only structurally invalid index entries', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-load-invalid-'));
  const body = Buffer.from('safe');
  const { indexPath } = await writePersistentEntry(
    root,
    'valid-index-entry',
    body
  );
  const validKey = fileKey('valid-index-entry');
  await writeFile(
    indexPath,
    JSON.stringify({
      [validKey]: { size: body.length },
      'not-a-safe-file-key': { size: 4 },
    })
  );
  const cache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  await cache.flush();
  assert.deepEqual(JSON.parse(await readFile(indexPath, 'utf8')), {
    [validKey]: { size: body.length },
  });
  assert.deepEqual(await cache.getAsync('valid-index-entry'), body);
});

test('startup orphan fingerprint supersedes a candidate replaced before intent publication', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-aba-'));
  const key = 'orphan-incarnation';
  const writer = createCache(context, root, { maxDiskBytes: 32 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await writer.whenReady();
  const { dataPath } = await writePersistentEntry(
    root,
    key,
    Buffer.from('old!')
  );

  let replaced = false;
  let deleteCalls = 0;
  const scanner = createCache(context, root, {
    maxDiskBytes: 32,
    fileSystem: {
      lstat: async (candidate) => {
        const observed = await lstat(candidate);
        if (!replaced && String(candidate) === dataPath) {
          replaced = true;
          writer.set(key, Buffer.from('new-content'));
          await writer.flush();
          // This scan observed an unsafe old entry. The process-local writer
          // replaced it before the scanner can publish its delete intent.
          return reportAsNonRegular(observed);
        }
        return observed;
      },
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });

  await scanner.whenReady();
  assert.equal(replaced, true);
  assert.equal(deleteCalls, 0);
  assert.equal((await readFile(dataPath)).toString(), 'new-content');
  assert.deepEqual(
    {
      scannerDiskCount: scanner.stats().diskCount,
      writerDiskBytes: writer.stats().diskBytes,
      writerDiskCount: writer.stats().diskCount,
    },
    { scannerDiskCount: 0, writerDiskBytes: 11, writerDiskCount: 1 }
  );

  await scanner.flush();
  await scanner.close();
  assert.equal(deleteCalls, 0);
  assert.equal((await readFile(dataPath)).toString(), 'new-content');
  const fresh = await writer.acquireDiskFile(key);
  assert(fresh);
  assert.equal((await readFile(fresh.path)).toString(), 'new-content');
  await fresh.release();
  await writer.close();
});

test('a superseded startup candidate cannot satisfy a later owner clear', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-target-clear-'));
  const directory = path.join(root, 'test-cache');
  const key = 'target-clear';
  const dataPath = path.join(directory, fileKey(key));
  await mkdir(directory, { recursive: true });
  const owner = createCache(context, root, { maxDiskBytes: 32 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await owner.whenReady();
  await writeFile(dataPath, 'OLD', { mode: 0o600 });

  const candidateObserved = deferred();
  const continueObservation = deferred();
  const revalidationEntered = deferred();
  const continueRevalidation = deferred();
  let targetLstatCalls = 0;
  const scanner = createCache(context, root, {
    maxDiskBytes: 32,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        if (String(candidate) !== dataPath) return stats;
        targetLstatCalls++;
        if (targetLstatCalls === 1) {
          candidateObserved.resolve();
          await continueObservation.promise;
        } else if (targetLstatCalls === 2) {
          revalidationEntered.resolve();
          await continueRevalidation.promise;
        }
        return stats;
      },
    },
  });

  await candidateObserved.promise;
  assert.equal(
    await owner.installPreparedFile(
      key,
      await prepared(owner, Buffer.from('NEW!')),
      4
    ),
    true
  );
  continueObservation.resolve();
  await revalidationEntered.promise;

  let clearSettled = false;
  const clearing = owner.clear().then(() => {
    clearSettled = true;
  });
  // clearOnce crosses only already-resolved lifecycle promises before dropping
  // the indexed entry and registering its current-incarnation successor.
  for (let turn = 0; turn < 8; turn++) await Promise.resolve();
  assert.equal(owner.stats().diskCount, 0);
  assert.equal(clearSettled, false);
  assert.equal((await readFile(dataPath)).toString(), 'NEW!');

  continueRevalidation.resolve();
  await Promise.all([scanner.whenReady(), clearing]);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  assert.equal(owner.stats().diskCount, 0);
  await Promise.all([owner.flush(), scanner.flush()]);
});

test('a superseded startup candidate cannot satisfy a later explicit delete', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-target-delete-'));
  const directory = path.join(root, 'test-cache');
  const key = 'target-delete';
  const dataPath = path.join(directory, fileKey(key));
  await mkdir(directory, { recursive: true });
  const owner = createCache(context, root, { maxDiskBytes: 32 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await owner.whenReady();
  await writeFile(dataPath, 'OLD', { mode: 0o600 });

  const candidateObserved = deferred();
  const continueObservation = deferred();
  const revalidationEntered = deferred();
  const continueRevalidation = deferred();
  let targetLstatCalls = 0;
  const scanner = createCache(context, root, {
    maxDiskBytes: 32,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        if (String(candidate) !== dataPath) return stats;
        targetLstatCalls++;
        if (targetLstatCalls === 1) {
          candidateObserved.resolve();
          await continueObservation.promise;
        } else if (targetLstatCalls === 2) {
          revalidationEntered.resolve();
          await continueRevalidation.promise;
        }
        return stats;
      },
    },
  });

  await candidateObserved.promise;
  assert.equal(
    await owner.installPreparedFile(
      key,
      await prepared(owner, Buffer.from('NEW!')),
      4
    ),
    true
  );
  continueObservation.resolve();
  await revalidationEntered.promise;

  assert.equal(await owner.delete(key), true);
  assert.equal(owner.stats().diskCount, 0);
  assert.equal((await readFile(dataPath)).toString(), 'NEW!');
  continueRevalidation.resolve();
  await scanner.whenReady();
  await owner.flush();
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await Promise.all([owner.flush(), scanner.flush()]);
});

test('different startup fingerprints use a fenced successor without a mutation gap', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-target-pair-'));
  const directory = path.join(root, 'test-cache');
  const key = 'target-pair';
  const dataPath = path.join(directory, fileKey(key));
  await mkdir(directory, { recursive: true });
  const writer = createCache(context, root, { maxDiskBytes: 32 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await writer.whenReady();
  await writeFile(dataPath, 'OLD', { mode: 0o600 });

  const firstObserved = deferred();
  const continueFirstObservation = deferred();
  const firstRevalidation = deferred();
  const continueFirstRevalidation = deferred();
  let firstLstatCalls = 0;
  const first = createCache(context, root, {
    maxDiskBytes: 32,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        if (String(candidate) !== dataPath) return stats;
        firstLstatCalls++;
        if (firstLstatCalls === 1) {
          firstObserved.resolve();
          await continueFirstObservation.promise;
        } else if (firstLstatCalls === 2) {
          firstRevalidation.resolve();
          await continueFirstRevalidation.promise;
        }
        return stats;
      },
    },
  });

  await firstObserved.promise;
  await rm(dataPath, { force: true });
  await writeFile(dataPath, 'NEWER', { mode: 0o600 });
  continueFirstObservation.resolve();
  await firstRevalidation.promise;

  const secondRevalidation = deferred();
  const continueSecondRevalidation = deferred();
  let secondLstatCalls = 0;
  let deleteCalls = 0;
  const second = createCache(context, root, {
    maxDiskBytes: 32,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        if (String(candidate) === dataPath && ++secondLstatCalls === 2) {
          secondRevalidation.resolve();
          await continueSecondRevalidation.promise;
        }
        return stats;
      },
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  await second.whenReady();

  continueFirstRevalidation.resolve();
  await secondRevalidation.promise;
  assert.equal(
    await writer.installPreparedFile(
      key,
      await prepared(writer, Buffer.from('BLOCKED')),
      7
    ),
    false
  );
  assert.equal((await readFile(dataPath)).toString(), 'NEWER');
  continueSecondRevalidation.resolve();

  await first.whenReady();
  await second.flush();
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await Promise.all([first.flush(), second.flush(), writer.flush()]);
  assert.equal(deleteCalls, 1);
});

test('unchanged startup orphan is removed exactly once after fingerprint revalidation', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-stable-'));
  const directory = path.join(root, 'test-cache');
  const dataPath = path.join(directory, fileKey('stable-orphan'));
  await mkdir(directory, { recursive: true });
  await writeFile(dataPath, 'orphan', { mode: 0o600 });
  let deleteCalls = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await cache.flush();
  await cache.close();
  assert.equal(deleteCalls, 1);
});

test('startup orphan disappearance completes as absent without a stale rm', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-absent-'));
  const directory = path.join(root, 'test-cache');
  const key = 'absent-orphan';
  const dataPath = path.join(directory, fileKey(key));
  await mkdir(directory, { recursive: true });
  await writeFile(dataPath, 'orphan', { mode: 0o600 });
  let removedAfterObservation = false;
  let deleteCalls = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        const observed = await lstat(candidate);
        if (!removedAfterObservation && String(candidate) === dataPath) {
          removedAfterObservation = true;
          await rm(dataPath, { force: true });
        }
        return observed;
      },
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  assert.equal(removedAfterObservation, true);
  assert.equal(deleteCalls, 0);
  await cache.flush();
  assert.equal(deleteCalls, 0);
  assert.equal(
    await cache.installPreparedFile(
      key,
      await prepared(cache, Buffer.from('new!')),
      4
    ),
    true
  );
  const fresh = await cache.acquireDiskFile(key);
  assert(fresh);
  assert.equal((await readFile(fresh.path)).toString(), 'new!');
  await fresh.release();
  await cache.close();
});

test('startup fingerprint revalidation error remains fenced until explicit retry', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-recheck-'));
  const directory = path.join(root, 'test-cache');
  const key = 'recheck-orphan';
  const dataPath = path.join(directory, fileKey(key));
  await mkdir(directory, { recursive: true });
  await writeFile(dataPath, 'orphan', { mode: 0o600 });
  let targetLstatCalls = 0;
  let deleteCalls = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        if (String(candidate) === dataPath && ++targetLstatCalls === 2) {
          throw codedError('EIO');
        }
        return lstat(candidate);
      },
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  assert.equal(targetLstatCalls, 2);
  assert.equal(deleteCalls, 0);
  await access(dataPath);
  assert.equal(
    await cache.installPreparedFile(
      key,
      await prepared(cache, Buffer.from('nope')),
      4
    ),
    false
  );

  await cache.flush();
  assert.equal(targetLstatCalls, 3);
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  assert.equal(
    await cache.installPreparedFile(
      key,
      await prepared(cache, Buffer.from('new!')),
      4
    ),
    true
  );
  await cache.close();
});

test('same-instance lease plus clear publishes delete intent before final release', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-own-clear-'));
  const { dataPath } = await writePersistentEntry(
    root,
    'own-clear',
    Buffer.from('data')
  );
  let deleteCalls = 0;
  const owner = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  const observer = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([owner.whenReady(), observer.whenReady()]);

  const lease = await owner.acquireDiskFile('own-clear');
  assert(lease);
  await owner.clear();
  assert.equal(deleteCalls, 0);
  await access(dataPath);

  assert.equal(await observer.acquireDiskFile('own-clear'), undefined);
  assert.deepEqual(
    {
      hits: observer.stats().hits,
      misses: observer.stats().misses,
      diskHits: observer.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );
  await lease.release();
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await observer.clear();
});

test('same-instance lease plus explicit delete blocks every later process lease', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-own-delete-'));
  const { dataPath } = await writePersistentEntry(
    root,
    'own-delete',
    Buffer.from('data')
  );
  let deleteCalls = 0;
  const owner = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  const observer = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([owner.whenReady(), observer.whenReady()]);

  const lease = await owner.acquireDiskFile('own-delete');
  assert(lease);
  assert.equal(await owner.delete('own-delete'), true);
  assert.equal(await observer.acquireDiskFile('own-delete'), undefined);
  assert.equal(deleteCalls, 0);
  await access(dataPath);

  await lease.release();
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await observer.clear();
});

test('LRU eviction with an owner lease fences foreign acquisition immediately', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-own-lru-'));
  const { dataPath } = await writePersistentEntry(
    root,
    'lru-old',
    Buffer.from('old!')
  );
  let deleteCalls = 0;
  const owner = createCache(context, root, {
    maxDiskBytes: 4,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  const observer = createCache(context, root, { maxDiskBytes: 4 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([owner.whenReady(), observer.whenReady()]);

  const lease = await owner.acquireDiskFile('lru-old');
  assert(lease);
  assert.equal(
    await owner.installPreparedFile(
      'lru-new',
      await prepared(owner, Buffer.from('new!')),
      4
    ),
    true
  );
  assert.equal(await observer.acquireDiskFile('lru-old'), undefined);
  assert.equal(deleteCalls, 0);
  assert.equal((await readFile(lease.path)).toString(), 'old!');

  await lease.release();
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await observer.clear();
});

test('process delete waits for every old lease while rejecting a third', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-many-old-'));
  const { dataPath } = await writePersistentEntry(
    root,
    'many-old',
    Buffer.from('data')
  );
  let deleteCalls = 0;
  const owner = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  const reader = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([owner.whenReady(), reader.whenReady()]);

  const first = await owner.acquireDiskFile('many-old');
  const second = await reader.acquireDiskFile('many-old');
  assert(first);
  assert(second);
  await owner.clear();
  assert.equal(await reader.acquireDiskFile('many-old'), undefined);
  assert.deepEqual(
    {
      hits: reader.stats().hits,
      misses: reader.stats().misses,
      diskHits: reader.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );

  await first.release();
  assert.equal(deleteCalls, 0);
  await access(dataPath);
  await second.release();
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await reader.clear();
});

test('failed indexed delete fences leases and writes until one shared retry completes', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-intent-retry-'));
  // This writer intentionally loads the empty incarnation before the old
  // indexed file appears, so its writes exercise the process fence rather than
  // a stale local index hit.
  const writer = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await writer.whenReady();
  const { dataPath } = await writePersistentEntry(
    root,
    'intent-retry',
    Buffer.from('old!')
  );
  let deleteCalls = 0;
  const owner = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath && ++deleteCalls === 1) {
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  const observer = createCache(context, root, { maxDiskBytes: 16 });
  await Promise.all([owner.whenReady(), observer.whenReady()]);

  const oldLease = await owner.acquireDiskFile('intent-retry');
  assert(oldLease);
  assert.equal(await owner.delete('intent-retry'), true);
  await assert.rejects(oldLease.release(), { code: 'EBUSY' });
  assert.equal(deleteCalls, 1);
  assert.equal((await readFile(dataPath)).toString(), 'old!');

  assert.equal(await observer.acquireDiskFile('intent-retry'), undefined);
  assert.deepEqual(
    {
      hits: observer.stats().hits,
      misses: observer.stats().misses,
      diskHits: observer.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );
  assert.equal(
    await writer.installPreparedFile(
      'intent-retry',
      await prepared(writer, Buffer.from('blocked-prepared')),
      16
    ),
    false
  );
  writer.set('intent-retry', Buffer.from('blocked-write'));
  await writer.flush();
  assert.equal(writer.stats().diskCount, 0);
  assert.equal((await readFile(dataPath)).toString(), 'old!');

  await owner.flush();
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  assert.equal(
    await writer.installPreparedFile(
      'intent-retry',
      await prepared(writer, Buffer.from('new!')),
      4
    ),
    true
  );

  // Every old participant has observed final completion. Repeated cleanup
  // cannot turn the resolved old token into a delete for the new incarnation.
  await owner.flush();
  await observer.flush();
  await owner.close();
  await observer.close();
  assert.equal(deleteCalls, 2);
  const newLease = await writer.acquireDiskFile('intent-retry');
  assert(newLease);
  assert.equal((await readFile(newLease.path)).toString(), 'new!');
  await newLease.release();
  assert.deepEqual(
    {
      diskBytes: writer.stats().diskBytes,
      diskCount: writer.stats().diskCount,
    },
    { diskBytes: 4, diskCount: 1 }
  );
  await writer.close();
});

test('startup orphan cleanup survives EBUSY and succeeds on flush', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-retry-'));
  const directory = path.join(root, 'test-cache');
  const dataPath = path.join(directory, fileKey('orphan-retry'));
  await mkdir(directory, { recursive: true });
  await writeFile(dataPath, 'orphan', { mode: 0o600 });
  let deleteCalls = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath && ++deleteCalls === 1) {
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  assert.equal(deleteCalls, 1);
  await access(dataPath);
  await cache.flush();
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
});

test('startup orphan wake failure remains fenced and retryable after foreign release', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-wake-'));
  const { dataPath, indexPath } = await writePersistentEntry(
    root,
    'orphan-wake',
    Buffer.from('data')
  );
  const reader = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await reader.whenReady();
  const lease = await reader.acquireDiskFile('orphan-wake');
  assert(lease);
  await rm(indexPath, { force: true });

  let deleteCalls = 0;
  const recovering = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath && ++deleteCalls === 1) {
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  await recovering.whenReady();
  assert.equal(deleteCalls, 0);
  assert.equal(await reader.acquireDiskFile('orphan-wake'), undefined);
  assert.deepEqual(
    {
      hits: reader.stats().hits,
      misses: reader.stats().misses,
      diskHits: reader.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );

  await assert.rejects(lease.release(), { code: 'EBUSY' });
  assert.equal(deleteCalls, 1);
  await access(dataPath);
  assert.equal(await reader.acquireDiskFile('orphan-wake'), undefined);
  assert.deepEqual(
    {
      hits: reader.stats().hits,
      misses: reader.stats().misses,
      diskHits: reader.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );
  await recovering.flush();
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await reader.clear();
});

test('a new cache retries an owner-closed intent after the final lease fails', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-owner-close-'));
  const { dataPath } = await writePersistentEntry(
    root,
    'owner-close',
    Buffer.from('old!')
  );
  let ownerDeleteCalls = 0;
  const owner = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) {
          ownerDeleteCalls++;
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  const reader = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([owner.whenReady(), reader.whenReady()]);
  const lease = await reader.acquireDiskFile('owner-close');
  assert(lease);

  await owner.clear();
  await owner.close();
  assert.equal(ownerDeleteCalls, 0);
  await assert.rejects(lease.release(), { code: 'EBUSY' });
  assert.equal(ownerDeleteCalls, 1);
  await access(dataPath);
  assert.equal(await reader.acquireDiskFile('owner-close'), undefined);
  assert.deepEqual(
    {
      hits: reader.stats().hits,
      misses: reader.stats().misses,
      diskHits: reader.stats().diskHits,
    },
    { hits: 0, misses: 0, diskHits: 0 }
  );

  // With the index removed by clear(), startup discovers the same physical
  // orphan and explicitly retries the still-unresolved shared intent.
  let recoveryDeleteCalls = 0;
  const recovering = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) recoveryDeleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  await recovering.whenReady();
  assert.equal(recoveryDeleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  assert.equal(
    await recovering.installPreparedFile(
      'owner-close',
      await prepared(recovering, Buffer.from('new!')),
      4
    ),
    true
  );

  await owner.close();
  await owner.flush();
  await reader.flush();
  await reader.close();
  assert.equal(ownerDeleteCalls, 1);
  assert.equal(recoveryDeleteCalls, 1);
  const newLease = await recovering.acquireDiskFile('owner-close');
  assert(newLease);
  assert.equal((await readFile(newLease.path)).toString(), 'new!');
  await newLease.release();
  await recovering.close();
});

test('close reports a persistently failing startup orphan delete', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-close-'));
  const directory = path.join(root, 'test-cache');
  const dataPath = path.join(directory, fileKey('orphan-close'));
  await mkdir(directory, { recursive: true });
  await writeFile(dataPath, 'orphan', { mode: 0o600 });
  let deleteCalls = 0;
  const cache = new DiskBackedCache<Buffer>({
    name: 'test-cache',
    dir: root,
    maxMemBytes: 0,
    maxDiskBytes: 16,
    serialize: (value) => Buffer.from(value),
    deserialize: (value) => Buffer.from(value),
    sizeOf: (value) => value.length,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) {
          deleteCalls++;
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  await cache.whenReady();
  assert.equal(deleteCalls, 1);
  await assert.rejects(cache.close(), (error: unknown) => {
    assert(error instanceof AggregateError);
    return error.errors.some(
      (candidate) =>
        candidate instanceof Error &&
        'code' in candidate &&
        candidate.code === 'EBUSY'
    );
  });
  assert.equal(deleteCalls, 2);
  await access(dataPath);
});

test('two startup scans coalesce orphan removal without concurrent rm', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-pair-'));
  const directory = path.join(root, 'test-cache');
  const dataPath = path.join(directory, fileKey('orphan-pair'));
  await mkdir(directory, { recursive: true });
  await writeFile(dataPath, 'orphan', { mode: 0o600 });
  const deleteEntered = deferred();
  const continueDelete = deferred();
  let deleteCalls = 0;
  let activeDeletes = 0;
  let peakDeletes = 0;
  const guardedRm: DiskBackedCacheFileSystem['rm'] = async (
    candidate,
    options
  ) => {
    if (String(candidate) !== dataPath) return rm(candidate, options);
    deleteCalls++;
    activeDeletes++;
    peakDeletes = Math.max(peakDeletes, activeDeletes);
    try {
      if (deleteCalls === 1) {
        deleteEntered.resolve();
        await continueDelete.promise;
      }
      return await rm(candidate, options);
    } finally {
      activeDeletes--;
    }
  };
  const first = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: { rm: guardedRm },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await deleteEntered.promise;
  const second = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: { rm: guardedRm },
  });
  await second.whenReady();
  assert.equal(deleteCalls, 1);
  continueDelete.resolve();
  await first.whenReady();
  await assert.rejects(access(dataPath), { code: 'ENOENT' });

  await second.flush();
  assert.equal(deleteCalls, 1);
  assert.equal(peakDeletes, 1);
  assert.equal(
    await second.installPreparedFile(
      'orphan-pair',
      await prepared(second, Buffer.from('new!')),
      4
    ),
    true
  );
  await first.flush();
  await second.flush();
  await first.close();
  assert.equal(deleteCalls, 1);
  const lease = await second.acquireDiskFile('orphan-pair');
  assert(lease);
  assert.equal((await readFile(lease.path)).toString(), 'new!');
  await lease.release();
  assert.deepEqual(
    {
      diskBytes: second.stats().diskBytes,
      diskCount: second.stats().diskCount,
    },
    { diskBytes: 4, diskCount: 1 }
  );
  await second.close();
});

test('a joined close waits for a shared orphan attempt and retries its failure', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-orphan-join-'));
  const directory = path.join(root, 'test-cache');
  const dataPath = path.join(directory, fileKey('orphan-join'));
  await mkdir(directory, { recursive: true });
  await writeFile(dataPath, 'orphan', { mode: 0o600 });
  const deleteEntered = deferred();
  const continueDelete = deferred();
  let deleteCalls = 0;
  const guardedRm: DiskBackedCacheFileSystem['rm'] = async (
    candidate,
    options
  ) => {
    if (String(candidate) !== dataPath) return rm(candidate, options);
    deleteCalls++;
    if (deleteCalls === 1) {
      deleteEntered.resolve();
      await continueDelete.promise;
      throw codedError('EBUSY');
    }
    return rm(candidate, options);
  };
  const first = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: { rm: guardedRm },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await deleteEntered.promise;
  const second = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: { rm: guardedRm },
  });
  await second.whenReady();

  let closeSettled = false;
  const closing = second.close().then(() => {
    closeSettled = true;
  });
  await Promise.resolve();
  assert.equal(closeSettled, false);
  assert.equal(deleteCalls, 1);

  continueDelete.resolve();
  await Promise.all([first.whenReady(), closing]);
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await second.close();
  assert.equal(deleteCalls, 2);
});

test('process path delete participants are hard-bounded with a typed error', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-intent-limit-'));
  const directory = path.join(root, 'test-cache');
  const dataPath = path.join(directory, fileKey('intent-limit'));
  await mkdir(directory, { recursive: true });
  await writeFile(dataPath, 'orphan', { mode: 0o600 });
  const deleteEntered = deferred();
  const continueDelete = deferred();
  let deleteCalls = 0;
  const guardedRm: DiskBackedCacheFileSystem['rm'] = async (
    candidate,
    options
  ) => {
    if (String(candidate) !== dataPath) return rm(candidate, options);
    deleteCalls++;
    if (deleteCalls === 1) {
      deleteEntered.resolve();
      await continueDelete.promise;
    }
    return rm(candidate, options);
  };
  const participants: DiskBackedCache<Buffer>[] = [];
  const first = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: { rm: guardedRm },
  });
  participants.push(first);
  await deleteEntered.promise;
  for (let index = 1; index < DISK_CACHE_DELETE_PARTICIPANT_LIMIT; index++) {
    const participant = createCache(context, root, {
      maxDiskBytes: 16,
      fileSystem: { rm: guardedRm },
    });
    participants.push(participant);
    await participant.whenReady();
  }

  const overflow = new DiskBackedCache<Buffer>({
    name: 'test-cache',
    dir: root,
    maxMemBytes: 0,
    maxDiskBytes: 16,
    serialize: (value) => Buffer.from(value),
    deserialize: (value) => Buffer.from(value),
    sizeOf: (value) => value.length,
    fileSystem: { rm: guardedRm },
  });
  await overflow.whenReady();
  await assert.rejects(overflow.close(), (error: unknown) => {
    assert(error instanceof AggregateError);
    return error.errors.some(
      (candidate) =>
        candidate instanceof DiskBackedCacheError &&
        candidate.code === 'DISK_CACHE_DELETE_PARTICIPANT_LIMIT'
    );
  });
  assert.equal(deleteCalls, 1);

  continueDelete.resolve();
  await Promise.all(participants.map((participant) => participant.whenReady()));
  await Promise.all(participants.map((participant) => participant.flush()));
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });

  const replacement = createCache(context, root, { maxDiskBytes: 16 });
  await replacement.whenReady();
  context.after(() => rm(root, { recursive: true, force: true }));
  assert.equal(
    await replacement.installPreparedFile(
      'intent-limit',
      await prepared(replacement, Buffer.from('new!')),
      4
    ),
    true
  );
  await assert.rejects(overflow.close(), hasParticipantLimitError);
  assert.equal((await readFile(dataPath)).toString(), 'new!');
  assert.equal(deleteCalls, 1);
});

test('a 65th clear cannot publish a stale delete for a later incarnation', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-cap-clear-'));
  const key = 'cap-clear';
  const writer = createCache(context, root, { maxDiskBytes: 16 });
  await writer.whenReady();
  const saturated = await saturateDeleteParticipants(context, root, key);
  context.after(() => rm(root, { recursive: true, force: true }));

  await assert.rejects(saturated.owner.clear(), hasParticipantLimitError);
  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
    },
    { diskBytes: 4, diskCount: 1 }
  );

  await saturated.lease.release();
  assert.equal(saturated.deleteCalls(), 1);
  await assert.rejects(access(saturated.dataPath), { code: 'ENOENT' });

  assert.equal(
    await writer.installPreparedFile(
      key,
      await prepared(writer, Buffer.from('new!')),
      4
    ),
    true
  );
  await writer.flush();
  await saturated.owner.flush();
  await saturated.owner.close();
  assert.equal((await readFile(saturated.dataPath)).toString(), 'new!');
  assert.equal(saturated.deleteCalls(), 1);
});

test('a saturated explicit delete leaves no unbound retryable state', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-cap-delete-'));
  const key = 'cap-delete';
  const writer = createCache(context, root, { maxDiskBytes: 16 });
  await writer.whenReady();
  const saturated = await saturateDeleteParticipants(context, root, key);
  context.after(() => rm(root, { recursive: true, force: true }));

  await assert.rejects(saturated.owner.delete(key), hasParticipantLimitError);
  assert.equal(saturated.owner.stats().diskCount, 1);
  await saturated.lease.release();

  assert.equal(
    await writer.installPreparedFile(
      key,
      await prepared(writer, Buffer.from('next')),
      4
    ),
    true
  );
  await saturated.owner.flush();
  await saturated.owner.close();
  assert.equal((await readFile(saturated.dataPath)).toString(), 'next');
  assert.equal(saturated.deleteCalls(), 1);
});

test('LRU saturation retains the over-budget entry without a latent delete', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-cap-lru-'));
  const key = 'cap-lru';
  const writer = createCache(context, root, { maxDiskBytes: 16 });
  await writer.whenReady();
  const saturated = await saturateDeleteParticipants(context, root, key);
  context.after(() => rm(root, { recursive: true, force: true }));

  saturated.owner.resize(0, 0);
  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
    },
    { diskBytes: 4, diskCount: 1 }
  );
  await saturated.lease.release();

  assert.equal(
    await writer.installPreparedFile(
      key,
      await prepared(writer, Buffer.from('safe')),
      4
    ),
    true
  );
  await saturated.owner.flush();
  await saturated.owner.close();
  assert.equal((await readFile(saturated.dataPath)).toString(), 'safe');
  assert.equal(saturated.deleteCalls(), 1);
});

test('participant capacity is reusable only by a newly initiated delete', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-cap-reuse-'));
  const key = 'cap-reuse';
  const writer = createCache(context, root, { maxDiskBytes: 16 });
  await writer.whenReady();
  const saturated = await saturateDeleteParticipants(context, root, key);
  context.after(() => rm(root, { recursive: true, force: true }));

  await assert.rejects(saturated.owner.delete(key), hasParticipantLimitError);
  await saturated.lease.release();
  assert.equal(
    await writer.installPreparedFile(
      key,
      await prepared(writer, Buffer.from('next')),
      4
    ),
    true
  );

  assert.equal(await saturated.owner.delete(key), true);
  await saturated.owner.flush();
  await assert.rejects(access(saturated.dataPath), { code: 'ENOENT' });
  assert.equal(saturated.deleteCalls(), 1);
});

test('the final file lease waits through a predecessor to its paused successor delete', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-lease-successor-')
  );
  const deleteEntered = deferred();
  const continueDelete = deferred();
  const scenario = await createLeasedSuccessorPath(
    context,
    root,
    'lease-successor',
    async (candidate, options) => {
      deleteEntered.resolve();
      await continueDelete.promise;
      return rm(candidate, options);
    }
  );
  context.after(() => rm(root, { recursive: true, force: true }));

  let releaseSettled = false;
  const release = scenario.lease.release().then(() => {
    releaseSettled = true;
  });
  await deleteEntered.promise;
  await Promise.resolve();
  assert.equal(releaseSettled, false);
  assert.equal((await readFile(scenario.dataPath)).toString(), 'new!');

  continueDelete.resolve();
  await release;
  assert.equal(releaseSettled, true);
  await assert.rejects(access(scenario.dataPath), { code: 'ENOENT' });
  await Promise.all([scenario.owner.flush(), scenario.observer.flush()]);
});

test('the final file lease idempotently propagates a successor error and leaves it retryable', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-lease-error-'));
  let deleteCalls = 0;
  const scenario = await createLeasedSuccessorPath(
    context,
    root,
    'lease-error',
    async (candidate, options) => {
      deleteCalls++;
      if (deleteCalls === 1) throw codedError('EBUSY');
      return rm(candidate, options);
    }
  );
  context.after(() => rm(root, { recursive: true, force: true }));

  const release = scenario.lease.release();
  assert.strictEqual(scenario.lease.release(), release);
  await assert.rejects(
    release,
    (error: unknown) =>
      error instanceof Error && 'code' in error && error.code === 'EBUSY'
  );
  assert.strictEqual(scenario.lease.release(), release);
  assert.equal((await readFile(scenario.dataPath)).toString(), 'new!');
  assert.equal(
    await scenario.observer.acquireDiskFile('lease-error'),
    undefined
  );
  assert.equal(
    await scenario.owner.installPreparedFile(
      'lease-error',
      await prepared(scenario.owner, Buffer.from('nope')),
      4
    ),
    false
  );

  await scenario.owner.flush();
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(scenario.dataPath), { code: 'ENOENT' });
});

test('a non-final lease returns promptly while the final lease owns delete completion', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-lease-final-'));
  const deleteEntered = deferred();
  const continueDelete = deferred();
  let deleteCalls = 0;
  const dataPath = path.join(root, 'test-cache', fileKey('lease-final'));
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === dataPath) {
          deleteCalls++;
          deleteEntered.resolve();
          await continueDelete.promise;
        }
        return rm(candidate, options);
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.whenReady();
  assert.equal(
    await cache.installPreparedFile(
      'lease-final',
      await prepared(cache, Buffer.from('data')),
      4
    ),
    true
  );
  const first = await cache.acquireDiskFile('lease-final');
  const second = await cache.acquireDiskFile('lease-final');
  assert(first);
  assert(second);
  assert.equal(await cache.delete('lease-final'), true);

  const firstRelease = first.release();
  assert.strictEqual(first.release(), firstRelease);
  await firstRelease;
  assert.equal(deleteCalls, 0);

  let finalSettled = false;
  const finalReleaseCompletion = second.release();
  assert.strictEqual(second.release(), finalReleaseCompletion);
  const finalRelease = finalReleaseCompletion.then(() => {
    finalSettled = true;
  });
  await deleteEntered.promise;
  await Promise.resolve();
  assert.equal(finalSettled, false);
  continueDelete.resolve();
  await finalRelease;
  assert.equal(finalSettled, true);
  assert.strictEqual(second.release(), finalReleaseCompletion);
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
});

test('the final lease follows two observed predecessors to its own delete intent', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-lease-chain-'));
  const directory = path.join(root, 'test-cache');
  const key = 'lease-chain';
  const dataPath = path.join(directory, fileKey(key));
  await mkdir(directory, { recursive: true });
  let deletePhase = false;
  let deleteCalls = 0;
  const owner = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      rm: async (candidate, options) => {
        if (deletePhase && String(candidate) === dataPath) deleteCalls++;
        return rm(candidate, options);
      },
    },
  });
  await owner.whenReady();
  await writeFile(dataPath, 'one!', { mode: 0o600 });

  const firstObserved = deferred();
  const continueFirst = deferred();
  let firstLstatCalls = 0;
  const first = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        if (String(candidate) === dataPath) {
          firstLstatCalls++;
          if (firstLstatCalls === 1) {
            firstObserved.resolve();
            await continueFirst.promise;
          }
        }
        return stats;
      },
    },
  });
  await firstObserved.promise;
  await rm(dataPath, { force: true });
  await writeFile(dataPath, 'two!', { mode: 0o600 });

  const secondObserved = deferred();
  const continueSecond = deferred();
  let secondLstatCalls = 0;
  const second = createCache(context, root, {
    maxDiskBytes: 16,
    fileSystem: {
      lstat: async (candidate) => {
        const stats = await lstat(candidate);
        if (String(candidate) === dataPath) {
          secondLstatCalls++;
          if (secondLstatCalls === 1) {
            secondObserved.resolve();
            await continueSecond.promise;
          }
        }
        return stats;
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await secondObserved.promise;
  assert.equal(
    await owner.installPreparedFile(
      key,
      await prepared(owner, Buffer.from('last')),
      4
    ),
    true
  );
  await owner.flush();
  const lease = await owner.acquireDiskFile(key);
  assert(lease);

  continueFirst.resolve();
  await first.whenReady();
  continueSecond.resolve();
  await second.whenReady();
  deletePhase = true;
  assert.equal(await owner.delete(key), true);
  await lease.release();

  assert.equal(firstLstatCalls, 2);
  assert.equal(secondLstatCalls, 2);
  assert.equal(deleteCalls, 1);
  await assert.rejects(access(dataPath), { code: 'ENOENT' });
  await Promise.all([first.flush(), second.flush(), owner.flush()]);
  assert.equal(deleteCalls, 1);
});

test('stale background destination cleanup remains retryable after clear', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-stale-write-'));
  const destination = path.join(
    root,
    'test-cache',
    fileKey('stale-background')
  );
  const renamed = deferred();
  const continueRename = deferred();
  let deleteCalls = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    renameFile: async (source, target) => {
      await rename(source, target);
      if (String(target) === destination) {
        renamed.resolve();
        await continueRename.promise;
      }
    },
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === destination && ++deleteCalls === 1) {
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  const writer = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([cache.whenReady(), writer.whenReady()]);

  cache.set('stale-background', Buffer.from('data'));
  await renamed.promise;
  const clearing = cache.clear();
  continueRename.resolve();
  await clearing;
  assert.equal(deleteCalls, 1);
  await access(destination);
  assert.equal(cache.stats().diskCount, 0);

  writer.set('stale-background', Buffer.from('blocked'));
  await writer.flush();
  assert.equal(writer.stats().diskCount, 0);
  assert.equal((await readFile(destination)).toString(), 'data');

  await cache.flush();
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(destination), { code: 'ENOENT' });
  writer.set('stale-background', Buffer.from('fresh'));
  await writer.flush();
  await cache.flush();
  assert.equal(deleteCalls, 2);
  const lease = await writer.acquireDiskFile('stale-background');
  assert(lease);
  assert.equal((await readFile(lease.path)).toString(), 'fresh');
  await lease.release();
});

test('stale prepared destination cleanup remains retryable after clear', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-stale-install-'));
  const destination = path.join(root, 'test-cache', fileKey('stale-prepared'));
  const renamed = deferred();
  const continueRename = deferred();
  let deleteCalls = 0;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    renameFile: async (source, target) => {
      await rename(source, target);
      if (String(target) === destination) {
        renamed.resolve();
        await continueRename.promise;
      }
    },
    fileSystem: {
      rm: async (candidate, options) => {
        if (String(candidate) === destination && ++deleteCalls === 1) {
          throw codedError('EBUSY');
        }
        return rm(candidate, options);
      },
    },
  });
  const writer = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await Promise.all([cache.whenReady(), writer.whenReady()]);

  const install = cache.installPreparedFile(
    'stale-prepared',
    await prepared(cache, Buffer.from('data')),
    4
  );
  await renamed.promise;
  const clearing = cache.clear();
  continueRename.resolve();
  await assert.rejects(install, { code: 'EBUSY' });
  await clearing;
  assert.equal(deleteCalls, 1);
  await access(destination);
  assert.equal(cache.stats().diskCount, 0);

  assert.equal(
    await writer.installPreparedFile(
      'stale-prepared',
      await prepared(writer, Buffer.from('nope')),
      4
    ),
    false
  );
  assert.equal((await readFile(destination)).toString(), 'data');

  await cache.flush();
  assert.equal(deleteCalls, 2);
  await assert.rejects(access(destination), { code: 'ENOENT' });
  assert.equal(
    await writer.installPreparedFile(
      'stale-prepared',
      await prepared(writer, Buffer.from('fresh')),
      5
    ),
    true
  );
  await cache.flush();
  assert.equal(deleteCalls, 2);
  const lease = await writer.acquireDiskFile('stale-prepared');
  assert(lease);
  assert.equal((await readFile(lease.path)).toString(), 'fresh');
  await lease.release();
});

test('prepared writes stay within the hard LRU budget behind a saturated oldest entry', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-budget-prepared-')
  );
  const saturated = await saturateDeleteParticipants(
    context,
    root,
    'blocked-old',
    8
  );
  context.after(() => rm(root, { recursive: true, force: true }));

  for (let index = 0; index < 20; index++) {
    await saturated.owner.installPreparedFile(
      `prepared-${index}`,
      await prepared(saturated.owner, Buffer.from('data')),
      4
    );
    await saturated.owner.flush();
    const stats = saturated.owner.stats();
    assert(stats.diskBytes <= 8);
    assert(stats.diskCount <= 2);
    assert.equal(await indexedEntryCount(root), stats.diskCount);
    assert.equal(await persistentEntryCount(root), stats.diskCount);
  }

  await saturated.lease.release();
  saturated.owner.resize(0, 0);
  await saturated.owner.flush();
});

test('background writes stay within the hard LRU budget behind a saturated oldest entry', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-budget-background-')
  );
  const saturated = await saturateDeleteParticipants(
    context,
    root,
    'blocked-old',
    8
  );
  context.after(() => rm(root, { recursive: true, force: true }));

  for (let index = 0; index < 20; index++) {
    saturated.owner.set(`background-${index}`, Buffer.from('data'));
    await saturated.owner.flush();
    const stats = saturated.owner.stats();
    assert(stats.diskBytes <= 8);
    assert(stats.diskCount <= 2);
    assert.equal(await indexedEntryCount(root), stats.diskCount);
    assert.equal(await persistentEntryCount(root), stats.diskCount);
  }

  await saturated.lease.release();
  saturated.owner.resize(0, 0);
  await saturated.owner.flush();
});

test('bounded eviction skips a saturated oldest entry and evicts the next LRU candidate', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-budget-skip-'));
  const saturated = await saturateDeleteParticipants(
    context,
    root,
    'blocked-old',
    8
  );
  context.after(() => rm(root, { recursive: true, force: true }));

  assert.equal(
    await saturated.owner.installPreparedFile(
      'second-oldest',
      await prepared(saturated.owner, Buffer.from('two!')),
      4
    ),
    true
  );
  assert.equal(
    await saturated.owner.installPreparedFile(
      'newest',
      await prepared(saturated.owner, Buffer.from('last')),
      4
    ),
    true
  );
  await saturated.owner.flush();

  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
    },
    { diskBytes: 8, diskCount: 2 }
  );
  assert.equal(
    await saturated.owner.acquireDiskFile('second-oldest'),
    undefined
  );
  const newest = await saturated.owner.acquireDiskFile('newest');
  assert(newest);
  await newest.release();

  await saturated.lease.release();
  saturated.owner.resize(0, 0);
  await saturated.owner.flush();
});

test('new writes are discarded when every old over-budget entry is participant-blocked', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-budget-all-blocked-')
  );
  const saturated = await saturateDeleteParticipants(
    context,
    root,
    'blocked-old',
    4
  );
  context.after(() => rm(root, { recursive: true, force: true }));

  assert.equal(
    await saturated.owner.installPreparedFile(
      'discarded-prepared',
      await prepared(saturated.owner, Buffer.from('next')),
      4
    ),
    false
  );
  saturated.owner.set('discarded-background', Buffer.from('more'));
  await saturated.owner.flush();

  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
      indexEntries: await indexedEntryCount(root),
      physicalEntries: await persistentEntryCount(root),
    },
    { diskBytes: 4, diskCount: 1, indexEntries: 1, physicalEntries: 1 }
  );

  await saturated.lease.release();
  saturated.owner.resize(0, 0);
  await saturated.owner.flush();
});

test('bounded flush and resize eviction progress after participant capacity is released', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-budget-progress-')
  );
  const saturated = await saturateDeleteParticipants(
    context,
    root,
    'blocked-old',
    4
  );
  context.after(() => rm(root, { recursive: true, force: true }));

  assert.equal(
    await saturated.owner.installPreparedFile(
      'blocked-new',
      await prepared(saturated.owner, Buffer.from('nope')),
      4
    ),
    false
  );
  assert.equal(saturated.owner.stats().diskBytes, 4);

  await saturated.lease.release();
  saturated.owner.resize(0, 0);
  await saturated.owner.flush();
  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
    },
    { diskBytes: 0, diskCount: 0 }
  );

  saturated.owner.resize(0, 4);
  assert.equal(
    await saturated.owner.installPreparedFile(
      'fresh',
      await prepared(saturated.owner, Buffer.from('safe')),
      4
    ),
    true
  );
  await saturated.owner.flush();
  assert.equal((await saturated.owner.getAsync('fresh'))?.toString(), 'safe');
});

test('parallel prepared writes settle without over-budget or double accounting', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-budget-parallel-')
  );
  const saturated = await saturateDeleteParticipants(
    context,
    root,
    'blocked-old',
    8
  );
  context.after(() => rm(root, { recursive: true, force: true }));
  const staged = await Promise.all(
    Array.from({ length: 8 }, (_, index) =>
      prepared(saturated.owner, Buffer.from(`p${index}`))
    )
  );

  await Promise.all(
    staged.map((entry, index) =>
      saturated.owner.installPreparedFile(`parallel-${index}`, entry, 2)
    )
  );
  await saturated.owner.flush();
  assert(saturated.owner.stats().diskBytes <= 8);
  assert(saturated.owner.stats().diskCount <= 3);
  assert.equal(
    await indexedEntryCount(root),
    saturated.owner.stats().diskCount
  );
  assert.equal(
    await persistentEntryCount(root),
    saturated.owner.stats().diskCount
  );

  await saturated.lease.release();
  saturated.owner.resize(0, 0);
  await saturated.owner.flush();
});

test('capacity-limited lease invalidation is explicit, idempotent, and retryable later', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-invalidate-cap-'));
  const saturated = await saturateDeleteParticipants(
    context,
    root,
    'corrupt',
    16
  );
  context.after(() => rm(root, { recursive: true, force: true }));

  assert.equal(saturated.lease.invalidateAsMiss(), false);
  assert.equal(saturated.lease.invalidateAsMiss(), false);
  saturated.lease.confirmHit();
  assert.deepEqual(
    {
      hits: saturated.owner.stats().hits,
      misses: saturated.owner.stats().misses,
      diskHits: saturated.owner.stats().diskHits,
      diskCount: saturated.owner.stats().diskCount,
    },
    { hits: 0, misses: 1, diskHits: 0, diskCount: 1 }
  );

  const release = saturated.lease.release();
  assert.strictEqual(saturated.lease.release(), release);
  await release;
  assert.equal(await saturated.owner.acquireDiskFile('corrupt'), undefined);
  assert.deepEqual(
    {
      misses: saturated.owner.stats().misses,
      diskCount: saturated.owner.stats().diskCount,
    },
    { misses: 2, diskCount: 0 }
  );
});

test('buffering deserialization corruption releases its lease under participant saturation', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-deserialize-cap-')
  );
  const readEntered = deferred();
  const continueRead = deferred();
  const key = 'deserialize-corrupt';
  const dataPath = path.join(root, 'test-cache', fileKey(key));
  let pauseRead = false;
  const cache = createCache(context, root, {
    maxDiskBytes: 16,
    deserialize: () => {
      throw new Error('synthetic corrupt payload');
    },
    fileSystem: {
      readFile: async (candidate, options) => {
        const body = await readFile(candidate, options);
        if (pauseRead && String(candidate) === dataPath) {
          readEntered.resolve();
          await continueRead.promise;
        }
        return body;
      },
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.whenReady();
  assert.equal(
    await cache.installPreparedFile(
      key,
      await prepared(cache, Buffer.from('bad!')),
      4
    ),
    true
  );
  await cache.flush();

  pauseRead = true;
  const lookup = cache.getAsync(key);
  await readEntered.promise;
  await saturateObservedDeleteParticipants(context, root, {
    name: 'test-cache',
    dataPath,
    maxDiskBytes: 16,
  });
  continueRead.resolve();

  assert.equal(await lookup, undefined);
  assert.deepEqual(
    {
      hits: cache.stats().hits,
      misses: cache.stats().misses,
      diskCount: cache.stats().diskCount,
    },
    { hits: 0, misses: 1, diskCount: 1 }
  );
  pauseRead = false;
  assert.equal(await cache.getAsync(key), undefined);
  assert.deepEqual(
    { misses: cache.stats().misses, diskCount: cache.stats().diskCount },
    { misses: 2, diskCount: 0 }
  );
});

test('successful invalidation callbacks cannot affect a newer file incarnation', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-invalidate-once-')
  );
  const cache = createCache(context, root, { maxDiskBytes: 16 });
  context.after(() => rm(root, { recursive: true, force: true }));
  await cache.whenReady();
  assert.equal(
    await cache.installPreparedFile(
      'same-key',
      await prepared(cache, Buffer.from('old!')),
      4
    ),
    true
  );
  const lease = await cache.acquireDiskFile('same-key');
  assert(lease);

  assert.equal(lease.invalidateAsMiss(), true);
  assert.equal(lease.invalidateAsMiss(), true);
  lease.confirmHit();
  const release = lease.release();
  assert.strictEqual(lease.release(), release);
  await release;
  assert.deepEqual(
    { hits: cache.stats().hits, misses: cache.stats().misses },
    { hits: 0, misses: 1 }
  );

  assert.equal(
    await cache.installPreparedFile(
      'same-key',
      await prepared(cache, Buffer.from('new!')),
      4
    ),
    true
  );
  assert.equal(lease.invalidateAsMiss(), true);
  lease.confirmHit();
  await cache.flush();
  assert.equal((await cache.getAsync('same-key'))?.toString(), 'new!');
});

test('a resolved participant fence retires its stale entry at the exact disk budget', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-retire-budget-'));
  const saturated = await exactBudgetParticipantBlock(context, root);
  context.after(() => rm(root, { recursive: true, force: true }));

  await saturated.lease.release();
  await saturated.owner.flush();

  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
      indexEntries: await indexedEntryCount(root),
      physicalEntries: await persistentEntryCount(root),
    },
    { diskBytes: 4, diskCount: 1, indexEntries: 1, physicalEntries: 1 }
  );
  assert.equal(saturated.deleteCalls(), 1);
});

test('an old cache cannot delete a same-key incarnation installed after fence retirement', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-retire-aba-'));
  const saturated = await exactBudgetParticipantBlock(context, root);
  await saturated.lease.release();
  await saturated.owner.flush();

  const replacement = createCache(context, root, { maxDiskBytes: 8 });
  await replacement.whenReady();
  assert.equal(
    await replacement.installPreparedFile(
      'blocked-old',
      await prepared(replacement, Buffer.from('new!')),
      4
    ),
    true
  );
  await replacement.flush();
  context.after(() => rm(root, { recursive: true, force: true }));

  await saturated.owner.flush();
  assert.equal(await saturated.owner.delete('blocked-old'), false);
  saturated.owner.resize(0, 0);
  await saturated.owner.flush();
  await saturated.owner.clear();

  assert.equal((await readFile(saturated.dataPath)).toString(), 'new!');
  assert.equal((await replacement.getAsync('blocked-old'))?.toString(), 'new!');
});

test('a superseded participant fence retires only the stale local incarnation', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-retire-superseded-')
  );
  const saturated = await exactBudgetParticipantBlock(context, root);
  context.after(() => rm(root, { recursive: true, force: true }));

  await rm(saturated.dataPath, { force: true });
  await writeFile(saturated.dataPath, Buffer.from('new!'), { mode: 0o600 });
  await saturated.lease.release();
  await saturated.owner.flush();

  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
      indexEntries: await indexedEntryCount(root),
      physicalEntries: await persistentEntryCount(root),
      replacement: (await readFile(saturated.dataPath)).toString(),
      deleteCalls: saturated.deleteCalls(),
    },
    {
      diskBytes: 4,
      diskCount: 1,
      indexEntries: 1,
      physicalEntries: 2,
      replacement: 'new!',
      deleteCalls: 0,
    }
  );
});

test('synchronous lookup retirement and entry identity protect an immediate local rewrite', async (context) => {
  const root = await mkdtemp(
    path.join(tmpdir(), 'disk-cache-retire-identity-')
  );
  const retirementEntered = deferred();
  const continueRetirement = deferred();
  const saturated = await exactBudgetParticipantBlock(context, root, {
    beforeParticipantBlockedRetirement: async () => {
      retirementEntered.resolve();
      await continueRetirement.promise;
    },
  });
  context.after(() => rm(root, { recursive: true, force: true }));

  const release = saturated.lease.release();
  await retirementEntered.promise;
  await release;
  assert.equal(await saturated.owner.acquireDiskFile('blocked-old'), undefined);
  assert.equal(
    await saturated.owner.installPreparedFile(
      'blocked-old',
      await prepared(saturated.owner, Buffer.from('new!')),
      4
    ),
    true
  );

  continueRetirement.resolve();
  await Promise.resolve();
  await saturated.owner.flush();
  assert.equal(
    (await saturated.owner.getAsync('blocked-old'))?.toString(),
    'new!'
  );
  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
      misses: saturated.owner.stats().misses,
    },
    { diskBytes: 8, diskCount: 2, misses: 1 }
  );
});

test('a transient fence failure stays blocked and retires once after explicit retry', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-retire-retry-'));
  const saturated = await saturateDeleteParticipants(
    context,
    root,
    'blocked-old',
    4,
    { failFirstDelete: true }
  );
  context.after(() => rm(root, { recursive: true, force: true }));

  assert.equal(
    await saturated.owner.installPreparedFile(
      'discarded-before-retry',
      await prepared(saturated.owner, Buffer.from('next')),
      4
    ),
    false
  );
  await assert.rejects(saturated.lease.release(), { code: 'EBUSY' });
  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
      deleteCalls: saturated.deleteCalls(),
    },
    { diskBytes: 4, diskCount: 1, deleteCalls: 1 }
  );
  assert.equal(await saturated.owner.acquireDiskFile('blocked-old'), undefined);
  assert.equal(
    await saturated.owner.installPreparedFile(
      'discarded-while-fenced',
      await prepared(saturated.owner, Buffer.from('more')),
      4
    ),
    false
  );
  assert.equal(saturated.owner.stats().diskBytes, 4);

  const retryOwner = saturated.participants[0];
  assert(retryOwner);
  await retryOwner.flush();
  await saturated.owner.flush();
  assert.deepEqual(
    {
      diskBytes: saturated.owner.stats().diskBytes,
      diskCount: saturated.owner.stats().diskCount,
      deleteCalls: saturated.deleteCalls(),
    },
    { diskBytes: 0, diskCount: 0, deleteCalls: 2 }
  );
});

test('close retires a resolved fence before its final index snapshot', async (context) => {
  const root = await mkdtemp(path.join(tmpdir(), 'disk-cache-retire-close-'));
  const retirementEntered = deferred();
  const continueRetirement = deferred();
  const saturated = await exactBudgetParticipantBlock(context, root, {
    beforeParticipantBlockedRetirement: async () => {
      retirementEntered.resolve();
      await continueRetirement.promise;
    },
  });

  const release = saturated.lease.release();
  await retirementEntered.promise;
  await release;
  await saturated.owner.close();
  assert.equal(await indexedEntryCount(root), 1);
  assert.equal(await persistentEntryCount(root), 1);

  continueRetirement.resolve();
  const restarted = createCache(context, root, { maxDiskBytes: 8 });
  await restarted.whenReady();
  context.after(() => rm(root, { recursive: true, force: true }));
  assert.deepEqual(
    {
      diskBytes: restarted.stats().diskBytes,
      diskCount: restarted.stats().diskCount,
      value: (await restarted.getAsync('survivor'))?.toString(),
    },
    { diskBytes: 4, diskCount: 1, value: 'keep' }
  );
});
