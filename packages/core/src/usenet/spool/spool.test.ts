import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import {
  open,
  mkdir,
  lstat,
  mkdtemp,
  readdir,
  rename,
  rm,
  stat,
  symlink,
  utimes,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test, { type TestContext } from 'node:test';
import type { Readable } from 'node:stream';
import type { SegmentSpoolingPlan } from '../resource-plan.js';
import { ByteBudget, type ByteLease } from '../pool/byte-budget.js';
import { UsenetSpoolError } from './errors.js';
import { SpoolManager, type SpoolManagerOptions } from './manager.js';
import type {
  SpoolFileHandle,
  SpoolFileSystem,
  SpoolPathStats,
  SpoolScheduledTask,
  SpoolScheduler,
} from './types.js';
import type { GrowingSpoolArtifact } from './growing-artifact.js';

function testPlan(
  overrides: Partial<SegmentSpoolingPlan> = {}
): SegmentSpoolingPlan {
  return {
    memoryBudgetBytes: 128,
    perStreamBufferBytes: 64,
    spoolBytes: 1024,
    minFreeDiskBytes: 0,
    decoderChunkBytes: 16,
    writerQueueBytes: 64,
    readerHighWaterMarkBytes: 3,
    perDownloadBaseLeaseBytes: 32,
    maxOpenSpoolFiles: 8,
    orphanTtlMs: 1000,
    ...overrides,
  };
}

interface TestManagerOptions {
  readonly plan?: SegmentSpoolingPlan;
  readonly fileSystem?: Partial<SpoolFileSystem>;
  readonly clock?: () => number;
  readonly idGenerator?: () => string;
  readonly scheduler?: SpoolScheduler;
  readonly maxArtifacts?: number;
}

async function testManager(
  context: TestContext,
  options: TestManagerOptions = {}
): Promise<{ readonly manager: SpoolManager; readonly cacheRoot: string }> {
  const cacheRoot = await mkdtemp(path.join(tmpdir(), 'aiostreams-spool-'));
  let nextId = 0;
  const managerOptions: SpoolManagerOptions = {
    plan: options.plan ?? testPlan(),
    engineId: 'test-engine',
    cacheRoot,
    fileSystem: options.fileSystem,
    clock: options.clock,
    idGenerator: options.idGenerator ?? (() => `test-id-${nextId++}`),
    scheduler: options.scheduler,
    maxArtifacts: options.maxArtifacts,
  };
  const manager = new SpoolManager(managerOptions);
  context.after(async () => {
    await Promise.allSettled([manager.close()]);
    await rm(cacheRoot, { recursive: true, force: true });
  });
  return { manager, cacheRoot };
}

class ControlledScheduler {
  private pending:
    | {
        readonly callback: () => Promise<void>;
        readonly delayMs: number;
      }
    | undefined;

  readonly schedule: SpoolScheduler = (callback, delayMs) => {
    assert.equal(
      this.pending,
      undefined,
      'heartbeat timer must remain bounded'
    );
    const task = { callback, delayMs };
    this.pending = task;
    let cancelled = false;
    const scheduledTask: SpoolScheduledTask = {
      cancel: () => {
        if (cancelled) return;
        cancelled = true;
        if (this.pending === task) this.pending = undefined;
      },
    };
    return scheduledTask;
  };

  get pendingCount(): number {
    return this.pending ? 1 : 0;
  }

  get nextDelayMs(): number | undefined {
    return this.pending?.delayMs;
  }

  async runNext(): Promise<void> {
    const task = this.pending;
    assert(task, 'expected a scheduled heartbeat');
    this.pending = undefined;
    await task.callback();
  }
}

async function leaseBuffer(
  budget: ByteBudget,
  value: string
): Promise<{ readonly chunk: Buffer; readonly lease: ByteLease }> {
  const bytes = Buffer.byteLength(value);
  const lease = await budget.acquire(bytes);
  return { chunk: Buffer.from(value), lease };
}

async function writeText(
  artifact: GrowingSpoolArtifact,
  budget: ByteBudget,
  value: string
): Promise<boolean> {
  const { chunk, lease } = await leaseBuffer(budget, value);
  try {
    return artifact.write(chunk, lease);
  } catch (error) {
    lease.release();
    throw error;
  }
}

async function collect(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    if (!Buffer.isBuffer(chunk)) {
      throw new Error('Expected a binary spool-reader chunk');
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function isSpoolError(error: unknown, code: UsenetSpoolError['code']): boolean {
  assert(error instanceof UsenetSpoolError);
  assert.equal(error.code, code);
  return true;
}

function streamError(stream: Readable): Promise<Error> {
  return new Promise((resolve) => stream.once('error', resolve));
}

async function exists(target: string): Promise<boolean> {
  try {
    await stat(target);
    return true;
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return false;
    }
    throw error;
  }
}

async function filesBelow(directory: string): Promise<string[]> {
  let entries;
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      return [];
    }
    throw error;
  }
  const files: string[] = [];
  for (const entry of entries) {
    const child = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await filesBelow(child)));
    else files.push(child);
  }
  return files;
}

async function openSpoolFile(
  filePath: string,
  flags: string,
  mode: number | undefined,
  onClose: () => void = () => undefined
): Promise<SpoolFileHandle> {
  const handle = await open(filePath, flags, mode);
  let closed = false;
  return {
    read: async (buffer, offset, length, position) => {
      const result = await handle.read(buffer, offset, length, position);
      return { bytesRead: result.bytesRead };
    },
    write: async (buffer, offset, length, position) => {
      const result = await handle.write(buffer, offset, length, position);
      return { bytesWritten: result.bytesWritten };
    },
    close: async () => {
      await handle.close();
      if (!closed) {
        closed = true;
        onClose();
      }
    },
  };
}

async function spoolLstat(target: string): Promise<SpoolPathStats> {
  const stats = await lstat(target);
  return {
    mtimeMs: stats.mtimeMs,
    isDirectory: () => stats.isDirectory(),
    isFile: () => stats.isFile(),
    isSymbolicLink: () => stats.isSymbolicLink(),
  };
}

interface ForeignNamespaceFixture {
  readonly processRoot: string;
  readonly markerPath: string;
  readonly engineRoot: string;
  readonly artifactPath: string;
  readonly lockRoot: string;
  readonly lockPath: string;
}

async function createForeignNamespace(
  cacheRoot: string,
  namespaceName: string,
  now: number
): Promise<ForeignNamespaceFixture> {
  const spoolRoot = path.join(cacheRoot, 'usenet-spool');
  const processRoot = path.join(spoolRoot, namespaceName);
  const markerPath = path.join(processRoot, '.alive');
  const engineRoot = path.join(processRoot, '1'.repeat(64));
  const artifactPath = path.join(engineRoot, `${'2'.repeat(64)}.ready`);
  const lockRoot = path.join(spoolRoot, '.liveness-locks');
  const lockPath = path.join(lockRoot, `${namespaceName}.lock`);
  await mkdir(engineRoot, { recursive: true, mode: 0o700 });
  for (const filePath of [markerPath, artifactPath]) {
    const handle = await open(filePath, 'w', 0o600);
    await handle.close();
  }
  const old = new Date(now - 5000);
  for (const target of [markerPath, artifactPath, engineRoot, processRoot]) {
    await utimes(target, old, old);
  }
  return {
    processRoot,
    markerPath,
    engineRoot,
    artifactPath,
    lockRoot,
    lockPath,
  };
}

function controlledWriterFileSystem(
  writeStarted: PromiseWithResolvers<void>,
  continueWrite: PromiseWithResolvers<void>,
  counters: { writeCalls: number }
): Partial<SpoolFileSystem> {
  return {
    open: async (filePath, flags, mode) => {
      const handle = await open(filePath, flags, mode);
      const writer = flags === 'wx+';
      const wrapped: SpoolFileHandle = {
        read: async (buffer, offset, length, position) => {
          const result = await handle.read(buffer, offset, length, position);
          return { bytesRead: result.bytesRead };
        },
        write: async (buffer, offset, length, position) => {
          if (writer) {
            counters.writeCalls++;
            if (counters.writeCalls === 1) {
              writeStarted.resolve();
              await continueWrite.promise;
            }
          }
          const result = await handle.write(buffer, offset, length, position);
          return { bytesWritten: result.bytesWritten };
        },
        close: () => handle.close(),
      };
      return wrapped;
    },
  };
}

test('tails committed writes before writer completion', async (context) => {
  const { manager } = await testManager(context, {
    plan: testPlan({ readerHighWaterMarkBytes: 5 }),
  });
  const memory = new ByteBudget(32);
  const artifact = await manager.createArtifact({
    sessionId: 'session',
    segmentId: '<raw-message@example>',
    initialReservationBytes: 16,
  });
  const reader = artifact.createReadStream({ highWaterMark: 5 });
  const iterator = reader[Symbol.asyncIterator]();
  const firstRead = iterator.next();

  await writeText(artifact, memory, 'hello');
  const first = await firstRead;
  assert.equal(first.done, false);
  assert(Buffer.isBuffer(first.value));
  assert.equal(first.value.toString(), 'hello');
  assert.equal(artifact.state, 'writing');
  assert.equal(artifact.committedBytes, 5);

  const secondRead = iterator.next();
  await writeText(artifact, memory, 'world');
  await artifact.complete();
  const second = await secondRead;
  assert.equal(second.done, false);
  assert(Buffer.isBuffer(second.value));
  assert.equal(second.value.toString(), 'world');
  assert.equal((await iterator.next()).done, true);
  assert.equal(artifact.state, 'complete');
  assert.equal(memory.stats().usedBytes, 0);

  await artifact.dispose();
});

test('reads exact start/endExclusive ranges without materializing the file', async (context) => {
  const { manager } = await testManager(context);
  const memory = new ByteBudget(32);
  const artifact = await manager.createArtifact({
    sessionId: 'range-session',
    segmentId: 'range-segment',
    initialReservationBytes: 16,
  });
  await writeText(artifact, memory, 'abcdefghij');
  await artifact.complete();

  const body = await collect(
    artifact.createReadStream({ start: 2, endExclusive: 7, highWaterMark: 2 })
  );
  assert.equal(body.toString(), 'cdefg');
  assert.throws(
    () => artifact.createReadStream({ highWaterMark: 4 }),
    (error) => isSpoolError(error, 'USENET_SPOOL_INVALID_ARGUMENT')
  );
  assert.equal(manager.stats().files.openFiles, 0);
  await artifact.dispose();
});

test('serializes writes, bounds the leased queue, and signals backpressure', async (context) => {
  const writeStarted = Promise.withResolvers<void>();
  const continueWrite = Promise.withResolvers<void>();
  const counters = { writeCalls: 0 };
  const { manager } = await testManager(context, {
    plan: testPlan({ writerQueueBytes: 4, readerHighWaterMarkBytes: 2 }),
    fileSystem: controlledWriterFileSystem(
      writeStarted,
      continueWrite,
      counters
    ),
  });
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'slow-session',
    segmentId: 'ordered-segment',
    initialReservationBytes: 8,
  });
  const slowReader = artifact.createReadStream({ highWaterMark: 2 });

  assert.equal(await writeText(artifact, memory, 'aa'), true);
  await writeStarted.promise;
  assert.equal(await writeText(artifact, memory, 'bb'), false);
  const overflow = await leaseBuffer(memory, 'c');
  assert.throws(
    () => artifact.write(overflow.chunk, overflow.lease),
    (error) => isSpoolError(error, 'USENET_MEMORY_BUDGET')
  );
  overflow.lease.release();
  assert.equal(counters.writeCalls, 1);
  assert.equal(artifact.committedBytes, 0);
  assert.equal(memory.stats().usedBytes, 4);

  const drained = Promise.withResolvers<void>();
  artifact.onceDrain(drained.resolve);
  continueWrite.resolve();
  await drained.promise;
  await artifact.complete();
  assert.equal(counters.writeCalls, 2);
  assert.equal(artifact.committedBytes, 4);
  assert.equal((await collect(slowReader)).toString(), 'aabb');
  assert.equal(memory.stats().usedBytes, 0);
  await artifact.dispose();
});

test('does not expose a partially written chunk as committed', async (context) => {
  const partialWriteFinished = Promise.withResolvers<void>();
  const continueWrite = Promise.withResolvers<void>();
  let writerCall = 0;
  const fileSystem: Partial<SpoolFileSystem> = {
    open: async (filePath, flags, mode) => {
      const handle = await open(filePath, flags, mode);
      const writer = flags === 'wx+';
      return {
        read: async (buffer, offset, length, position) => {
          const result = await handle.read(buffer, offset, length, position);
          return { bytesRead: result.bytesRead };
        },
        write: async (buffer, offset, length, position) => {
          if (writer && writerCall++ === 0) {
            const result = await handle.write(buffer, offset, 2, position);
            partialWriteFinished.resolve();
            return { bytesWritten: result.bytesWritten };
          }
          if (writer) await continueWrite.promise;
          const result = await handle.write(buffer, offset, length, position);
          return { bytesWritten: result.bytesWritten };
        },
        close: () => handle.close(),
      };
    },
  };
  const { manager } = await testManager(context, { fileSystem });
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'partial-write-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });

  await writeText(artifact, memory, 'data');
  await partialWriteFinished.promise;
  assert.equal(artifact.committedBytes, 0);
  assert.equal(manager.stats().budget.actualBytes, 0);
  continueWrite.resolve();
  await artifact.complete();
  assert.equal(artifact.committedBytes, 4);
  assert.equal(manager.stats().budget.actualBytes, 4);
  assert.equal(
    (await collect(artifact.createReadStream({ highWaterMark: 2 }))).toString(),
    'data'
  );
  await artifact.dispose();
});

test('supports multiple independent readers of a growing artifact', async (context) => {
  const { manager } = await testManager(context);
  const memory = new ByteBudget(32);
  const artifact = await manager.createArtifact({
    sessionId: 'multi-reader',
    segmentId: 'segment',
    initialReservationBytes: 16,
  });
  const first = collect(artifact.createReadStream({ highWaterMark: 2 }));
  const second = collect(
    artifact.createReadStream({
      start: 3,
      endExclusive: 8,
      highWaterMark: 2,
    })
  );

  await writeText(artifact, memory, 'abcde');
  await writeText(artifact, memory, 'fghij');
  await artifact.complete();
  assert.equal((await first).toString(), 'abcdefghij');
  assert.equal((await second).toString(), 'defgh');
  await artifact.dispose();
});

test('aborts a waiting growing reader and closes its file lease', async (context) => {
  const { manager } = await testManager(context);
  const artifact = await manager.createArtifact({
    sessionId: 'abort-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  const controller = new AbortController();
  const reader = artifact.createReadStream({ signal: controller.signal });
  const failure = streamError(reader);
  reader.resume();
  assert.equal(getEventListeners(controller.signal, 'abort').length, 1);
  controller.abort(new Error('test abort'));

  assert(isSpoolError(await failure, 'USENET_SPOOL_ABORTED'));
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  assert.equal(manager.stats().files.openFiles, 1);
  await artifact.dispose();
  assert.equal(manager.stats().files.openFiles, 0);
});

test('propagates a classified writer failure to every reader', async (context) => {
  const fileSystem: Partial<SpoolFileSystem> = {
    open: async (filePath, flags, mode) => {
      const handle = await open(filePath, flags, mode);
      const writer = flags === 'wx+';
      return {
        read: async (buffer, offset, length, position) => {
          const result = await handle.read(buffer, offset, length, position);
          return { bytesRead: result.bytesRead };
        },
        write: async (buffer, offset, length, position) => {
          if (writer) {
            const error = new Error('synthetic disk full');
            Object.defineProperty(error, 'code', { value: 'ENOSPC' });
            throw error;
          }
          const result = await handle.write(buffer, offset, length, position);
          return { bytesWritten: result.bytesWritten };
        },
        close: () => handle.close(),
      };
    },
  };
  const { manager } = await testManager(context, { fileSystem });
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'failure-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  const firstReader = artifact.createReadStream();
  const secondReader = artifact.createReadStream();
  const firstReaderFailure = streamError(firstReader);
  const secondReaderFailure = streamError(secondReader);
  firstReader.resume();
  secondReader.resume();

  await writeText(artifact, memory, 'boom');
  await assert.rejects(artifact.complete(), (error) =>
    isSpoolError(error, 'USENET_SPOOL_DISK_FULL')
  );
  const firstError = await firstReaderFailure;
  const secondError = await secondReaderFailure;
  assert(isSpoolError(firstError, 'USENET_SPOOL_DISK_FULL'));
  assert.strictEqual(secondError, firstError);
  const futureReader = artifact.createReadStream();
  const futureReaderFailure = streamError(futureReader);
  futureReader.resume();
  assert.strictEqual(await futureReaderFailure, firstError);
  assert.equal(artifact.state, 'failed');
  assert.equal(artifact.committedBytes, 0);
  assert.equal(memory.stats().usedBytes, 0);
  await artifact.dispose();
  assert.equal(manager.stats().budget.reservedBytes, 0);
  assert.deepEqual(await filesBelow(manager.engineRoot), []);
});

test('waits for cache-promotion references before deleting and releasing', async (context) => {
  const { manager } = await testManager(context);
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'promotion-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  await writeText(artifact, memory, 'data');
  await artifact.complete();
  const promotion = artifact.acquirePromotion();
  const disposal = artifact.dispose();

  assert.equal(artifact.state, 'disposed');
  assert.equal(await exists(promotion.path), true);
  assert.equal(manager.stats().budget.reservedBytes, 8);
  promotion.release();
  promotion.release();
  await disposal;
  await artifact.dispose();
  assert.equal(await exists(promotion.path), false);
  assert.equal(manager.stats().budget.reservedBytes, 0);
});

test('dispose during an in-flight write waits, aborts readers, and leaks nothing', async (context) => {
  const writeStarted = Promise.withResolvers<void>();
  const continueWrite = Promise.withResolvers<void>();
  const counters = { writeCalls: 0 };
  const { manager } = await testManager(context, {
    plan: testPlan({ writerQueueBytes: 4 }),
    fileSystem: controlledWriterFileSystem(
      writeStarted,
      continueWrite,
      counters
    ),
  });
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'dispose-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  const reader = artifact.createReadStream();
  const readerFailure = streamError(reader);
  reader.resume();
  assert.equal(await writeText(artifact, memory, 'data'), false);
  await writeStarted.promise;

  const backpressureReleased = Promise.withResolvers<void>();
  artifact.onceDrain(backpressureReleased.resolve);
  const disposal = artifact.dispose();
  await backpressureReleased.promise;
  continueWrite.resolve();
  assert(isSpoolError(await readerFailure, 'USENET_SPOOL_CLOSED'));
  await disposal;
  assert.equal(memory.stats().usedBytes, 0);
  assert.equal(manager.stats().budget.reservedBytes, 0);
  assert.equal(manager.stats().files.openFiles, 0);
  assert.deepEqual(await filesBelow(manager.engineRoot), []);
});

test('dispose serializes with an in-flight completion rename', async (context) => {
  const renameStarted = Promise.withResolvers<void>();
  const continueRename = Promise.withResolvers<void>();
  const events: string[] = [];
  const fileSystem: Partial<SpoolFileSystem> = {
    rename: async (oldPath, newPath) => {
      events.push('rename-start');
      renameStarted.resolve();
      await continueRename.promise;
      await rename(oldPath, newPath);
      events.push('rename-end');
    },
    rm: async (target, options) => {
      if (target.endsWith('.partial') || target.endsWith('.ready')) {
        events.push('remove');
      }
      await rm(target, options);
    },
  };
  const { manager } = await testManager(context, { fileSystem });
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'rename-race-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  await writeText(artifact, memory, 'data');
  const completion = artifact.complete();
  await renameStarted.promise;

  const disposal = artifact.dispose();
  await Promise.resolve();
  await Promise.resolve();
  assert.deepEqual(events, ['rename-start']);
  continueRename.resolve();
  await assert.rejects(completion, (error) =>
    isSpoolError(error, 'USENET_SPOOL_CLOSED')
  );
  await disposal;
  assert.equal(artifact.state, 'disposed');
  assert(events.indexOf('rename-end') < events.indexOf('remove'));
  assert.deepEqual(await filesBelow(manager.engineRoot), []);
  assert.equal(manager.stats().budget.reservedBytes, 0);
});

test('reader falls back to ready while rename completion is still pending', async (context) => {
  const fileRenamed = Promise.withResolvers<void>();
  const finishRename = Promise.withResolvers<void>();
  const fileSystem: Partial<SpoolFileSystem> = {
    rename: async (oldPath, newPath) => {
      await rename(oldPath, newPath);
      fileRenamed.resolve();
      await finishRename.promise;
    },
  };
  const { manager } = await testManager(context, {
    plan: testPlan({ readerHighWaterMarkBytes: 4 }),
    fileSystem,
  });
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'reader-rename-race-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  await writeText(artifact, memory, 'data');
  const completion = artifact.complete();
  await fileRenamed.promise;
  assert.equal(artifact.state, 'writing');

  const reader = artifact.createReadStream({ highWaterMark: 4 });
  const iterator = reader[Symbol.asyncIterator]();
  const first = await iterator.next();
  assert.equal(first.done, false);
  assert(Buffer.isBuffer(first.value));
  assert.equal(first.value.toString(), 'data');
  const end = iterator.next();

  finishRename.resolve();
  await completion;
  assert.equal((await end).done, true);
  assert.equal(artifact.state, 'complete');
  await artifact.dispose();
  assert.equal(manager.stats().files.openFiles, 0);
  assert.equal(manager.stats().budget.reservedBytes, 0);
  assert.equal(manager.stats().artifacts, 0);
});

test('enforces one global open-file cap across writer and reader', async (context) => {
  const { manager } = await testManager(context, {
    plan: testPlan({ maxOpenSpoolFiles: 1 }),
  });
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'file-cap-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  assert.equal(manager.stats().files.openFiles, 1);

  const bodyPromise = collect(artifact.createReadStream());
  assert.equal(manager.stats().files.openFiles, 1);
  assert.equal(manager.stats().files.waiting, 1);
  await writeText(artifact, memory, 'data');
  await artifact.complete();
  assert.equal((await bodyPromise).toString(), 'data');
  assert.deepEqual(manager.stats().files, {
    maxFiles: 1,
    openFiles: 0,
    waiting: 0,
    peakOpenFiles: 1,
  });
  await artifact.dispose();
});

test('uses only hashed secure paths and leaves no process files after cleanup', async (context) => {
  const { manager, cacheRoot } = await testManager(context);
  const memory = new ByteBudget(8);
  const rawId = '../../<raw-message@example>';
  const artifact = await manager.createArtifact({
    sessionId: '../raw-session',
    segmentId: rawId,
    initialReservationBytes: 8,
  });
  await writeText(artifact, memory, 'data');
  await artifact.complete();
  const promotion = artifact.acquirePromotion();

  assert.equal(promotion.path.startsWith(manager.engineRoot), true);
  assert.equal(promotion.path.includes(rawId), false);
  assert.match(path.basename(promotion.path), /^[a-f0-9]{64}\.ready$/);
  assert.deepEqual(await filesBelow(manager.engineRoot), [promotion.path]);
  if (process.platform !== 'win32') {
    const mode = (await stat(promotion.path)).mode & 0o777;
    assert.equal(mode, 0o600);
    const markerMode =
      (await stat(path.join(manager.processRoot, '.alive'))).mode & 0o777;
    assert.equal(markerMode, 0o600);
    const directoryMode = (await stat(manager.engineRoot)).mode & 0o777;
    assert.equal(directoryMode, 0o700);
  }
  promotion.release();
  await artifact.dispose();
  await manager.close();
  await manager.close();
  assert.deepEqual(await filesBelow(cacheRoot), []);
});

test('rejects a symlinked spool root without traversing it', async (context) => {
  if (process.platform === 'win32') return;
  const cacheRoot = await mkdtemp(path.join(tmpdir(), 'aiostreams-root-'));
  const externalRoot = await mkdtemp(
    path.join(tmpdir(), 'aiostreams-external-')
  );
  context.after(async () => {
    await rm(cacheRoot, { recursive: true, force: true });
    await rm(externalRoot, { recursive: true, force: true });
  });
  await symlink(externalRoot, path.join(cacheRoot, 'usenet-spool'));
  const manager = new SpoolManager({
    plan: testPlan(),
    engineId: 'symlink-test',
    cacheRoot,
    idGenerator: () => 'symlink-process',
  });

  await assert.rejects(manager.initialize(), (error) =>
    isSpoolError(error, 'USENET_SPOOL_UNAVAILABLE')
  );
  await mkdir(manager.processRoot, { recursive: true });
  await manager.close();
  assert.equal(await exists(externalRoot), true);
  assert.equal(await exists(manager.processRoot), true);
});

test('removes only old hashed foreign namespaces during initialization', async (context) => {
  const now = Date.now();
  const cacheRoot = await mkdtemp(path.join(tmpdir(), 'aiostreams-orphans-'));
  context.after(() => rm(cacheRoot, { recursive: true, force: true }));
  const spoolRoot = path.join(cacheRoot, 'usenet-spool');
  const oldNamespace = path.join(spoolRoot, 'a'.repeat(64));
  const oldMarker = path.join(oldNamespace, '.alive');
  const oldEngine = path.join(oldNamespace, '1'.repeat(64));
  const oldArtifact = path.join(oldEngine, `${'2'.repeat(64)}.ready`);
  const recentNamespace = path.join(spoolRoot, 'b'.repeat(64));
  const recentMarker = path.join(recentNamespace, '.alive');
  const withinTtlNamespace = path.join(spoolRoot, 'c'.repeat(64));
  const withinTtlMarker = path.join(withinTtlNamespace, '.alive');
  const activeNamespace = path.join(spoolRoot, 'd'.repeat(64));
  const activeMarker = path.join(activeNamespace, '.alive');
  const activeEngine = path.join(activeNamespace, 'e'.repeat(64));
  const activeArtifact = path.join(activeEngine, `${'f'.repeat(64)}.partial`);
  const unknownNamespace = path.join(spoolRoot, '8'.repeat(64));
  const unknownMarker = path.join(unknownNamespace, '.alive');
  const unknownEntry = path.join(unknownNamespace, 'unexpected');
  const symlinkTarget = path.join(cacheRoot, 'symlink-target');
  const symlinkNamespace = path.join(spoolRoot, '9'.repeat(64));
  const unknownDirectory = path.join(spoolRoot, 'not-a-spool-namespace');
  await mkdir(oldEngine, { recursive: true });
  await mkdir(recentNamespace, { recursive: true });
  await mkdir(withinTtlNamespace, { recursive: true });
  await mkdir(activeEngine, { recursive: true });
  await mkdir(unknownNamespace, { recursive: true });
  await mkdir(symlinkTarget, { recursive: true });
  if (process.platform !== 'win32') {
    await symlink(symlinkTarget, symlinkNamespace);
  }
  await mkdir(unknownDirectory, { recursive: true });
  for (const filePath of [
    oldMarker,
    oldArtifact,
    recentMarker,
    withinTtlMarker,
    activeMarker,
    activeArtifact,
    unknownMarker,
    unknownEntry,
  ]) {
    const handle = await open(filePath, 'w', 0o600);
    await handle.close();
  }
  for (const oldPath of [
    oldMarker,
    oldArtifact,
    oldEngine,
    oldNamespace,
    activeMarker,
    activeEngine,
    activeNamespace,
    unknownMarker,
    unknownEntry,
    unknownNamespace,
  ]) {
    await utimes(oldPath, new Date(now - 5000), new Date(now - 5000));
  }
  await utimes(recentNamespace, new Date(now - 5000), new Date(now - 5000));
  await utimes(recentMarker, new Date(now), new Date(now));
  await utimes(withinTtlNamespace, new Date(now - 5000), new Date(now - 5000));
  await utimes(withinTtlMarker, new Date(now - 500), new Date(now - 500));
  await utimes(activeArtifact, new Date(now), new Date(now));
  await utimes(unknownDirectory, new Date(now - 5000), new Date(now - 5000));

  const manager = new SpoolManager({
    plan: testPlan({ orphanTtlMs: 1000 }),
    engineId: 'orphan-test',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'own-process',
  });
  await manager.initialize();
  assert.equal(await exists(oldNamespace), false);
  assert.equal(await exists(recentNamespace), true);
  assert.equal(await exists(withinTtlNamespace), true);
  assert.equal(await exists(activeNamespace), true);
  assert.equal(await exists(unknownNamespace), true);
  if (process.platform !== 'win32') {
    assert.equal(await exists(symlinkNamespace), true);
  }
  assert.equal(await exists(unknownDirectory), true);
  await manager.close();
  assert.equal(await exists(recentNamespace), true);
  assert.equal(await exists(withinTtlNamespace), true);
  assert.equal(await exists(activeNamespace), true);
  assert.equal(await exists(unknownNamespace), true);
  if (process.platform !== 'win32') {
    assert.equal(await exists(symlinkNamespace), true);
  }
  assert.equal(await exists(unknownDirectory), true);
});

test('fresh heartbeat protects a live idle manager from orphan cleanup', async (context) => {
  const cacheRoot = await mkdtemp(path.join(tmpdir(), 'aiostreams-live-'));
  context.after(() => rm(cacheRoot, { recursive: true, force: true }));
  let now = Date.now();
  const plan = testPlan({ orphanTtlMs: 900 });
  const schedulerA = new ControlledScheduler();
  const schedulerB = new ControlledScheduler();
  const managerA = new SpoolManager({
    plan,
    engineId: 'engine-a',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'process-a',
    scheduler: schedulerA.schedule,
  });
  const managerB = new SpoolManager({
    plan,
    engineId: 'engine-b',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'process-b',
    scheduler: schedulerB.schedule,
  });
  await managerA.initialize();
  await utimes(
    managerA.processRoot,
    new Date(now - 5000),
    new Date(now - 5000)
  );
  await utimes(managerA.engineRoot, new Date(now - 5000), new Date(now - 5000));
  now += 2000;
  assert.equal(schedulerA.nextDelayMs, 300);
  await schedulerA.runNext();
  assert.equal(schedulerA.pendingCount, 1);

  await managerB.initialize();
  assert.equal(await exists(managerA.processRoot), true);
  const memory = new ByteBudget(8);
  const artifact = await managerA.createArtifact({
    sessionId: 'live-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  await writeText(artifact, memory, 'data');
  await artifact.complete();
  assert.equal((await collect(artifact.createReadStream())).toString(), 'data');
  await artifact.dispose();
  assert.equal(managerA.stats().budget.reservedBytes, 0);

  await managerB.close();
  await managerA.close();
  assert.equal(schedulerA.pendingCount, 0);
  assert.equal(schedulerB.pendingCount, 0);
});

test('heartbeat wins after an initially stale marker snapshot', async () => {
  const cacheRoot = await mkdtemp(
    path.join(tmpdir(), 'aiostreams-fence-race-')
  );
  let now = Date.now();
  const plan = testPlan({ orphanTtlMs: 900 });
  const schedulerA = new ControlledScheduler();
  const schedulerB = new ControlledScheduler();
  const managerA = new SpoolManager({
    plan,
    engineId: 'fence-race-a',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'fence-race-process-a',
    scheduler: schedulerA.schedule,
  });
  const memory = new ByteBudget(8);
  const artifact = await managerA.createArtifact({
    sessionId: 'fence-race-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  await writeText(artifact, memory, 'data');
  await artifact.complete();
  const promotion = artifact.acquirePromotion();
  const markerPath = path.join(managerA.processRoot, '.alive');
  const initialMarkerRead = Promise.withResolvers<void>();
  const continueInitialScan = Promise.withResolvers<void>();
  let markerReads = 0;
  const managerB = new SpoolManager({
    plan,
    engineId: 'fence-race-b',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'fence-race-process-b',
    scheduler: schedulerB.schedule,
    fileSystem: {
      lstat: async (target) => {
        const snapshot = await spoolLstat(target);
        if (target === markerPath && markerReads++ === 0) {
          initialMarkerRead.resolve();
          await continueInitialScan.promise;
        }
        return snapshot;
      },
    },
  });

  try {
    const old = new Date(now - 5000);
    for (const target of [
      markerPath,
      promotion.path,
      managerA.engineRoot,
      managerA.processRoot,
    ]) {
      await utimes(target, old, old);
    }
    now += 2000;

    const initializationB = managerB.initialize();
    await initialMarkerRead.promise;
    await schedulerA.runNext();
    continueInitialScan.resolve();
    await initializationB;

    assert.equal(markerReads, 2);
    assert.equal(await exists(managerA.processRoot), true);
    assert.equal(await exists(promotion.path), true);
    assert.equal(
      (await collect(artifact.createReadStream())).toString(),
      'data'
    );
    const lockPath = path.join(
      managerA.spoolRoot,
      '.liveness-locks',
      `${path.basename(managerA.processRoot)}.lock`
    );
    assert.equal(await exists(lockPath), false);

    promotion.release();
    await artifact.dispose();
    assert.equal(managerA.stats().budget.reservedBytes, 0);
    assert.equal(managerA.stats().files.openFiles, 0);
    assert.equal(managerA.stats().artifacts, 0);
  } finally {
    continueInitialScan.resolve();
    promotion.release();
    await Promise.allSettled([
      artifact.dispose(),
      managerB.close(),
      managerA.close(),
    ]);
    await rm(cacheRoot, { recursive: true, force: true });
  }
});

test('cleaner fence blocks heartbeat and stale manager recovers afterward', async () => {
  const cacheRoot = await mkdtemp(
    path.join(tmpdir(), 'aiostreams-cleaner-wins-')
  );
  let now = Date.now();
  const plan = testPlan({ orphanTtlMs: 900 });
  const schedulerA = new ControlledScheduler();
  const schedulerB = new ControlledScheduler();
  const managerA = new SpoolManager({
    plan,
    engineId: 'cleaner-wins-a',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'cleaner-wins-process-a',
    scheduler: schedulerA.schedule,
  });
  await managerA.initialize();
  const markerPath = path.join(managerA.processRoot, '.alive');
  const lockPath = path.join(
    managerA.spoolRoot,
    '.liveness-locks',
    `${path.basename(managerA.processRoot)}.lock`
  );
  const cleanerHasFence = Promise.withResolvers<void>();
  const continueCleaner = Promise.withResolvers<void>();
  let openControlHandles = 0;
  let namespaceDeletes = 0;
  const managerB = new SpoolManager({
    plan,
    engineId: 'cleaner-wins-b',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'cleaner-wins-process-b',
    scheduler: schedulerB.schedule,
    fileSystem: {
      open: async (filePath, flags, mode) => {
        openControlHandles++;
        const handle = await openSpoolFile(filePath, flags, mode, () => {
          openControlHandles--;
        });
        if (filePath === lockPath && flags === 'wx') {
          cleanerHasFence.resolve();
          await continueCleaner.promise;
        }
        return handle;
      },
      rm: async (target, options) => {
        if (target === managerA.processRoot) namespaceDeletes++;
        await rm(target, options);
      },
    },
  });

  try {
    const old = new Date(now - 5000);
    for (const target of [
      markerPath,
      managerA.engineRoot,
      managerA.processRoot,
    ]) {
      await utimes(target, old, old);
    }
    now += 2000;

    const initializationB = managerB.initialize();
    await cleanerHasFence.promise;
    assert.match(path.basename(lockPath), /^[a-f0-9]{64}\.lock$/);
    if (process.platform !== 'win32') {
      assert.equal((await stat(lockPath)).mode & 0o777, 0o600);
      assert.equal((await stat(path.dirname(lockPath))).mode & 0o777, 0o700);
    }
    await schedulerA.runNext();
    assert.equal(schedulerA.pendingCount, 1);
    assert.equal(await exists(managerA.processRoot), true);
    await assert.rejects(
      managerA.createArtifact({
        sessionId: 'fenced-activity',
        segmentId: 'segment',
        initialReservationBytes: 8,
      }),
      (error) => isSpoolError(error, 'USENET_SPOOL_UNAVAILABLE')
    );
    assert.equal(managerA.stats().budget.reservedBytes, 0);
    assert.equal(managerA.stats().files.openFiles, 0);

    continueCleaner.resolve();
    await initializationB;
    assert.equal(namespaceDeletes, 1);
    assert.equal(await exists(managerA.processRoot), false);
    assert.equal(await exists(lockPath), false);
    assert.equal(openControlHandles, 0);

    const memory = new ByteBudget(8);
    const artifact = await managerA.createArtifact({
      sessionId: 'post-fence-recovery',
      segmentId: 'segment',
      initialReservationBytes: 8,
    });
    await writeText(artifact, memory, 'data');
    await artifact.complete();
    assert.equal(
      (await collect(artifact.createReadStream())).toString(),
      'data'
    );
    await artifact.dispose();
    assert.equal(managerA.stats().budget.reservedBytes, 0);
    assert.equal(managerA.stats().files.openFiles, 0);
  } finally {
    continueCleaner.resolve();
    await Promise.allSettled([managerB.close(), managerA.close()]);
    assert.equal(openControlHandles, 0);
    await rm(cacheRoot, { recursive: true, force: true });
  }
});

test('fresh marker revalidation under the cleaner fence prevents deletion', async () => {
  const cacheRoot = await mkdtemp(
    path.join(tmpdir(), 'aiostreams-fence-recheck-')
  );
  const now = Date.now();
  const fixture = await createForeignNamespace(cacheRoot, 'a'.repeat(64), now);
  let cleanerClaims = 0;
  const manager = new SpoolManager({
    plan: testPlan({ orphanTtlMs: 1000 }),
    engineId: 'fence-recheck',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'fence-recheck-process',
    fileSystem: {
      open: async (filePath, flags, mode) => {
        const handle = await openSpoolFile(filePath, flags, mode);
        if (filePath === fixture.lockPath && flags === 'wx') {
          cleanerClaims++;
          await utimes(fixture.markerPath, new Date(now), new Date(now));
        }
        return handle;
      },
    },
  });
  const secondManager = new SpoolManager({
    plan: testPlan({ orphanTtlMs: 1000 }),
    engineId: 'fence-recheck-second',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'fence-recheck-second-process',
  });

  try {
    await manager.initialize();
    assert.equal(cleanerClaims, 1);
    assert.equal(await exists(fixture.processRoot), true);
    assert.equal(await exists(fixture.artifactPath), true);
    assert.equal(await exists(fixture.lockPath), false);

    await secondManager.initialize();
    assert.equal(await exists(fixture.processRoot), true);
    assert.equal(await exists(fixture.artifactPath), true);
    assert.equal(await exists(fixture.lockPath), false);
  } finally {
    await Promise.allSettled([manager.close(), secondManager.close()]);
    await rm(cacheRoot, { recursive: true, force: true });
  }
});

test('unsafe or inaccessible namespace controls fail safe without deletion', async () => {
  const cacheRoot = await mkdtemp(
    path.join(tmpdir(), 'aiostreams-fence-safety-')
  );
  const now = Date.now();
  const protectedFixture = await createForeignNamespace(
    cacheRoot,
    'a'.repeat(64),
    now
  );
  const unknownFixture = await createForeignNamespace(
    cacheRoot,
    'b'.repeat(64),
    now
  );
  const unknownControl = path.join(
    unknownFixture.processRoot,
    '.unexpected-control'
  );
  const unknownHandle = await open(unknownControl, 'w', 0o600);
  await unknownHandle.close();
  await utimes(
    unknownFixture.processRoot,
    new Date(now - 5000),
    new Date(now - 5000)
  );
  const scheduler = new ControlledScheduler();
  let deniedClaims = 0;
  const deniedManager = new SpoolManager({
    plan: testPlan({ orphanTtlMs: 1000 }),
    engineId: 'fence-denied',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'fence-denied-process',
    scheduler: scheduler.schedule,
    fileSystem: {
      open: async (filePath, flags, mode) => {
        if (filePath === protectedFixture.lockPath && flags === 'wx') {
          deniedClaims++;
          const error = new Error('synthetic namespace-control denial');
          Object.defineProperty(error, 'code', { value: 'EACCES' });
          throw error;
        }
        return openSpoolFile(filePath, flags, mode);
      },
    },
  });

  try {
    await assert.rejects(deniedManager.initialize(), (error) =>
      isSpoolError(error, 'USENET_SPOOL_UNAVAILABLE')
    );
    assert.equal(deniedClaims, 1);
    assert.equal(await exists(protectedFixture.processRoot), true);
    assert.equal(await exists(protectedFixture.artifactPath), true);
    assert.equal(await exists(unknownFixture.processRoot), true);
    assert.equal(scheduler.pendingCount, 0);

    await mkdir(protectedFixture.lockRoot, {
      recursive: true,
      mode: 0o700,
    });
    const staleLock = await open(protectedFixture.lockPath, 'wx', 0o600);
    await staleLock.close();
    const staleLockManager = new SpoolManager({
      plan: testPlan({ orphanTtlMs: 1000 }),
      engineId: 'fence-stale-lock',
      cacheRoot,
      clock: () => now,
      idGenerator: () => 'fence-stale-lock-process',
    });
    try {
      await staleLockManager.initialize();
      assert.equal(await exists(protectedFixture.processRoot), true);
      assert.equal(await exists(protectedFixture.artifactPath), true);
      assert.equal(await exists(unknownFixture.processRoot), true);
      assert.equal(staleLockManager.stats().files.openFiles, 0);
    } finally {
      await staleLockManager.close();
    }
    await rm(protectedFixture.lockPath, { force: false });

    if (process.platform !== 'win32') {
      const symlinkTarget = path.join(cacheRoot, 'lock-symlink-target');
      const targetHandle = await open(symlinkTarget, 'w', 0o600);
      await targetHandle.close();
      await symlink(symlinkTarget, protectedFixture.lockPath);
      const conservativeManager = new SpoolManager({
        plan: testPlan({ orphanTtlMs: 1000 }),
        engineId: 'fence-symlink',
        cacheRoot,
        clock: () => now,
        idGenerator: () => 'fence-symlink-process',
      });
      try {
        await conservativeManager.initialize();
        assert.equal(await exists(protectedFixture.processRoot), true);
        assert.equal(await exists(protectedFixture.artifactPath), true);
        assert.equal(await exists(unknownFixture.processRoot), true);
        assert.equal(
          (await lstat(protectedFixture.lockPath)).isSymbolicLink(),
          true
        );
        assert.equal(conservativeManager.stats().files.openFiles, 0);
      } finally {
        await conservativeManager.close();
      }
    }
  } finally {
    await Promise.allSettled([deniedManager.close()]);
    assert.equal(scheduler.pendingCount, 0);
    await rm(cacheRoot, { recursive: true, force: true });
  }
});

test('recovers an externally removed namespace with at most one open retry', async (context) => {
  let artifactOpenCalls = 0;
  const fileSystem: Partial<SpoolFileSystem> = {
    open: async (filePath, flags, mode) => {
      if (flags === 'wx+' && artifactOpenCalls++ === 0) {
        await rm(path.dirname(path.dirname(filePath)), {
          recursive: true,
          force: true,
        });
      }
      const handle = await open(filePath, flags, mode);
      return {
        read: async (buffer, offset, length, position) => {
          const result = await handle.read(buffer, offset, length, position);
          return { bytesRead: result.bytesRead };
        },
        write: async (buffer, offset, length, position) => {
          const result = await handle.write(buffer, offset, length, position);
          return { bytesWritten: result.bytesWritten };
        },
        close: () => handle.close(),
      };
    },
  };
  const scheduler = new ControlledScheduler();
  const { manager, cacheRoot } = await testManager(context, {
    fileSystem,
    scheduler: scheduler.schedule,
  });
  await manager.initialize();
  await rm(manager.processRoot, { recursive: true, force: true });

  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'recovery-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  assert.equal(artifactOpenCalls, 2);
  await writeText(artifact, memory, 'data');
  await artifact.complete();
  assert.equal((await collect(artifact.createReadStream())).toString(), 'data');
  await artifact.dispose();
  assert.equal(manager.stats().budget.reservedBytes, 0);
  assert.equal(manager.stats().files.openFiles, 0);
  assert.deepEqual(await filesBelow(manager.engineRoot), []);
  await manager.close();
  assert.deepEqual(await filesBelow(cacheRoot), []);
});

test('close waits for a fenced heartbeat while a cleaner contends', async () => {
  const cacheRoot = await mkdtemp(
    path.join(tmpdir(), 'aiostreams-close-fence-')
  );
  const heartbeatStarted = Promise.withResolvers<void>();
  const continueHeartbeat = Promise.withResolvers<void>();
  let now = Date.now();
  let touchCalls = 0;
  let openControlHandles = 0;
  const fileSystem: Partial<SpoolFileSystem> = {
    open: async (filePath, flags, mode) => {
      let counted = false;
      const handle = await openSpoolFile(filePath, flags, mode, () => {
        if (counted) openControlHandles--;
      });
      counted = true;
      openControlHandles++;
      return handle;
    },
    utimes: async (target, atimeMs, mtimeMs) => {
      touchCalls++;
      if (touchCalls === 2) {
        heartbeatStarted.resolve();
        await continueHeartbeat.promise;
      }
      await utimes(target, new Date(atimeMs), new Date(mtimeMs));
    },
  };
  const plan = testPlan({ orphanTtlMs: 900 });
  const schedulerA = new ControlledScheduler();
  const schedulerB = new ControlledScheduler();
  const managerA = new SpoolManager({
    plan,
    engineId: 'close-fence-a',
    cacheRoot,
    fileSystem,
    clock: () => now,
    idGenerator: () => 'close-fence-process-a',
    scheduler: schedulerA.schedule,
  });
  const managerB = new SpoolManager({
    plan,
    engineId: 'close-fence-b',
    cacheRoot,
    clock: () => now,
    idGenerator: () => 'close-fence-process-b',
    scheduler: schedulerB.schedule,
  });

  try {
    await managerA.initialize();
    assert.equal(openControlHandles, 0);
    const old = new Date(now - 5000);
    for (const target of [
      path.join(managerA.processRoot, '.alive'),
      managerA.engineRoot,
      managerA.processRoot,
    ]) {
      await utimes(target, old, old);
    }
    now += 2000;

    assert.equal(schedulerA.pendingCount, 1);
    const heartbeat = schedulerA.runNext();
    await heartbeatStarted.promise;
    assert.equal(openControlHandles, 1);

    await managerB.initialize();
    assert.equal(await exists(managerA.processRoot), true);

    let closed = false;
    const closing = managerA.close().then(() => {
      closed = true;
    });
    assert.equal(schedulerA.pendingCount, 0);
    await Promise.resolve();
    assert.equal(closed, false);
    assert.equal(openControlHandles, 1);

    continueHeartbeat.resolve();
    await heartbeat;
    await closing;
    await managerA.close();
    assert.equal(touchCalls, 2);
    assert.equal(schedulerA.pendingCount, 0);
    assert.equal(openControlHandles, 0);
    assert.equal(managerA.stats().files.openFiles, 0);
    assert.equal(await exists(managerA.processRoot), false);
  } finally {
    continueHeartbeat.resolve();
    await Promise.allSettled([managerA.close(), managerB.close()]);
    assert.equal(openControlHandles, 0);
    assert.equal(schedulerA.pendingCount, 0);
    assert.equal(schedulerB.pendingCount, 0);
    await rm(cacheRoot, { recursive: true, force: true });
  }
});

test('already removed files still release budget during idempotent cleanup', async (context) => {
  const { manager } = await testManager(context);
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'removed-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  await writeText(artifact, memory, 'data');
  await artifact.complete();
  const promotion = artifact.acquirePromotion();
  promotion.release();
  await rm(promotion.path);

  await artifact.dispose();
  await artifact.dispose();
  assert.equal(manager.stats().budget.reservedBytes, 0);
  assert.equal(manager.stats().artifacts, 0);
});

test('file-open permission failures are classified and roll back reservations', async (context) => {
  let artifactOpenCalls = 0;
  const fileSystem: Partial<SpoolFileSystem> = {
    open: async (filePath, flags, mode) => {
      if (flags === 'wx+') {
        artifactOpenCalls++;
        const error = new Error('synthetic permission failure');
        Object.defineProperty(error, 'code', { value: 'EACCES' });
        throw error;
      }
      const handle = await open(filePath, flags, mode);
      return {
        read: async (buffer, offset, length, position) => {
          const result = await handle.read(buffer, offset, length, position);
          return { bytesRead: result.bytesRead };
        },
        write: async (buffer, offset, length, position) => {
          const result = await handle.write(buffer, offset, length, position);
          return { bytesWritten: result.bytesWritten };
        },
        close: () => handle.close(),
      };
    },
  };
  const { manager } = await testManager(context, { fileSystem });

  await assert.rejects(
    manager.createArtifact({
      sessionId: 'permission-session',
      segmentId: 'segment',
      initialReservationBytes: 8,
    }),
    (error) => isSpoolError(error, 'USENET_SPOOL_UNAVAILABLE')
  );
  assert.equal(manager.stats().budget.reservedBytes, 0);
  assert.equal(manager.stats().files.openFiles, 0);
  assert.equal(manager.stats().artifacts, 0);
  assert.equal(artifactOpenCalls, 1);
});

test('rejects a writer chunk without an exact memory lease', async (context) => {
  const { manager } = await testManager(context);
  const memory = new ByteBudget(8);
  const artifact = await manager.createArtifact({
    sessionId: 'lease-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
  });
  const lease = await memory.acquire(2);

  assert.throws(
    () => artifact.write(Buffer.from('abc'), lease),
    (error) => isSpoolError(error, 'USENET_MEMORY_BUDGET')
  );
  assert.equal(artifact.state, 'created');
  assert.equal(artifact.committedBytes, 0);
  assert.equal(memory.stats().usedBytes, 2);
  lease.release();
  await artifact.dispose();
});

test('aborted creation removes an untracked partial file', async (context) => {
  const openStarted = Promise.withResolvers<void>();
  const continueOpen = Promise.withResolvers<void>();
  const fileSystem: Partial<SpoolFileSystem> = {
    open: async (filePath, flags, mode) => {
      if (flags === 'wx+') {
        openStarted.resolve();
        await continueOpen.promise;
      }
      const handle = await open(filePath, flags, mode);
      return {
        read: async (buffer, offset, length, position) => {
          const result = await handle.read(buffer, offset, length, position);
          return { bytesRead: result.bytesRead };
        },
        write: async (buffer, offset, length, position) => {
          const result = await handle.write(buffer, offset, length, position);
          return { bytesWritten: result.bytesWritten };
        },
        close: () => handle.close(),
      };
    },
  };
  const { manager } = await testManager(context, { fileSystem });
  const controller = new AbortController();
  const creation = manager.createArtifact({
    sessionId: 'aborted-create-session',
    segmentId: 'segment',
    initialReservationBytes: 8,
    signal: controller.signal,
  });
  await openStarted.promise;
  controller.abort();
  continueOpen.resolve();

  await assert.rejects(creation, (error) =>
    isSpoolError(error, 'USENET_SPOOL_ABORTED')
  );
  assert.deepEqual(await filesBelow(manager.engineRoot), []);
  assert.equal(manager.stats().budget.reservedBytes, 0);
  assert.equal(manager.stats().files.openFiles, 0);
});

test('retains disk accounting until manager cleanup removes a failed creation', async (context) => {
  const controller = new AbortController();
  let failArtifactRemoval = true;
  const fileSystem: Partial<SpoolFileSystem> = {
    open: async (filePath, flags, mode) => {
      const handle = await open(filePath, flags, mode);
      if (flags === 'wx+') controller.abort();
      return {
        read: async (buffer, offset, length, position) => {
          const result = await handle.read(buffer, offset, length, position);
          return { bytesRead: result.bytesRead };
        },
        write: async (buffer, offset, length, position) => {
          const result = await handle.write(buffer, offset, length, position);
          return { bytesWritten: result.bytesWritten };
        },
        close: () => handle.close(),
      };
    },
    rm: async (target, options) => {
      if (failArtifactRemoval && target.endsWith('.partial')) {
        failArtifactRemoval = false;
        const error = new Error('synthetic cleanup permission failure');
        Object.defineProperty(error, 'code', { value: 'EACCES' });
        throw error;
      }
      await rm(target, options);
    },
  };
  const { manager, cacheRoot } = await testManager(context, { fileSystem });

  await assert.rejects(
    manager.createArtifact({
      sessionId: 'cleanup-failure-session',
      segmentId: 'segment',
      initialReservationBytes: 8,
      signal: controller.signal,
    }),
    (error) => isSpoolError(error, 'USENET_SPOOL_UNAVAILABLE')
  );
  assert.equal(manager.stats().budget.reservedBytes, 8);
  assert.equal(manager.stats().artifacts, 1);

  await manager.close();
  assert.equal(manager.stats().budget.reservedBytes, 0);
  assert.equal(manager.stats().artifacts, 0);
  assert.deepEqual(await filesBelow(cacheRoot), []);
});

test('close waits for global initialization before cleaning its namespace', async (context) => {
  const mkdirStarted = Promise.withResolvers<void>();
  const continueMkdir = Promise.withResolvers<void>();
  let firstMkdir = true;
  const fileSystem: Partial<SpoolFileSystem> = {
    mkdir: async (directory, options) => {
      if (firstMkdir) {
        firstMkdir = false;
        mkdirStarted.resolve();
        await continueMkdir.promise;
      }
      await mkdir(directory, options);
    },
  };
  const { manager, cacheRoot } = await testManager(context, { fileSystem });
  const initialization = manager.initialize();
  await mkdirStarted.promise;
  const closing = manager.close();
  continueMkdir.resolve();

  await assert.rejects(initialization, (error) =>
    isSpoolError(error, 'USENET_SPOOL_CLOSED')
  );
  await closing;
  assert.equal(await exists(manager.processRoot), false);
  assert.deepEqual(await filesBelow(cacheRoot), []);
});
