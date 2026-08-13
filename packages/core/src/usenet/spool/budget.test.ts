import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import test from 'node:test';
import { SpoolBudget } from './budget.js';
import { UsenetSpoolError } from './errors.js';

function isSpoolError(error: unknown, code: UsenetSpoolError['code']): boolean {
  assert(error instanceof UsenetSpoolError);
  assert.equal(error.code, code);
  return true;
}

test('reserves, grows, records, and idempotently releases disk budget', async () => {
  let statfsCalls = 0;
  const budget = new SpoolBudget({
    maxBytes: 10,
    minFreeDiskBytes: 20,
    statfs: async () => {
      statfsCalls++;
      return { bavail: 100, bsize: 1 };
    },
  });

  const lease = await budget.reserve(6);
  await lease.grow(4);
  lease.recordWritten(7);
  assert.equal(lease.reservedBytes, 10);
  assert.equal(lease.writtenBytes, 7);
  assert.equal(statfsCalls, 2);
  assert.deepEqual(budget.stats(), {
    maxBytes: 10,
    reservedBytes: 10,
    actualBytes: 7,
    peakReservedBytes: 10,
    peakActualBytes: 7,
    waiting: 0,
  });

  lease.release();
  lease.release();
  assert.deepEqual(budget.stats(), {
    maxBytes: 10,
    reservedBytes: 0,
    actualBytes: 0,
    peakReservedBytes: 10,
    peakActualBytes: 7,
    waiting: 0,
  });
});

test('waits at the hard cap and grants FIFO after release', async () => {
  const budget = new SpoolBudget({
    maxBytes: 10,
    minFreeDiskBytes: 0,
    statfs: async () => ({ bavail: 100, bsize: 1 }),
  });
  const owner = await budget.reserve(10);
  const firstPromise = budget.reserve(6);
  const secondPromise = budget.reserve(4);
  assert.equal(budget.stats().waiting, 2);

  owner.release();
  const first = await firstPromise;
  const second = await secondPromise;
  assert.equal(first.reservedBytes, 6);
  assert.equal(second.reservedBytes, 4);
  assert.equal(budget.stats().reservedBytes, 10);
  first.release();
  second.release();
});

test('enforces min-free-disk against outstanding unwritten reservations', async () => {
  let statfsCalls = 0;
  const budget = new SpoolBudget({
    maxBytes: 100,
    minFreeDiskBytes: 50,
    statfs: async () => {
      statfsCalls++;
      return { bavail: 100, bsize: 1 };
    },
  });

  const first = await budget.reserve(30);
  await assert.rejects(budget.reserve(21), (error) =>
    isSpoolError(error, 'USENET_SPOOL_DISK_FULL')
  );
  assert.equal(statfsCalls, 2);
  assert.equal(budget.stats().reservedBytes, 30);
  first.release();
});

test('abort removes a pending reservation and listener completely', async () => {
  const budget = new SpoolBudget({
    maxBytes: 1,
    minFreeDiskBytes: 0,
    statfs: async () => ({ bavail: 100, bsize: 1 }),
  });
  const owner = await budget.reserve(1);
  const controller = new AbortController();
  const pending = budget.reserve(1, { signal: controller.signal });

  assert.equal(budget.stats().waiting, 1);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 1);
  controller.abort();
  await assert.rejects(pending, (error) =>
    isSpoolError(error, 'USENET_SPOOL_ABORTED')
  );
  assert.equal(budget.stats().waiting, 0);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  owner.release();
});

test('rejects oversized and invalid budget requests without accounting', async () => {
  const budget = new SpoolBudget({
    maxBytes: 10,
    minFreeDiskBytes: 0,
    statfs: async () => ({ bavail: 100, bsize: 1 }),
  });

  await assert.rejects(budget.reserve(11), (error) =>
    isSpoolError(error, 'USENET_SPOOL_CAPACITY')
  );
  for (const invalid of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    await assert.rejects(budget.reserve(invalid), (error) =>
      isSpoolError(error, 'USENET_SPOOL_INVALID_ARGUMENT')
    );
  }
  assert.equal(budget.stats().reservedBytes, 0);
  assert.equal(budget.stats().waiting, 0);
});

test('bounds pending reservations and reuses a slot after abort', async () => {
  const budget = new SpoolBudget({
    maxBytes: 1,
    minFreeDiskBytes: 0,
    maxWaiters: 1,
    statfs: async () => ({ bavail: 100, bsize: 1 }),
  });
  const owner = await budget.reserve(1);
  const controller = new AbortController();
  const first = budget.reserve(1, { signal: controller.signal });

  await assert.rejects(budget.reserve(1), (error) =>
    isSpoolError(error, 'USENET_SPOOL_CAPACITY')
  );
  assert.equal(budget.stats().waiting, 1);

  controller.abort();
  await assert.rejects(first, (error) =>
    isSpoolError(error, 'USENET_SPOOL_ABORTED')
  );
  const replacement = budget.reserve(1);
  assert.equal(budget.stats().waiting, 1);
  owner.release();
  (await replacement).release();
  assert.equal(budget.stats().waiting, 0);
});

test('close rejects pending and future reservations without leaking accounting', async () => {
  const budget = new SpoolBudget({
    maxBytes: 1,
    minFreeDiskBytes: 0,
    statfs: async () => ({ bavail: 100, bsize: 1 }),
  });
  const owner = await budget.reserve(1);
  const pending = budget.reserve(1);
  budget.close();

  await assert.rejects(pending, (error) =>
    isSpoolError(error, 'USENET_SPOOL_CLOSED')
  );
  await assert.rejects(budget.reserve(1), (error) =>
    isSpoolError(error, 'USENET_SPOOL_CLOSED')
  );
  owner.release();
  assert.equal(budget.stats().reservedBytes, 0);
  assert.equal(budget.stats().waiting, 0);
});
