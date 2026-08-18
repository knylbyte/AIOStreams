import assert from 'node:assert/strict';
import test from 'node:test';
import { CommandPriority } from '../types.js';
import { PrioritySemaphore } from './priority-semaphore.js';

async function settledTurn(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

test('round-robins contended owners while preserving owner-local FIFO', async () => {
  const semaphore = new PrioritySemaphore(1);
  const initial = await semaphore.acquire(CommandPriority.High, undefined, 'A');
  const grants: string[] = [];
  const acquire = (owner: string, label: string) =>
    semaphore
      .acquire(CommandPriority.High, undefined, owner)
      .then((release) => {
        grants.push(label);
        return release;
      });

  const a1 = acquire('A', 'A1');
  const a2 = acquire('A', 'A2');
  const b1 = acquire('B', 'B1');
  const b2 = acquire('B', 'B2');
  initial();

  const releaseA1 = await a1;
  assert.deepEqual(grants, ['A1']);
  releaseA1();
  const releaseB1 = await b1;
  assert.deepEqual(grants, ['A1', 'B1']);
  releaseB1();
  const releaseA2 = await a2;
  assert.deepEqual(grants, ['A1', 'B1', 'A2']);
  releaseA2();
  const releaseB2 = await b2;
  releaseB2();
  assert.deepEqual(grants, ['A1', 'B1', 'A2', 'B2']);
  assert.equal(semaphore.inUse, 0);
  assert.equal(semaphore.waiting, 0);
});

test('a later stream owner is not queued behind one owner prefetch tail', async () => {
  const semaphore = new PrioritySemaphore(4);
  const occupied = await Promise.all(
    Array.from({ length: 4 }, () =>
      semaphore.acquire(CommandPriority.High, undefined, 'stream-a')
    )
  );
  const grants: string[] = [];
  const queuedA = Array.from({ length: 8 }, (_, index) =>
    semaphore
      .acquire(CommandPriority.High, undefined, 'stream-a')
      .then((release) => {
        grants.push(`A${index}`);
        return release;
      })
  );
  const queuedB = semaphore
    .acquire(CommandPriority.High, undefined, 'stream-b')
    .then((release) => {
      grants.push('B');
      return release;
    });

  occupied[0]();
  const releaseA = await queuedA[0];
  occupied[1]();
  const releaseB = await queuedB;
  assert.deepEqual(grants.slice(0, 2), ['A0', 'B']);

  releaseA();
  releaseB();
  for (const release of occupied.slice(2)) release();
  for (const promise of queuedA.slice(1)) (await promise)();
  assert.equal(semaphore.inUse, 0);
});

test('aborting a sole waiter removes its owner turn completely', async () => {
  const semaphore = new PrioritySemaphore(1);
  const occupied = await semaphore.acquire(
    CommandPriority.High,
    undefined,
    'stream-a'
  );
  const controller = new AbortController();
  const aborted = semaphore.acquire(
    CommandPriority.High,
    controller.signal,
    'stream-b'
  );
  controller.abort();
  await assert.rejects(aborted, /aborted/);
  assert.equal(semaphore.waiting, 0);

  const next = semaphore.acquire(CommandPriority.High, undefined, 'stream-c');
  occupied();
  const release = await next;
  release();
  await settledTurn();
  assert.equal(semaphore.inUse, 0);
  assert.equal(semaphore.waiting, 0);
});

test('high priority remains ahead of low-priority owner fairness', async () => {
  const semaphore = new PrioritySemaphore(1, 0.8);
  const occupied = await semaphore.acquire(
    CommandPriority.High,
    undefined,
    'occupied'
  );
  const order: string[] = [];
  const low = semaphore
    .acquire(CommandPriority.Low, undefined, 'low-owner')
    .then((release) => {
      order.push('low');
      return release;
    });
  const high = semaphore
    .acquire(CommandPriority.High, undefined, 'high-owner')
    .then((release) => {
      order.push('high');
      return release;
    });

  occupied();
  const highRelease = await high;
  assert.deepEqual(order, ['high']);
  highRelease();
  const lowRelease = await low;
  lowRelease();
  assert.deepEqual(order, ['high', 'low']);
});

test('owner waiter capacity is finite and closing cleans every waiter', async () => {
  const semaphore = new PrioritySemaphore(1);
  const occupied = await semaphore.acquire(
    CommandPriority.High,
    undefined,
    'occupied'
  );
  const queued = Array.from({ length: 1024 }, () =>
    semaphore.acquire(CommandPriority.High, undefined, 'bounded-owner')
  );
  await assert.rejects(
    semaphore.acquire(CommandPriority.High, undefined, 'bounded-owner'),
    /owner waiter capacity/
  );
  assert.equal(semaphore.waiting, 1024);
  const closed = new Error('closed');
  semaphore.close(closed);
  const results = await Promise.allSettled(queued);
  assert(results.every((result) => result.status === 'rejected'));
  assert.equal(semaphore.waiting, 0);
  occupied();
});
