import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import test from 'node:test';
import { CommandPriority } from '../types.js';
import { SegmentSpoolingHotpathCounters } from './hotpath-counters.js';
import {
  MAX_ACTIVE_SEMAPHORE_OWNERS,
  PrioritySemaphore,
  PrioritySemaphoreError,
  type PrioritySemaphoreErrorCode,
} from './priority-semaphore.js';

async function settledTurn(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

function hasCode(code: PrioritySemaphoreErrorCode) {
  return (error: unknown): boolean => {
    assert(error instanceof PrioritySemaphoreError);
    assert.equal(error.code, code);
    if (
      code === 'SEMAPHORE_GLOBAL_CAPACITY' ||
      code === 'SEMAPHORE_OWNER_CAPACITY' ||
      code === 'SEMAPHORE_ACTIVE_OWNER_CAPACITY'
    ) {
      assert.equal(error.faultDomain, 'local');
    }
    return true;
  };
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
  const c1 = acquire('C', 'C1');
  const c2 = acquire('C', 'C2');
  initial();

  const releaseA1 = await a1;
  assert.deepEqual(grants, ['A1']);
  releaseA1();
  const releaseB1 = await b1;
  assert.deepEqual(grants, ['A1', 'B1']);
  releaseB1();
  const releaseC1 = await c1;
  assert.deepEqual(grants, ['A1', 'B1', 'C1']);
  releaseC1();
  const releaseA2 = await a2;
  assert.deepEqual(grants, ['A1', 'B1', 'C1', 'A2']);
  releaseA2();
  const releaseB2 = await b2;
  releaseB2();
  const releaseC2 = await c2;
  releaseC2();
  assert.deepEqual(grants, ['A1', 'B1', 'C1', 'A2', 'B2', 'C2']);
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

test('a later playback first-byte turn precedes an existing low-priority prefetch tail', async () => {
  const semaphore = new PrioritySemaphore(1, 0.8);
  const occupied = await semaphore.acquire(
    CommandPriority.High,
    undefined,
    'stream-a'
  );
  const grants: string[] = [];
  const prefetch = Array.from({ length: 8 }, (_, index) =>
    semaphore
      .acquire(CommandPriority.Low, undefined, 'stream-a')
      .then((release) => {
        grants.push(`A-prefetch-${index}`);
        return release;
      })
  );
  const streamB = semaphore
    .acquire(CommandPriority.High, undefined, 'stream-b')
    .then((release) => {
      grants.push('B-first-byte-admission');
      return release;
    });

  occupied();
  const releaseB = await streamB;
  assert.deepEqual(grants, ['B-first-byte-admission']);
  releaseB();
  for (const pending of prefetch) (await pending)();
  assert.equal(semaphore.waiting, 0);
  assert.equal(semaphore.activeOwners, 0);
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
    hasCode('SEMAPHORE_OWNER_CAPACITY')
  );
  assert.equal(semaphore.waiting, 1024);
  const closed = new Error('closed');
  semaphore.close(closed);
  const results = await Promise.allSettled(queued);
  assert(results.every((result) => result.status === 'rejected'));
  assert.equal(semaphore.waiting, 0);
  occupied();
});

test('the 128th queued owner is accepted and the 129th is rejected without mutation', async () => {
  const semaphore = new PrioritySemaphore(1);
  const occupied = await semaphore.acquire(CommandPriority.High);
  const queued = Array.from(
    { length: MAX_ACTIVE_SEMAPHORE_OWNERS },
    (_, index) =>
      semaphore.acquire(CommandPriority.High, undefined, `owner-${index}`)
  );
  assert.equal(semaphore.activeOwners, MAX_ACTIVE_SEMAPHORE_OWNERS);
  assert.equal(semaphore.waiting, MAX_ACTIVE_SEMAPHORE_OWNERS);

  const controller = new AbortController();
  await assert.rejects(
    semaphore.acquire(
      CommandPriority.High,
      controller.signal,
      'owner-overflow'
    ),
    hasCode('SEMAPHORE_ACTIVE_OWNER_CAPACITY')
  );
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  assert.equal(semaphore.activeOwners, MAX_ACTIVE_SEMAPHORE_OWNERS);
  assert.equal(semaphore.waiting, MAX_ACTIVE_SEMAPHORE_OWNERS);

  semaphore.close();
  const results = await Promise.allSettled(queued);
  assert(results.every((result) => result.status === 'rejected'));
  assert.equal(semaphore.activeOwners, 0);
  assert.equal(semaphore.waiting, 0);
  occupied();
});

test('global and per-owner limits reject +1 and cleanup restores capacity', async () => {
  const perOwner = new PrioritySemaphore(1, 1, {
    maxWaiters: 4,
    maxWaitersPerOwner: 2,
    maxActiveOwners: 4,
  });
  const occupiedPerOwner = await perOwner.acquire();
  const ownerWaiters = [
    perOwner.acquire(CommandPriority.High, undefined, 'A'),
    perOwner.acquire(CommandPriority.High, undefined, 'A'),
  ];
  await assert.rejects(
    perOwner.acquire(CommandPriority.High, undefined, 'A'),
    hasCode('SEMAPHORE_OWNER_CAPACITY')
  );
  perOwner.close();
  await Promise.allSettled(ownerWaiters);
  occupiedPerOwner();

  const global = new PrioritySemaphore(1, 1, {
    maxWaiters: 4,
    maxWaitersPerOwner: 2,
    maxActiveOwners: 4,
  });
  const occupiedGlobal = await global.acquire();
  const globalWaiters = ['A', 'A', 'B', 'B'].map((owner) =>
    global.acquire(CommandPriority.High, undefined, owner)
  );
  await assert.rejects(
    global.acquire(CommandPriority.High, undefined, 'C'),
    hasCode('SEMAPHORE_GLOBAL_CAPACITY')
  );
  global.close();
  await Promise.allSettled(globalWaiters);
  occupiedGlobal();

  const reusable = new PrioritySemaphore(1, 1, {
    maxWaiters: 1,
    maxWaitersPerOwner: 1,
    maxActiveOwners: 1,
  });
  const occupiedReusable = await reusable.acquire();
  const abortController = new AbortController();
  const aborted = reusable.acquire(
    CommandPriority.High,
    abortController.signal,
    'A'
  );
  abortController.abort();
  await assert.rejects(aborted, hasCode('SEMAPHORE_ABORTED'));
  assert.equal(reusable.waiting, 0);
  assert.equal(reusable.activeOwners, 0);
  const replacement = reusable.acquire(CommandPriority.High, undefined, 'B');
  occupiedReusable();
  (await replacement)();
});

test('abort removes head, middle, tail, cursor and anonymous owners without ring corruption', async () => {
  const semaphore = new PrioritySemaphore(1);
  const occupied = await semaphore.acquire();
  const order: string[] = [];
  const controllers = Array.from({ length: 5 }, () => new AbortController());
  const queue = (owner: string, label: string, controller?: AbortController) =>
    semaphore
      .acquire(CommandPriority.High, controller?.signal, owner)
      .then((release) => {
        order.push(label);
        return release;
      });
  const abortedHead = queue('A', 'A1', controllers[0]);
  const a2 = queue('A', 'A2');
  const abortedMiddle = queue('A', 'A3', controllers[1]);
  const a4 = queue('A', 'A4');
  const abortedTail = queue('A', 'A5', controllers[2]);
  const abortedCursor = queue('B', 'B1', controllers[3]);
  const b2 = queue('B', 'B2');
  const abortedAnonymous = queue('', 'anonymous', controllers[4]);
  for (const controller of controllers) controller.abort();
  await Promise.all([
    assert.rejects(abortedHead, hasCode('SEMAPHORE_ABORTED')),
    assert.rejects(abortedMiddle, hasCode('SEMAPHORE_ABORTED')),
    assert.rejects(abortedTail, hasCode('SEMAPHORE_ABORTED')),
    assert.rejects(abortedCursor, hasCode('SEMAPHORE_ABORTED')),
    assert.rejects(abortedAnonymous, hasCode('SEMAPHORE_ABORTED')),
  ]);

  occupied();
  const releaseA2 = await a2;
  releaseA2();
  const releaseB2 = await b2;
  releaseB2();
  const releaseA4 = await a4;
  releaseA4();
  assert.deepEqual(order, ['A2', 'B2', 'A4']);
  assert.equal(semaphore.waiting, 0);
  assert.equal(semaphore.activeOwners, 0);
  assert(
    controllers.every(
      (controller) => getEventListeners(controller.signal, 'abort').length === 0
    )
  );
});

test('instrumented contention performs no global waiter scans', async () => {
  const counters = new SegmentSpoolingHotpathCounters();
  const semaphore = new PrioritySemaphore(1, 0.8, {
    maxWaiters: 512,
    maxWaitersPerOwner: 8,
    maxActiveOwners: 128,
    hotpathCounters: counters,
  });
  const occupied = await semaphore.acquire();
  const controllers = Array.from({ length: 64 }, () => new AbortController());
  const queued = controllers.flatMap((controller, owner) =>
    Array.from({ length: 4 }, () =>
      semaphore
        .acquire(
          owner % 2 === 0 ? CommandPriority.High : CommandPriority.Low,
          controller.signal,
          `owner-${owner}`
        )
        .then((release) => release())
    )
  );
  for (let index = 0; index < controllers.length; index += 3) {
    controllers[index].abort();
  }
  occupied();
  await Promise.allSettled(queued);
  const snapshot = counters.snapshot();
  assert.equal(snapshot.semaphoreGlobalScans, 0);
  assert(snapshot.semaphoreOwnerCountPeak > 0);
  assert(snapshot.semaphoreWaiterCountPeak > 0);
  assert(snapshot.semaphoreGrants > 0);
  assert(snapshot.semaphoreAborts > 0);
  assert(snapshot.semaphoreOwnerTurns > 0);
  assert.equal(semaphore.waiting, 0);
  assert.equal(semaphore.activeOwners, 0);
});

test('close and future acquires expose the typed closed contract', async () => {
  const semaphore = new PrioritySemaphore(1);
  const occupied = await semaphore.acquire();
  const pending = semaphore.acquire();
  semaphore.close();
  await assert.rejects(pending, hasCode('SEMAPHORE_CLOSED'));
  await assert.rejects(semaphore.acquire(), hasCode('SEMAPHORE_CLOSED'));
  occupied();
});
