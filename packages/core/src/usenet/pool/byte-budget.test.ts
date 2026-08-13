import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import test from 'node:test';
import { CommandPriority } from '../types.js';
import { ByteBudget, ByteBudgetError, type ByteLease } from './byte-budget.js';

function isBudgetError(error: unknown, code: ByteBudgetError['code']): boolean {
  assert(error instanceof ByteBudgetError);
  assert.equal(error.code, code);
  return true;
}

test('acquires and releases immediately without exceeding the hard cap', async () => {
  const budget = new ByteBudget(10);
  const first = await budget.acquire(4);
  const second = budget.tryAcquire(6);

  assert(second);
  assert.equal(first.bytes, 4);
  assert.equal(second.bytes, 6);
  assert.deepEqual(budget.stats(), {
    maxBytes: 10,
    usedBytes: 10,
    waiting: 0,
    peakBytes: 10,
  });
  assert.equal(budget.tryAcquire(1), null);

  first.release();
  second.release();
  assert.equal(budget.stats().usedBytes, 0);
});

test('serves multiple same-priority waiters in FIFO order', async () => {
  const budget = new ByteBudget(1);
  const owner = await budget.acquire(1);
  const order: number[] = [];
  const firstPromise = budget.acquire(1).then((lease) => {
    order.push(1);
    return lease;
  });
  const secondPromise = budget.acquire(1).then((lease) => {
    order.push(2);
    return lease;
  });
  const thirdPromise = budget.acquire(1).then((lease) => {
    order.push(3);
    return lease;
  });

  assert.equal(budget.stats().waiting, 3);
  owner.release();
  const first = await firstPromise;
  assert.deepEqual(order, [1]);
  first.release();
  const second = await secondPromise;
  assert.deepEqual(order, [1, 2]);
  second.release();
  const third = await thirdPromise;
  assert.deepEqual(order, [1, 2, 3]);
  third.release();
});

test('serves High before Low while contention is below the fairness limit', async () => {
  const budget = new ByteBudget(1);
  const owner = await budget.acquire(1);
  const order: string[] = [];
  const lowPromise = budget
    .acquire(1, { priority: CommandPriority.Low })
    .then((lease) => {
      order.push('low');
      return lease;
    });
  const highPromise = budget
    .acquire(1, { priority: CommandPriority.High })
    .then((lease) => {
      order.push('high');
      return lease;
    });

  owner.release();
  const high = await highPromise;
  assert.deepEqual(order, ['high']);
  high.release();
  const low = await lowPromise;
  assert.deepEqual(order, ['high', 'low']);
  low.release();
});

test('reserves every fourth contended grant for Low to prevent starvation', async () => {
  const budget = new ByteBudget(1);
  const owner = await budget.acquire(1);
  const order: string[] = [];
  const lowPromise = budget
    .acquire(1, { priority: CommandPriority.Low })
    .then((lease) => {
      order.push('low');
      return lease;
    });
  const highPromises = ['high-1', 'high-2', 'high-3', 'high-4'].map((name) =>
    budget.acquire(1, { priority: CommandPriority.High }).then((lease) => {
      order.push(name);
      return lease;
    })
  );

  owner.release();
  for (let index = 0; index < 3; index++) {
    const lease = await highPromises[index];
    assert.equal(order[index], `high-${index + 1}`);
    lease.release();
  }

  const low = await lowPromise;
  assert.deepEqual(order, ['high-1', 'high-2', 'high-3', 'low']);
  low.release();
  const lastHigh = await highPromises[3];
  assert.equal(order[4], 'high-4');
  lastHigh.release();
});

test('grants a fitting Low head when the preferred High head cannot fit', async () => {
  const budget = new ByteBudget(10);
  const owner = await budget.acquire(8);
  const order: string[] = [];
  const highPromise = budget
    .acquire(3, { priority: CommandPriority.High })
    .then((lease) => {
      order.push('high');
      return lease;
    });

  assert.equal(budget.stats().waiting, 1);
  const low = await budget.acquire(2, { priority: CommandPriority.Low });
  order.push('low');
  assert.deepEqual(order, ['low']);
  assert.deepEqual(budget.stats(), {
    maxBytes: 10,
    usedBytes: 10,
    waiting: 1,
    peakBytes: 10,
  });

  low.release();
  assert.equal(budget.stats().waiting, 1);
  owner.release();
  const high = await highPromise;
  assert.deepEqual(order, ['low', 'high']);
  assert(budget.stats().usedBytes <= budget.stats().maxBytes);
  high.release();
});

test('keeps the Low turn reserved after three contended High grants', async () => {
  const budget = new ByteBudget(10);
  const owner = await budget.acquire(8);
  const order: string[] = [];
  const lowPromise = budget
    .acquire(3, { priority: CommandPriority.Low })
    .then((lease) => {
      order.push('low');
      return lease;
    });
  const highPromises = ['high-1', 'high-2', 'high-3', 'high-4'].map((name) =>
    budget.acquire(2, { priority: CommandPriority.High }).then((lease) => {
      order.push(name);
      return lease;
    })
  );

  for (let index = 0; index < 3; index++) {
    const lease = await highPromises[index];
    assert.equal(order[index], `high-${index + 1}`);
    lease.release();
  }

  assert.deepEqual(order, ['high-1', 'high-2', 'high-3']);
  assert.deepEqual(budget.stats(), {
    maxBytes: 10,
    usedBytes: 8,
    waiting: 2,
    peakBytes: 10,
  });

  owner.release();
  const [low, lastHigh] = await Promise.all([lowPromise, highPromises[3]]);
  assert.deepEqual(order, ['high-1', 'high-2', 'high-3', 'low', 'high-4']);
  assert(budget.stats().usedBytes <= budget.stats().maxBytes);
  low.release();
  lastHigh.release();
});

test('cross-priority fallback never bypasses a same-priority head', async () => {
  const budget = new ByteBudget(10);
  const owner = await budget.acquire(8);
  const order: string[] = [];
  const firstHighPromise = budget
    .acquire(3, { priority: CommandPriority.High })
    .then((lease) => {
      order.push('high-1');
      return lease;
    });
  const secondHighPromise = budget
    .acquire(2, { priority: CommandPriority.High })
    .then((lease) => {
      order.push('high-2');
      return lease;
    });

  const low = await budget.acquire(2, { priority: CommandPriority.Low });
  order.push('low');
  assert.deepEqual(order, ['low']);
  assert.equal(budget.stats().waiting, 2);

  low.release();
  assert.deepEqual(order, ['low']);
  assert.equal(budget.stats().waiting, 2);
  owner.release();
  const [firstHigh, secondHigh] = await Promise.all([
    firstHighPromise,
    secondHighPromise,
  ]);
  assert.deepEqual(order, ['low', 'high-1', 'high-2']);
  firstHigh.release();
  secondHigh.release();
});

test('rejects an already-aborted acquire without adding a waiter', async () => {
  const budget = new ByteBudget(1);
  const controller = new AbortController();
  controller.abort();

  await assert.rejects(budget.acquire(1, { signal: controller.signal }), {
    name: 'AbortError',
  });
  assert.equal(budget.stats().waiting, 0);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
});

test('abort removes a queued waiter and its listener completely', async () => {
  const budget = new ByteBudget(1);
  const owner = await budget.acquire(1);
  const controller = new AbortController();
  const pending = budget.acquire(1, { signal: controller.signal });

  assert.equal(budget.stats().waiting, 1);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 1);
  controller.abort();
  await assert.rejects(pending, { name: 'AbortError' });
  assert.equal(budget.stats().waiting, 0);
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);

  owner.release();
  assert.equal(budget.stats().usedBytes, 0);
});

test('grant removes the queued waiter abort listener', async () => {
  const budget = new ByteBudget(1);
  const owner = await budget.acquire(1);
  const controller = new AbortController();
  const pending = budget.acquire(1, { signal: controller.signal });

  assert.equal(getEventListeners(controller.signal, 'abort').length, 1);
  owner.release();
  const lease = await pending;
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  lease.release();
});

test('rejects beyond the configured waiter limit without adding a listener', async () => {
  const budget = new ByteBudget(1, { maxWaiters: 2 });
  const owner = await budget.acquire(1);
  const firstController = new AbortController();
  const secondController = new AbortController();
  const overflowController = new AbortController();
  const first = budget.acquire(1, { signal: firstController.signal });
  const second = budget.acquire(1, { signal: secondController.signal });

  assert.equal(budget.stats().waiting, 2);
  await assert.rejects(
    budget.acquire(1, { signal: overflowController.signal }),
    (error) => isBudgetError(error, 'BYTE_BUDGET_QUEUE_FULL')
  );
  assert.equal(budget.stats().waiting, 2);
  assert.equal(getEventListeners(overflowController.signal, 'abort').length, 0);

  const settledPromise = Promise.allSettled([first, second]);
  budget.close(new Error('test cleanup'));
  await settledPromise;
  owner.release();
});

test('an aborted waiter frees its bounded queue slot', async () => {
  const budget = new ByteBudget(1, { maxWaiters: 1 });
  const owner = await budget.acquire(1);
  const abortedController = new AbortController();
  const aborted = budget.acquire(1, { signal: abortedController.signal });

  assert.equal(budget.stats().waiting, 1);
  abortedController.abort();
  await assert.rejects(aborted, { name: 'AbortError' });
  assert.equal(budget.stats().waiting, 0);
  assert.equal(getEventListeners(abortedController.signal, 'abort').length, 0);

  const replacementController = new AbortController();
  const replacementPromise = budget.acquire(1, {
    signal: replacementController.signal,
  });
  assert.equal(budget.stats().waiting, 1);
  owner.release();
  const replacement = await replacementPromise;
  assert.equal(budget.stats().waiting, 0);
  assert.equal(
    getEventListeners(replacementController.signal, 'abort').length,
    0
  );
  replacement.release();
});

test('grant and close drain bounded queue slots and abort listeners', async () => {
  const budget = new ByteBudget(1, { maxWaiters: 2 });
  const owner = await budget.acquire(1);
  const firstController = new AbortController();
  const secondController = new AbortController();
  const firstPromise = budget.acquire(1, {
    signal: firstController.signal,
  });
  const secondPromise = budget.acquire(1, {
    signal: secondController.signal,
  });

  assert.equal(budget.stats().waiting, 2);
  owner.release();
  const first = await firstPromise;
  assert.equal(budget.stats().waiting, 1);
  assert.equal(getEventListeners(firstController.signal, 'abort').length, 0);
  assert.equal(getEventListeners(secondController.signal, 'abort').length, 1);

  const closeError = new Error('test close');
  const secondRejection = assert.rejects(
    secondPromise,
    (error) => error === closeError
  );
  budget.close(closeError);
  await secondRejection;
  assert.equal(budget.stats().waiting, 0);
  assert.equal(getEventListeners(secondController.signal, 'abort').length, 0);
  first.release();
});

test('close rejects all waiters, removes listeners, and preserves active accounting', async () => {
  const budget = new ByteBudget(2);
  const owner = await budget.acquire(2);
  const controller = new AbortController();
  const first = budget.acquire(1, { signal: controller.signal });
  const second = budget.acquire(2, { priority: CommandPriority.Low });
  const settledPromise = Promise.allSettled([first, second]);
  const closeError = new Error('test close');

  budget.close(closeError);
  const settled = await settledPromise;
  assert.equal(settled[0].status, 'rejected');
  assert.equal(settled[1].status, 'rejected');
  if (settled[0].status === 'rejected') {
    assert.equal(settled[0].reason, closeError);
  }
  if (settled[1].status === 'rejected') {
    assert.equal(settled[1].reason, closeError);
  }
  assert.deepEqual(budget.stats(), {
    maxBytes: 2,
    usedBytes: 2,
    waiting: 0,
    peakBytes: 2,
  });
  assert.equal(getEventListeners(controller.signal, 'abort').length, 0);
  await assert.rejects(budget.acquire(1), (error) => error === closeError);
  assert.throws(
    () => budget.tryAcquire(1),
    (error) => error === closeError
  );

  budget.close(new Error('ignored second close'));
  owner.release();
  assert.equal(budget.stats().usedBytes, 0);
});

test('default close uses the typed closed error for pending and later acquires', async () => {
  const budget = new ByteBudget(1);
  const owner = await budget.acquire(1);
  const pending = budget.acquire(1);
  const pendingRejection = assert.rejects(pending, (error) =>
    isBudgetError(error, 'BYTE_BUDGET_CLOSED')
  );

  budget.close();
  await pendingRejection;
  await assert.rejects(budget.acquire(1), (error) =>
    isBudgetError(error, 'BYTE_BUDGET_CLOSED')
  );
  assert.throws(
    () => budget.tryAcquire(1),
    (error) => isBudgetError(error, 'BYTE_BUDGET_CLOSED')
  );
  owner.release();
});

test('rejects oversized requests immediately with a typed error', async () => {
  const budget = new ByteBudget(10);

  await assert.rejects(budget.acquire(11), (error) =>
    isBudgetError(error, 'BYTE_BUDGET_REQUEST_TOO_LARGE')
  );
  assert.throws(
    () => budget.tryAcquire(11),
    (error) => isBudgetError(error, 'BYTE_BUDGET_REQUEST_TOO_LARGE')
  );
  assert.equal(budget.stats().waiting, 0);
  assert.equal(budget.stats().usedBytes, 0);
});

test('accepts only finite, safe, positive integer byte counts', async () => {
  const invalidValues = [
    0,
    -1,
    1.5,
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.MAX_SAFE_INTEGER + 1,
  ];

  for (const value of invalidValues) {
    assert.throws(
      () => new ByteBudget(value),
      (error) => isBudgetError(error, 'BYTE_BUDGET_INVALID_BYTES')
    );
  }

  const budget = new ByteBudget(10);
  for (const value of invalidValues) {
    await assert.rejects(budget.acquire(value), (error) =>
      isBudgetError(error, 'BYTE_BUDGET_INVALID_BYTES')
    );
    assert.throws(
      () => budget.tryAcquire(value),
      (error) => isBudgetError(error, 'BYTE_BUDGET_INVALID_BYTES')
    );
    assert.throws(
      () => new ByteBudget(10, { maxWaiters: value }),
      (error) => isBudgetError(error, 'BYTE_BUDGET_INVALID_MAX_WAITERS')
    );
  }
});

test('rejects an invalid runtime priority with a typed error', async () => {
  const budget = new ByteBudget(1);
  const options: { priority?: CommandPriority } = {};
  Object.defineProperty(options, 'priority', { value: 99, enumerable: true });

  await assert.rejects(budget.acquire(1, options), (error) =>
    isBudgetError(error, 'BYTE_BUDGET_INVALID_PRIORITY')
  );
  assert.deepEqual(budget.stats(), {
    maxBytes: 1,
    usedBytes: 0,
    waiting: 0,
    peakBytes: 0,
  });
});

test('release is idempotent and peakBytes remains the historical maximum', async () => {
  const budget = new ByteBudget(10);
  const first = await budget.acquire(7);
  first.release();
  first.release();
  const second = await budget.acquire(5);

  assert.deepEqual(budget.stats(), {
    maxBytes: 10,
    usedBytes: 5,
    waiting: 0,
    peakBytes: 7,
  });
  second.release();
  second.release();
  assert.equal(budget.stats().usedBytes, 0);
  assert.equal(budget.stats().peakBytes, 7);
});

test('controlled parallel releases preserve FIFO and never overbook', async () => {
  const budget = new ByteBudget(8);
  const owner = await budget.acquire(8);
  const order: string[] = [];
  const firstAcquired = Promise.withResolvers<void>();
  const secondAcquired = Promise.withResolvers<void>();
  const thirdAcquired = Promise.withResolvers<void>();
  const releaseFirst = Promise.withResolvers<void>();
  const releaseSecond = Promise.withResolvers<void>();
  const releaseThird = Promise.withResolvers<void>();

  const run = async (
    name: string,
    bytes: number,
    acquired: PromiseWithResolvers<void>,
    release: PromiseWithResolvers<void>
  ): Promise<void> => {
    const lease: ByteLease = await budget.acquire(bytes);
    order.push(name);
    assert(budget.stats().usedBytes <= budget.stats().maxBytes);
    acquired.resolve();
    await release.promise;
    lease.release();
  };

  const tasks = [
    run('first', 6, firstAcquired, releaseFirst),
    run('second', 4, secondAcquired, releaseSecond),
    run('third', 2, thirdAcquired, releaseThird),
  ];
  assert.equal(budget.stats().waiting, 3);

  owner.release();
  await firstAcquired.promise;
  assert.deepEqual(order, ['first']);
  assert.deepEqual(budget.stats(), {
    maxBytes: 8,
    usedBytes: 6,
    waiting: 2,
    peakBytes: 8,
  });

  releaseFirst.resolve();
  await Promise.all([secondAcquired.promise, thirdAcquired.promise]);
  assert.deepEqual(order, ['first', 'second', 'third']);
  assert.equal(budget.stats().usedBytes, 6);
  assert.equal(budget.stats().waiting, 0);

  releaseSecond.resolve();
  releaseThird.resolve();
  await Promise.all(tasks);
  assert.equal(budget.stats().usedBytes, 0);
  assert.equal(budget.stats().peakBytes, 8);
});
