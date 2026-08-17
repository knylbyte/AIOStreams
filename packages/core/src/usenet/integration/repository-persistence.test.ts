import assert from 'node:assert/strict';
import test from 'node:test';
import {
  RepositoryPersistenceError,
  RepositoryPersistenceOwner,
} from './repository-persistence.js';

test('close flushes the latest pending layout and hole writes exactly once', async () => {
  const owner = new RepositoryPersistenceOwner(4, 4);
  const layoutGate = Promise.withResolvers<void>();
  const layoutEntered = Promise.withResolvers<void>();
  const holeGate = Promise.withResolvers<void>();
  const holeEntered = Promise.withResolvers<void>();
  const calls: string[] = [];
  const errors: unknown[] = [];

  assert.equal(
    owner.schedule(
      'layout:file',
      60_000,
      async () => {
        calls.push('stale-layout');
      },
      (error) => errors.push(error)
    ),
    true
  );
  assert.equal(
    owner.schedule(
      'layout:file',
      60_000,
      async () => {
        calls.push('layout');
        layoutEntered.resolve();
        await layoutGate.promise;
      },
      (error) => errors.push(error)
    ),
    true
  );
  assert.equal(
    owner.schedule(
      'holes:file',
      60_000,
      async () => {
        calls.push('holes');
        holeEntered.resolve();
        await holeGate.promise;
      },
      (error) => errors.push(error)
    ),
    true
  );

  let settled = false;
  const closing = owner.close().then(() => {
    settled = true;
  });
  await layoutEntered.promise;
  assert.deepEqual(calls, ['layout']);
  assert.equal(owner.pendingWrites, 0);
  assert.equal(owner.activeWrites, 1);
  assert.equal(settled, false);

  layoutGate.resolve();
  await holeEntered.promise;
  assert.deepEqual(calls, ['layout', 'holes']);
  assert.equal(owner.activeWrites, 1);
  holeGate.resolve();
  await closing;
  assert.equal(owner.activeWrites, 0);
  assert.deepEqual(errors, []);
  assert.equal(
    owner.schedule(
      'late',
      0,
      async () => undefined,
      () => {}
    ),
    false
  );
  assert.equal(
    owner.run(
      async () => undefined,
      () => {}
    ),
    false
  );
});

test('close observes an already-started repository failure', async () => {
  const owner = new RepositoryPersistenceOwner(2, 2);
  const entered = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  const failure = new Error('repository write failed');
  const observed: unknown[] = [];
  assert.equal(
    owner.run(
      async () => {
        entered.resolve();
        await gate.promise;
        throw failure;
      },
      (error) => observed.push(error)
    ),
    true
  );
  await entered.promise;

  let settled = false;
  const closing = owner.close().finally(() => {
    settled = true;
  });
  await Promise.resolve();
  assert.equal(settled, false);
  gate.resolve();

  await assert.rejects(
    closing,
    (error) => error instanceof AggregateError && error.errors.includes(failure)
  );
  assert.deepEqual(observed, [failure]);
  assert.equal(owner.pendingWrites, 0);
  assert.equal(owner.activeWrites, 0);
});

test('pending and active persistence structures are hard bounded', async () => {
  const owner = new RepositoryPersistenceOwner(1, 1);
  const gate = Promise.withResolvers<void>();
  assert.equal(
    owner.schedule(
      'one',
      60_000,
      async () => undefined,
      () => {}
    ),
    true
  );
  assert.equal(
    owner.schedule(
      'two',
      60_000,
      async () => undefined,
      () => {}
    ),
    false
  );
  assert.equal(
    owner.run(
      async () => gate.promise,
      () => undefined
    ),
    true
  );
  assert.equal(
    owner.run(
      async () => undefined,
      () => undefined
    ),
    false
  );
  gate.resolve();
  await owner.close();
  assert.equal(owner.pendingWrites, 0);
  assert.equal(owner.activeWrites, 0);
});

test('invalid persistence bounds fail with a stable typed error', () => {
  assert.throws(
    () => new RepositoryPersistenceOwner(0, 1),
    (error) =>
      error instanceof RepositoryPersistenceError &&
      error.code === 'USENET_SESSION_PERSISTENCE_CAPACITY'
  );
});
