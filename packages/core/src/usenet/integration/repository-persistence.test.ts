import assert from 'node:assert/strict';
import test from 'node:test';
import {
  RepositoryPersistenceError,
  RepositoryPersistenceOwner,
} from './repository-persistence.js';

const ignoreError = (): void => undefined;

interface StoredStatus {
  value: 'available' | 'degraded' | 'failed';
  reason?: string;
}

function persistDegraded(status: StoredStatus): void {
  if (status.value !== 'failed') status.value = 'degraded';
}

function persistFailed(status: StoredStatus, reason: string): void {
  status.value = 'failed';
  status.reason = reason;
}

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
  await Promise.all([layoutEntered.promise, holeEntered.promise]);
  assert.deepEqual(calls.toSorted(), ['holes', 'layout']);
  assert.equal(owner.pendingWrites, 0);
  assert.equal(owner.activeWrites, 2);
  assert.equal(settled, false);

  layoutGate.resolve();
  holeGate.resolve();
  await closing;
  assert.equal(owner.activeWrites, 0);
  assert.equal(owner.trackedKeys, 0);
  assert.deepEqual(errors, []);
  assert.equal(
    owner.schedule('late', 0, async () => undefined, ignoreError),
    false
  );
  assert.equal(
    owner.run('late', async () => undefined, ignoreError),
    false
  );
});

test('an active layout patch serializes a newer invalidation', async () => {
  const owner = new RepositoryPersistenceOwner(4, 2);
  const oldEntered = Promise.withResolvers<void>();
  const oldGate = Promise.withResolvers<void>();
  const clearDone = Promise.withResolvers<void>();
  const calls: string[] = [];
  let stored: string | null = 'initial';

  assert.equal(
    owner.run(
      'layout:file',
      async () => {
        oldEntered.resolve();
        await oldGate.promise;
        stored = 'old-layout';
        calls.push('old-layout');
      },
      ignoreError
    ),
    true
  );
  await oldEntered.promise;
  assert.equal(
    owner.run(
      'layout:file',
      async () => {
        stored = null;
        calls.push('clear-layout');
        clearDone.resolve();
      },
      ignoreError
    ),
    true
  );
  await Promise.resolve();
  assert.deepEqual(calls, []);
  assert.equal(owner.activeWrites, 1);
  assert.equal(owner.pendingWrites, 1);

  oldGate.resolve();
  await clearDone.promise;
  assert.deepEqual(calls, ['old-layout', 'clear-layout']);
  assert.equal(stored, null);
  await owner.close();
  assert.equal(owner.trackedKeys, 0);
});

test('hole generations are serialized and finish with the newest value', async () => {
  const owner = new RepositoryPersistenceOwner(4, 2);
  const oldEntered = Promise.withResolvers<void>();
  const oldGate = Promise.withResolvers<void>();
  const newDone = Promise.withResolvers<void>();
  const calls: string[] = [];
  let stored = 'initial';

  owner.run(
    'holes:file',
    async () => {
      oldEntered.resolve();
      await oldGate.promise;
      stored = 'old';
      calls.push('old');
    },
    ignoreError
  );
  await oldEntered.promise;
  assert.equal(
    owner.schedule(
      'holes:file',
      0,
      async () => {
        stored = 'new';
        calls.push('new');
        newDone.resolve();
      },
      ignoreError
    ),
    true
  );
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.deepEqual(calls, []);
  assert.equal(owner.pendingWrites, 1);

  oldGate.resolve();
  await newDone.promise;
  assert.deepEqual(calls, ['old', 'new']);
  assert.equal(stored, 'new');
  await owner.close();
});

test('multiple writes behind one active key coalesce to the latest successor', async () => {
  const owner = new RepositoryPersistenceOwner(2, 1);
  const activeEntered = Promise.withResolvers<void>();
  const activeGate = Promise.withResolvers<void>();
  const latestDone = Promise.withResolvers<void>();
  const calls: string[] = [];

  owner.run(
    'layout:file',
    async () => {
      activeEntered.resolve();
      await activeGate.promise;
      calls.push('active');
    },
    ignoreError
  );
  await activeEntered.promise;
  for (const generation of ['first', 'second', 'latest']) {
    assert.equal(
      owner.run(
        'layout:file',
        async () => {
          calls.push(generation);
          if (generation === 'latest') latestDone.resolve();
        },
        ignoreError
      ),
      true
    );
  }
  assert.equal(owner.pendingWrites, 1);
  assert.equal(owner.trackedKeys, 1);

  activeGate.resolve();
  await latestDone.promise;
  assert.deepEqual(calls, ['active', 'latest']);
  await owner.close();
});

test('status writes for different hashes remain parallel within the active bound', async () => {
  const owner = new RepositoryPersistenceOwner(2, 2);
  const firstEntered = Promise.withResolvers<void>();
  const secondEntered = Promise.withResolvers<void>();
  const firstGate = Promise.withResolvers<void>();
  const secondGate = Promise.withResolvers<void>();

  owner.run(
    'status:failed:first',
    async () => {
      firstEntered.resolve();
      await firstGate.promise;
    },
    ignoreError
  );
  owner.run(
    'status:failed:second',
    async () => {
      secondEntered.resolve();
      await secondGate.promise;
    },
    ignoreError
  );
  await Promise.all([firstEntered.promise, secondEntered.promise]);
  assert.equal(owner.activeWrites, 2);

  firstGate.resolve();
  secondGate.resolve();
  await owner.close();
  assert.equal(owner.activeWrites, 0);
  assert.equal(owner.trackedKeys, 0);
});

test('close waits an active predecessor and its latest successor', async () => {
  const owner = new RepositoryPersistenceOwner(2, 2);
  const activeEntered = Promise.withResolvers<void>();
  const activeGate = Promise.withResolvers<void>();
  const successorEntered = Promise.withResolvers<void>();
  const successorGate = Promise.withResolvers<void>();

  owner.run(
    'holes:file',
    async () => {
      activeEntered.resolve();
      await activeGate.promise;
    },
    ignoreError
  );
  await activeEntered.promise;
  owner.run(
    'holes:file',
    async () => {
      successorEntered.resolve();
      await successorGate.promise;
    },
    ignoreError
  );

  let settled = false;
  const closing = owner.close().then(() => {
    settled = true;
  });
  await Promise.resolve();
  assert.equal(settled, false);
  activeGate.resolve();
  await successorEntered.promise;
  assert.equal(settled, false);
  successorGate.resolve();
  await closing;
  assert.equal(owner.pendingWrites, 0);
  assert.equal(owner.activeWrites, 0);
  assert.equal(owner.trackedKeys, 0);
});

test('a keyed successor error is reported once and aggregated once by close', async () => {
  const owner = new RepositoryPersistenceOwner(1, 1);
  const activeEntered = Promise.withResolvers<void>();
  const activeGate = Promise.withResolvers<void>();
  const failure = new Error('repository write failed');
  const failureObserved = Promise.withResolvers<void>();
  const observed: unknown[] = [];

  owner.run(
    'layout:file',
    async () => {
      activeEntered.resolve();
      await activeGate.promise;
    },
    ignoreError
  );
  await activeEntered.promise;
  assert.equal(
    owner.run(
      'layout:file',
      async () => {
        throw failure;
      },
      (error) => {
        observed.push(error);
        failureObserved.resolve();
      }
    ),
    true
  );
  assert.equal(
    owner.run('layout:other', async () => undefined, ignoreError),
    false
  );

  activeGate.resolve();
  await failureObserved.promise;
  const closing = owner.close();
  await assert.rejects(
    closing,
    (error) =>
      error instanceof AggregateError &&
      error.errors.length === 1 &&
      error.errors[0] === failure
  );
  assert.deepEqual(observed, [failure]);
  assert.equal(owner.pendingWrites, 0);
  assert.equal(owner.activeWrites, 0);
  assert.equal(owner.trackedKeys, 0);
});

test('pending and active persistence structures are hard bounded', async () => {
  const owner = new RepositoryPersistenceOwner(1, 1);
  const gate = Promise.withResolvers<void>();
  assert.equal(
    owner.schedule('one', 60_000, async () => undefined, ignoreError),
    true
  );
  assert.equal(
    owner.schedule('two', 60_000, async () => undefined, ignoreError),
    false
  );
  assert.equal(owner.cancel('one'), true);
  assert.equal(
    owner.run('active', async () => gate.promise, ignoreError),
    true
  );
  assert.equal(
    owner.run('queued', async () => undefined, ignoreError),
    true
  );
  assert.equal(
    owner.run('overflow', async () => undefined, ignoreError),
    false
  );
  gate.resolve();
  await owner.close();
  assert.equal(owner.pendingWrites, 0);
  assert.equal(owner.activeWrites, 0);
  assert.equal(owner.trackedKeys, 0);
});

test('invalid persistence bounds fail with a stable typed error', () => {
  assert.throws(
    () => new RepositoryPersistenceOwner(0, 1),
    (error) =>
      error instanceof RepositoryPersistenceError &&
      error.code === 'USENET_SESSION_PERSISTENCE_CAPACITY'
  );
});

test('a later degraded successor cannot displace a pending failed transition', async () => {
  const owner = new RepositoryPersistenceOwner(4, 1);
  const activeEntered = Promise.withResolvers<void>();
  const activeGate = Promise.withResolvers<void>();
  const status: StoredStatus = { value: 'available' };
  const calls: string[] = [];

  owner.run(
    'status:degraded:release',
    async () => {
      activeEntered.resolve();
      await activeGate.promise;
      persistDegraded(status);
      calls.push('active degraded');
    },
    ignoreError
  );
  await activeEntered.promise;
  assert.equal(
    owner.run(
      'status:failed:release',
      async () => {
        persistFailed(status, 'terminal');
        calls.push('failed');
      },
      ignoreError
    ),
    true
  );
  assert.equal(
    owner.run(
      'status:degraded:release',
      async () => {
        persistDegraded(status);
        calls.push('later degraded');
      },
      ignoreError
    ),
    true
  );

  const closing = owner.close();
  activeGate.resolve();
  await closing;
  assert.equal(status.value, 'failed');
  assert.equal(status.reason, 'terminal');
  assert.deepEqual(calls, ['active degraded', 'later degraded', 'failed']);
});

test('a degraded update cannot resurrect an actively persisted failed status', async () => {
  const owner = new RepositoryPersistenceOwner(2, 1);
  const failedEntered = Promise.withResolvers<void>();
  const failedGate = Promise.withResolvers<void>();
  const status: StoredStatus = { value: 'available' };

  owner.run(
    'status:failed:release',
    async () => {
      failedEntered.resolve();
      await failedGate.promise;
      persistFailed(status, 'terminal');
    },
    ignoreError
  );
  await failedEntered.promise;
  owner.run(
    'status:degraded:release',
    async () => persistDegraded(status),
    ignoreError
  );

  const closing = owner.close();
  failedGate.resolve();
  await closing;
  assert.deepEqual(status, { value: 'failed', reason: 'terminal' });
});

test('terminal status successors stay bounded and retain the newest reason', async () => {
  const owner = new RepositoryPersistenceOwner(2, 1);
  const activeEntered = Promise.withResolvers<void>();
  const activeGate = Promise.withResolvers<void>();
  const status: StoredStatus = { value: 'available' };
  const calls: string[] = [];

  owner.run(
    'status:failed:release',
    async () => {
      activeEntered.resolve();
      await activeGate.promise;
      persistFailed(status, 'first');
      calls.push('first');
    },
    ignoreError
  );
  await activeEntered.promise;
  for (const reason of ['second', 'latest']) {
    assert.equal(
      owner.run(
        'status:failed:release',
        async () => {
          persistFailed(status, reason);
          calls.push(reason);
        },
        ignoreError
      ),
      true
    );
  }
  assert.equal(owner.pendingWrites, 1);
  assert.equal(owner.trackedKeys, 1);

  const closing = owner.close();
  activeGate.resolve();
  await closing;
  assert.deepEqual(calls, ['first', 'latest']);
  assert.deepEqual(status, { value: 'failed', reason: 'latest' });
});

test('parallel sessions preserve failed after a later degradation report', async () => {
  const owner = new RepositoryPersistenceOwner(2, 2);
  const failedEntered = Promise.withResolvers<void>();
  const degradedEntered = Promise.withResolvers<void>();
  const failedGate = Promise.withResolvers<void>();
  const degradedGate = Promise.withResolvers<void>();
  const failedDone = Promise.withResolvers<void>();
  const status: StoredStatus = { value: 'available' };

  owner.run(
    'status:failed:shared-release',
    async () => {
      failedEntered.resolve();
      await failedGate.promise;
      persistFailed(status, 'session-a');
      failedDone.resolve();
    },
    ignoreError
  );
  owner.run(
    'status:degraded:shared-release',
    async () => {
      degradedEntered.resolve();
      await degradedGate.promise;
      persistDegraded(status);
    },
    ignoreError
  );
  await Promise.all([failedEntered.promise, degradedEntered.promise]);

  failedGate.resolve();
  await failedDone.promise;
  degradedGate.resolve();
  await owner.close();
  assert.deepEqual(status, { value: 'failed', reason: 'session-a' });
});

test('close freezes pending writes against later cancellation and admission', async () => {
  const owner = new RepositoryPersistenceOwner(2, 1);
  const entered = Promise.withResolvers<void>();
  const gate = Promise.withResolvers<void>();
  let writes = 0;

  owner.schedule(
    'layout:file',
    60_000,
    async () => {
      writes++;
      entered.resolve();
      await gate.promise;
    },
    ignoreError
  );
  const closing = owner.close();
  assert.equal(owner.cancel('layout:file'), false);
  assert.equal(
    owner.run('layout:file', async () => undefined, ignoreError),
    false
  );
  assert.equal(
    owner.schedule('layout:file', 0, async () => undefined, ignoreError),
    false
  );

  await entered.promise;
  assert.equal(writes, 1);
  gate.resolve();
  await closing;
  assert.equal(owner.pendingWrites, 0);
  assert.equal(owner.activeWrites, 0);
  assert.equal(owner.trackedKeys, 0);
  assert.equal(owner.cancel('layout:file'), false);
});

test('pending layout invalidation atomically replaces its stale patch', async () => {
  const owner = new RepositoryPersistenceOwner(1, 1);
  const clearDone = Promise.withResolvers<void>();
  const calls: string[] = [];

  assert.equal(
    owner.schedule(
      'layout:file',
      60_000,
      async () => {
        calls.push('stale layout');
      },
      ignoreError
    ),
    true
  );
  assert.equal(
    owner.run(
      'layout:file',
      async () => {
        calls.push('clear layout');
        clearDone.resolve();
      },
      ignoreError
    ),
    true
  );
  await clearDone.promise;
  await owner.close();
  assert.deepEqual(calls, ['clear layout']);
});
