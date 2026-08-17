import assert from 'node:assert/strict';
import test from 'node:test';
import {
  CensusShadowOwner,
  CensusShadowOwnerError,
  type CensusShadowHandle,
} from './census-shadow-owner.js';

interface TestSnapshot {
  readonly complete: boolean;
  readonly generation: string;
}

interface ControlledCensus<T> {
  readonly census: {
    readonly done: Promise<T>;
    cancel(): void;
  };
  readonly cancelCalls: () => number;
}

function controlledCensus<T>(done: Promise<T>): ControlledCensus<T> {
  let cancels = 0;
  return {
    census: {
      done,
      cancel: () => {
        cancels++;
      },
    },
    cancelCalls: () => cancels,
  };
}

function requiredHandle(
  handle: CensusShadowHandle | undefined
): CensusShadowHandle {
  assert(handle);
  return handle;
}

function noError(error: unknown): never {
  throw error;
}

test('close fences a completed census before its first repository step', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(2);
  const beforeWrite = Promise.withResolvers<void>();
  const permitWrite = Promise.withResolvers<void>();
  const source = controlledCensus(
    Promise.resolve({ complete: true, generation: 'A' })
  );
  let writes = 0;
  const shadow = requiredHandle(
    owner.spawn({
      nzbHash: 'same-hash',
      census: source.census,
      apply: async (_snapshot, publication) => {
        beforeWrite.resolve();
        await permitWrite.promise;
        await publication.step(async () => {
          writes++;
        });
      },
      onError: noError,
    })
  );

  await beforeWrite.promise;
  let closeSettled = false;
  const closing = owner.close().then(() => {
    closeSettled = true;
  });
  assert.equal(source.cancelCalls(), 1);
  await Promise.resolve();
  assert.equal(closeSettled, false);

  permitWrite.resolve();
  await closing;
  await shadow.done;
  assert.equal(writes, 0);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
});

test('close waits for a repository step that already owns its operation', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(2);
  const writeEntered = Promise.withResolvers<void>();
  const permitWrite = Promise.withResolvers<void>();
  const source = controlledCensus(
    Promise.resolve({ complete: true, generation: 'A' })
  );
  let writes = 0;
  const shadow = requiredHandle(
    owner.spawn({
      nzbHash: 'started-write',
      census: source.census,
      apply: async (_snapshot, publication) => {
        await publication.step(async () => {
          writeEntered.resolve();
          await permitWrite.promise;
          writes++;
        });
      },
      onError: noError,
    })
  );

  await writeEntered.promise;
  let closeSettled = false;
  const closing = owner.close().then(() => {
    closeSettled = true;
  });
  await Promise.resolve();
  assert.equal(closeSettled, false);
  assert.equal(writes, 0);

  permitWrite.resolve();
  await closing;
  await shadow.done;
  assert.equal(writes, 1);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
});

test('a same-hash generation waits for and supersedes its predecessor', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(2);
  const oldBeforeWrite = Promise.withResolvers<void>();
  const permitOld = Promise.withResolvers<void>();
  const oldSource = controlledCensus(
    Promise.resolve({ complete: true, generation: 'old' })
  );
  const newSource = controlledCensus(
    Promise.resolve({ complete: true, generation: 'new' })
  );
  const writes: string[] = [];
  const oldShadow = requiredHandle(
    owner.spawn({
      nzbHash: 'replacement',
      census: oldSource.census,
      apply: async (snapshot, publication) => {
        oldBeforeWrite.resolve();
        await permitOld.promise;
        await publication.step(async () => {
          writes.push(snapshot.generation);
        });
      },
      onError: noError,
    })
  );
  await oldBeforeWrite.promise;

  const newShadow = requiredHandle(
    owner.spawn({
      nzbHash: 'replacement',
      census: newSource.census,
      apply: async (snapshot, publication) => {
        await publication.step(async () => {
          writes.push(snapshot.generation);
        });
      },
      onError: noError,
    })
  );
  assert.equal(oldSource.cancelCalls(), 1);
  assert.equal(owner.currentGenerations, 1);
  assert.equal(owner.activeTasks, 2);

  permitOld.resolve();
  await Promise.all([oldShadow.done, newShadow.done]);
  assert.deepEqual(writes, ['new']);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  await owner.close();
});

test('an incomplete census cancelled by close publishes nothing and is removed', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(1);
  const snapshot = Promise.withResolvers<TestSnapshot>();
  const source = controlledCensus(snapshot.promise);
  let writes = 0;
  const shadow = requiredHandle(
    owner.spawn({
      nzbHash: 'incomplete',
      census: source.census,
      apply: async (value, publication) => {
        if (!value.complete) return;
        await publication.step(async () => {
          writes++;
        });
      },
      onError: noError,
    })
  );

  const closing = owner.close();
  assert.equal(source.cancelCalls(), 1);
  snapshot.resolve({ complete: false, generation: 'incomplete' });
  await Promise.all([shadow.done, closing]);
  assert.equal(writes, 0);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
});

test('close aggregates a crossing repository error after every shadow settles', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(2);
  const firstEntered = Promise.withResolvers<void>();
  const secondEntered = Promise.withResolvers<void>();
  const permitWrites = Promise.withResolvers<void>();
  const repositoryError = Object.assign(new Error('shadow repository EIO'), {
    code: 'EIO',
  });
  const observed: unknown[] = [];
  let successfulWrites = 0;

  const failed = requiredHandle(
    owner.spawn({
      nzbHash: 'failed-write',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'failed' })
      ).census,
      apply: async (_snapshot, publication) => {
        await publication.step(async () => {
          firstEntered.resolve();
          await permitWrites.promise;
          throw repositoryError;
        });
      },
      onError: (error) => observed.push(error),
    })
  );
  const successful = requiredHandle(
    owner.spawn({
      nzbHash: 'successful-write',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'successful' })
      ).census,
      apply: async (_snapshot, publication) => {
        await publication.step(async () => {
          secondEntered.resolve();
          await permitWrites.promise;
          successfulWrites++;
        });
      },
      onError: noError,
    })
  );

  await Promise.all([firstEntered.promise, secondEntered.promise]);
  const closing = owner.close();
  permitWrites.resolve();
  await Promise.all([failed.done, successful.done]);
  await assert.rejects(closing, (error: unknown) => {
    assert(error instanceof AggregateError);
    assert(error.errors.includes(repositoryError));
    return true;
  });
  assert.deepEqual(observed, [repositoryError]);
  assert.equal(successfulWrites, 1);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
});

test('capacity rejects without replacing the current generation or leaking state', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(1);
  const firstSnapshot = Promise.withResolvers<TestSnapshot>();
  const firstSource = controlledCensus(firstSnapshot.promise);
  const first = requiredHandle(
    owner.spawn({
      nzbHash: 'occupied',
      census: firstSource.census,
      apply: async () => undefined,
      onError: noError,
    })
  );
  const rejectionCodes: string[] = [];
  const sameHash = controlledCensus(
    Promise.resolve({ complete: true, generation: 'same-hash' })
  );
  const otherHash = controlledCensus(
    Promise.resolve({ complete: true, generation: 'other-hash' })
  );

  assert.equal(
    owner.spawn({
      nzbHash: 'occupied',
      census: sameHash.census,
      apply: async () => undefined,
      onError: noError,
      onRejected: (error) => rejectionCodes.push(error.code),
    }),
    undefined
  );
  assert.equal(
    owner.spawn({
      nzbHash: 'other',
      census: otherHash.census,
      apply: async () => undefined,
      onError: noError,
      onRejected: (error) => rejectionCodes.push(error.code),
    }),
    undefined
  );
  assert.equal(firstSource.cancelCalls(), 0);
  assert.equal(sameHash.cancelCalls(), 1);
  assert.equal(otherHash.cancelCalls(), 1);
  assert.deepEqual(rejectionCodes, [
    'USENET_CENSUS_SHADOW_CAPACITY',
    'USENET_CENSUS_SHADOW_CAPACITY',
  ]);
  assert.equal(owner.activeTasks, 1);
  assert.equal(owner.currentGenerations, 1);

  firstSnapshot.resolve({ complete: true, generation: 'first' });
  await first.done;
  await owner.close();
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);

  const afterClose = controlledCensus(
    Promise.resolve({ complete: true, generation: 'closed' })
  );
  let closedError: CensusShadowOwnerError | undefined;
  assert.equal(
    owner.spawn({
      nzbHash: 'closed',
      census: afterClose.census,
      apply: async () => undefined,
      onError: noError,
      onRejected: (error) => {
        closedError = error;
      },
    }),
    undefined
  );
  assert(closedError instanceof CensusShadowOwnerError);
  assert.equal(closedError.code, 'USENET_CENSUS_SHADOW_CLOSED');
  assert.equal(afterClose.cancelCalls(), 1);
  assert.equal(owner.activeTasks, 0);
});
