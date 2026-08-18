import assert from 'node:assert/strict';
import test from 'node:test';
import {
  CensusShadowOwner,
  CensusShadowOwnerError,
  publishCensusShadowMutations,
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
  assert.equal(owner.retirementTails, 0);
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
  assert.equal(owner.retirementTails, 0);
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
  assert.equal(owner.retirementTails, 0);
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
  assert.equal(owner.retirementTails, 0);
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
  assert.equal(owner.retirementTails, 0);
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
  assert.equal(owner.retirementTails, 0);

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
  assert.equal(owner.retirementTails, 0);
});

test('handle cancel retains a same-hash retirement tail until the old write settles', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(2);
  const oldWriteEntered = Promise.withResolvers<void>();
  const permitOldWrite = Promise.withResolvers<void>();
  const writes: string[] = [];
  const oldSource = controlledCensus(
    Promise.resolve({ complete: true, generation: 'old' })
  );
  const oldShadow = requiredHandle(
    owner.spawn({
      nzbHash: 'cancel-tail',
      census: oldSource.census,
      apply: async (snapshot, publication) => {
        await publication.step(async () => {
          oldWriteEntered.resolve();
          await permitOldWrite.promise;
          writes.push(snapshot.generation);
        });
      },
      onError: noError,
    })
  );
  await oldWriteEntered.promise;

  oldShadow.cancel();
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 1);
  const newShadow = requiredHandle(
    owner.spawn({
      nzbHash: 'cancel-tail',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'new' })
      ).census,
      apply: async (snapshot, publication) => {
        await publication.step(async () => {
          writes.push(snapshot.generation);
        });
      },
      onError: noError,
    })
  );
  let newSettled = false;
  void newShadow.done.then(() => {
    newSettled = true;
  });
  await Promise.resolve();
  assert.equal(newSettled, false);
  assert.deepEqual(writes, []);

  permitOldWrite.resolve();
  await Promise.all([oldShadow.done, newShadow.done]);
  assert.deepEqual(writes, ['old', 'new']);
  assert.equal(oldSource.cancelCalls(), 1);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
  await owner.close();
});

test('concurrent invalidations share the complete retirement tail', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(1);
  const writeEntered = Promise.withResolvers<void>();
  const permitWrite = Promise.withResolvers<void>();
  let writes = 0;
  const shadow = requiredHandle(
    owner.spawn({
      nzbHash: 'double-invalidate',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'old' })
      ).census,
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

  let firstSettled = false;
  let secondSettled = false;
  const first = owner.invalidate('double-invalidate').then(() => {
    firstSettled = true;
  });
  const second = owner.invalidate('double-invalidate').then(() => {
    secondSettled = true;
  });
  await Promise.resolve();
  assert.equal(firstSettled, false);
  assert.equal(secondSettled, false);
  assert.equal(owner.retirementTails, 1);

  permitWrite.resolve();
  await Promise.all([first, second, shadow.done]);
  assert.equal(writes, 1);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
  await owner.close();
});

test('three same-hash replacements retain one bounded tail and only the newest publishes', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(3);
  const oldApplyEntered = Promise.withResolvers<void>();
  const permitOldApply = Promise.withResolvers<void>();
  const writes: string[] = [];
  const spawn = (generation: string): CensusShadowHandle =>
    requiredHandle(
      owner.spawn({
        nzbHash: 'triple-replacement',
        census: controlledCensus(
          Promise.resolve({ complete: true, generation })
        ).census,
        apply: async (snapshot, publication) => {
          if (snapshot.generation === 'A') {
            oldApplyEntered.resolve();
            await permitOldApply.promise;
          }
          await publication.step(async () => {
            writes.push(snapshot.generation);
          });
        },
        onError: noError,
      })
    );

  const first = spawn('A');
  await oldApplyEntered.promise;
  const second = spawn('B');
  const third = spawn('C');
  assert.equal(owner.activeTasks, 3);
  assert.equal(owner.currentGenerations, 1);
  assert.equal(owner.retirementTails, 1);

  permitOldApply.resolve();
  await Promise.all([first.done, second.done, third.done]);
  assert.deepEqual(writes, ['C']);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
  await owner.close();
});

test('close waits an externally cancelled retirement tail without deadlock', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(1);
  const writeEntered = Promise.withResolvers<void>();
  const permitWrite = Promise.withResolvers<void>();
  const shadow = requiredHandle(
    owner.spawn({
      nzbHash: 'cancelled-close-tail',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'old' })
      ).census,
      apply: async (_snapshot, publication) => {
        await publication.step(async () => {
          writeEntered.resolve();
          await permitWrite.promise;
        });
      },
      onError: noError,
    })
  );
  await writeEntered.promise;
  shadow.cancel();

  let closeSettled = false;
  const closing = owner.close().then(() => {
    closeSettled = true;
  });
  await Promise.resolve();
  assert.equal(closeSettled, false);
  assert.equal(owner.retirementTails, 1);

  permitWrite.resolve();
  await Promise.all([shadow.done, closing]);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
});

test('close during the first feedback key prevents the second key from starting', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(1);
  const firstEntered = Promise.withResolvers<void>();
  const permitFirst = Promise.withResolvers<void>();
  const feedback: string[] = [];
  const shadow = requiredHandle(
    owner.spawn({
      nzbHash: 'dead-feedback-close',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'old' })
      ).census,
      apply: async (_snapshot, publication) => {
        await publishCensusShadowMutations(
          publication,
          ['dead-key-1', 'dead-key-2'],
          async (key) => {
            if (key === 'dead-key-1') {
              firstEntered.resolve();
              await permitFirst.promise;
            }
            feedback.push(key);
          }
        );
      },
      onError: noError,
    })
  );
  await firstEntered.promise;

  let closeSettled = false;
  const closing = owner.close().then(() => {
    closeSettled = true;
  });
  await Promise.resolve();
  assert.equal(closeSettled, false);
  assert.deepEqual(feedback, []);

  permitFirst.resolve();
  await Promise.all([shadow.done, closing]);
  assert.deepEqual(feedback, ['dead-key-1']);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.retirementTails, 0);
});

test('same-hash invalidation stops old retract keys and gates the new generation', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(2);
  const firstEntered = Promise.withResolvers<void>();
  const permitFirst = Promise.withResolvers<void>();
  const feedback: string[] = [];
  const oldShadow = requiredHandle(
    owner.spawn({
      nzbHash: 'retract-feedback-reimport',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'old' })
      ).census,
      apply: async (_snapshot, publication) => {
        await publishCensusShadowMutations(
          publication,
          ['old-retract-1', 'old-retract-2'],
          async (key) => {
            if (key === 'old-retract-1') {
              firstEntered.resolve();
              await permitFirst.promise;
            }
            feedback.push(key);
          }
        );
      },
      onError: noError,
    })
  );
  await firstEntered.promise;

  const invalidating = owner.invalidate('retract-feedback-reimport');
  const newShadow = requiredHandle(
    owner.spawn({
      nzbHash: 'retract-feedback-reimport',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'new' })
      ).census,
      apply: async (_snapshot, publication) => {
        await publishCensusShadowMutations(
          publication,
          ['new-retract-1', 'new-retract-2'],
          async (key) => {
            feedback.push(key);
          }
        );
      },
      onError: noError,
    })
  );
  await Promise.resolve();
  assert.deepEqual(feedback, []);

  permitFirst.resolve();
  await Promise.all([invalidating, oldShadow.done, newShadow.done]);
  assert.deepEqual(feedback, [
    'old-retract-1',
    'new-retract-1',
    'new-retract-2',
  ]);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
  await owner.close();
});

test('an uninterrupted generation persists both feedback keys exactly once', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(1);
  const feedback: string[] = [];
  const shadow = requiredHandle(
    owner.spawn({
      nzbHash: 'feedback-success',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'current' })
      ).census,
      apply: async (_snapshot, publication) => {
        await publishCensusShadowMutations(
          publication,
          ['feedback-key-1', 'feedback-key-2'],
          async (key) => {
            feedback.push(key);
          }
        );
      },
      onError: noError,
    })
  );

  await shadow.done;
  assert.deepEqual(feedback, ['feedback-key-1', 'feedback-key-2']);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
  await owner.close();
});

test('a cancelled successor retains capacity and its tail until its own census settles', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(2);
  const firstSnapshot = Promise.withResolvers<TestSnapshot>();
  const secondSnapshot = Promise.withResolvers<TestSnapshot>();
  const firstSource = controlledCensus(firstSnapshot.promise);
  const secondSource = controlledCensus(secondSnapshot.promise);
  const first = requiredHandle(
    owner.spawn({
      nzbHash: 'cancelled-successor',
      census: firstSource.census,
      apply: async () => undefined,
      onError: noError,
    })
  );
  const second = requiredHandle(
    owner.spawn({
      nzbHash: 'cancelled-successor',
      census: secondSource.census,
      apply: async () => undefined,
      onError: noError,
    })
  );
  second.cancel();
  let secondSettled = false;
  let closeSettled = false;
  void second.done.then(() => {
    secondSettled = true;
  });
  const closing = owner.close().then(() => {
    closeSettled = true;
  });

  firstSnapshot.resolve({ complete: false, generation: 'A' });
  await first.done;
  await Promise.resolve();
  assert.equal(secondSettled, false);
  assert.equal(closeSettled, false);
  assert.equal(owner.activeTasks, 1);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 1);
  assert.equal(firstSource.cancelCalls(), 1);
  assert.equal(secondSource.cancelCalls(), 1);

  secondSnapshot.resolve({ complete: false, generation: 'B' });
  await Promise.all([second.done, closing]);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
});

test('a third generation waits for a cancelled successor census before publishing', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(3);
  const firstSnapshot = Promise.withResolvers<TestSnapshot>();
  const secondSnapshot = Promise.withResolvers<TestSnapshot>();
  const writes: string[] = [];
  const first = requiredHandle(
    owner.spawn({
      nzbHash: 'three-census-generations',
      census: controlledCensus(firstSnapshot.promise).census,
      apply: async () => undefined,
      onError: noError,
    })
  );
  const second = requiredHandle(
    owner.spawn({
      nzbHash: 'three-census-generations',
      census: controlledCensus(secondSnapshot.promise).census,
      apply: async () => undefined,
      onError: noError,
    })
  );
  second.cancel();
  const third = requiredHandle(
    owner.spawn({
      nzbHash: 'three-census-generations',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'C' })
      ).census,
      apply: async (snapshot, publication) => {
        await publication.step(async () => {
          writes.push(snapshot.generation);
        });
      },
      onError: noError,
    })
  );

  firstSnapshot.resolve({ complete: false, generation: 'A' });
  await first.done;
  await Promise.resolve();
  assert.deepEqual(writes, []);
  assert.equal(owner.activeTasks, 2);
  assert.equal(owner.retirementTails, 1);

  secondSnapshot.resolve({ complete: false, generation: 'B' });
  await Promise.all([second.done, third.done]);
  assert.deepEqual(writes, ['C']);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
  await owner.close();
});

test('a cancelled census does not free bounded shadow capacity before settlement', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(1);
  const firstSnapshot = Promise.withResolvers<TestSnapshot>();
  const first = requiredHandle(
    owner.spawn({
      nzbHash: 'capacity-until-census-done',
      census: controlledCensus(firstSnapshot.promise).census,
      apply: async () => undefined,
      onError: noError,
    })
  );
  first.cancel();
  const rejected = controlledCensus(
    Promise.resolve({ complete: false, generation: 'rejected' })
  );
  let rejection: CensusShadowOwnerError | undefined;

  assert.equal(
    owner.spawn({
      nzbHash: 'capacity-rejected',
      census: rejected.census,
      apply: async () => undefined,
      onError: noError,
      onRejected: (error) => {
        rejection = error;
      },
    }),
    undefined
  );
  assert.equal(rejection?.code, 'USENET_CENSUS_SHADOW_CAPACITY');
  assert.equal(rejected.cancelCalls(), 1);
  assert.equal(owner.activeTasks, 1);

  firstSnapshot.resolve({ complete: false, generation: 'first' });
  await first.done;
  const accepted = requiredHandle(
    owner.spawn({
      nzbHash: 'capacity-released',
      census: controlledCensus(
        Promise.resolve({ complete: true, generation: 'accepted' })
      ).census,
      apply: async () => undefined,
      onError: noError,
    })
  );
  await accepted.done;
  await owner.close();
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.retirementTails, 0);
});

test('owner close awaits a cancelled census finalizer with no repository work', async () => {
  const owner = new CensusShadowOwner<TestSnapshot>(1);
  const snapshot = Promise.withResolvers<TestSnapshot>();
  const source = controlledCensus(snapshot.promise);
  const shadow = requiredHandle(
    owner.spawn({
      nzbHash: 'close-census-finalizer',
      census: source.census,
      apply: async () => undefined,
      onError: noError,
    })
  );
  let closeSettled = false;
  const closing = owner.close().then(() => {
    closeSettled = true;
  });

  await Promise.resolve();
  assert.equal(closeSettled, false);
  assert.equal(source.cancelCalls(), 1);
  assert.equal(owner.activeTasks, 1);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 1);

  snapshot.resolve({ complete: false, generation: 'cancelled' });
  await Promise.all([shadow.done, closing]);
  assert.equal(owner.activeTasks, 0);
  assert.equal(owner.currentGenerations, 0);
  assert.equal(owner.retirementTails, 0);
});
