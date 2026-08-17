import assert from 'node:assert/strict';
import test from 'node:test';
import {
  BoundedOpeningFlights,
  OpeningFlightError,
} from './opening-flights.js';

test('one aborted request leaves a shared opening flight running for another waiter', async () => {
  const flights = new BoundedOpeningFlights<number>(2, 2);
  const started = Promise.withResolvers<void>();
  const proceed = Promise.withResolvers<number>();
  const firstController = new AbortController();
  let starts = 0;
  const start = async (): Promise<number> => {
    starts++;
    started.resolve();
    return proceed.promise;
  };

  const first = flights.run('shared', start, [firstController.signal]);
  const second = flights.run('shared', start);
  await started.promise;
  const stopped = new Error('first request stopped');
  firstController.abort(stopped);

  await assert.rejects(first, (error) => error === stopped);
  assert.equal(flights.activeFlights, 1);
  assert.equal(flights.waitingRequests, 1);
  assert.equal(starts, 1);

  proceed.resolve(42);
  assert.equal(await second, 42);
  await Promise.resolve();
  assert.equal(flights.activeFlights, 0);
  assert.equal(flights.waitingRequests, 0);
});

test('process close aborts one shared task and rejects all request waiters', async () => {
  const flights = new BoundedOpeningFlights<number>(2, 4);
  const started = Promise.withResolvers<void>();
  let aborts = 0;
  const start = (signal: AbortSignal): Promise<number> =>
    new Promise<number>((_resolve, reject) => {
      started.resolve();
      signal.addEventListener(
        'abort',
        () => {
          aborts++;
          reject(signal.reason);
        },
        { once: true }
      );
    });

  const first = flights.run('shared', start);
  const second = flights.run('shared', start);
  await started.promise;
  const shutdownError = new Error('process shutdown');
  const closing = flights.close(shutdownError);

  await assert.rejects(first, (error) => error === shutdownError);
  await assert.rejects(second, (error) => error === shutdownError);
  await closing;
  assert.equal(aborts, 1);
  assert.equal(flights.activeFlights, 0);
  assert.equal(flights.waitingRequests, 0);
  assert.equal(flights.close(new Error('ignored')), closing);
  await assert.rejects(
    flights.run('late', async () => 1),
    (error) => error === shutdownError
  );
});

test('shutdown waits an uncancellable setup step and prevents publication after it resumes', async () => {
  const flights = new BoundedOpeningFlights<number>(1, 1);
  const entered = Promise.withResolvers<void>();
  const proceed = Promise.withResolvers<void>();
  let publications = 0;
  const opening = flights.run('crossing', async (signal) => {
    entered.resolve();
    await proceed.promise;
    signal.throwIfAborted();
    publications++;
    return 1;
  });
  await entered.promise;

  const shutdownError = new Error('process shutdown');
  let closed = false;
  const closing = flights.close(shutdownError).then(() => {
    closed = true;
  });
  await assert.rejects(opening, (error) => error === shutdownError);
  await Promise.resolve();
  assert.equal(closed, false);

  proceed.resolve();
  await closing;
  assert.equal(publications, 0);
  assert.equal(flights.activeFlights, 0);
});

test('flight and waiter limits are hard and typed', async () => {
  const flights = new BoundedOpeningFlights<number>(1, 1);
  const proceed = Promise.withResolvers<number>();
  const first = flights.run('one', async () => proceed.promise);

  await assert.rejects(
    flights.run('one', async () => 2),
    (error: unknown) =>
      error instanceof OpeningFlightError &&
      error.code === 'USENET_SESSION_OPEN_CAPACITY'
  );
  await assert.rejects(
    flights.run('two', async () => 2),
    (error: unknown) =>
      error instanceof OpeningFlightError &&
      error.code === 'USENET_SESSION_OPEN_CAPACITY'
  );

  proceed.resolve(1);
  assert.equal(await first, 1);
});
