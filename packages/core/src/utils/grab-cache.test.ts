import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import './general.js';
import { GrabCache, GrabCacheError } from './grab-cache.js';

function createCache(
  dir: string,
  options: { maxFlights?: number; maxWaitersPerKey?: number } = {}
): GrabCache<Buffer> {
  return new GrabCache<Buffer>({
    name: 'grab-owner-test',
    dir,
    maxMemBytes: 1024,
    maxDiskBytes: 4096,
    serialize: (value) => value,
    deserialize: (value) => value,
    sizeOf: (value) => value.length,
    ...options,
  });
}

async function tempDirectory(t: test.TestContext): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'aiostreams-grab-owner-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  return dir;
}

test('foreign grab and later waiter share one closeable owner', async (t) => {
  const dir = await tempDirectory(t);
  const cache = createCache(dir);
  const entered = Promise.withResolvers<void>();
  let producerCalls = 0;
  let ownerAborts = 0;
  const produce = (signal: AbortSignal): Promise<Buffer> => {
    producerCalls++;
    entered.resolve();
    return new Promise<Buffer>((_resolve, reject) => {
      signal.addEventListener(
        'abort',
        () => {
          ownerAborts++;
          reject(signal.reason);
        },
        { once: true }
      );
    });
  };

  const foreign = cache.fetch('same-url', produce);
  await entered.promise;
  const nativeController = new AbortController();
  const native = cache.fetch(
    'same-url',
    () => Promise.reject(new Error('second producer must not start')),
    { signal: nativeController.signal }
  );
  await Promise.resolve();
  assert.equal(producerCalls, 1);

  const nativeAbort = new DOMException('native opening closed', 'AbortError');
  nativeController.abort(nativeAbort);
  await assert.rejects(native, (error) => error === nativeAbort);
  assert.equal(cache.activeFlights, 1);

  const shutdownError = new Error('process shutdown');
  const closing = cache.close(shutdownError);
  await assert.rejects(foreign, (error) => error === shutdownError);
  await closing;
  assert.equal(ownerAborts, 1);
  assert.equal(cache.activeFlights, 0);
  assert.equal(cache.waitingRequests, 0);

  const reopened = createCache(dir);
  assert.equal(await reopened.cached('same-url'), undefined);
  await reopened.close();
});

test('one request abort does not cancel a shared producer', async (t) => {
  const cache = createCache(await tempDirectory(t));
  const entered = Promise.withResolvers<void>();
  const complete = Promise.withResolvers<Buffer>();
  let producerCalls = 0;
  let ownerAborts = 0;
  const firstController = new AbortController();
  const produce = (signal: AbortSignal): Promise<Buffer> => {
    producerCalls++;
    signal.addEventListener('abort', () => ownerAborts++, { once: true });
    entered.resolve();
    return complete.promise;
  };

  const first = cache.fetch('shared', produce, {
    signal: firstController.signal,
  });
  await entered.promise;
  const second = cache.fetch('shared', produce);
  await Promise.resolve();

  const requestAbort = new DOMException('client left', 'AbortError');
  firstController.abort(requestAbort);
  await assert.rejects(first, (error) => error === requestAbort);
  assert.equal(ownerAborts, 0);

  complete.resolve(Buffer.from('nzb'));
  assert.deepEqual(await second, Buffer.from('nzb'));
  assert.equal(producerCalls, 1);
  await cache.close();
});

test('process close rejects every waiter and awaits the producer finalizer', async (t) => {
  const cache = createCache(await tempDirectory(t));
  const entered = Promise.withResolvers<void>();
  const ownerAborted = Promise.withResolvers<void>();
  const finalizerGate = Promise.withResolvers<void>();
  let aborts = 0;
  const produce = async (signal: AbortSignal): Promise<Buffer> => {
    entered.resolve();
    await new Promise<void>((resolve) =>
      signal.addEventListener(
        'abort',
        () => {
          aborts++;
          ownerAborted.resolve();
          resolve();
        },
        { once: true }
      )
    );
    await finalizerGate.promise;
    throw signal.reason;
  };
  const first = cache.fetch('shared-close', produce);
  const second = cache.fetch('shared-close', produce);
  await entered.promise;

  const closeError = new Error('process shutdown');
  let closeSettled = false;
  const closing = cache.close(closeError).then(() => {
    closeSettled = true;
  });
  await ownerAborted.promise;
  await Promise.all([
    assert.rejects(first, (error) => error === closeError),
    assert.rejects(second, (error) => error === closeError),
  ]);
  assert.equal(aborts, 1);
  assert.equal(closeSettled, false);

  finalizerGate.resolve();
  await closing;
  assert.equal(cache.activeFlights, 0);
  assert.equal(cache.waitingRequests, 0);
});

test('grab flight and waiter bounds reject without leaking capacity', async (t) => {
  const cache = createCache(await tempDirectory(t), {
    maxFlights: 1,
    maxWaitersPerKey: 1,
  });
  const entered = Promise.withResolvers<void>();
  const ownerGate = Promise.withResolvers<Buffer>();
  const first = cache.fetch('first', async () => {
    entered.resolve();
    return ownerGate.promise;
  });
  await entered.promise;

  await assert.rejects(
    cache.fetch('first', () => Promise.resolve(Buffer.from('unexpected'))),
    (error) =>
      error instanceof GrabCacheError &&
      error.code === 'GRAB_CACHE_WAITER_CAPACITY'
  );
  await assert.rejects(
    cache.fetch('second', () => Promise.resolve(Buffer.from('unexpected'))),
    (error) =>
      error instanceof GrabCacheError &&
      error.code === 'GRAB_CACHE_FLIGHT_CAPACITY'
  );
  assert.equal(cache.activeFlights, 1);
  assert.equal(cache.waitingRequests, 1);

  ownerGate.resolve(Buffer.from('done'));
  await first;
  await cache.close();
  assert.equal(cache.activeFlights, 0);
  assert.equal(cache.waitingRequests, 0);
});

test('close fences new work and cache hits synchronously', async (t) => {
  const cache = createCache(await tempDirectory(t));
  assert.deepEqual(
    await cache.fetch('hit', () => Promise.resolve(Buffer.from('cached'))),
    Buffer.from('cached')
  );
  const closing = cache.close();

  await assert.rejects(
    cache.fetch('hit', () => Promise.resolve(Buffer.from('late'))),
    (error) =>
      error instanceof GrabCacheError && error.code === 'GRAB_CACHE_CLOSED'
  );
  await assert.rejects(
    cache.cached('hit'),
    (error) =>
      error instanceof GrabCacheError && error.code === 'GRAB_CACHE_CLOSED'
  );
  await closing;
});
