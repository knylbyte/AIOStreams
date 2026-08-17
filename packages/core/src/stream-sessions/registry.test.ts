import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import test from 'node:test';
import '../config/index.js';
import { StreamRegistry, StreamStoppedError } from './registry.js';

const input = {
  transport: 'usenet' as const,
  username: 'shutdown-user',
  targetKey: 'shutdown-target',
};

function openHandle(registry: StreamRegistry) {
  const opened = registry.open(input);
  assert.equal(opened.ok, true);
  if (!opened.ok) throw new Error('test admission unexpectedly failed');
  return opened.handle;
}

test('a handle cannot attach resources after its registry session is sealed', async () => {
  const registry = new StreamRegistry(() => ({ ok: true }));
  const handle = openHandle(registry);

  registry.sealAndCloseAll('stale');
  const stopped = handle.signal.reason;
  assert.ok(stopped instanceof StreamStoppedError);
  assert.equal(stopped.reason, 'stale');

  let streamError: unknown;
  const closed = Promise.withResolvers<void>();
  const stream = new Readable({ read() {} });
  stream.once('error', (error) => {
    streamError = error;
  });
  stream.once('close', () => closed.resolve());

  handle.attach(stream);
  assert.equal(stream.destroyed, true);
  await closed.promise;
  assert.equal(streamError, stopped);
  assert.deepEqual(registry.snapshot(), []);

  handle.addBytes(1024);
  handle.setInfo({ size: 4096, filename: 'too-late.bin' });
  handle.close();
  assert.deepEqual(registry.snapshot(), []);
});

test('a kill callback registered after finalisation runs exactly once', () => {
  const registry = new StreamRegistry(() => ({ ok: true }));
  const handle = openHandle(registry);
  registry.sealAndCloseAll('limit');

  let calls = 0;
  handle.onKill(() => {
    calls++;
  });
  handle.onKill(() => {
    calls++;
  });
  registry.sealAndCloseAll('stale');
  handle.close();

  assert.equal(calls, 1);
  assert.ok(handle.signal.reason instanceof StreamStoppedError);
  assert.equal(handle.signal.reason.reason, 'limit');
});

test('normal handle close is terminal for late callbacks and attachments', async () => {
  const registry = new StreamRegistry(() => ({ ok: true }));
  const handle = openHandle(registry);
  handle.close();

  let killed = 0;
  handle.onKill(() => {
    killed++;
  });
  const closed = Promise.withResolvers<void>();
  const stream = new Readable({ read() {} });
  stream.on('error', () => undefined);
  stream.once('close', () => closed.resolve());
  handle.attach(stream);
  await closed.promise;

  assert.equal(killed, 1);
  assert.equal(stream.destroyed, true);
  const [session] = registry.snapshot();
  assert.equal(session.activeReads, 0);
  assert.equal(session.bytesServed, 0);
  assert.equal(session.size, 0);
  assert.equal(session.filename, undefined);
});
