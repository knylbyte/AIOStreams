import assert from 'node:assert/strict';
import test from 'node:test';
import { EngineRetirementBarrier } from './engine-retirement.js';

test('a failed retirement does not poison the serialized cleanup tail', async () => {
  const barrier = new EngineRetirementBarrier();
  const events: string[] = [];
  const first = barrier.enqueue(async () => {
    events.push('first');
    throw new Error('first retirement failed');
  });
  const second = barrier.enqueue(async () => {
    events.push('second');
    throw new Error('second retirement failed');
  });
  const third = barrier.enqueue(async () => {
    events.push('third');
  });

  await assert.rejects(first, /first retirement failed/);
  await assert.rejects(second, /second retirement failed/);
  await third;
  await barrier.snapshot();
  assert.deepEqual(events, ['first', 'second', 'third']);
  const failure = barrier.error();
  assert(failure);
  assert.equal(failure.errors.length, 2);
});
