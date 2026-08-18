import assert from 'node:assert/strict';
import test from 'node:test';
import type { ByteLease } from './byte-budget.js';
import {
  SpoolingSegmentSink,
  type SpoolingSinkArtifact,
} from './spooling-segment-sink.js';
import { SegmentSpoolingHotpathCounters } from './hotpath-counters.js';

interface PendingWrite {
  readonly chunk: Buffer;
  readonly lease: ByteLease;
}

class ControlledArtifact implements SpoolingSinkArtifact {
  reservedBytes = 16 * 1024 * 1024;
  committedBytes = 0;
  readonly writes: PendingWrite[] = [];
  completeCalls = 0;
  failure: Error | undefined;

  write(chunk: Buffer, lease: ByteLease): boolean {
    this.writes.push({ chunk, lease });
    return false;
  }

  grow(bytes: number): Promise<void> {
    this.reservedBytes += bytes;
    return Promise.resolve();
  }

  complete(): Promise<void> {
    this.completeCalls++;
    return Promise.resolve();
  }

  fail(error: Error): void {
    this.failure ??= error;
  }

  settle(index: number): void {
    const pending = this.writes[index];
    assert(pending);
    this.committedBytes += pending.chunk.length;
    pending.lease.release();
  }
}

function drain(sink: SpoolingSegmentSink): Promise<void> {
  return new Promise<void>((resolve) => sink.onceDrain(resolve));
}

test('reuses one output backing only after writer settlement and batches steady-state chunks', async () => {
  const chunkBytes = 256 * 1024;
  const artifact = new ControlledArtifact();
  const counters = new SegmentSpoolingHotpathCounters();
  const sink = new SpoolingSegmentSink(
    artifact,
    2 * chunkBytes,
    chunkBytes,
    counters
  );

  const first = sink.acquireDecodeTarget(chunkBytes);
  first.fill(0x11);
  assert.equal(sink.commitDecoded(chunkBytes, true), false);
  assert.equal(artifact.writes.length, 1);
  assert.deepEqual(artifact.writes[0].chunk, Buffer.alloc(chunkBytes, 0x11));
  assert.throws(() => sink.acquireDecodeTarget(chunkBytes));
  assert.deepEqual(artifact.writes[0].chunk, Buffer.alloc(chunkBytes, 0x11));

  const firstDrain = drain(sink);
  artifact.settle(0);
  await firstDrain;
  const second = sink.acquireDecodeTarget(chunkBytes);
  assert.equal(second.buffer, first.buffer);
  second.fill(0x22);
  assert.equal(sink.commitDecoded(chunkBytes, true), true);
  const third = sink.acquireDecodeTarget(chunkBytes);
  assert.equal(third.buffer, first.buffer);
  third.fill(0x33);
  assert.equal(sink.commitDecoded(chunkBytes, true), false);
  assert.equal(artifact.writes.length, 2);
  assert.equal(artifact.writes[1].chunk.length, 2 * chunkBytes);
  assert.deepEqual(
    artifact.writes[1].chunk.subarray(0, chunkBytes),
    Buffer.alloc(chunkBytes, 0x22)
  );
  assert.deepEqual(
    artifact.writes[1].chunk.subarray(chunkBytes),
    Buffer.alloc(chunkBytes, 0x33)
  );

  const secondDrain = drain(sink);
  artifact.settle(1);
  await secondDrain;
  const snapshot = counters.snapshot();
  assert.equal(snapshot.yencOutputBackingAllocations, 1);
  assert.equal(snapshot.decodedBatchesCommitted, 2);
  assert.equal(snapshot.sinkDrainCycles, 2);
});

test('flushes one final partial batch without losing ownership', async () => {
  const chunkBytes = 16;
  const artifact = new ControlledArtifact();
  const sink = new SpoolingSegmentSink(artifact, 2 * chunkBytes, chunkBytes);

  const first = sink.acquireDecodeTarget(chunkBytes);
  first.fill(0x41);
  assert.equal(sink.commitDecoded(chunkBytes, true), false);
  const firstDrain = drain(sink);
  artifact.settle(0);
  await firstDrain;

  const final = sink.acquireDecodeTarget(7);
  final.fill(0x5a);
  assert.equal(sink.commitDecoded(7, true), true);
  const ending = sink.end();
  assert.equal(artifact.writes.length, 2);
  assert.equal(artifact.writes[1].chunk.toString('latin1'), 'ZZZZZZZ');
  artifact.settle(1);
  await ending;
  assert.equal(artifact.completeCalls, 1);
  assert.equal(artifact.committedBytes, chunkBytes + 7);
});

test('abort while a batch is writer-owned is idempotent and wakes drain', async () => {
  const artifact = new ControlledArtifact();
  const sink = new SpoolingSegmentSink(artifact, 32, 16);
  const target = sink.acquireDecodeTarget(16);
  target.fill(1);
  assert.equal(sink.commitDecoded(16, true), false);
  const drained = drain(sink);
  const failure = new Error('aborted');
  sink.fail(failure);
  sink.fail(new Error('ignored'));
  await drained;
  assert.equal(artifact.failure, failure);
  artifact.settle(0);
  await assert.rejects(sink.end(), failure);
});

test('abort discards empty and partially filled local batches without publishing them', async () => {
  for (const fillBytes of [0, 7]) {
    const artifact = new ControlledArtifact();
    const sink = new SpoolingSegmentSink(artifact, 32, 16);
    if (fillBytes > 0) {
      const first = sink.acquireDecodeTarget(16);
      first.fill(0x41);
      assert.equal(sink.commitDecoded(16, true), false);
      const firstDrain = drain(sink);
      artifact.settle(0);
      await firstDrain;
      const target = sink.acquireDecodeTarget(fillBytes);
      target.fill(0x7a);
      assert.equal(sink.commitDecoded(fillBytes, true), true);
    }
    const failure = new Error(`aborted-${fillBytes}`);
    sink.fail(failure);
    await assert.rejects(sink.end(), failure);
    assert.equal(artifact.writes.length, fillBytes > 0 ? 1 : 0);
    assert.equal(artifact.failure, failure);
  }
});
