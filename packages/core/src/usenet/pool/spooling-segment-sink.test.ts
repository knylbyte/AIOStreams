import assert from 'node:assert/strict';
import test from 'node:test';
import yencode from 'yencode';
import type { ByteLease } from './byte-budget.js';
import {
  SpoolingSegmentSink,
  type SpoolingSinkArtifact,
} from './spooling-segment-sink.js';
import { SegmentSpoolingHotpathCounters } from './hotpath-counters.js';
import { StreamingYencArticleDecoder } from './streaming-yenc-article-decoder.js';

interface PendingWrite {
  readonly chunk: Buffer;
  readonly lease: ByteLease;
  settled: boolean;
}

class ControlledArtifact implements SpoolingSinkArtifact {
  reservedBytes = 16 * 1024 * 1024;
  committedBytes = 0;
  readonly writes: PendingWrite[] = [];
  completeCalls = 0;
  growCalls = 0;
  heldBytes = 0;
  failure: Error | undefined;
  growHandler: ((bytes: number) => Promise<void>) | undefined;

  write(chunk: Buffer, lease: ByteLease): boolean {
    this.writes.push({ chunk, lease, settled: false });
    this.heldBytes += lease.bytes;
    return false;
  }

  grow(bytes: number): Promise<void> {
    this.growCalls++;
    if (this.growHandler) return this.growHandler(bytes);
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
    assert.equal(pending.settled, false);
    pending.settled = true;
    this.committedBytes += pending.chunk.length;
    this.heldBytes -= pending.lease.bytes;
    pending.lease.release();
  }
}

function drain(sink: SpoolingSegmentSink): Promise<void> {
  return new Promise<void>((resolve) => sink.onceDrain(resolve));
}

function commitDecoded(
  sink: SpoolingSegmentSink,
  bytes: number,
  articleEnded = false
): boolean {
  return sink.commitDecoded(bytes, {
    inputBoundary: true,
    articleEnded,
  });
}

async function assertPending(promise: Promise<unknown>): Promise<void> {
  let settled = false;
  void promise.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    }
  );
  await Promise.resolve();
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
}

test('reuses one output backing only after writer settlement and batches steady-state chunks', async () => {
  const chunkBytes = 256 * 1024;
  const artifact = new ControlledArtifact();
  const counters = new SegmentSpoolingHotpathCounters();
  let firstCommitCalls = 0;
  const sink = new SpoolingSegmentSink(
    artifact,
    2 * chunkBytes,
    chunkBytes,
    counters,
    { onFirstSinkCommit: () => firstCommitCalls++ }
  );

  const first = sink.acquireDecodeTarget(chunkBytes);
  first.fill(0x11);
  assert.equal(commitDecoded(sink, chunkBytes), false);
  assert.equal(artifact.writes.length, 1);
  assert.deepEqual(artifact.writes[0].chunk, Buffer.alloc(chunkBytes, 0x11));
  assert.throws(() => sink.acquireDecodeTarget(chunkBytes));
  assert.deepEqual(artifact.writes[0].chunk, Buffer.alloc(chunkBytes, 0x11));

  const firstDrain = drain(sink);
  artifact.settle(0);
  await firstDrain;
  assert.equal(firstCommitCalls, 1);
  const second = sink.acquireDecodeTarget(chunkBytes);
  assert.equal(second.buffer, first.buffer);
  second.fill(0x22);
  assert.equal(commitDecoded(sink, chunkBytes), true);
  const third = sink.acquireDecodeTarget(chunkBytes);
  assert.equal(third.buffer, first.buffer);
  third.fill(0x33);
  assert.equal(commitDecoded(sink, chunkBytes), false);
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
  assert.equal(firstCommitCalls, 1);
  const snapshot = counters.snapshot();
  assert.equal(snapshot.yencOutputBackingAllocations, 1);
  assert.equal(snapshot.decodedBatchesCommitted, 2);
  assert.equal(snapshot.sinkDrainCycles, 2);
});

test('distinct playback owners yield a local half-batch without publishing it', async () => {
  const artifact = new ControlledArtifact();
  let contended = true;
  const sink = new SpoolingSegmentSink(artifact, 32, 16, undefined, {
    shouldYieldBeforeNextDecodeInput: () => contended,
  });

  const first = sink.acquireDecodeTarget(16);
  first.fill(0x11);
  assert.equal(commitDecoded(sink, 16), false);
  const firstDrain = drain(sink);
  artifact.settle(0);
  await firstDrain;

  const local = sink.acquireDecodeTarget(8);
  local.fill(0x22);
  assert.equal(commitDecoded(sink, 8), false);
  assert.equal(artifact.writes.length, 1);
  let resumed = false;
  const localDrain = drain(sink).then(() => {
    resumed = true;
  });
  await Promise.resolve();
  assert.equal(resumed, false);
  await localDrain;

  contended = false;
  const ending = sink.end();
  assert.equal(artifact.writes.length, 2);
  assert.equal(artifact.writes[1].chunk.length, 8);
  artifact.settle(1);
  await ending;
  assert.equal(artifact.completeCalls, 1);
  assert.equal(artifact.heldBytes, 0);
});

test('flushes one final partial batch without losing ownership', async () => {
  const chunkBytes = 16;
  const artifact = new ControlledArtifact();
  const sink = new SpoolingSegmentSink(artifact, 2 * chunkBytes, chunkBytes);

  const first = sink.acquireDecodeTarget(chunkBytes);
  first.fill(0x41);
  assert.equal(commitDecoded(sink, chunkBytes), false);
  const firstDrain = drain(sink);
  artifact.settle(0);
  await firstDrain;

  const final = sink.acquireDecodeTarget(7);
  final.fill(0x5a);
  assert.equal(commitDecoded(sink, 7), true);
  const ending = sink.end();
  assert.equal(artifact.writes.length, 2);
  assert.equal(artifact.writes[1].chunk.toString('latin1'), 'ZZZZZZZ');
  artifact.settle(1);
  await ending;
  assert.equal(artifact.completeCalls, 1);
  assert.equal(artifact.committedBytes, chunkBytes + 7);
});

test('a terminal yend in the accepted decode window settles without reserving a hypothetical next batch', async () => {
  const body = Buffer.alloc(64, 0x4a);
  const raw = yencode.post('terminal.bin', body, 128);
  const artifact = new ControlledArtifact();
  artifact.reservedBytes = body.length;
  artifact.growHandler = async () => {
    throw new Error('spool cap after final batch');
  };
  const counters = new SegmentSpoolingHotpathCounters();
  const sink = new SpoolingSegmentSink(artifact, 512, 256, counters);
  const decoder = new StreamingYencArticleDecoder(sink);

  assert.equal(decoder.write(raw), false);
  assert.equal(artifact.writes.length, 1);
  assert.equal(artifact.heldBytes, body.length);
  const drained = new Promise<void>((resolve) => decoder.onceDrain(resolve));
  artifact.settle(0);
  await drained;
  await decoder.finish();

  assert.deepEqual(artifact.writes[0]?.chunk, body);
  assert.equal(artifact.committedBytes, body.length);
  assert.equal(artifact.completeCalls, 1);
  assert.equal(artifact.growCalls, 0);
  assert.equal(artifact.heldBytes, 0);
  assert.equal(counters.spoolGrowthRequests, 0);
  assert.equal(counters.terminalGrowthRequests, 0);
});

test('a terminal local partial batch is published once and never grows after writer settlement', async () => {
  const artifact = new ControlledArtifact();
  const counters = new SegmentSpoolingHotpathCounters();
  const sink = new SpoolingSegmentSink(artifact, 32, 16, counters);

  const first = sink.acquireDecodeTarget(16);
  first.fill(0x31);
  assert.equal(commitDecoded(sink, 16), false);
  const firstDrain = drain(sink);
  artifact.settle(0);
  await firstDrain;

  const final = sink.acquireDecodeTarget(7);
  final.fill(0x32);
  assert.equal(commitDecoded(sink, 7, true), true);
  const ending = sink.end();
  assert.equal(artifact.writes.length, 2);
  assert.equal(artifact.writes[1]?.chunk.length, 7);
  artifact.settle(1);
  await ending;

  assert.equal(artifact.writes.length, 2);
  assert.equal(artifact.completeCalls, 1);
  assert.equal(artifact.growCalls, 0);
  assert.equal(counters.terminalGrowthRequests, 0);
});

test('a nonterminal settled batch still grows once before publishing drain', async () => {
  const artifact = new ControlledArtifact();
  artifact.reservedBytes = 16;
  const growth = Promise.withResolvers<void>();
  artifact.growHandler = () => growth.promise;
  const counters = new SegmentSpoolingHotpathCounters();
  const sink = new SpoolingSegmentSink(artifact, 32, 16, counters);
  const target = sink.acquireDecodeTarget(16);
  target.fill(0x41);
  assert.equal(commitDecoded(sink, 16), false);
  const drained = drain(sink);
  let drainObserved = false;
  void drained.then(() => {
    drainObserved = true;
  });

  artifact.settle(0);
  await Promise.resolve();
  assert.equal(artifact.growCalls, 1);
  assert.equal(drainObserved, false);
  assert.equal(counters.spoolGrowthRequests, 1);
  assert.equal(counters.spoolGrowthBytes, 1024 * 1024);
  assert.equal(counters.terminalGrowthRequests, 0);

  growth.resolve();
  await drained;
  await sink.end();
  assert.equal(artifact.completeCalls, 1);
});

test('terminal writer settlement crossing a client abort never grows or completes twice', async () => {
  const artifact = new ControlledArtifact();
  artifact.reservedBytes = 16;
  const counters = new SegmentSpoolingHotpathCounters();
  const sink = new SpoolingSegmentSink(artifact, 32, 16, counters);
  const target = sink.acquireDecodeTarget(16);
  target.fill(0x51);
  assert.equal(commitDecoded(sink, 16, true), false);
  const drained = drain(sink);
  const abort = new Error('client closed after article end');
  sink.fail(abort);
  const ending = sink.end();
  await drained;
  await assertPending(ending);
  artifact.settle(0);
  await assert.rejects(ending, (error: unknown) => error === abort);

  assert.equal(artifact.growCalls, 0);
  assert.equal(artifact.completeCalls, 0);
  assert.equal(artifact.heldBytes, 0);
  assert.equal(counters.terminalGrowthRequests, 0);
});

test('a final writer failure remains visible without admitting another reservation', async () => {
  const artifact = new ControlledArtifact();
  artifact.reservedBytes = 16;
  const counters = new SegmentSpoolingHotpathCounters();
  const sink = new SpoolingSegmentSink(artifact, 32, 16, counters);
  const target = sink.acquireDecodeTarget(16);
  target.fill(0x61);
  assert.equal(commitDecoded(sink, 16, true), false);
  const failure = Object.assign(new Error('final writer failed'), {
    code: 'EIO',
  });
  sink.fail(failure);
  const ending = sink.end();
  await assertPending(ending);
  artifact.settle(0);

  await assert.rejects(ending, (error: unknown) => error === failure);
  assert.equal(artifact.growCalls, 0);
  assert.equal(artifact.heldBytes, 0);
  assert.equal(counters.terminalGrowthRequests, 0);
});

test('abort while a batch is writer-owned is idempotent and wakes drain', async () => {
  const artifact = new ControlledArtifact();
  const sink = new SpoolingSegmentSink(artifact, 32, 16);
  const target = sink.acquireDecodeTarget(16);
  target.fill(1);
  assert.equal(commitDecoded(sink, 16), false);
  const drained = drain(sink);
  const failure = new Error('aborted');
  sink.fail(failure);
  sink.fail(new Error('ignored'));
  await drained;
  assert.equal(artifact.failure, failure);
  artifact.settle(0);
  await assert.rejects(sink.end(), failure);
});

test('writer settlement after failure does not restart growth or replace the root cause', async () => {
  const artifact = new ControlledArtifact();
  artifact.reservedBytes = 16;
  const sink = new SpoolingSegmentSink(artifact, 32, 16);
  const target = sink.acquireDecodeTarget(16);
  target.fill(1);
  assert.equal(commitDecoded(sink, 16), false);
  const original = new Error('original segment failure');
  sink.fail(original);
  const ending = sink.end();

  await assertPending(ending);
  assert.equal(artifact.committedBytes, 0);
  assert.equal(artifact.growCalls, 0);
  artifact.settle(0);
  await assert.rejects(ending, (error: unknown) => error === original);
  assert.equal(artifact.heldBytes, 0);
  assert.equal(artifact.growCalls, 0);
  assert.equal(artifact.failure, original);
});

test('a secondary preparation failure is aggregated without obscuring the first error', async () => {
  const artifact = new ControlledArtifact();
  artifact.reservedBytes = 16;
  const growth = Promise.withResolvers<void>();
  artifact.growHandler = () => growth.promise;
  const sink = new SpoolingSegmentSink(artifact, 32, 16);
  const target = sink.acquireDecodeTarget(16);
  target.fill(1);
  assert.equal(commitDecoded(sink, 16), false);
  artifact.settle(0);
  assert.equal(artifact.growCalls, 1);

  const original = new Error('original segment failure');
  sink.fail(original);
  const secondary = Object.assign(new Error('growth cleanup failed'), {
    code: 'EIO',
  });
  const ending = sink.end();
  await assertPending(ending);
  growth.reject(secondary);

  await assert.rejects(ending, (error: unknown) => {
    assert(error instanceof AggregateError);
    assert.equal(error.cause, original);
    assert.deepEqual(error.errors, [original, secondary]);
    return true;
  });
  assert.equal(artifact.failure, original);
});

test('multiple end callers share writer settlement and complete exactly once', async () => {
  const artifact = new ControlledArtifact();
  const sink = new SpoolingSegmentSink(artifact, 32, 16);
  const target = sink.acquireDecodeTarget(7);
  target.fill(0x61);
  assert.equal(commitDecoded(sink, 7), false);

  const first = sink.end();
  const second = sink.end();
  assert.equal(first, second);
  assert.equal(artifact.writes.length, 1);
  await assertPending(first);

  artifact.settle(0);
  await Promise.all([first, second]);
  assert.equal(artifact.writes.length, 1);
  assert.equal(artifact.growCalls, 0);
  assert.equal(artifact.completeCalls, 1);
  assert.equal(artifact.committedBytes, 7);
  assert.equal(artifact.heldBytes, 0);
  assert.throws(() => sink.acquireDecodeTarget(1), {
    name: 'UsenetSpoolError',
  });
});

test('end followed by abort still waits the one writer owner and releases it once', async () => {
  const artifact = new ControlledArtifact();
  const sink = new SpoolingSegmentSink(artifact, 32, 16);
  const target = sink.acquireDecodeTarget(16);
  target.fill(0x71);
  assert.equal(commitDecoded(sink, 16), false);

  const ending = sink.end();
  const failure = new Error('range aborted after end started');
  sink.fail(failure);
  await assertPending(ending);
  assert.equal(artifact.heldBytes, 16);
  artifact.settle(0);

  await assert.rejects(ending, (error: unknown) => error === failure);
  assert.equal(artifact.heldBytes, 0);
  assert.equal(artifact.writes.length, 1);
  assert.equal(artifact.completeCalls, 0);
  assert.equal(sink.end(), ending);
});

test('abort discards empty and partially filled local batches without publishing them', async () => {
  for (const fillBytes of [0, 7]) {
    const artifact = new ControlledArtifact();
    const sink = new SpoolingSegmentSink(artifact, 32, 16);
    if (fillBytes > 0) {
      const first = sink.acquireDecodeTarget(16);
      first.fill(0x41);
      assert.equal(commitDecoded(sink, 16), false);
      const firstDrain = drain(sink);
      artifact.settle(0);
      await firstDrain;
      const target = sink.acquireDecodeTarget(fillBytes);
      target.fill(0x7a);
      assert.equal(commitDecoded(sink, fillBytes), true);
    }
    const failure = new Error(`aborted-${fillBytes}`);
    sink.fail(failure);
    await assert.rejects(sink.end(), failure);
    assert.equal(artifact.writes.length, fillBytes > 0 ? 1 : 0);
    assert.equal(artifact.failure, failure);
  }
});
