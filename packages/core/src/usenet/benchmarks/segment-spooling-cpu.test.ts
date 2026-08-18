import assert from 'node:assert/strict';
import test from 'node:test';
import {
  benchmarkTimedValidationPlan,
  measureProviderChildCpuIsolation,
  runSegmentSpoolingCpuBenchmark,
  type SlowClientBenchmarkContext,
} from './segment-spooling-cpu.js';

const MEBIBYTE_BYTES = 1024 * 1024;

function deferred<T>(): {
  readonly promise: Promise<T>;
  readonly resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

test('production TLS CPU benchmark reports bounded owners and fixed counters', async () => {
  const report = await runSegmentSpoolingCpuBenchmark({
    totalBytes: 4 * 1024 * 1024,
    segmentBytes: 1024 * 1024,
    warmupBytes: 1024 * 1024,
    runs: 1,
    scenarios: ['S1', 'S2', 'S3', 'S4', 'S5'],
    slowPauseMs: 0,
    slowBytesPerSecond: Number.MAX_SAFE_INTEGER,
  });

  assert.equal(report.scenarios.length, 5);
  assert.equal(report.measurement.cpuScope, 'parent-process-only');
  assert.equal(report.measurement.providerProcess, 'separate-node-child');
  assert.equal(report.measurement.fullIntegrityInsideCpuWindow, false);
  assert.equal(report.measurement.correctnessRun, true);
  assert.match(report.identity.harnessSha256, /^[a-f\d]{64}$/);
  assert.match(report.identity.providerChildSha256, /^[a-f\d]{64}$/);
  for (const summary of report.scenarios) {
    assert.equal(summary.runs.length, 1);
    const run = summary.runs[0];
    assert(run.decodedBytes > 0);
    assert(run.deliveredBytes >= run.decodedBytes);
    assert.equal(run.timedHashUpdates, 0);
    assert(run.timedValidationSamples > 0);
    assert(run.firstByteMs >= 0);
    assert(run.hotpath.rawReadCallbacks > 0);
    assert(run.hotpath.yencDecodeCalls > 0);
    assert(run.hotpath.yencOutputBackingAllocations > 0);
    const admits = run.hotpath.perStreamDownloadAdmits.reduce(
      (sum, owner) => sum + owner.admits,
      run.hotpath.downloadOwnerOverflowAdmits
    );
    assert(run.hotpath.yencOutputBackingAllocations <= admits);
    assert.equal(run.hotpath.headerTransitionCopies, 0);
    assert(run.hotpath.spoolWriteOperations > 0);
    assert.equal(
      run.hotpath.spoolWriteOperations,
      run.hotpath.decodedBatchesCommitted
    );
    assert(run.hotpath.spoolWriteSyscalls >= run.hotpath.spoolWriteOperations);
    assert(
      run.hotpath.spoolWriteOperations < run.hotpath.yencDecodeCalls * 0.65
    );
    assert(run.hotpath.sinkDrainCycles < run.hotpath.yencDecodeCalls * 0.65);
    assert(run.hotpath.socketPauseCalls < run.hotpath.yencDecodeCalls * 0.65);
    assert(
      run.hotpath.resourceLogRecordsEmitted <=
        run.hotpath.resourceEventsObserved * 0.1
    );
    assert(
      run.hotpath.resourceLogRecordsSuppressed >=
        run.hotpath.resourceEventsObserved * 0.9
    );
    assert.equal(run.hotpath.activeDownloads, 0);
    assert.equal(run.hotpath.semaphoreGlobalScans, 0);
    assert.equal(run.internalMemory.final, 0);
    assert.equal(run.spool.finalReserved, 0);
    assert.equal(run.spool.finalActual, 0);
    assert.equal(run.spool.finalArtifacts, 0);
    assert.equal(run.spool.finalOpenFiles, 0);
  }

  const shared = report.scenarios.find((entry) => entry.scenario === 'S4');
  assert(shared);
  assert.equal(shared.runs[0].deliveredBytes, 2 * shared.runs[0].decodedBytes);
});

test('provider CPU is isolated in a separate process', async () => {
  const result = await measureProviderChildCpuIsolation(20_000_000);
  assert(result.childCpuMs > 5);
  assert(result.childCpuMs > result.parentCpuMs * 2);
  assert(Number.isSafeInteger(result.checksum));
});

test('full correctness run catches valid-yEnc payload corruption outside timing', async () => {
  await assert.rejects(
    runSegmentSpoolingCpuBenchmark({
      totalBytes: MEBIBYTE_BYTES,
      segmentBytes: MEBIBYTE_BYTES,
      warmupBytes: MEBIBYTE_BYTES,
      runs: 1,
      scenarios: ['S1'],
      slowPauseMs: 0,
      slowBytesPerSecond: Number.MAX_SAFE_INTEGER,
      testHooks: { corruptCorrectness: true },
    }),
    assert.AssertionError
  );
});

test('timed validation work is independent of reader chunk size and hashes nothing', () => {
  const plans = [64, 128, 256].map((kibibytes) =>
    benchmarkTimedValidationPlan(
      32 * MEBIBYTE_BYTES,
      MEBIBYTE_BYTES,
      kibibytes * 1024
    )
  );
  assert.deepEqual(plans[0], plans[1]);
  assert.deepEqual(plans[1], plans[2]);
  assert.equal(plans[0].hashUpdates, 0);
  assert(plans[0].samples > 0);
});

test('S5 holds a real pause barrier, resumes and rate-limits without owner leaks', async () => {
  const paused = deferred<SlowClientBenchmarkContext>();
  const release = deferred<void>();
  const benchmark = runSegmentSpoolingCpuBenchmark({
    totalBytes: 8 * MEBIBYTE_BYTES,
    segmentBytes: MEBIBYTE_BYTES,
    warmupBytes: MEBIBYTE_BYTES,
    runs: 1,
    scenarios: ['S5'],
    runCorrectness: false,
    slowPauseMs: 0,
    slowBytesPerSecond: 64 * MEBIBYTE_BYTES,
    testHooks: {
      onSlowClientPaused: async (context) => {
        paused.resolve(context);
        await release.promise;
      },
    },
  });
  const context = await paused.promise;
  await context.waitForPipelineTurn();
  await context.waitForPipelineTurn();
  const pausedStats = context.stats();
  assert(pausedStats.memory.usedBytes <= pausedStats.memory.maxBytes);
  assert(
    pausedStats.spool.budget.reservedBytes <= pausedStats.spool.budget.maxBytes
  );
  release.resolve();
  const report = await benchmark;
  const run = report.scenarios[0].runs[0];
  assert.equal(run.slowClient.pauseCount, 1);
  assert.equal(run.slowClient.resumeCount, 1);
  assert(run.slowClient.rateLimitWaits > 0);
  assert.equal(run.internalMemory.final, 0);
  assert.equal(run.spool.finalReserved, 0);
  assert.equal(run.spool.finalArtifacts, 0);
  assert.equal(run.hotpath.activeDownloads, 0);
});
