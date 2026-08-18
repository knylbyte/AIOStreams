import assert from 'node:assert/strict';
import test from 'node:test';
import { runSegmentSpoolingCpuBenchmark } from './segment-spooling-cpu.js';

test('production TLS CPU benchmark reports bounded owners and fixed counters', async () => {
  const report = await runSegmentSpoolingCpuBenchmark({
    totalBytes: 4 * 1024 * 1024,
    segmentBytes: 1024 * 1024,
    warmupBytes: 1024 * 1024,
    runs: 1,
    scenarios: ['S1', 'S2', 'S3', 'S4', 'S5'],
  });

  assert.equal(report.scenarios.length, 5);
  for (const summary of report.scenarios) {
    assert.equal(summary.runs.length, 1);
    const run = summary.runs[0];
    assert(run.decodedBytes > 0);
    assert(run.deliveredBytes >= run.decodedBytes);
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
