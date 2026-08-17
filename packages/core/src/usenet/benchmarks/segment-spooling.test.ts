import assert from 'node:assert/strict';
import test from 'node:test';
import { runSegmentSpoolingBenchmark } from './segment-spooling.js';

test('benchmark acceptance gates use internal budgets instead of RSS', async () => {
  const result = await runSegmentSpoolingBenchmark({
    totalBytes: 512 * 1024,
    segmentBytes: 256 * 1024,
    chunkBytes: 64 * 1024,
  });
  assert.equal(result.bytes, 512 * 1024);
  assert.equal(result.segments, 2);
  assert.equal(result.configuration.prefetchSegments, 64);
  assert.equal(result.configuration.maxConcurrentDownloads, 60);
  assert.equal(result.checksum.length, 64);
  assert(result.firstByteMs >= 0);
  assert(result.throughputBytesPerSecond > 0);
  assert(result.eventLoopLagMs.mean >= 0);
  assert(result.eventLoopLagMs.max >= 0);
  assert(result.internalMemory.peak <= result.internalMemory.max);
  assert.equal(result.internalMemory.final, 0);
  assert.equal(result.pipeline.activeDownloadsPeak, 2);
  assert(result.pipeline.completedReadAheadPeak > 1);
  assert(result.pipeline.slowConsumerYields > 0);
  assert.equal(result.pipeline.completionWasOutOfOrder, true);
  assert(result.spool.peakReserved <= result.spool.max);
  assert(result.spool.peakActual > result.configuration.segmentBytes);
  assert.equal(result.spool.finalReserved, 0);
  assert.equal(result.spool.finalActual, 0);
  assert.equal(result.spool.finalArtifacts, 0);
  assert.equal(result.spool.finalOpenFiles, 0);
});
