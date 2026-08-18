import assert from 'node:assert/strict';
import test from 'node:test';
import {
  runSegmentSpoolingBenchmark,
  type SegmentSpoolingBenchmarkOwnershipSnapshot,
  type SegmentSpoolingBenchmarkStage,
} from './segment-spooling.js';
import { resolveSegmentSpoolingPlan } from '../resource-plan.js';
import { resolveSegmentStreamMemoryBytes } from '../stream-queue-budget.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;
const PRODUCTION_DECODER_CHUNK_BYTES = 256 * KIBIBYTE_BYTES;
const PRODUCTION_DOWNLOAD_BASE_BYTES = 1_572_864;
const BENCHMARK_READER_HIGH_WATER_MARK_BYTES = 256 * KIBIBYTE_BYTES;

function productionPlan(memoryBudgetBytes = 16 * MEBIBYTE_BYTES) {
  return resolveSegmentSpoolingPlan({
    streamingMode: 'segment_spooling',
    maxConcurrentDownloads: 60,
    segmentMemoryCacheBytes: 0,
    segmentSpoolingMemoryBudgetBytes: memoryBudgetBytes,
    segmentSpoolingStreamBufferBytes: 2 * MEBIBYTE_BYTES,
    segmentSpoolingSpoolBytes: 64 * MEBIBYTE_BYTES,
    segmentSpoolingMinFreeDiskBytes: 0,
  });
}

function assertOwnershipZero(
  snapshot: SegmentSpoolingBenchmarkOwnershipSnapshot | undefined
): void {
  assert.ok(snapshot);
  assert.deepEqual(snapshot, {
    activeDownloads: 0,
    downloadBaseLeases: 0,
    writerChildLeases: 0,
    writerChildBytes: 0,
    memoryUsedBytes: 0,
    artifacts: 0,
    openFiles: 0,
    spoolReservedBytes: 0,
    spoolActualBytes: 0,
  });
}

test('benchmark acceptance gates use the production download plan instead of RSS', async () => {
  const production = productionPlan();
  const result = await runSegmentSpoolingBenchmark({
    totalBytes: 512 * KIBIBYTE_BYTES,
    segmentBytes: 256 * KIBIBYTE_BYTES,
    chunkBytes: 64 * KIBIBYTE_BYTES,
  });
  assert.equal(result.bytes, 512 * KIBIBYTE_BYTES);
  assert.equal(result.segments, 2);
  assert.equal(result.configuration.prefetchSegments, 64);
  assert.equal(result.configuration.maxConcurrentDownloads, 60);
  assert.equal(result.configuration.memoryBudgetBytes, 16 * MEBIBYTE_BYTES);
  assert.equal(result.configuration.effectiveDownloadLimit, 2);
  assert.equal(
    result.configuration.decoderChunkBytes,
    production.decoderChunkBytes
  );
  assert.equal(
    result.configuration.perDownloadBaseLeaseBytes,
    production.perDownloadBaseLeaseBytes
  );
  assert.equal(result.configuration.decoderChunkBytes, 262_144);
  assert.equal(result.configuration.writerChunkBytes, 65_536);
  assert.equal(result.configuration.perDownloadBaseLeaseBytes, 1_572_864);
  assert(result.configuration.streamMemoryBytes > 0);
  assert.equal(result.checksum.length, 64);
  assert(result.firstByteMs >= 0);
  assert(result.throughputBytesPerSecond > 0);
  assert(result.eventLoopLagMs.mean >= 0);
  assert(result.eventLoopLagMs.max >= 0);
  assert(result.internalMemory.peak <= result.internalMemory.max);
  assert.equal(result.internalMemory.final, 0);
  assert.equal(result.pipeline.activeDownloadsPeak, 2);
  assert.equal(result.pipeline.downloadMemoryLeasesPeak, 2);
  assert.equal(result.pipeline.downloadMemoryLeasesFinal, 0);
  assert.equal(result.pipeline.writerChildLeasesPeak, 2);
  assert.equal(result.pipeline.writerChildLeasesFinal, 0);
  assert.equal(result.pipeline.writerChildBytesFinal, 0);
  assert.equal(result.pipeline.writerChildReleasedWhileBaseLeaseHeld, true);
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

test('writer output chunk size cannot change the production base lease', async () => {
  for (const writerChunkBytes of [32, 64, 128].map(
    (kibibytes) => kibibytes * KIBIBYTE_BYTES
  )) {
    const result = await runSegmentSpoolingBenchmark({
      totalBytes: 512 * KIBIBYTE_BYTES,
      segmentBytes: 256 * KIBIBYTE_BYTES,
      chunkBytes: writerChunkBytes,
    });
    assert.equal(result.configuration.writerChunkBytes, writerChunkBytes);
    assert.equal(
      result.configuration.decoderChunkBytes,
      PRODUCTION_DECODER_CHUNK_BYTES
    );
    assert.equal(
      result.configuration.perDownloadBaseLeaseBytes,
      PRODUCTION_DOWNLOAD_BASE_BYTES
    );
  }
});

test('16 MiB admits exactly ten production download windows', async () => {
  const segmentBytes = 64 * KIBIBYTE_BYTES;
  const result = await runSegmentSpoolingBenchmark({
    totalBytes: 20 * segmentBytes,
    segmentBytes,
    chunkBytes: 16 * KIBIBYTE_BYTES,
    prefetchSegments: 20,
    maxConcurrentDownloads: 60,
    memoryBudgetBytes: 16 * MEBIBYTE_BYTES,
  });

  assert.equal(result.configuration.streamMemoryBytes, 786_430);
  assert.equal(
    result.configuration.perDownloadBaseLeaseBytes,
    PRODUCTION_DOWNLOAD_BASE_BYTES
  );
  assert.equal(result.configuration.effectiveDownloadLimit, 10);
  assert.equal(result.pipeline.activeDownloadsPeak, 10);
  assert.equal(result.pipeline.downloadMemoryLeasesPeak, 10);
  assert(result.internalMemory.peak <= result.internalMemory.max);
  assert.equal(result.pipeline.downloadMemoryLeasesFinal, 0);
  assert.equal(result.pipeline.writerChildLeasesFinal, 0);
  assert.equal(result.pipeline.writerChildBytesFinal, 0);
  assert.equal(result.internalMemory.final, 0);
  assert.equal(result.spool.finalArtifacts, 0);
  assert.equal(result.spool.finalOpenFiles, 0);
});

test('80 MiB does not claim the full production 60-download peak', async () => {
  const segmentBytes = 64 * KIBIBYTE_BYTES;
  const memoryBudgetBytes = 80 * MEBIBYTE_BYTES;
  const result = await runSegmentSpoolingBenchmark({
    totalBytes: 64 * segmentBytes,
    segmentBytes,
    chunkBytes: segmentBytes,
    prefetchSegments: 64,
    maxConcurrentDownloads: 60,
    memoryBudgetBytes,
  });
  const expected = Math.floor(
    (memoryBudgetBytes - result.configuration.streamMemoryBytes) /
      PRODUCTION_DOWNLOAD_BASE_BYTES
  );

  assert(expected < 60);
  assert.equal(result.configuration.effectiveDownloadLimit, expected);
  assert.equal(result.pipeline.activeDownloadsPeak, expected);
  assert.equal(result.pipeline.downloadMemoryLeasesPeak, expected);
  assert.equal(result.pipeline.downloadMemoryLeasesFinal, 0);
  assert.equal(result.pipeline.writerChildLeasesFinal, 0);
  assert.equal(result.pipeline.writerChildBytesFinal, 0);
});

test('96 MiB reaches all 60 production download windows', async () => {
  const segmentBytes = 64 * KIBIBYTE_BYTES;
  const result = await runSegmentSpoolingBenchmark({
    totalBytes: 64 * segmentBytes,
    segmentBytes,
    chunkBytes: segmentBytes,
    prefetchSegments: 64,
    maxConcurrentDownloads: 60,
    memoryBudgetBytes: 96 * MEBIBYTE_BYTES,
  });

  assert.equal(result.configuration.effectiveDownloadLimit, 60);
  assert.equal(result.pipeline.activeDownloadsPeak, 60);
  assert.equal(result.pipeline.downloadMemoryLeasesPeak, 60);
  assert.equal(result.pipeline.downloadMemoryLeasesFinal, 0);
  assert.equal(result.pipeline.writerChildLeasesFinal, 0);
  assert.equal(result.pipeline.writerChildBytesFinal, 0);
  assert(result.internalMemory.peak <= result.internalMemory.max);
  assert.equal(result.internalMemory.final, 0);
  assert.equal(result.spool.finalReserved, 0);
  assert.equal(result.spool.finalActual, 0);
});

test('writer child lease stays inside one globally acquired production window', async () => {
  const streamMemoryBytes = resolveSegmentStreamMemoryBytes(
    BENCHMARK_READER_HIGH_WATER_MARK_BYTES
  );
  const memoryBudgetBytes = streamMemoryBytes + PRODUCTION_DOWNLOAD_BASE_BYTES;
  const writerChunkBytes = 64 * KIBIBYTE_BYTES;
  const result = await runSegmentSpoolingBenchmark({
    totalBytes: 2 * writerChunkBytes,
    segmentBytes: 2 * writerChunkBytes,
    chunkBytes: writerChunkBytes,
    prefetchSegments: 1,
    maxConcurrentDownloads: 1,
    memoryBudgetBytes,
  });

  assert.equal(result.configuration.effectiveDownloadLimit, 1);
  assert.equal(result.pipeline.downloadMemoryLeasesPeak, 1);
  assert.equal(result.pipeline.writerChildLeasesPeak, 1);
  assert.equal(result.pipeline.writerChildBytesPeak, writerChunkBytes);
  assert.equal(result.pipeline.writerChildReleasedWhileBaseLeaseHeld, true);
  assert.equal(result.internalMemory.max, memoryBudgetBytes);
  assert.equal(result.internalMemory.peak, memoryBudgetBytes);
  assert.equal(result.pipeline.writerChildLeasesFinal, 0);
  assert.equal(result.pipeline.writerChildBytesFinal, 0);
  assert.equal(result.pipeline.downloadMemoryLeasesFinal, 0);
  assert.equal(result.internalMemory.final, 0);
});

test('artifact and writer failures release base, child, memory and spool ownership', async () => {
  const failureStages: readonly SegmentSpoolingBenchmarkStage[] = [
    'artifact-create',
    'first-writer-chunk',
    'later-writer-chunk',
    'artifact-complete',
  ];
  for (const failAt of failureStages) {
    let finalOwnership: SegmentSpoolingBenchmarkOwnershipSnapshot | undefined;
    await assert.rejects(
      runSegmentSpoolingBenchmark({
        totalBytes: 96 * KIBIBYTE_BYTES,
        segmentBytes: 96 * KIBIBYTE_BYTES,
        chunkBytes: 32 * KIBIBYTE_BYTES,
        prefetchSegments: 1,
        maxConcurrentDownloads: 1,
        testHooks: {
          failAt,
          onSettled: (snapshot) => {
            finalOwnership = snapshot;
          },
        },
      }),
      new RegExp(`Injected benchmark failure at ${failAt}`)
    );
    assertOwnershipZero(finalOwnership);
  }
});

test('benchmark abort after writer-child admission releases every owner', async () => {
  const controller = new AbortController();
  let finalOwnership: SegmentSpoolingBenchmarkOwnershipSnapshot | undefined;
  let aborted = false;
  await assert.rejects(
    runSegmentSpoolingBenchmark({
      totalBytes: 96 * KIBIBYTE_BYTES,
      segmentBytes: 96 * KIBIBYTE_BYTES,
      chunkBytes: 32 * KIBIBYTE_BYTES,
      prefetchSegments: 1,
      maxConcurrentDownloads: 1,
      signal: controller.signal,
      testHooks: {
        onStage: (stage, details) => {
          if (
            !aborted &&
            stage === 'writer-child-acquired' &&
            details.segmentIndex === 0
          ) {
            aborted = true;
            controller.abort(new Error('Injected benchmark abort'));
          }
        },
        onSettled: (snapshot) => {
          finalOwnership = snapshot;
        },
      },
    }),
    /aborted/
  );
  assertOwnershipZero(finalOwnership);
});

test('one production base window is the exact benchmark admission boundary', async () => {
  const streamMemoryBytes = resolveSegmentStreamMemoryBytes(
    BENCHMARK_READER_HIGH_WATER_MARK_BYTES
  );
  const exactBudget = streamMemoryBytes + PRODUCTION_DOWNLOAD_BASE_BYTES;
  const result = await runSegmentSpoolingBenchmark({
    totalBytes: 64 * KIBIBYTE_BYTES,
    segmentBytes: 64 * KIBIBYTE_BYTES,
    chunkBytes: 32 * KIBIBYTE_BYTES,
    prefetchSegments: 1,
    maxConcurrentDownloads: 1,
    memoryBudgetBytes: exactBudget,
  });
  assert.equal(result.configuration.effectiveDownloadLimit, 1);
  assert.equal(result.pipeline.activeDownloadsPeak, 1);

  await assert.rejects(
    runSegmentSpoolingBenchmark({
      totalBytes: 64 * KIBIBYTE_BYTES,
      segmentBytes: 64 * KIBIBYTE_BYTES,
      chunkBytes: 32 * KIBIBYTE_BYTES,
      prefetchSegments: 1,
      maxConcurrentDownloads: 1,
      memoryBudgetBytes: exactBudget - 1,
    }),
    /cannot admit one stream and one download/
  );
});
