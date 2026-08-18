import assert from 'node:assert/strict';
import test from 'node:test';
import {
  resolveEngineResourcePlan,
  resolveEstimatedDecodedSegmentBytes,
  resolveSegmentArenaBytes,
  resolveSegmentBufferingArenaBytes,
  resolveSegmentSpoolingArenaBytes,
  resolveSegmentSpoolingDownloadMemoryPlan,
  resolveSegmentSpoolingMaxOpenFiles,
  resolveSegmentSpoolingReaderHighWaterMarkBytes,
  resolveSegmentSpoolingWriterQueueBytes,
  resolveStreamResourcePlan,
  UsenetResourcePlanConfigError,
  type EngineResourcePlanOptions,
  type UsenetResourcePlanConfigField,
  type UsenetResourcePlanConfigIssueCode,
} from './resource-plan.js';
import { DEFAULT_ENGINE_OPTIONS } from './types.js';
import {
  NNTP_READ_CARRY_MAX_BYTES,
  NNTP_READ_CARRY_MAX_CHUNKS,
  NNTP_READ_WINDOW_BYTES,
} from './nntp/read-carry.js';
import {
  SEGMENT_STREAM_MAX_CHUNK_BYTES,
  resolveSegmentStreamMemoryBytes,
} from './stream-queue-budget.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;

function resourceOptions(
  overrides: Partial<EngineResourcePlanOptions> = {}
): EngineResourcePlanOptions {
  return {
    streamingMode: 'segment_spooling',
    maxConcurrentDownloads: 60,
    segmentMemoryCacheBytes: 0,
    segmentSpoolingMemoryBudgetBytes: 128_000_000,
    segmentSpoolingStreamBufferBytes: 8_000_000,
    segmentSpoolingSpoolBytes: 2_000_000_000,
    segmentSpoolingMinFreeDiskBytes: 512_000_000,
    ...overrides,
  };
}

function expectConfigIssue(
  options: EngineResourcePlanOptions,
  code: UsenetResourcePlanConfigIssueCode,
  field: UsenetResourcePlanConfigField
): void {
  assert.throws(
    () => resolveEngineResourcePlan(options),
    (error: unknown) => {
      assert.ok(error instanceof UsenetResourcePlanConfigError);
      assert.equal(error.code, 'USENET_RESOURCE_PLAN_CONFIG_INVALID');
      assert.ok(error.message.includes(field));
      assert.ok(
        error.issues.some(
          (issue) => issue.code === code && issue.field === field
        )
      );
      return true;
    }
  );
}

test('segment-buffering arena auto-sizing preserves every clamp region', () => {
  assert.equal(resolveSegmentBufferingArenaBytes(1), 64 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentBufferingArenaBytes(42), 64 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentBufferingArenaBytes(43), 64.5 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentBufferingArenaBytes(106), 159 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentBufferingArenaBytes(107), 160 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentBufferingArenaBytes(1_000), 160 * MEBIBYTE_BYTES);
});

test('segment-spooling arena auto-sizing uses the smaller formula and clamps', () => {
  assert.equal(resolveSegmentSpoolingArenaBytes(1), 16 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentSpoolingArenaBytes(32), 16 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentSpoolingArenaBytes(33), 16.5 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentSpoolingArenaBytes(96), 48 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentSpoolingArenaBytes(97), 48 * MEBIBYTE_BYTES);
  assert.equal(resolveSegmentSpoolingArenaBytes(1_000), 48 * MEBIBYTE_BYTES);
});

test('a positive segment-memory-cache override bypasses both arena formulas', () => {
  const explicitBytes = 7_654_321;
  assert.equal(
    resolveSegmentArenaBytes({
      streamingMode: 'segment_buffering',
      maxConcurrentDownloads: 1_000,
      segmentMemoryCacheBytes: explicitBytes,
    }),
    explicitBytes
  );
  assert.equal(
    resolveSegmentArenaBytes({
      streamingMode: 'segment_spooling',
      maxConcurrentDownloads: 1_000,
      segmentMemoryCacheBytes: explicitBytes,
    }),
    explicitBytes
  );
});

test('segment-spooling queue and file-cap formulas cover their clamp bounds', () => {
  assert.equal(resolveSegmentSpoolingWriterQueueBytes(0), 1 * MEBIBYTE_BYTES);
  assert.equal(
    resolveSegmentSpoolingWriterQueueBytes(2 * MEBIBYTE_BYTES),
    1 * MEBIBYTE_BYTES
  );
  assert.equal(
    resolveSegmentSpoolingWriterQueueBytes(5 * MEBIBYTE_BYTES),
    2.5 * MEBIBYTE_BYTES
  );
  assert.equal(
    resolveSegmentSpoolingWriterQueueBytes(8 * MEBIBYTE_BYTES),
    4 * MEBIBYTE_BYTES
  );
  assert.equal(
    resolveSegmentSpoolingWriterQueueBytes(10 * MEBIBYTE_BYTES),
    4 * MEBIBYTE_BYTES
  );

  assert.equal(
    resolveSegmentSpoolingReaderHighWaterMarkBytes(0),
    256 * KIBIBYTE_BYTES
  );
  assert.equal(
    resolveSegmentSpoolingReaderHighWaterMarkBytes(1 * MEBIBYTE_BYTES),
    256 * KIBIBYTE_BYTES
  );
  assert.equal(
    resolveSegmentSpoolingReaderHighWaterMarkBytes(3 * MEBIBYTE_BYTES),
    768 * KIBIBYTE_BYTES
  );
  assert.equal(
    resolveSegmentSpoolingReaderHighWaterMarkBytes(8 * MEBIBYTE_BYTES),
    2 * MEBIBYTE_BYTES
  );
  assert.equal(
    resolveSegmentSpoolingReaderHighWaterMarkBytes(12 * MEBIBYTE_BYTES),
    2 * MEBIBYTE_BYTES
  );

  assert.equal(resolveSegmentSpoolingMaxOpenFiles(1), 32);
  assert.equal(resolveSegmentSpoolingMaxOpenFiles(16), 32);
  assert.equal(resolveSegmentSpoolingMaxOpenFiles(17), 34);
  assert.equal(resolveSegmentSpoolingMaxOpenFiles(128), 256);
  assert.equal(resolveSegmentSpoolingMaxOpenFiles(129), 256);
});

test('default engine options resolve to the specified effective plans', () => {
  const buffering = resolveEngineResourcePlan(DEFAULT_ENGINE_OPTIONS);
  assert.deepEqual(buffering, {
    mode: 'segment_buffering',
    arenaBytes: 90 * MEBIBYTE_BYTES,
  });

  const spooling = resolveEngineResourcePlan({
    ...DEFAULT_ENGINE_OPTIONS,
    streamingMode: 'segment_spooling',
  });
  assert.equal(spooling.mode, 'segment_spooling');
  assert.equal(spooling.arenaBytes, 30 * MEBIBYTE_BYTES);
  assert.deepEqual(spooling.segmentSpooling, {
    memoryBudgetBytes: 128_000_000,
    perStreamBufferBytes: 8_000_000,
    spoolBytes: 2_000_000_000,
    minFreeDiskBytes: 512_000_000,
    decoderChunkBytes: 256 * KIBIBYTE_BYTES,
    writerQueueBytes: 4_000_000,
    readerHighWaterMarkBytes: 2_000_000,
    perDownloadBaseLeaseBytes: 512 * KIBIBYTE_BYTES + NNTP_READ_CARRY_MAX_BYTES,
    maxOpenSpoolFiles: 120,
    orphanTtlMs: 24 * 60 * 60_000,
  });
});

test('download admission reserves the two-chunk direct batch and TLS carry atomically', () => {
  const plan = resolveEngineResourcePlan(
    resourceOptions({
      segmentSpoolingMemoryBudgetBytes: 16 * MEBIBYTE_BYTES,
      segmentSpoolingStreamBufferBytes: 2 * MEBIBYTE_BYTES,
    })
  ).segmentSpooling;
  assert.ok(plan);
  assert.equal(NNTP_READ_WINDOW_BYTES, 256 * KIBIBYTE_BYTES);
  assert.equal(NNTP_READ_CARRY_MAX_CHUNKS, 256);
  assert.equal(NNTP_READ_CARRY_MAX_BYTES, 1 * MEBIBYTE_BYTES);
  assert.equal(
    plan.perDownloadBaseLeaseBytes,
    2 * plan.decoderChunkBytes + NNTP_READ_CARRY_MAX_BYTES
  );
  assert(
    plan.perDownloadBaseLeaseBytes <=
      plan.memoryBudgetBytes - plan.perStreamBufferBytes,
    'the 16 MiB minimum must admit one download beside one stream window'
  );
});

test('128 KiB artifact chunks retain one file stream inside the 2 MiB minimum', () => {
  const readerHighWaterMarkBytes =
    resolveSegmentSpoolingReaderHighWaterMarkBytes(2 * MEBIBYTE_BYTES);
  assert.equal(SEGMENT_STREAM_MAX_CHUNK_BYTES, 128 * KIBIBYTE_BYTES);
  assert(
    resolveSegmentStreamMemoryBytes(
      readerHighWaterMarkBytes,
      readerHighWaterMarkBytes
    ) <=
      2 * MEBIBYTE_BYTES
  );
});

test('download memory helper is the single production admission source', () => {
  const downloadMemory = resolveSegmentSpoolingDownloadMemoryPlan();
  const plan = resolveEngineResourcePlan(resourceOptions()).segmentSpooling;
  assert.ok(plan);
  assert.deepEqual(downloadMemory, {
    decoderChunkBytes: 256 * KIBIBYTE_BYTES,
    carryBytes: 1 * MEBIBYTE_BYTES,
    perDownloadBaseLeaseBytes: 1_572_864,
  });
  assert.equal(plan.decoderChunkBytes, downloadMemory.decoderChunkBytes);
  assert.equal(
    plan.perDownloadBaseLeaseBytes,
    downloadMemory.perDownloadBaseLeaseBytes
  );
});

test('segment-spooling validation accepts every exact boundary', () => {
  const halfMemoryBoundaryPlan = resolveEngineResourcePlan(
    resourceOptions({
      segmentSpoolingMemoryBudgetBytes: 16 * MEBIBYTE_BYTES,
      segmentSpoolingStreamBufferBytes: 8 * MEBIBYTE_BYTES,
      segmentSpoolingSpoolBytes: 64 * MEBIBYTE_BYTES,
      segmentSpoolingMinFreeDiskBytes: 0,
    })
  );
  assert.equal(
    halfMemoryBoundaryPlan.segmentSpooling?.memoryBudgetBytes,
    16 * MEBIBYTE_BYTES
  );
  assert.equal(
    halfMemoryBoundaryPlan.segmentSpooling?.perStreamBufferBytes,
    8 * MEBIBYTE_BYTES
  );
  assert.equal(
    halfMemoryBoundaryPlan.segmentSpooling?.spoolBytes,
    64 * MEBIBYTE_BYTES
  );
  assert.equal(halfMemoryBoundaryPlan.segmentSpooling?.minFreeDiskBytes, 0);

  const minimumStreamBufferPlan = resolveEngineResourcePlan(
    resourceOptions({
      segmentSpoolingStreamBufferBytes: 2 * MEBIBYTE_BYTES,
    })
  );
  assert.equal(
    minimumStreamBufferPlan.segmentSpooling?.perStreamBufferBytes,
    2 * MEBIBYTE_BYTES
  );
});

test('segment-spooling validation rejects each invalid budget rule', () => {
  expectConfigIssue(
    resourceOptions({
      segmentSpoolingMemoryBudgetBytes: 16 * MEBIBYTE_BYTES - 1,
    }),
    'segment_spooling_memory_budget_too_small',
    'usenet.segmentSpoolingMemoryBudgetBytes'
  );
  expectConfigIssue(
    resourceOptions({
      segmentSpoolingStreamBufferBytes: 2 * MEBIBYTE_BYTES - 1,
    }),
    'segment_spooling_stream_buffer_too_small',
    'usenet.segmentSpoolingStreamBufferBytes'
  );
  expectConfigIssue(
    resourceOptions({
      segmentSpoolingMemoryBudgetBytes: 16 * MEBIBYTE_BYTES,
      segmentSpoolingStreamBufferBytes: 8 * MEBIBYTE_BYTES + 1,
    }),
    'segment_spooling_stream_buffer_exceeds_half_memory_budget',
    'usenet.segmentSpoolingStreamBufferBytes'
  );
  expectConfigIssue(
    resourceOptions({
      segmentSpoolingSpoolBytes: 64 * MEBIBYTE_BYTES - 1,
    }),
    'segment_spooling_spool_budget_too_small',
    'usenet.segmentSpoolingSpoolBytes'
  );
  expectConfigIssue(
    resourceOptions({ segmentSpoolingMinFreeDiskBytes: -1 }),
    'invalid_byte_value',
    'usenet.segmentSpoolingMinFreeDiskBytes'
  );
});

test('segment buffering ignores dormant segment-spooling combinations', () => {
  const plan = resolveEngineResourcePlan(
    resourceOptions({
      streamingMode: 'segment_buffering',
      segmentSpoolingMemoryBudgetBytes: -1,
      segmentSpoolingStreamBufferBytes: Number.NaN,
      segmentSpoolingSpoolBytes: 0,
      segmentSpoolingMinFreeDiskBytes: -1,
    })
  );
  assert.deepEqual(plan, {
    mode: 'segment_buffering',
    arenaBytes: 90 * MEBIBYTE_BYTES,
  });
});

test('segment-memory-cache byte validity applies in both modes', () => {
  expectConfigIssue(
    resourceOptions({
      streamingMode: 'segment_buffering',
      segmentMemoryCacheBytes: -1,
    }),
    'invalid_byte_value',
    'usenet.segmentMemoryCacheBytes'
  );
  expectConfigIssue(
    resourceOptions({ segmentMemoryCacheBytes: Number.POSITIVE_INFINITY }),
    'invalid_byte_value',
    'usenet.segmentMemoryCacheBytes'
  );
});

test('resource-plan configuration errors preserve an optional cause', () => {
  const cause = new Error('source validation failed');
  const error = new UsenetResourcePlanConfigError(
    [
      {
        code: 'invalid_byte_value',
        field: 'usenet.segmentMemoryCacheBytes',
        message: 'invalid arena budget',
      },
    ],
    { cause }
  );
  assert.equal(error.cause, cause);
});

test('segment reservation and stream estimates follow the conservative formula', () => {
  assert.equal(resolveEstimatedDecodedSegmentBytes({}), 1 * MEBIBYTE_BYTES);
  assert.equal(
    resolveEstimatedDecodedSegmentBytes({
      avgDecodedSegmentBytes: 512 * KIBIBYTE_BYTES,
    }),
    1 * MEBIBYTE_BYTES
  );
  assert.equal(
    resolveEstimatedDecodedSegmentBytes({
      avgDecodedSegmentBytes: 2 * MEBIBYTE_BYTES,
    }),
    2 * MEBIBYTE_BYTES
  );
  assert.equal(
    resolveEstimatedDecodedSegmentBytes({
      segmentBytes: 3 * MEBIBYTE_BYTES,
      avgDecodedSegmentBytes: 4 * MEBIBYTE_BYTES,
    }),
    3 * MEBIBYTE_BYTES
  );
  assert.equal(
    resolveEstimatedDecodedSegmentBytes({
      segmentBytes: 512 * KIBIBYTE_BYTES,
      avgDecodedSegmentBytes: 4 * MEBIBYTE_BYTES,
    }),
    1 * MEBIBYTE_BYTES
  );

  const spooling = resolveEngineResourcePlan(resourceOptions()).segmentSpooling;
  assert.ok(spooling);
  assert.deepEqual(
    resolveStreamResourcePlan({
      prefetchSegments: 32,
      avgDecodedSegmentBytes: 2 * MEBIBYTE_BYTES,
      segmentSpooling: spooling,
    }),
    {
      prefetchSegments: 32,
      avgSegmentBytes: 2 * MEBIBYTE_BYTES,
      estimatedSpoolWindowBytes: 64 * MEBIBYTE_BYTES,
      readerHighWaterMarkBytes: 2_000_000,
      writerQueueBytes: 4_000_000,
    }
  );
});

test('unchanged buffering configuration matches the pre-patch arena formula', () => {
  const legacyArenaBytes = (maxConcurrentDownloads: number): number =>
    Math.min(
      160 * MEBIBYTE_BYTES,
      Math.max(
        64 * MEBIBYTE_BYTES,
        Math.floor(maxConcurrentDownloads * 1.5 * MEBIBYTE_BYTES)
      )
    );

  for (const maxConcurrentDownloads of [
    0, 1, 16, 30, 42, 43, 60, 106, 107, 256,
  ]) {
    const actual = resolveEngineResourcePlan(
      resourceOptions({
        streamingMode: 'segment_buffering',
        maxConcurrentDownloads,
        segmentMemoryCacheBytes: 0,
      })
    );
    assert.equal(actual.arenaBytes, legacyArenaBytes(maxConcurrentDownloads));
  }
});
