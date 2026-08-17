import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import { promises as fs } from 'node:fs';
import path from 'node:path';
import { tmpdir } from 'node:os';
import { pathToFileURL } from 'node:url';
import { ByteBudget } from '../pool/byte-budget.js';
import { SpoolManager } from '../spool/manager.js';
import type { SegmentSpoolingPlan } from '../resource-plan.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;

export interface SegmentSpoolingBenchmarkOptions {
  readonly totalBytes?: number;
  readonly segmentBytes?: number;
  readonly chunkBytes?: number;
}

export interface SegmentSpoolingBenchmarkResult {
  readonly bytes: number;
  readonly segments: number;
  readonly firstByteMs: number;
  readonly durationMs: number;
  readonly throughputBytesPerSecond: number;
  readonly arrayBuffers: {
    readonly before: number;
    readonly peak: number;
    readonly after: number;
  };
  readonly external: {
    readonly before: number;
    readonly peak: number;
    readonly after: number;
  };
  readonly eventLoopLagMs: { readonly mean: number; readonly max: number };
  readonly internalMemory: {
    readonly max: number;
    readonly peak: number;
    readonly final: number;
  };
  readonly spool: {
    readonly max: number;
    readonly peakReserved: number;
    readonly peakActual: number;
    readonly finalReserved: number;
    readonly finalActual: number;
  };
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a safe positive integer`);
  }
  return value;
}

function benchmarkPlan(chunkBytes: number): SegmentSpoolingPlan {
  const writerQueueBytes = Math.max(4 * chunkBytes, 256 * KIBIBYTE_BYTES);
  return {
    memoryBudgetBytes: 16 * MEBIBYTE_BYTES,
    perStreamBufferBytes: 2 * MEBIBYTE_BYTES,
    spoolBytes: 512 * MEBIBYTE_BYTES,
    minFreeDiskBytes: 0,
    decoderChunkBytes: chunkBytes,
    writerQueueBytes,
    readerHighWaterMarkBytes: 256 * KIBIBYTE_BYTES,
    perDownloadBaseLeaseBytes: writerQueueBytes,
    maxOpenSpoolFiles: 16,
    orphanTtlMs: 60_000,
  };
}

/**
 * Reproducible single-process spool benchmark. Correctness gates use only the
 * explicit byte owners; V8 `arrayBuffers`/`external` are reported as diagnostic
 * measurements and never used as CI pass/fail thresholds.
 */
export async function runSegmentSpoolingBenchmark(
  options: SegmentSpoolingBenchmarkOptions = {}
): Promise<SegmentSpoolingBenchmarkResult> {
  const totalBytes = positiveInteger(
    options.totalBytes ?? 64 * MEBIBYTE_BYTES,
    'totalBytes'
  );
  const segmentBytes = positiveInteger(
    options.segmentBytes ?? 2 * MEBIBYTE_BYTES,
    'segmentBytes'
  );
  const chunkBytes = positiveInteger(
    options.chunkBytes ?? 64 * KIBIBYTE_BYTES,
    'chunkBytes'
  );
  if (chunkBytes > segmentBytes) {
    throw new Error('chunkBytes cannot exceed segmentBytes');
  }
  const plan = benchmarkPlan(chunkBytes);
  const cacheRoot = await fs.mkdtemp(path.join(tmpdir(), 'spool-benchmark-'));
  const manager = new SpoolManager({
    plan,
    engineId: 'benchmark',
    cacheRoot,
  });
  const memory = new ByteBudget(plan.memoryBudgetBytes);
  const before = process.memoryUsage();
  const eventLoopDelay = monitorEventLoopDelay({ resolution: 10 });
  eventLoopDelay.enable();
  let arrayBuffersPeak = before.arrayBuffers;
  let externalPeak = before.external;
  let firstByteAt: number | undefined;
  let consumedBytes = 0;
  const startedAt = performance.now();
  const segmentCount = Math.ceil(totalBytes / segmentBytes);

  const sampleMemory = (): void => {
    const usage = process.memoryUsage();
    arrayBuffersPeak = Math.max(arrayBuffersPeak, usage.arrayBuffers);
    externalPeak = Math.max(externalPeak, usage.external);
  };

  try {
    for (let segment = 0; segment < segmentCount; segment++) {
      const size = Math.min(segmentBytes, totalBytes - consumedBytes);
      const artifact = await manager.createArtifact({
        sessionId: 'benchmark-session',
        segmentId: `segment-${segment}`,
        initialReservationBytes: size,
      });
      const reader = artifact.createReadStream({
        highWaterMark: plan.readerHighWaterMarkBytes,
      });
      const consuming = (async () => {
        let received = 0;
        for await (const chunk of reader) {
          if (!Buffer.isBuffer(chunk))
            throw new Error('unexpected reader chunk');
          firstByteAt ??= performance.now();
          received += chunk.length;
          sampleMemory();
        }
        return received;
      })();

      for (let offset = 0; offset < size; offset += chunkBytes) {
        const length = Math.min(chunkBytes, size - offset);
        const lease = await memory.acquire(length);
        const chunk = Buffer.alloc(length, (segment + offset) % 251);
        try {
          if (!artifact.write(chunk, lease)) {
            await new Promise<void>((resolve) => artifact.onceDrain(resolve));
          }
        } catch (error) {
          lease.release();
          throw error;
        }
        sampleMemory();
      }
      await artifact.complete();
      const received = await consuming;
      if (received !== size) throw new Error('benchmark reader lost bytes');
      consumedBytes += received;
      sampleMemory();
      await artifact.dispose();
    }
  } finally {
    eventLoopDelay.disable();
    await manager.close();
    memory.close();
    await fs.rm(cacheRoot, { recursive: true, force: true });
  }

  const finishedAt = performance.now();
  const after = process.memoryUsage();
  const memoryStats = memory.stats();
  const spoolStats = manager.stats().budget;
  if (
    memoryStats.peakBytes > memoryStats.maxBytes ||
    memoryStats.usedBytes !== 0
  ) {
    throw new Error('internal memory budget invariant failed');
  }
  if (
    spoolStats.peakReservedBytes > spoolStats.maxBytes ||
    spoolStats.reservedBytes !== 0 ||
    spoolStats.actualBytes !== 0
  ) {
    throw new Error('internal spool budget invariant failed');
  }
  const durationMs = finishedAt - startedAt;
  return {
    bytes: consumedBytes,
    segments: segmentCount,
    firstByteMs: (firstByteAt ?? finishedAt) - startedAt,
    durationMs,
    throughputBytesPerSecond: Math.round(
      consumedBytes / Math.max(durationMs / 1000, Number.EPSILON)
    ),
    arrayBuffers: {
      before: before.arrayBuffers,
      peak: arrayBuffersPeak,
      after: after.arrayBuffers,
    },
    external: {
      before: before.external,
      peak: externalPeak,
      after: after.external,
    },
    eventLoopLagMs: {
      mean: Number.isFinite(eventLoopDelay.mean)
        ? eventLoopDelay.mean / 1_000_000
        : 0,
      max: eventLoopDelay.max / 1_000_000,
    },
    internalMemory: {
      max: memoryStats.maxBytes,
      peak: memoryStats.peakBytes,
      final: memoryStats.usedBytes,
    },
    spool: {
      max: spoolStats.maxBytes,
      peakReserved: spoolStats.peakReservedBytes,
      peakActual: spoolStats.peakActualBytes,
      finalReserved: spoolStats.reservedBytes,
      finalActual: spoolStats.actualBytes,
    },
  };
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href
) {
  const result = await runSegmentSpoolingBenchmark({
    totalBytes: process.env.USENET_BENCHMARK_BYTES
      ? Number(process.env.USENET_BENCHMARK_BYTES)
      : undefined,
    segmentBytes: process.env.USENET_BENCHMARK_SEGMENT_BYTES
      ? Number(process.env.USENET_BENCHMARK_SEGMENT_BYTES)
      : undefined,
    chunkBytes: process.env.USENET_BENCHMARK_CHUNK_BYTES
      ? Number(process.env.USENET_BENCHMARK_CHUNK_BYTES)
      : undefined,
  });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}
