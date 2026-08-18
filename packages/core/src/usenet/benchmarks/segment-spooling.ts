import { createHash } from 'node:crypto';
import { promises as fs } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import { pathToFileURL } from 'node:url';
import '../../config/index.js';
import type { SegmentSpoolingPlan } from '../resource-plan.js';
import { NNTP_READ_CARRY_MAX_BYTES } from '../nntp/read-carry.js';
import { resolveSegmentStreamMemoryBytes } from '../stream-queue-budget.js';
import { GrowingSpoolArtifactAdapter } from '../pool/segment-artifact.js';
import type {
  SegmentArtifact,
  SegmentArtifactFetchOptions,
} from '../pool/segment-artifact.js';
import { ByteBudget, type ByteLease } from '../pool/byte-budget.js';
import { PrioritySemaphore } from '../pool/priority-semaphore.js';
import type { SegmentStreamCleanupCause } from '../pool/resource-events.js';
import {
  SpoolingSegmentsStream,
  type SpoolingSegmentArtifactSource,
} from '../pool/spooling-segments-stream.js';
import { SpoolManager } from '../spool/manager.js';
import { CommandPriority, type NzbSegmentRef } from '../types.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;
const DEFAULT_SEGMENTS = 500;
const DEFAULT_SEGMENT_BYTES = MEBIBYTE_BYTES;
const DEFAULT_CHUNK_BYTES = 64 * KIBIBYTE_BYTES;
const DEFAULT_PREFETCH_SEGMENTS = 64;
const DEFAULT_MAX_CONCURRENT_DOWNLOADS = 60;

export interface SegmentSpoolingBenchmarkOptions {
  readonly totalBytes?: number;
  readonly segmentBytes?: number;
  readonly chunkBytes?: number;
  readonly prefetchSegments?: number;
  readonly maxConcurrentDownloads?: number;
  readonly memoryBudgetBytes?: number;
}

export interface SegmentSpoolingBenchmarkResult {
  readonly configuration: {
    readonly segmentBytes: number;
    readonly chunkBytes: number;
    readonly prefetchSegments: number;
    readonly maxConcurrentDownloads: number;
    readonly memoryBudgetBytes: number;
    readonly streamMemoryBytes: number;
    readonly perDownloadBaseLeaseBytes: number;
    readonly effectiveDownloadLimit: number;
  };
  readonly bytes: number;
  readonly segments: number;
  readonly checksum: string;
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
  readonly pipeline: {
    readonly activeDownloadsPeak: number;
    readonly downloadMemoryLeasesPeak: number;
    readonly downloadMemoryLeasesFinal: number;
    readonly completedReadAheadPeak: number;
    readonly slowConsumerYields: number;
    readonly completionWasOutOfOrder: boolean;
  };
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
    readonly finalArtifacts: number;
    readonly finalOpenFiles: number;
  };
}

interface CompletionWaveMember {
  readonly index: number;
  readonly proceed: PromiseWithResolvers<void>;
  readonly completed: PromiseWithResolvers<void>;
}

interface ArtifactOwnership {
  completed: boolean;
  released: boolean;
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a safe positive integer`);
  }
  return value;
}

function immediate(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

function abortError(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) return signal.reason;
  const error = new Error('Benchmark pipeline aborted', {
    cause: signal.reason,
  });
  error.name = 'AbortError';
  return error;
}

function awaitAbortable(
  operation: Promise<void>,
  signal: AbortSignal | undefined
): Promise<void> {
  if (!signal) return operation;
  if (signal.aborted) return Promise.reject(abortError(signal));
  return new Promise<void>((resolve, reject) => {
    const onAbort = (): void => {
      signal.removeEventListener('abort', onAbort);
      reject(abortError(signal));
    };
    signal.addEventListener('abort', onAbort, { once: true });
    operation.then(
      () => {
        signal.removeEventListener('abort', onAbort);
        resolve();
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      }
    );
  });
}

/**
 * Releases each full wave in descending segment order. At most one bounded
 * wave (`maxConcurrentDownloads`) is retained; the final partial wave is
 * published when its last segment arrives.
 */
class DeterministicCompletionWaves {
  private pending: CompletionWaveMember[] = [];
  private drainTail: Promise<void> = Promise.resolve();
  private arrived = 0;

  constructor(
    private readonly width: number,
    private readonly lastIndex: number
  ) {}

  arrive(index: number): {
    readonly proceed: Promise<void>;
    readonly completed: () => void;
  } {
    const member: CompletionWaveMember = {
      index,
      proceed: Promise.withResolvers<void>(),
      completed: Promise.withResolvers<void>(),
    };
    this.pending.push(member);
    this.arrived++;
    if (
      this.pending.length === this.width ||
      this.arrived === this.lastIndex + 1
    ) {
      this.publishPending();
    }
    return {
      proceed: member.proceed.promise,
      completed: member.completed.resolve,
    };
  }

  releasePending(): void {
    if (this.pending.length > 0) this.publishPending();
  }

  settled(): Promise<void> {
    this.releasePending();
    return this.drainTail;
  }

  private publishPending(): void {
    const wave = this.pending;
    this.pending = [];
    this.drainTail = this.drainTail.then(async () => {
      for (const member of wave.sort((a, b) => b.index - a.index)) {
        member.proceed.resolve();
        await member.completed.promise;
      }
    });
  }
}

function benchmarkPlan(
  totalBytes: number,
  segmentBytes: number,
  chunkBytes: number,
  prefetchSegments: number,
  maxConcurrentDownloads: number,
  memoryBudgetBytes: number
): SegmentSpoolingPlan {
  const writerQueueBytes = Math.max(4 * chunkBytes, 256 * KIBIBYTE_BYTES);
  const readAheadBytes = Math.min(totalBytes, prefetchSegments * segmentBytes);
  const spoolBytes = Math.max(
    64 * MEBIBYTE_BYTES,
    readAheadBytes + 2 * segmentBytes
  );
  if (!Number.isSafeInteger(spoolBytes)) {
    throw new Error('benchmark spool budget exceeds safe integer range');
  }
  return {
    memoryBudgetBytes,
    perStreamBufferBytes: 2 * MEBIBYTE_BYTES,
    spoolBytes,
    minFreeDiskBytes: 0,
    decoderChunkBytes: chunkBytes,
    writerQueueBytes,
    readerHighWaterMarkBytes: 256 * KIBIBYTE_BYTES,
    perDownloadBaseLeaseBytes: 2 * chunkBytes + NNTP_READ_CARRY_MAX_BYTES,
    maxOpenSpoolFiles: Math.max(64, maxConcurrentDownloads + 4),
    orphanTtlMs: 60_000,
  };
}

/**
 * Derive a deadlock-free producer ceiling from every hard owner that may exist
 * while one simulated BODY is active. The real stream lease is acquired as one
 * window; each admitted producer additionally owns its production-equivalent
 * decoder/sink/carry lease and may have one writer chunk in flight.
 */
function resolveBenchmarkDownloadLimit(
  plan: SegmentSpoolingPlan,
  chunkBytes: number,
  maxConcurrentDownloads: number,
  segmentCount: number
): { readonly streamMemoryBytes: number; readonly downloads: number } {
  const streamMemoryBytes = resolveSegmentStreamMemoryBytes(
    plan.readerHighWaterMarkBytes
  );
  const perDownloadBytes = plan.perDownloadBaseLeaseBytes + chunkBytes;
  const availableBytes = plan.memoryBudgetBytes - streamMemoryBytes;
  const memoryLimited = Math.floor(availableBytes / perDownloadBytes);
  const downloads = Math.min(
    maxConcurrentDownloads,
    segmentCount,
    memoryLimited
  );
  if (downloads < 1) {
    throw new Error(
      'benchmark memory budget cannot admit one stream and one download'
    );
  }
  return { streamMemoryBytes, downloads };
}

function segmentLength(
  index: number,
  count: number,
  totalBytes: number,
  segmentBytes: number
): number {
  return index === count - 1 ? totalBytes - index * segmentBytes : segmentBytes;
}

function fillChunk(
  index: number,
  offset: number,
  bytes: number,
  chunkBytes: number
): Buffer {
  return Buffer.alloc(bytes, (index + Math.floor(offset / chunkBytes)) % 251);
}

function expectedChecksum(
  segmentCount: number,
  totalBytes: number,
  segmentBytes: number,
  chunkBytes: number
): string {
  const hash = createHash('sha256');
  for (let index = 0; index < segmentCount; index++) {
    const length = segmentLength(index, segmentCount, totalBytes, segmentBytes);
    for (let offset = 0; offset < length; offset += chunkBytes) {
      hash.update(
        fillChunk(
          index,
          offset,
          Math.min(chunkBytes, length - offset),
          chunkBytes
        )
      );
    }
  }
  return hash.digest('hex');
}

/** Bounded producer used by the real ordered spooling Readable. */
class BenchmarkArtifactSource implements SpoolingSegmentArtifactSource {
  private readonly downloads: PrioritySemaphore;
  private readonly waves: DeterministicCompletionWaves;
  private readonly tasks = new Set<Promise<void>>();
  private activeDownloads = 0;
  private downloadMemoryLeases = 0;
  private completedOwned = 0;
  private cleanupCause: SegmentStreamCleanupCause | undefined;
  readonly completionOrder: number[] = [];
  activeDownloadsPeak = 0;
  downloadMemoryLeasesPeak = 0;
  completedReadAheadPeak = 0;

  constructor(
    private readonly manager: SpoolManager,
    private readonly memory: ByteBudget,
    private readonly totalBytes: number,
    private readonly segmentBytes: number,
    private readonly chunkBytes: number,
    private readonly perDownloadBaseLeaseBytes: number,
    private readonly segmentCount: number,
    effectiveDownloadLimit: number,
    private readonly sampleMemory: () => void
  ) {
    this.downloads = new PrioritySemaphore(effectiveDownloadLimit);
    this.waves = new DeterministicCompletionWaves(
      Math.min(effectiveDownloadLimit, segmentCount),
      segmentCount - 1
    );
  }

  async fetchSegmentArtifact(
    segment: NzbSegmentRef,
    _nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority,
    options: SegmentArtifactFetchOptions = {}
  ): Promise<SegmentArtifact> {
    const index = Number(segment.messageId.slice('benchmark-'.length));
    if (
      !Number.isSafeInteger(index) ||
      index < 0 ||
      index >= this.segmentCount
    ) {
      throw new Error('benchmark segment id is invalid');
    }
    const length = segmentLength(
      index,
      this.segmentCount,
      this.totalBytes,
      this.segmentBytes
    );
    const begin = index * this.segmentBytes;
    const metadata = {
      byteRange: [begin, begin + length] as const,
      fileSize: this.totalBytes,
      totalParts: this.segmentCount,
      name: 'benchmark.bin',
      size: length,
    };
    const releaseDownload = await this.downloads.acquire(priority, signal);
    let downloadMemoryLease: ByteLease;
    try {
      downloadMemoryLease = await this.memory.acquire(
        this.perDownloadBaseLeaseBytes,
        { priority, signal }
      );
    } catch (error) {
      releaseDownload();
      throw error;
    }
    this.downloadMemoryLeases++;
    this.downloadMemoryLeasesPeak = Math.max(
      this.downloadMemoryLeasesPeak,
      this.downloadMemoryLeases
    );
    this.activeDownloads++;
    this.activeDownloadsPeak = Math.max(
      this.activeDownloadsPeak,
      this.activeDownloads
    );
    let artifact;
    try {
      artifact = await this.manager.createArtifact({
        sessionId: 'benchmark-session',
        segmentId: segment.messageId,
        initialReservationBytes: length,
        signal,
        priority,
      });
    } catch (error) {
      this.activeDownloads--;
      this.downloadMemoryLeases--;
      downloadMemoryLease.release();
      releaseDownload();
      throw error;
    }
    const completion = Promise.withResolvers<typeof metadata>();
    void completion.promise.catch(() => undefined);
    const ownership: ArtifactOwnership = { completed: false, released: false };
    const adapter = new GrowingSpoolArtifactAdapter(
      artifact,
      metadata,
      async () => {
        if (ownership.released) return;
        ownership.released = true;
        if (ownership.completed) this.completedOwned--;
        await artifact.dispose();
      },
      completion.promise,
      () => metadata
    );
    const producer = this.produce(
      index,
      length,
      artifact,
      ownership,
      completion,
      signal,
      priority
    ).finally(() => {
      this.activeDownloads--;
      this.downloadMemoryLeases--;
      downloadMemoryLease.release();
      releaseDownload();
    });
    const settled = producer.then(
      () => undefined,
      () => undefined
    );
    this.tasks.add(settled);
    void settled.finally(() => this.tasks.delete(settled));

    if (options.allowGrowing) {
      void producer.catch(() => undefined);
      return adapter;
    }
    try {
      await producer;
      return adapter;
    } catch (error) {
      await adapter.release();
      throw error;
    }
  }

  acquireSegmentStreamMemory(
    bytes: number,
    priority: CommandPriority,
    signal?: AbortSignal
  ): Promise<ByteLease> {
    return this.memory.acquire(bytes, { priority, signal });
  }

  recordSegmentStreamCleanup(cause: SegmentStreamCleanupCause): void {
    this.cleanupCause = cause;
  }

  async settled(): Promise<void> {
    this.waves.releasePending();
    await Promise.allSettled([...this.tasks]);
    await this.waves.settled();
  }

  assertCompletedNormally(): void {
    if (this.cleanupCause !== 'eof') {
      throw new Error(`benchmark stream cleanup was ${this.cleanupCause}`);
    }
    if (this.activeDownloads !== 0 || this.downloadMemoryLeases !== 0) {
      throw new Error('benchmark download ownership did not settle to zero');
    }
  }

  get downloadMemoryLeasesFinal(): number {
    return this.downloadMemoryLeases;
  }

  private async produce(
    index: number,
    length: number,
    artifact: Awaited<ReturnType<SpoolManager['createArtifact']>>,
    ownership: ArtifactOwnership,
    completion: PromiseWithResolvers<{
      readonly byteRange: readonly [number, number];
      readonly fileSize: number;
      readonly totalParts: number;
      readonly name: string;
      readonly size: number;
    }>,
    signal: AbortSignal | undefined,
    priority: CommandPriority
  ): Promise<void> {
    const wave = this.waves.arrive(index);
    let offset = 0;
    try {
      // The first artifact is genuinely growing: publish one bounded chunk,
      // then keep its producer open while later segments complete to disk.
      if (index === 0) {
        offset = await this.writeChunk(
          artifact,
          index,
          offset,
          length,
          signal,
          priority
        );
      }
      await awaitAbortable(wave.proceed, signal);
      while (offset < length) {
        offset = await this.writeChunk(
          artifact,
          index,
          offset,
          length,
          signal,
          priority
        );
      }
      await artifact.complete();
      ownership.completed = true;
      this.completedOwned++;
      this.completedReadAheadPeak = Math.max(
        this.completedReadAheadPeak,
        this.completedOwned
      );
      this.completionOrder.push(index);
      completion.resolve({
        byteRange: [
          index * this.segmentBytes,
          index * this.segmentBytes + length,
        ],
        fileSize: this.totalBytes,
        totalParts: this.segmentCount,
        name: 'benchmark.bin',
        size: length,
      });
      this.sampleMemory();
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      artifact.fail(failure);
      completion.reject(failure);
      throw failure;
    } finally {
      wave.completed();
    }
  }

  private async writeChunk(
    artifact: Awaited<ReturnType<SpoolManager['createArtifact']>>,
    index: number,
    offset: number,
    length: number,
    signal: AbortSignal | undefined,
    priority: CommandPriority
  ): Promise<number> {
    const bytes = Math.min(this.chunkBytes, length - offset);
    const lease = await this.memory.acquire(bytes, { signal, priority });
    const chunk = fillChunk(index, offset, bytes, this.chunkBytes);
    try {
      if (!artifact.write(chunk, lease)) {
        await new Promise<void>((resolve) => artifact.onceDrain(resolve));
      }
    } catch (error) {
      lease.release();
      throw error;
    }
    this.sampleMemory();
    return offset + bytes;
  }
}

/**
 * Reproducible ordered-spooling pipeline benchmark. Correctness gates use only
 * explicit owners and byte budgets. V8 memory, throughput and event-loop data
 * are diagnostic and intentionally have no fixed CI threshold.
 */
export async function runSegmentSpoolingBenchmark(
  options: SegmentSpoolingBenchmarkOptions = {}
): Promise<SegmentSpoolingBenchmarkResult> {
  const segmentBytes = positiveInteger(
    options.segmentBytes ?? DEFAULT_SEGMENT_BYTES,
    'segmentBytes'
  );
  const totalBytes = positiveInteger(
    options.totalBytes ?? DEFAULT_SEGMENTS * segmentBytes,
    'totalBytes'
  );
  const chunkBytes = positiveInteger(
    options.chunkBytes ?? DEFAULT_CHUNK_BYTES,
    'chunkBytes'
  );
  const prefetchSegments = positiveInteger(
    options.prefetchSegments ?? DEFAULT_PREFETCH_SEGMENTS,
    'prefetchSegments'
  );
  const maxConcurrentDownloads = positiveInteger(
    options.maxConcurrentDownloads ?? DEFAULT_MAX_CONCURRENT_DOWNLOADS,
    'maxConcurrentDownloads'
  );
  const memoryBudgetBytes = positiveInteger(
    options.memoryBudgetBytes ?? 16 * MEBIBYTE_BYTES,
    'memoryBudgetBytes'
  );
  if (chunkBytes > Math.min(segmentBytes, DEFAULT_CHUNK_BYTES)) {
    throw new Error('chunkBytes cannot exceed the segment size or 64 KiB');
  }
  const segmentCount = Math.ceil(totalBytes / segmentBytes);
  const plan = benchmarkPlan(
    totalBytes,
    segmentBytes,
    chunkBytes,
    prefetchSegments,
    maxConcurrentDownloads,
    memoryBudgetBytes
  );
  const admission = resolveBenchmarkDownloadLimit(
    plan,
    chunkBytes,
    maxConcurrentDownloads,
    segmentCount
  );
  const cacheRoot = await fs.mkdtemp(path.join(tmpdir(), 'spool-benchmark-'));
  const manager = new SpoolManager({
    plan,
    engineId: 'benchmark',
    cacheRoot,
    maxArtifacts: prefetchSegments + maxConcurrentDownloads,
  });
  const memory = new ByteBudget(plan.memoryBudgetBytes, {
    maxWaiters: prefetchSegments + maxConcurrentDownloads + 8,
  });
  const expected = expectedChecksum(
    segmentCount,
    totalBytes,
    segmentBytes,
    chunkBytes
  );
  const before = process.memoryUsage();
  const eventLoopDelay = monitorEventLoopDelay({ resolution: 10 });
  eventLoopDelay.enable();
  let arrayBuffersPeak = before.arrayBuffers;
  let externalPeak = before.external;
  let firstByteAt: number | undefined;
  let consumedBytes = 0;
  let slowConsumerYields = 0;
  const startedAt = performance.now();

  const sampleMemory = (): void => {
    const usage = process.memoryUsage();
    arrayBuffersPeak = Math.max(arrayBuffersPeak, usage.arrayBuffers);
    externalPeak = Math.max(externalPeak, usage.external);
  };
  const source = new BenchmarkArtifactSource(
    manager,
    memory,
    totalBytes,
    segmentBytes,
    chunkBytes,
    plan.perDownloadBaseLeaseBytes,
    segmentCount,
    admission.downloads,
    sampleMemory
  );
  const segments: NzbSegmentRef[] = Array.from(
    { length: segmentCount },
    (_, index) => ({
      messageId: `benchmark-${index}`,
      bytes: segmentLength(index, segmentCount, totalBytes, segmentBytes),
    })
  );
  const stream = new SpoolingSegmentsStream({
    pool: source,
    segments,
    nzbHash: 'benchmark',
    maxPrefetchSegments: prefetchSegments,
    readerHighWaterMarkBytes: plan.readerHighWaterMarkBytes,
    firstSegmentStartByte: 0,
    fileEndByte: totalBytes,
    layoutHint: 'global-range',
    priority: CommandPriority.High,
    sizeForSegment: (index) =>
      segmentLength(index, segmentCount, totalBytes, segmentBytes),
    byteRangeForSegment: (index) => {
      const begin = index * segmentBytes;
      return [
        begin,
        begin + segmentLength(index, segmentCount, totalBytes, segmentBytes),
      ];
    },
  });
  const outputHash = createHash('sha256');

  let benchmarkError: unknown;
  try {
    for await (const chunk of stream) {
      if (!Buffer.isBuffer(chunk)) throw new Error('unexpected stream chunk');
      firstByteAt ??= performance.now();
      consumedBytes += chunk.length;
      outputHash.update(chunk);
      slowConsumerYields++;
      sampleMemory();
      // One deterministic event-loop yield per delivered chunk models a slow
      // player and lets provider/disk read-ahead reach its bounded windows.
      await immediate();
    }
    await source.settled();
    source.assertCompletedNormally();
  } catch (error) {
    benchmarkError = error;
    stream.destroy(error instanceof Error ? error : new Error(String(error)));
    await source.settled();
  } finally {
    eventLoopDelay.disable();
    await manager.close().catch((error: unknown) => {
      benchmarkError ??= error;
    });
    memory.close();
    await fs.rm(cacheRoot, { recursive: true, force: true });
  }
  if (benchmarkError !== undefined) throw benchmarkError;

  const finishedAt = performance.now();
  const after = process.memoryUsage();
  const memoryStats = memory.stats();
  const managerStats = manager.stats();
  const spoolStats = managerStats.budget;
  const checksum = outputHash.digest('hex');
  const completionWasOutOfOrder = source.completionOrder.some(
    (value, index, order) => index > 0 && value < order[index - 1]
  );
  if (consumedBytes !== totalBytes || checksum !== expected) {
    throw new Error('ordered spooling benchmark output identity failed');
  }
  if (
    memoryStats.peakBytes > memoryStats.maxBytes ||
    memoryStats.usedBytes !== 0
  ) {
    throw new Error('internal memory budget invariant failed');
  }
  if (
    source.activeDownloadsPeak > admission.downloads ||
    source.downloadMemoryLeasesPeak > admission.downloads ||
    source.downloadMemoryLeasesFinal !== 0
  ) {
    throw new Error('benchmark download memory admission invariant failed');
  }
  if (
    spoolStats.peakReservedBytes > spoolStats.maxBytes ||
    spoolStats.reservedBytes !== 0 ||
    spoolStats.actualBytes !== 0 ||
    managerStats.artifacts !== 0 ||
    managerStats.files.openFiles !== 0
  ) {
    throw new Error('internal spool owner invariant failed');
  }
  if (
    segmentCount > 1 &&
    (!completionWasOutOfOrder || source.completedReadAheadPeak <= 1)
  ) {
    throw new Error('benchmark did not exercise out-of-order disk read-ahead');
  }
  const durationMs = finishedAt - startedAt;
  return {
    configuration: {
      segmentBytes,
      chunkBytes,
      prefetchSegments,
      maxConcurrentDownloads,
      memoryBudgetBytes,
      streamMemoryBytes: admission.streamMemoryBytes,
      perDownloadBaseLeaseBytes: plan.perDownloadBaseLeaseBytes,
      effectiveDownloadLimit: admission.downloads,
    },
    bytes: consumedBytes,
    segments: segmentCount,
    checksum,
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
    pipeline: {
      activeDownloadsPeak: source.activeDownloadsPeak,
      downloadMemoryLeasesPeak: source.downloadMemoryLeasesPeak,
      downloadMemoryLeasesFinal: source.downloadMemoryLeasesFinal,
      completedReadAheadPeak: source.completedReadAheadPeak,
      slowConsumerYields,
      completionWasOutOfOrder,
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
      finalArtifacts: managerStats.artifacts,
      finalOpenFiles: managerStats.files.openFiles,
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
    prefetchSegments: process.env.USENET_BENCHMARK_PREFETCH_SEGMENTS
      ? Number(process.env.USENET_BENCHMARK_PREFETCH_SEGMENTS)
      : undefined,
    maxConcurrentDownloads: process.env
      .USENET_BENCHMARK_MAX_CONCURRENT_DOWNLOADS
      ? Number(process.env.USENET_BENCHMARK_MAX_CONCURRENT_DOWNLOADS)
      : undefined,
    memoryBudgetBytes: process.env.USENET_BENCHMARK_MEMORY_BYTES
      ? Number(process.env.USENET_BENCHMARK_MEMORY_BYTES)
      : undefined,
  });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}
