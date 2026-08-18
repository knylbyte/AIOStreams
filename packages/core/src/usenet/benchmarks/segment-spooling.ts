import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { promises as fs } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import { pathToFileURL } from 'node:url';
import '../../config/index.js';
import {
  resolveSegmentSpoolingDownloadMemoryPlan,
  type SegmentSpoolingPlan,
} from '../resource-plan.js';
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
  readonly signal?: AbortSignal;
  /** Deterministic failure/ownership seams used only by benchmark tests. */
  readonly testHooks?: SegmentSpoolingBenchmarkTestHooks;
}

export type SegmentSpoolingBenchmarkStage =
  | 'artifact-create'
  | 'first-writer-chunk'
  | 'later-writer-chunk'
  | 'writer-child-acquired'
  | 'artifact-complete';

export interface SegmentSpoolingBenchmarkOwnershipSnapshot {
  readonly activeDownloads: number;
  readonly downloadBaseLeases: number;
  readonly writerChildLeases: number;
  readonly writerChildBytes: number;
  readonly memoryUsedBytes: number;
  readonly artifacts: number;
  readonly openFiles: number;
  readonly spoolReservedBytes: number;
  readonly spoolActualBytes: number;
}

export interface SegmentSpoolingBenchmarkTestHooks {
  readonly failAt?: SegmentSpoolingBenchmarkStage;
  readonly onStage?: (
    stage: SegmentSpoolingBenchmarkStage,
    details: { readonly segmentIndex: number; readonly chunkIndex?: number }
  ) => void;
  readonly onSettled?: (
    snapshot: SegmentSpoolingBenchmarkOwnershipSnapshot
  ) => void;
}

export interface SegmentSpoolingBenchmarkResult {
  readonly configuration: {
    readonly segmentBytes: number;
    readonly decoderChunkBytes: number;
    readonly writerChunkBytes: number;
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
    readonly writerChildLeasesPeak: number;
    readonly writerChildLeasesFinal: number;
    readonly writerChildBytesPeak: number;
    readonly writerChildBytesFinal: number;
    readonly writerChildReleasedWhileBaseLeaseHeld: boolean;
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
  writerChunkBytes: number,
  prefetchSegments: number,
  maxConcurrentDownloads: number,
  memoryBudgetBytes: number
): SegmentSpoolingPlan {
  const downloadMemory = resolveSegmentSpoolingDownloadMemoryPlan();
  const writerQueueBytes = Math.max(4 * writerChunkBytes, 256 * KIBIBYTE_BYTES);
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
    decoderChunkBytes: downloadMemory.decoderChunkBytes,
    writerQueueBytes,
    readerHighWaterMarkBytes: 256 * KIBIBYTE_BYTES,
    perDownloadBaseLeaseBytes: downloadMemory.perDownloadBaseLeaseBytes,
    maxOpenSpoolFiles: Math.max(64, maxConcurrentDownloads + 4),
    orphanTtlMs: 60_000,
  };
}

/**
 * Derive a deadlock-free producer ceiling from every hard owner that may exist
 * while one simulated BODY is active. The real stream lease is acquired as one
 * window; each admitted producer additionally owns its production decoder,
 * sink and TLS-carry base lease. Writer chunks are logical children inside
 * that already reserved base window and therefore are not subtracted again.
 */
function resolveBenchmarkDownloadLimit(
  plan: SegmentSpoolingPlan,
  maxConcurrentDownloads: number,
  segmentCount: number
): { readonly streamMemoryBytes: number; readonly downloads: number } {
  const streamMemoryBytes = resolveSegmentStreamMemoryBytes(
    plan.readerHighWaterMarkBytes
  );
  const availableBytes = plan.memoryBudgetBytes - streamMemoryBytes;
  const memoryLimited = Math.floor(
    availableBytes / plan.perDownloadBaseLeaseBytes
  );
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
  writerChunkBytes: number
): Buffer {
  return Buffer.alloc(
    bytes,
    (index + Math.floor(offset / writerChunkBytes)) % 251
  );
}

function expectedChecksum(
  segmentCount: number,
  totalBytes: number,
  segmentBytes: number,
  writerChunkBytes: number
): string {
  const hash = createHash('sha256');
  for (let index = 0; index < segmentCount; index++) {
    const length = segmentLength(index, segmentCount, totalBytes, segmentBytes);
    for (let offset = 0; offset < length; offset += writerChunkBytes) {
      hash.update(
        fillChunk(
          index,
          offset,
          Math.min(writerChunkBytes, length - offset),
          writerChunkBytes
        )
      );
    }
  }
  return hash.digest('hex');
}

interface BenchmarkWriterChildLease {
  readonly lease: ByteLease;
  readonly released: Promise<void>;
}

/**
 * One globally leased production download window with exactly one possible
 * writer child. The child is accounting-only: its bytes are already covered
 * by `baseLease` and must never acquire the global ByteBudget a second time.
 */
class BenchmarkDownloadMemoryWindow {
  private writerChildBytes = 0;
  private writerChildLeases = 0;
  private activeChild: PromiseWithResolvers<void> | undefined;
  private closing = false;
  private released = false;
  private closePromise: Promise<void> | undefined;

  constructor(
    private readonly baseLease: ByteLease,
    private readonly maxWriterChildBytes: number,
    private readonly onChildAcquired: (bytes: number) => void,
    private readonly onChildReleased: (bytes: number) => void
  ) {
    assert(Number.isSafeInteger(maxWriterChildBytes));
    assert(maxWriterChildBytes > 0);
    assert(baseLease.bytes >= maxWriterChildBytes);
    this.assertInvariants();
  }

  acquireWriterChild(bytes: number): BenchmarkWriterChildLease {
    assert(!this.closing);
    assert(!this.released);
    assert.equal(this.writerChildLeases, 0);
    assert(Number.isSafeInteger(bytes));
    assert(bytes > 0);
    assert(bytes <= this.maxWriterChildBytes);

    const settled = Promise.withResolvers<void>();
    this.activeChild = settled;
    this.writerChildLeases = 1;
    this.writerChildBytes = bytes;
    this.onChildAcquired(bytes);
    this.assertInvariants();

    let childReleased = false;
    return {
      lease: {
        bytes,
        release: () => {
          if (childReleased) return;
          childReleased = true;
          this.writerChildLeases = 0;
          this.writerChildBytes = 0;
          this.onChildReleased(bytes);
          this.assertInvariants();
          settled.resolve();
        },
      },
      released: settled.promise,
    };
  }

  close(): Promise<void> {
    if (!this.closePromise) this.closePromise = this.closeOnce();
    return this.closePromise;
  }

  private async closeOnce(): Promise<void> {
    this.closing = true;
    await this.activeChild?.promise;
    this.assertInvariants();
    assert.equal(this.writerChildLeases, 0);
    assert.equal(this.writerChildBytes, 0);
    this.released = true;
    this.baseLease.release();
    this.assertInvariants();
  }

  private assertInvariants(): void {
    assert(Number.isSafeInteger(this.writerChildBytes));
    assert(this.writerChildBytes >= 0);
    assert(this.writerChildBytes <= this.maxWriterChildBytes);
    assert(this.writerChildLeases === 0 || this.writerChildLeases === 1);
    assert.equal(this.writerChildBytes === 0, this.writerChildLeases === 0);
    if (this.released) {
      assert.equal(this.writerChildBytes, 0);
      assert.equal(this.writerChildLeases, 0);
    }
  }
}

/** Bounded producer used by the real ordered spooling Readable. */
class BenchmarkArtifactSource implements SpoolingSegmentArtifactSource {
  private readonly downloads: PrioritySemaphore;
  private readonly waves: DeterministicCompletionWaves;
  private readonly tasks = new Set<Promise<void>>();
  private activeDownloads = 0;
  private downloadMemoryLeases = 0;
  private writerChildLeases = 0;
  private writerChildBytes = 0;
  private completedOwned = 0;
  private cleanupCause: SegmentStreamCleanupCause | undefined;
  readonly completionOrder: number[] = [];
  activeDownloadsPeak = 0;
  downloadMemoryLeasesPeak = 0;
  writerChildLeasesPeak = 0;
  writerChildBytesPeak = 0;
  writerChildReleasedWhileBaseLeaseHeld = false;
  completedReadAheadPeak = 0;

  constructor(
    private readonly manager: SpoolManager,
    private readonly memory: ByteBudget,
    private readonly totalBytes: number,
    private readonly segmentBytes: number,
    private readonly writerChunkBytes: number,
    private readonly decoderChunkBytes: number,
    private readonly perDownloadBaseLeaseBytes: number,
    private readonly segmentCount: number,
    effectiveDownloadLimit: number,
    private readonly sampleMemory: () => void,
    private readonly testHooks: SegmentSpoolingBenchmarkTestHooks | undefined
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
    const downloadMemoryWindow = new BenchmarkDownloadMemoryWindow(
      downloadMemoryLease,
      this.decoderChunkBytes,
      (bytes) => this.writerChildAcquired(bytes),
      (bytes) => this.writerChildReleased(bytes)
    );
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
    let downloadOwnershipReleased = false;
    const releaseDownloadOwnership = async (): Promise<void> => {
      if (downloadOwnershipReleased) return;
      downloadOwnershipReleased = true;
      try {
        await downloadMemoryWindow.close();
      } finally {
        this.activeDownloads--;
        this.downloadMemoryLeases--;
        releaseDownload();
      }
    };
    let artifact;
    try {
      this.reachStage('artifact-create', index, signal);
      artifact = await this.manager.createArtifact({
        sessionId: 'benchmark-session',
        segmentId: segment.messageId,
        initialReservationBytes: length,
        signal,
        priority,
      });
    } catch (error) {
      await releaseDownloadOwnership();
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
      downloadMemoryWindow
    );
    const delivery = options.allowGrowing
      ? producer.finally(releaseDownloadOwnership).then(() => adapter)
      : (async () => {
          try {
            await producer;
            return adapter;
          } catch (error) {
            await adapter.release();
            throw error;
          } finally {
            await releaseDownloadOwnership();
          }
        })();
    const settled = delivery.then(
      () => undefined,
      () => undefined
    );
    this.tasks.add(settled);
    void settled.finally(() => this.tasks.delete(settled));

    if (options.allowGrowing) {
      void delivery.catch(() => undefined);
      return adapter;
    }
    return await delivery;
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
    if (
      this.activeDownloads !== 0 ||
      this.downloadMemoryLeases !== 0 ||
      this.writerChildLeases !== 0 ||
      this.writerChildBytes !== 0
    ) {
      throw new Error('benchmark download ownership did not settle to zero');
    }
  }

  get downloadMemoryLeasesFinal(): number {
    return this.downloadMemoryLeases;
  }

  get writerChildLeasesFinal(): number {
    return this.writerChildLeases;
  }

  get writerChildBytesFinal(): number {
    return this.writerChildBytes;
  }

  ownershipSnapshot(
    memoryUsedBytes: number,
    manager: ReturnType<SpoolManager['stats']>
  ): SegmentSpoolingBenchmarkOwnershipSnapshot {
    return {
      activeDownloads: this.activeDownloads,
      downloadBaseLeases: this.downloadMemoryLeases,
      writerChildLeases: this.writerChildLeases,
      writerChildBytes: this.writerChildBytes,
      memoryUsedBytes,
      artifacts: manager.artifacts,
      openFiles: manager.files.openFiles,
      spoolReservedBytes: manager.budget.reservedBytes,
      spoolActualBytes: manager.budget.actualBytes,
    };
  }

  private writerChildAcquired(bytes: number): void {
    assert(this.downloadMemoryLeases > 0);
    this.writerChildLeases++;
    this.writerChildBytes += bytes;
    this.writerChildLeasesPeak = Math.max(
      this.writerChildLeasesPeak,
      this.writerChildLeases
    );
    this.writerChildBytesPeak = Math.max(
      this.writerChildBytesPeak,
      this.writerChildBytes
    );
    this.assertWriterChildInvariants();
  }

  private writerChildReleased(bytes: number): void {
    assert(this.downloadMemoryLeases > 0);
    this.writerChildReleasedWhileBaseLeaseHeld = true;
    this.writerChildLeases--;
    this.writerChildBytes -= bytes;
    this.assertWriterChildInvariants();
  }

  private assertWriterChildInvariants(): void {
    assert(Number.isSafeInteger(this.writerChildLeases));
    assert(Number.isSafeInteger(this.writerChildBytes));
    assert(this.writerChildLeases >= 0);
    assert(this.writerChildLeases <= this.activeDownloads);
    assert(this.writerChildBytes >= 0);
    assert(
      this.writerChildBytes <= this.writerChildLeases * this.decoderChunkBytes
    );
    assert.equal(this.writerChildBytes === 0, this.writerChildLeases === 0);
  }

  private reachStage(
    stage: SegmentSpoolingBenchmarkStage,
    segmentIndex: number,
    signal: AbortSignal | undefined,
    chunkIndex?: number
  ): void {
    this.testHooks?.onStage?.(stage, { segmentIndex, chunkIndex });
    if (signal?.aborted) throw abortError(signal);
    if (segmentIndex === 0 && this.testHooks?.failAt === stage) {
      throw new Error(`Injected benchmark failure at ${stage}`);
    }
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
    downloadMemoryWindow: BenchmarkDownloadMemoryWindow
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
          downloadMemoryWindow
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
          downloadMemoryWindow
        );
      }
      this.reachStage('artifact-complete', index, signal);
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
    downloadMemoryWindow: BenchmarkDownloadMemoryWindow
  ): Promise<number> {
    const bytes = Math.min(this.writerChunkBytes, length - offset);
    const chunkIndex = Math.floor(offset / this.writerChunkBytes);
    this.reachStage(
      offset === 0 ? 'first-writer-chunk' : 'later-writer-chunk',
      index,
      signal,
      chunkIndex
    );
    const child = downloadMemoryWindow.acquireWriterChild(bytes);
    try {
      this.reachStage('writer-child-acquired', index, signal, chunkIndex);
      const chunk = fillChunk(index, offset, bytes, this.writerChunkBytes);
      artifact.write(chunk, child.lease);
      await child.released;
    } catch (error) {
      child.lease.release();
      await child.released;
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
  const writerChunkBytes = positiveInteger(
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
  const segmentCount = Math.ceil(totalBytes / segmentBytes);
  const plan = benchmarkPlan(
    totalBytes,
    segmentBytes,
    writerChunkBytes,
    prefetchSegments,
    maxConcurrentDownloads,
    memoryBudgetBytes
  );
  if (writerChunkBytes > Math.min(segmentBytes, plan.decoderChunkBytes)) {
    throw new Error(
      'chunkBytes cannot exceed the segment size or production decoder chunk'
    );
  }
  const admission = resolveBenchmarkDownloadLimit(
    plan,
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
    writerChunkBytes
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
    writerChunkBytes,
    plan.decoderChunkBytes,
    plan.perDownloadBaseLeaseBytes,
    segmentCount,
    admission.downloads,
    sampleMemory,
    options.testHooks
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
    signal: options.signal,
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
    try {
      options.testHooks?.onSettled?.(
        source.ownershipSnapshot(memory.stats().usedBytes, manager.stats())
      );
    } catch (error) {
      benchmarkError ??= error;
    }
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
    source.downloadMemoryLeasesFinal !== 0 ||
    source.writerChildLeasesPeak > admission.downloads ||
    source.writerChildLeasesFinal !== 0 ||
    source.writerChildBytesFinal !== 0 ||
    !source.writerChildReleasedWhileBaseLeaseHeld
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
      decoderChunkBytes: plan.decoderChunkBytes,
      writerChunkBytes,
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
      writerChildLeasesPeak: source.writerChildLeasesPeak,
      writerChildLeasesFinal: source.writerChildLeasesFinal,
      writerChildBytesPeak: source.writerChildBytesPeak,
      writerChildBytesFinal: source.writerChildBytesFinal,
      writerChildReleasedWhileBaseLeaseHeld:
        source.writerChildReleasedWhileBaseLeaseHeld,
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
