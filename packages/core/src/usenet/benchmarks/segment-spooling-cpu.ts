import assert from 'node:assert/strict';
import { fork, type ChildProcess } from 'node:child_process';
import { createHash } from 'node:crypto';
import { promises as fs } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { arch, cpus, platform, tmpdir } from 'node:os';
import path from 'node:path';
import {
  monitorEventLoopDelay,
  PerformanceObserver,
  performance,
} from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';
import '../../config/index.js';
import { DEFAULT_ENGINE_OPTIONS, type ProviderConfig } from '../types.js';
import { resolveSegmentSpoolingPlan } from '../resource-plan.js';
import type {
  SegmentSpoolingHotpathCounters,
  SegmentSpoolingHotpathSnapshot,
} from '../pool/hotpath-counters.js';
import { SegmentSpoolingRuntime } from '../pool/segment-spooling-runtime.js';
import type { SegmentSpoolingRuntimeStats } from '../pool/segment-spooling-runtime.js';
import { SegmentCache } from '../pool/segment-cache.js';
import { MultiProviderPool } from '../pool/multi-provider-pool.js';
import { StatsAccumulator } from '../stats/accumulator.js';
import { FileStream } from '../pool/file-stream.js';
import type { NzbSegmentRef } from '../types.js';
import type {
  BenchmarkProviderRequest,
  BenchmarkProviderResponse,
  BenchmarkProviderStreamSpec,
} from './segment-spooling-cpu-provider.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;
const GIBIBYTE_BYTES = MEBIBYTE_BYTES * KIBIBYTE_BYTES;
const DEFAULT_TOTAL_BYTES = 512 * MEBIBYTE_BYTES;
const DEFAULT_SEGMENT_BYTES = MEBIBYTE_BYTES;
const DEFAULT_RUNS = 5;
const DEFAULT_WARMUP_BYTES = 32 * MEBIBYTE_BYTES;
const DEFAULT_SLOW_PAUSE_MS = 30_000;
const DEFAULT_SLOW_BYTES_PER_SECOND = 16 * MEBIBYTE_BYTES;
const MAX_TIMED_BOUNDARY_SAMPLES = 64;
const PROVIDER_CONTROL_TIMEOUT_MS = 30_000;
const MEMORY_SAMPLE_INTERVAL_MS = 100;

export type SegmentSpoolingCpuScenario = 'S1' | 'S2' | 'S3' | 'S4' | 'S5';

export interface SlowClientBenchmarkContext {
  readonly stats: () => SegmentSpoolingRuntimeStats;
  readonly hotpath: () => SegmentSpoolingHotpathSnapshot;
  /** One deterministic event-loop handoff; this is not a timer or poll. */
  readonly waitForPipelineTurn: () => Promise<void>;
}

export interface SegmentSpoolingCpuBenchmarkOptions {
  readonly totalBytes?: number;
  readonly segmentBytes?: number;
  readonly runs?: number;
  readonly warmupBytes?: number;
  readonly scenarios?: readonly SegmentSpoolingCpuScenario[];
  readonly slowPauseMs?: number;
  readonly slowBytesPerSecond?: number;
  readonly runCorrectness?: boolean;
  readonly testHooks?: {
    readonly corruptCorrectness?: boolean;
    readonly onSlowClientPaused?: (
      context: SlowClientBenchmarkContext
    ) => Promise<void>;
  };
}

export interface SegmentSpoolingCpuRunResult {
  readonly scenario: SegmentSpoolingCpuScenario;
  readonly decodedBytes: number;
  readonly deliveredBytes: number;
  readonly checksum: string;
  readonly timedHashUpdates: 0;
  readonly timedValidationSamples: number;
  readonly cpuUserMs: number;
  readonly cpuSystemMs: number;
  readonly cpuMsPerDecodedGiB: number;
  readonly wallTimeMs: number;
  readonly firstByteMs: number;
  readonly throughputBytesPerSecond: number;
  readonly eventLoopDelayMs: {
    readonly mean: number;
    readonly p95: number;
    readonly max: number;
  };
  readonly gc: { readonly count: number; readonly durationMs: number };
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
  readonly internalMemory: {
    readonly peak: number;
    readonly final: number;
    readonly waitingPeak: number;
  };
  readonly spool: {
    readonly peakReserved: number;
    readonly peakActual: number;
    readonly waitingPeak: number;
    readonly finalReserved: number;
    readonly finalActual: number;
    readonly finalArtifacts: number;
    readonly finalOpenFiles: number;
  };
  readonly slowClient: {
    readonly pauseCount: number;
    readonly resumeCount: number;
    readonly rateLimitWaits: number;
    readonly configuredPauseMs: number;
    readonly configuredBytesPerSecond: number;
  };
  readonly hotpath: SegmentSpoolingHotpathSnapshot;
}

export interface SegmentSpoolingCpuScenarioSummary {
  readonly scenario: SegmentSpoolingCpuScenario;
  readonly runs: readonly SegmentSpoolingCpuRunResult[];
  readonly median: Pick<
    SegmentSpoolingCpuRunResult,
    | 'cpuMsPerDecodedGiB'
    | 'wallTimeMs'
    | 'firstByteMs'
    | 'throughputBytesPerSecond'
  >;
  readonly p95: Pick<
    SegmentSpoolingCpuRunResult,
    | 'cpuMsPerDecodedGiB'
    | 'wallTimeMs'
    | 'firstByteMs'
    | 'throughputBytesPerSecond'
  >;
}

export interface SegmentSpoolingCpuBenchmarkReport {
  readonly configuration: {
    readonly totalBytes: number;
    readonly segmentBytes: number;
    readonly runs: number;
    readonly warmupBytes: number;
    readonly scenarios: readonly SegmentSpoolingCpuScenario[];
    readonly slowPauseMs: number;
    readonly slowBytesPerSecond: number;
  };
  readonly identity: {
    readonly harnessSha256: string;
    readonly providerChildSha256: string;
    readonly fixtureSha256: string;
    readonly configurationSha256: string;
    readonly nodeVersion: string;
    readonly platform: string;
    readonly architecture: string;
    readonly cpuModel: string;
    readonly cpuCount: number;
  };
  readonly measurement: {
    readonly cpuScope: 'parent-process-only';
    readonly providerProcess: 'separate-node-child';
    readonly fullIntegrityInsideCpuWindow: false;
    readonly correctnessRun: boolean;
    readonly providerChildCpuMs: number;
  };
  readonly scenarios: readonly SegmentSpoolingCpuScenarioSummary[];
}

interface BenchmarkStreamSpec extends BenchmarkProviderStreamSpec {}

interface ProviderPendingOperation {
  readonly expected: BenchmarkProviderResponse['type'];
  readonly resolve: (response: BenchmarkProviderResponse) => void;
  readonly reject: (error: Error) => void;
  readonly timer: NodeJS.Timeout;
}

interface ScenarioExecutionOptions {
  readonly mode: 'correctness' | 'timed';
  readonly correctnessLabel: string;
  readonly slowPauseMs: number;
  readonly slowBytesPerSecond: number;
  readonly invocation: number;
  readonly corruptFirstSegment: boolean;
  readonly onSlowClientPaused?: (
    context: SlowClientBenchmarkContext
  ) => Promise<void>;
}

interface CorrectnessExecution {
  readonly kind: 'correctness';
  readonly checksums: readonly string[];
}

interface TimedExecution {
  readonly kind: 'timed';
  readonly result: SegmentSpoolingCpuRunResult;
}

type ScenarioExecution = CorrectnessExecution | TimedExecution;

interface HotpathCounterContract extends Omit<
  SegmentSpoolingHotpathSnapshot,
  'perStreamDownloadAdmits' | 'downloadOwnerOverflowAdmits'
> {
  downloadStarted(ownerKey: string): void;
  downloadEnded(): void;
  semaphoreQueued(ownerCount: number, waiterCount: number): void;
  semaphoreGrant(): void;
  semaphoreAbort(): void;
  semaphoreCapacityReject(): void;
  semaphoreOwnerTurn(): void;
  snapshot(): SegmentSpoolingHotpathSnapshot;
}

/**
 * The original 09.13 performance baseline predates optional production
 * counters. This zero-work fallback keeps the measurement harness byte-identical
 * across both worktrees without changing the baseline hot path.
 */
class UninstrumentedHotpathCounters implements HotpathCounterContract {
  rawReadCallbacks = 0;
  rawReadBytes = 0;
  yencDecodeCalls = 0;
  yencDecodedBytes = 0;
  yencOutputBackingAllocations = 0;
  yencOutputBackingReuses = 0;
  decodedBatchesCommitted = 0;
  sinkDrainCycles = 0;
  socketPauseCalls = 0;
  socketResumeCalls = 0;
  spoolWriteOperations = 0;
  spoolWriteSyscalls = 0;
  spoolShortWrites = 0;
  spoolBytesWritten = 0;
  headerLinesParsed = 0;
  headerTransitionCopies = 0;
  resourceEventsObserved = 0;
  resourceLogRecordsEmitted = 0;
  resourceLogRecordsSuppressed = 0;
  promotionsStarted = 0;
  promotionsSkippedForForegroundPressure = 0;
  promotionBytesCopied = 0;
  activeDownloads = 0;
  activeDownloadsPeak = 0;
  semaphoreOwnerCountPeak = 0;
  semaphoreWaiterCountPeak = 0;
  semaphoreGrants = 0;
  semaphoreAborts = 0;
  semaphoreCapacityRejects = 0;
  semaphoreOwnerTurns = 0;
  readonly semaphoreGlobalScans = 0;
  readonly perStreamDownloadAdmits = [];
  readonly downloadOwnerOverflowAdmits = 0;

  downloadStarted(_ownerKey: string): void {}
  downloadEnded(): void {}
  semaphoreQueued(_ownerCount: number, _waiterCount: number): void {}
  semaphoreGrant(): void {}
  semaphoreAbort(): void {}
  semaphoreCapacityReject(): void {}
  semaphoreOwnerTurn(): void {}

  snapshot(): SegmentSpoolingHotpathSnapshot {
    return {
      rawReadCallbacks: this.rawReadCallbacks,
      rawReadBytes: this.rawReadBytes,
      yencDecodeCalls: this.yencDecodeCalls,
      yencDecodedBytes: this.yencDecodedBytes,
      yencOutputBackingAllocations: this.yencOutputBackingAllocations,
      yencOutputBackingReuses: this.yencOutputBackingReuses,
      decodedBatchesCommitted: this.decodedBatchesCommitted,
      sinkDrainCycles: this.sinkDrainCycles,
      socketPauseCalls: this.socketPauseCalls,
      socketResumeCalls: this.socketResumeCalls,
      spoolWriteOperations: this.spoolWriteOperations,
      spoolWriteSyscalls: this.spoolWriteSyscalls,
      spoolShortWrites: this.spoolShortWrites,
      spoolBytesWritten: this.spoolBytesWritten,
      headerLinesParsed: this.headerLinesParsed,
      headerTransitionCopies: this.headerTransitionCopies,
      resourceEventsObserved: this.resourceEventsObserved,
      resourceLogRecordsEmitted: this.resourceLogRecordsEmitted,
      resourceLogRecordsSuppressed: this.resourceLogRecordsSuppressed,
      promotionsStarted: this.promotionsStarted,
      promotionsSkippedForForegroundPressure:
        this.promotionsSkippedForForegroundPressure,
      promotionBytesCopied: this.promotionBytesCopied,
      activeDownloads: this.activeDownloads,
      activeDownloadsPeak: this.activeDownloadsPeak,
      semaphoreOwnerCountPeak: this.semaphoreOwnerCountPeak,
      semaphoreWaiterCountPeak: this.semaphoreWaiterCountPeak,
      semaphoreGrants: this.semaphoreGrants,
      semaphoreAborts: this.semaphoreAborts,
      semaphoreCapacityRejects: this.semaphoreCapacityRejects,
      semaphoreOwnerTurns: this.semaphoreOwnerTurns,
      semaphoreGlobalScans: this.semaphoreGlobalScans,
      perStreamDownloadAdmits: this.perStreamDownloadAdmits,
      downloadOwnerOverflowAdmits: this.downloadOwnerOverflowAdmits,
    };
  }
}

async function createHotpathCounters(): Promise<
  SegmentSpoolingHotpathCounters | undefined
> {
  try {
    const module = await import('../pool/hotpath-counters.js');
    return new module.SegmentSpoolingHotpathCounters();
  } catch (error) {
    if (
      error instanceof Error &&
      'code' in error &&
      error.code === 'ERR_MODULE_NOT_FOUND'
    ) {
      return undefined;
    }
    throw error;
  }
}

function isProviderResponse(
  value: unknown
): value is BenchmarkProviderResponse {
  if (typeof value !== 'object' || value === null) return false;
  const type = Reflect.get(value, 'type');
  return (
    type === 'ready' ||
    type === 'registered' ||
    type === 'burned' ||
    type === 'closed' ||
    type === 'error'
  );
}

function providerExecArgv(): string[] {
  const result: string[] = [];
  for (let index = 0; index < process.execArgv.length; index++) {
    const argument = process.execArgv[index];
    if (argument === '--cpu-prof-dir' || argument === '--cpu-prof-name') {
      index++;
      continue;
    }
    if (
      argument === '--cpu-prof' ||
      argument.startsWith('--cpu-prof=') ||
      argument.startsWith('--cpu-prof-dir=') ||
      argument.startsWith('--cpu-prof-name=') ||
      argument === '--test' ||
      argument.startsWith('--test-')
    ) {
      continue;
    }
    result.push(argument);
  }
  return result;
}

/** One bounded IPC operation at a time; BODY bytes never cross IPC. */
class BenchmarkProviderChild {
  private readonly child: ChildProcess;
  private readonly readyPromise: Promise<number>;
  private readyResolve: ((port: number) => void) | undefined;
  private readyReject: ((error: Error) => void) | undefined;
  private pending: ProviderPendingOperation | undefined;
  private closePromise:
    | Promise<{
        readonly cpuUserMicros: number;
        readonly cpuSystemMicros: number;
      }>
    | undefined;
  private exited = false;

  constructor() {
    const extension = path.extname(fileURLToPath(import.meta.url));
    const childUrl = new URL(
      `./segment-spooling-cpu-provider${extension}`,
      import.meta.url
    );
    this.child = fork(fileURLToPath(childUrl), [], {
      execArgv: providerExecArgv(),
      stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
    });
    this.readyPromise = new Promise<number>((resolve, reject) => {
      this.readyResolve = resolve;
      this.readyReject = reject;
    });
    this.child.on('message', (message: unknown) => this.onMessage(message));
    this.child.once('error', (error) => this.fail(error));
    this.child.once('exit', (code, signal) => {
      this.exited = true;
      if (!this.closePromise || this.pending) {
        this.fail(
          new Error(
            `benchmark provider exited before settlement (${String(code)}/${String(signal)})`
          )
        );
      }
    });
  }

  ready(): Promise<number> {
    return this.readyPromise;
  }

  async register(spec: BenchmarkStreamSpec): Promise<void> {
    const response = await this.request(
      { type: 'register', spec },
      'registered'
    );
    assert.equal(response.type, 'registered');
  }

  async burn(iterations: number): Promise<{
    readonly cpuUserMicros: number;
    readonly cpuSystemMicros: number;
    readonly checksum: number;
  }> {
    const response = await this.request({ type: 'burn', iterations }, 'burned');
    assert.equal(response.type, 'burned');
    return response;
  }

  close(): Promise<{
    readonly cpuUserMicros: number;
    readonly cpuSystemMicros: number;
  }> {
    if (this.closePromise) return this.closePromise;
    this.closePromise = (async () => {
      if (this.exited) return { cpuUserMicros: 0, cpuSystemMicros: 0 };
      const response = await this.request({ type: 'close' }, 'closed');
      assert.equal(response.type, 'closed');
      return response;
    })();
    void this.closePromise.catch(() => undefined);
    return this.closePromise;
  }

  private request(
    message: BenchmarkProviderRequest,
    expected: BenchmarkProviderResponse['type']
  ): Promise<BenchmarkProviderResponse> {
    if (this.exited) {
      return Promise.reject(new Error('benchmark provider is closed'));
    }
    if (this.pending) {
      return Promise.reject(
        new Error('benchmark provider control operation already pending')
      );
    }
    return new Promise<BenchmarkProviderResponse>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending?.timer !== timer) return;
        this.pending = undefined;
        reject(new Error('benchmark provider control operation timed out'));
        this.child.kill();
      }, PROVIDER_CONTROL_TIMEOUT_MS);
      timer.unref();
      this.pending = { expected, resolve, reject, timer };
      this.child.send(message, (error) => {
        if (!error || this.pending?.timer !== timer) return;
        clearTimeout(timer);
        this.pending = undefined;
        reject(error);
      });
    });
  }

  private onMessage(message: unknown): void {
    if (!isProviderResponse(message)) {
      this.fail(new Error('benchmark provider sent an invalid response'));
      return;
    }
    if (message.type === 'ready') {
      const resolve = this.readyResolve;
      this.readyResolve = undefined;
      this.readyReject = undefined;
      resolve?.(message.port);
      return;
    }
    const pending = this.pending;
    if (!pending) {
      this.fail(new Error('benchmark provider sent an unexpected response'));
      return;
    }
    clearTimeout(pending.timer);
    this.pending = undefined;
    if (message.type === 'error') {
      pending.reject(new Error(message.message));
      return;
    }
    if (message.type !== pending.expected) {
      pending.reject(new Error('benchmark provider response type mismatch'));
      return;
    }
    pending.resolve(message);
  }

  private fail(error: Error): void {
    const rejectReady = this.readyReject;
    this.readyResolve = undefined;
    this.readyReject = undefined;
    rejectReady?.(error);
    const pending = this.pending;
    if (pending) {
      clearTimeout(pending.timer);
      this.pending = undefined;
      pending.reject(error);
    }
  }
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a safe positive integer`);
  }
  return value;
}

function nonNegativeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a safe non-negative integer`);
  }
  return value;
}

function percentile(values: readonly number[], fraction: number): number {
  assert(values.length > 0);
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[
    Math.min(sorted.length - 1, Math.ceil(sorted.length * fraction) - 1)
  ];
}

function sha256(buffers: readonly Buffer[]): string {
  const hash = createHash('sha256');
  for (const buffer of buffers) hash.update(buffer);
  return hash.digest('hex');
}

function expectedFillSha256(bytes: number, fill: number): string {
  const hash = createHash('sha256');
  const block = Buffer.alloc(Math.min(MEBIBYTE_BYTES, bytes), fill);
  let remaining = bytes;
  while (remaining > 0) {
    const length = Math.min(block.length, remaining);
    hash.update(block.subarray(0, length));
    remaining -= length;
  }
  return hash.digest('hex');
}

function makeBoundaryPositions(
  totalBytes: number,
  segmentBytes: number
): readonly number[] {
  const positions = new Set<number>([0, totalBytes - 1]);
  const segments = Math.ceil(totalBytes / segmentBytes);
  const available = MAX_TIMED_BOUNDARY_SAMPLES - positions.size;
  const stride = Math.max(1, Math.ceil(segments / Math.max(1, available / 2)));
  for (let segment = 1; segment < segments; segment += stride) {
    if (positions.size >= MAX_TIMED_BOUNDARY_SAMPLES) break;
    const boundary = segment * segmentBytes;
    positions.add(boundary - 1);
    if (positions.size < MAX_TIMED_BOUNDARY_SAMPLES) positions.add(boundary);
  }
  return [...positions].sort((left, right) => left - right);
}

class TimedBoundaryValidator {
  private readonly positions: readonly number[];
  private cursor = 0;
  private offset = 0;

  constructor(
    totalBytes: number,
    segmentBytes: number,
    private readonly fill: number
  ) {
    this.positions = makeBoundaryPositions(totalBytes, segmentBytes);
  }

  observe(chunk: Buffer): void {
    const end = this.offset + chunk.length;
    while (
      this.cursor < this.positions.length &&
      this.positions[this.cursor] < end
    ) {
      const position = this.positions[this.cursor];
      if (position >= this.offset) {
        assert.equal(chunk[position - this.offset], this.fill);
        this.cursor++;
      }
    }
    this.offset = end;
  }

  finish(totalBytes: number): number {
    assert.equal(this.offset, totalBytes);
    assert.equal(this.cursor, this.positions.length);
    return this.cursor;
  }
}

/** Structural proof that reader chunk size cannot change validation work. */
export function benchmarkTimedValidationPlan(
  totalBytes: number,
  segmentBytes: number,
  readerChunkBytes: number
): { readonly samples: number; readonly hashUpdates: 0 } {
  positiveInteger(totalBytes, 'totalBytes');
  positiveInteger(segmentBytes, 'segmentBytes');
  positiveInteger(readerChunkBytes, 'readerChunkBytes');
  return {
    samples: makeBoundaryPositions(totalBytes, segmentBytes).length,
    hashUpdates: 0,
  };
}

function makeSegments(spec: BenchmarkStreamSpec): NzbSegmentRef[] {
  const count = Math.ceil(spec.totalBytes / spec.segmentBytes);
  return Array.from({ length: count }, (_, index) => ({
    messageId: `${spec.key}-${index}`,
    bytes:
      Math.min(spec.segmentBytes, spec.totalBytes - index * spec.segmentBytes) +
      16 * KIBIBYTE_BYTES,
  }));
}

function makeFile(
  pool: MultiProviderPool,
  spec: BenchmarkStreamSpec,
  options: typeof DEFAULT_ENGINE_OPTIONS,
  plan: ReturnType<typeof resolveSegmentSpoolingPlan>
): FileStream {
  return new FileStream(
    pool,
    {
      segments: makeSegments(spec),
      knownSize: spec.totalBytes,
      filename: `${spec.key}.bin`,
    },
    spec.key,
    options,
    undefined,
    undefined,
    {
      mode: 'segment_spooling',
      arenaBytes: 16 * MEBIBYTE_BYTES,
      segmentSpooling: plan,
    }
  );
}

function provider(port: number): ProviderConfig {
  return {
    id: 'cpu-benchmark-provider',
    name: 'cpu-benchmark-provider',
    host: '127.0.0.1',
    port,
    tls: true,
    tlsSkipVerify: true,
    maxConnections: 8,
    pipelineDepth: 1,
    priority: 0,
  };
}

async function eventLoopTurn(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

async function delay(milliseconds: number): Promise<void> {
  if (milliseconds === 0) {
    await eventLoopTurn();
    return;
  }
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, milliseconds);
    timer.unref();
  });
}

async function consumeCorrectness(
  file: FileStream,
  fill: number
): Promise<string> {
  const hash = createHash('sha256');
  let bytes = 0;
  for await (const chunk of file.createReadStream()) {
    assert(Buffer.isBuffer(chunk));
    hash.update(chunk);
    bytes += chunk.length;
  }
  assert.equal(bytes, file.size());
  const digest = hash.digest('hex');
  assert.equal(digest, expectedFillSha256(file.size(), fill));
  return digest;
}

async function runScenario(
  child: BenchmarkProviderChild,
  port: number,
  scenario: SegmentSpoolingCpuScenario,
  totalBytes: number,
  segmentBytes: number,
  execution: ScenarioExecutionOptions
): Promise<ScenarioExecution> {
  const root = await fs.mkdtemp(path.join(tmpdir(), 'spooling-cpu-'));
  const productionHotpath = await createHotpathCounters();
  const hotpath: HotpathCounterContract =
    productionHotpath ?? new UninstrumentedHotpathCounters();
  let resourceEvents = 0;
  const plan = resolveSegmentSpoolingPlan({
    streamingMode: 'segment_spooling',
    maxConcurrentDownloads: 8,
    segmentMemoryCacheBytes: 0,
    segmentSpoolingMemoryBudgetBytes: 96 * MEBIBYTE_BYTES,
    segmentSpoolingStreamBufferBytes: 8 * MEBIBYTE_BYTES,
    segmentSpoolingSpoolBytes: 256 * MEBIBYTE_BYTES,
    segmentSpoolingMinFreeDiskBytes: 0,
  });
  const promotionEnabled = scenario === 'S2';
  const cache = new SegmentCache({
    arenaBytes: 16 * MEBIBYTE_BYTES,
    diskBytes: promotionEnabled ? 2 * totalBytes : 0,
    diskPath: root,
    namespace: 'cpu-benchmark-cache',
  });
  const runtime = new SegmentSpoolingRuntime({
    plan,
    engineId: 'cpu-benchmark',
    cacheRoot: root,
    artifactCache: cache,
    hotpathCounters: productionHotpath,
    onEvent: () => {
      resourceEvents++;
    },
  });
  const engineOptions = {
    ...DEFAULT_ENGINE_OPTIONS,
    streamingMode: 'segment_spooling' as const,
    maxConcurrentDownloads: 8,
    prefetchSegments: 8,
    segmentDiskCacheBytes: promotionEnabled ? 2 * totalBytes : 0,
    segmentSpoolingMemoryBudgetBytes: plan.memoryBudgetBytes,
    segmentSpoolingStreamBufferBytes: plan.perStreamBufferBytes,
    segmentSpoolingSpoolBytes: plan.spoolBytes,
    segmentSpoolingMinFreeDiskBytes: plan.minFreeDiskBytes,
    segmentTimeoutMs: 60_000,
    segmentStallTimeoutMs: 30_000,
  };
  const pool = new MultiProviderPool(
    [provider(port)],
    engineOptions,
    cache,
    new StatsAccumulator(),
    { spooling: runtime }
  );

  const perStreamBytes =
    scenario === 'S3' || scenario === 'S4'
      ? Math.max(segmentBytes, Math.floor(totalBytes / 2))
      : totalBytes;
  const prefix = `cpu-${scenario.toLowerCase()}-${execution.invocation}`;
  const first: BenchmarkStreamSpec = {
    key: `${prefix}-a`,
    totalBytes: perStreamBytes,
    segmentBytes,
    fill: 0x31,
    corruptFirstSegment: execution.corruptFirstSegment,
  };
  const second: BenchmarkStreamSpec = {
    key: `${prefix}-b`,
    totalBytes: perStreamBytes,
    segmentBytes,
    fill: 0x52,
  };
  await child.register(first);
  if (scenario === 'S3') await child.register(second);

  const files = [makeFile(pool, first, engineOptions, plan)];
  const fills = [first.fill];
  if (scenario === 'S3') {
    files.push(makeFile(pool, second, engineOptions, plan));
    fills.push(second.fill);
  }
  if (scenario === 'S4') {
    files.push(makeFile(pool, first, engineOptions, plan));
    fills.push(first.fill);
  }
  await Promise.all(files.map((file) => file.open()));

  if (execution.mode === 'correctness') {
    try {
      const checksums = await Promise.all(
        files.map((file, index) => consumeCorrectness(file, fills[index]))
      );
      await cache.close();
      await pool.close();
      return { kind: 'correctness', checksums };
    } finally {
      await Promise.allSettled([pool.close(), cache.close()]);
      await fs.rm(root, { recursive: true, force: true });
    }
  }

  let deliveredBytes = 0;
  let firstByteMs = Number.POSITIVE_INFINITY;
  let memoryPeak = 0;
  let memoryWaitingPeak = 0;
  let spoolReservedPeak = 0;
  let spoolActualPeak = 0;
  let spoolWaitingPeak = 0;
  let validationSamples = 0;
  const memoryBefore = process.memoryUsage();
  let arrayBuffersPeak = memoryBefore.arrayBuffers;
  let externalPeak = memoryBefore.external;
  let gcCount = 0;
  let gcDurationMs = 0;
  const slowClient = {
    pauseCount: 0,
    resumeCount: 0,
    rateLimitWaits: 0,
  };
  const gcObserver = new PerformanceObserver((entries) => {
    for (const entry of entries.getEntries()) {
      gcCount++;
      gcDurationMs += entry.duration;
    }
  });
  gcObserver.observe({ entryTypes: ['gc'] });
  const eventLoop = monitorEventLoopDelay({ resolution: 10 });
  eventLoop.enable();
  const startedAt = performance.now();

  const sample = (): void => {
    const stats = runtime.stats();
    memoryPeak = Math.max(memoryPeak, stats.memory.peakBytes);
    memoryWaitingPeak = Math.max(memoryWaitingPeak, stats.memory.waiting);
    spoolReservedPeak = Math.max(
      spoolReservedPeak,
      stats.spool.budget.peakReservedBytes
    );
    spoolActualPeak = Math.max(
      spoolActualPeak,
      stats.spool.budget.peakActualBytes
    );
    spoolWaitingPeak = Math.max(
      spoolWaitingPeak,
      stats.spool.budget.waiting,
      stats.spool.files.waiting
    );
    const memory = process.memoryUsage();
    arrayBuffersPeak = Math.max(arrayBuffersPeak, memory.arrayBuffers);
    externalPeak = Math.max(externalPeak, memory.external);
  };
  sample();
  const sampler = setInterval(sample, MEMORY_SAMPLE_INTERVAL_MS);
  sampler.unref();
  const cpuStarted = process.cpuUsage();

  const consume = async (
    file: FileStream,
    fill: number,
    slow: boolean
  ): Promise<void> => {
    const validator = new TimedBoundaryValidator(
      file.size(),
      segmentBytes,
      fill
    );
    let localBytes = 0;
    let resumedAt = 0;
    let rateLimitedBytes = 0;
    for await (const chunk of file.createReadStream()) {
      assert(Buffer.isBuffer(chunk));
      if (!Number.isFinite(firstByteMs)) {
        firstByteMs = performance.now() - startedAt;
      }
      validator.observe(chunk);
      localBytes += chunk.length;
      deliveredBytes += chunk.length;
      if (slow && slowClient.pauseCount === 0) {
        slowClient.pauseCount++;
        if (execution.onSlowClientPaused) {
          await execution.onSlowClientPaused({
            stats: () => runtime.stats(),
            hotpath: () => hotpath.snapshot(),
            waitForPipelineTurn: eventLoopTurn,
          });
        } else {
          await delay(execution.slowPauseMs);
        }
        slowClient.resumeCount++;
        resumedAt = performance.now();
      }
      if (slow && slowClient.resumeCount > 0) {
        rateLimitedBytes += chunk.length;
        const expectedElapsedMs =
          (rateLimitedBytes * 1000) / execution.slowBytesPerSecond;
        const waitMs = expectedElapsedMs - (performance.now() - resumedAt);
        if (waitMs > 0) {
          slowClient.rateLimitWaits++;
          await delay(Math.ceil(waitMs));
        }
      }
    }
    assert.equal(localBytes, file.size());
    validationSamples += validator.finish(file.size());
  };

  try {
    await Promise.all(
      files.map((file, index) =>
        consume(file, fills[index], scenario === 'S5' && index === 0)
      )
    );
    await cache.close();
    await pool.close();
  } finally {
    clearInterval(sampler);
    eventLoop.disable();
    gcObserver.disconnect();
    await Promise.allSettled([pool.close(), cache.close()]);
    await fs.rm(root, { recursive: true, force: true });
  }
  const cpu = process.cpuUsage(cpuStarted);
  const wallTimeMs = performance.now() - startedAt;
  sample();
  const final = runtime.stats();
  const memoryAfter = process.memoryUsage();
  const hotpathSnapshot = hotpath.snapshot();
  const decodedBytes =
    hotpathSnapshot.yencDecodedBytes > 0
      ? hotpathSnapshot.yencDecodedBytes
      : scenario === 'S4'
        ? perStreamBytes
        : deliveredBytes;
  assert(decodedBytes > 0);
  assert(Number.isFinite(firstByteMs));
  assert.equal(hotpathSnapshot.activeDownloads, 0);
  assert.equal(final.memory.usedBytes, 0);
  assert.equal(final.spool.budget.reservedBytes, 0);
  assert.equal(final.spool.budget.actualBytes, 0);
  assert.equal(final.spool.artifacts, 0);
  assert.equal(final.spool.files.openFiles, 0);
  if (productionHotpath) {
    assert.equal(resourceEvents, hotpathSnapshot.resourceEventsObserved);
  }
  const cpuUserMs = cpu.user / 1000;
  const cpuSystemMs = cpu.system / 1000;
  return {
    kind: 'timed',
    result: {
      scenario,
      decodedBytes,
      deliveredBytes,
      checksum: execution.correctnessLabel,
      timedHashUpdates: 0,
      timedValidationSamples: validationSamples,
      cpuUserMs,
      cpuSystemMs,
      cpuMsPerDecodedGiB:
        ((cpuUserMs + cpuSystemMs) * GIBIBYTE_BYTES) / decodedBytes,
      wallTimeMs,
      firstByteMs,
      throughputBytesPerSecond: (deliveredBytes * 1000) / wallTimeMs,
      eventLoopDelayMs: {
        mean: Number.isFinite(eventLoop.mean) ? eventLoop.mean / 1e6 : 0,
        p95: eventLoop.percentile(95) / 1e6,
        max: eventLoop.max / 1e6,
      },
      gc: { count: gcCount, durationMs: gcDurationMs },
      arrayBuffers: {
        before: memoryBefore.arrayBuffers,
        peak: arrayBuffersPeak,
        after: memoryAfter.arrayBuffers,
      },
      external: {
        before: memoryBefore.external,
        peak: externalPeak,
        after: memoryAfter.external,
      },
      internalMemory: {
        peak: memoryPeak,
        final: final.memory.usedBytes,
        waitingPeak: memoryWaitingPeak,
      },
      spool: {
        peakReserved: spoolReservedPeak,
        peakActual: spoolActualPeak,
        waitingPeak: spoolWaitingPeak,
        finalReserved: final.spool.budget.reservedBytes,
        finalActual: final.spool.budget.actualBytes,
        finalArtifacts: final.spool.artifacts,
        finalOpenFiles: final.spool.files.openFiles,
      },
      slowClient: {
        ...slowClient,
        configuredPauseMs: execution.slowPauseMs,
        configuredBytesPerSecond: execution.slowBytesPerSecond,
      },
      hotpath: hotpathSnapshot,
    },
  };
}

function aggregate(
  scenario: SegmentSpoolingCpuScenario,
  runs: readonly SegmentSpoolingCpuRunResult[]
): SegmentSpoolingCpuScenarioSummary {
  const value = (
    key:
      | 'cpuMsPerDecodedGiB'
      | 'wallTimeMs'
      | 'firstByteMs'
      | 'throughputBytesPerSecond',
    fraction: number
  ): number =>
    percentile(
      runs.map((run) => run[key]),
      fraction
    );
  return {
    scenario,
    runs,
    median: {
      cpuMsPerDecodedGiB: value('cpuMsPerDecodedGiB', 0.5),
      wallTimeMs: value('wallTimeMs', 0.5),
      firstByteMs: value('firstByteMs', 0.5),
      throughputBytesPerSecond: value('throughputBytesPerSecond', 0.5),
    },
    p95: {
      cpuMsPerDecodedGiB: value('cpuMsPerDecodedGiB', 0.95),
      wallTimeMs: value('wallTimeMs', 0.95),
      firstByteMs: value('firstByteMs', 0.95),
      throughputBytesPerSecond: value('throughputBytesPerSecond', 0.95),
    },
  };
}

async function benchmarkIdentity(
  configuration: object
): Promise<SegmentSpoolingCpuBenchmarkReport['identity']> {
  const extension = path.extname(fileURLToPath(import.meta.url));
  const providerUrl = new URL(
    `./segment-spooling-cpu-provider${extension}`,
    import.meta.url
  );
  const [harness, providerChild, key, cert] = await Promise.all([
    readFile(new URL(import.meta.url)),
    readFile(providerUrl),
    readFile(
      new URL('../../../test/fixtures/nntp-test-key.pem', import.meta.url)
    ),
    readFile(
      new URL('../../../test/fixtures/nntp-test-cert.pem', import.meta.url)
    ),
  ]);
  const hostCpus = cpus();
  return {
    harnessSha256: sha256([harness]),
    providerChildSha256: sha256([providerChild]),
    fixtureSha256: sha256([key, cert]),
    configurationSha256: sha256([
      Buffer.from(JSON.stringify(configuration), 'utf8'),
    ]),
    nodeVersion: process.version,
    platform: platform(),
    architecture: arch(),
    cpuModel: hostCpus[0]?.model ?? 'unknown',
    cpuCount: hostCpus.length,
  };
}

export async function measureProviderChildCpuIsolation(
  iterations: number
): Promise<{
  readonly parentCpuMs: number;
  readonly childCpuMs: number;
  readonly checksum: number;
}> {
  positiveInteger(iterations, 'iterations');
  const child = new BenchmarkProviderChild();
  try {
    await child.ready();
    const started = process.cpuUsage();
    const burned = await child.burn(iterations);
    const parent = process.cpuUsage(started);
    return {
      parentCpuMs: (parent.user + parent.system) / 1000,
      childCpuMs: (burned.cpuUserMicros + burned.cpuSystemMicros) / 1000,
      checksum: burned.checksum,
    };
  } finally {
    await child.close();
  }
}

export async function runSegmentSpoolingCpuBenchmark(
  options: SegmentSpoolingCpuBenchmarkOptions = {}
): Promise<SegmentSpoolingCpuBenchmarkReport> {
  const totalBytes = positiveInteger(
    options.totalBytes ?? DEFAULT_TOTAL_BYTES,
    'totalBytes'
  );
  const segmentBytes = positiveInteger(
    options.segmentBytes ?? DEFAULT_SEGMENT_BYTES,
    'segmentBytes'
  );
  const runs = positiveInteger(options.runs ?? DEFAULT_RUNS, 'runs');
  const warmupBytes = positiveInteger(
    options.warmupBytes ?? DEFAULT_WARMUP_BYTES,
    'warmupBytes'
  );
  const slowPauseMs = nonNegativeInteger(
    options.slowPauseMs ?? DEFAULT_SLOW_PAUSE_MS,
    'slowPauseMs'
  );
  const slowBytesPerSecond = positiveInteger(
    options.slowBytesPerSecond ?? DEFAULT_SLOW_BYTES_PER_SECOND,
    'slowBytesPerSecond'
  );
  const scenarios = options.scenarios ?? ['S1', 'S2', 'S3', 'S4', 'S5'];
  for (const scenario of scenarios) {
    if (!isCpuScenario(scenario)) {
      throw new Error('Unknown CPU benchmark scenario');
    }
  }
  const configuration = {
    totalBytes,
    segmentBytes,
    runs,
    warmupBytes,
    scenarios,
    slowPauseMs,
    slowBytesPerSecond,
  };
  const identity = await benchmarkIdentity(configuration);
  const child = new BenchmarkProviderChild();
  let invocation = 0;
  try {
    const port = await child.ready();
    const correctnessLabels = new Map<SegmentSpoolingCpuScenario, string>();
    const runCorrectness = options.runCorrectness ?? true;
    if (runCorrectness) {
      for (const scenario of scenarios) {
        const execution = await runScenario(
          child,
          port,
          scenario,
          totalBytes,
          segmentBytes,
          {
            mode: 'correctness',
            correctnessLabel: '',
            slowPauseMs: 0,
            slowBytesPerSecond: Number.MAX_SAFE_INTEGER,
            invocation: invocation++,
            corruptFirstSegment: options.testHooks?.corruptCorrectness === true,
          }
        );
        assert.equal(execution.kind, 'correctness');
        correctnessLabels.set(scenario, execution.checksums.join(':'));
      }
    }

    const warmup = await runScenario(
      child,
      port,
      'S1',
      warmupBytes,
      Math.min(segmentBytes, warmupBytes),
      {
        mode: 'timed',
        correctnessLabel: 'warmup',
        slowPauseMs: 0,
        slowBytesPerSecond: Number.MAX_SAFE_INTEGER,
        invocation: invocation++,
        corruptFirstSegment: false,
      }
    );
    assert.equal(warmup.kind, 'timed');

    const summaries: SegmentSpoolingCpuScenarioSummary[] = [];
    for (const scenario of scenarios) {
      const measured: SegmentSpoolingCpuRunResult[] = [];
      for (let index = 0; index < runs; index++) {
        const execution = await runScenario(
          child,
          port,
          scenario,
          totalBytes,
          segmentBytes,
          {
            mode: 'timed',
            correctnessLabel:
              correctnessLabels.get(scenario) ?? 'boundary-samples-only',
            slowPauseMs,
            slowBytesPerSecond,
            invocation: invocation++,
            corruptFirstSegment: false,
            onSlowClientPaused: options.testHooks?.onSlowClientPaused,
          }
        );
        assert.equal(execution.kind, 'timed');
        measured.push(execution.result);
      }
      summaries.push(aggregate(scenario, measured));
    }
    const childCpu = await child.close();
    return {
      configuration,
      identity,
      measurement: {
        cpuScope: 'parent-process-only',
        providerProcess: 'separate-node-child',
        fullIntegrityInsideCpuWindow: false,
        correctnessRun: runCorrectness,
        providerChildCpuMs:
          (childCpu.cpuUserMicros + childCpu.cpuSystemMicros) / 1000,
      },
      scenarios: summaries,
    };
  } finally {
    await child.close();
  }
}

function envPositiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  return positiveInteger(Number(raw), name);
}

function envNonNegativeInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  return nonNegativeInteger(Number(raw), name);
}

function envScenarios(): SegmentSpoolingCpuScenario[] {
  const raw = process.env.AIOSTREAMS_CPU_BENCHMARK_SCENARIOS;
  if (!raw) return ['S1', 'S2', 'S3', 'S4', 'S5'];
  const result: SegmentSpoolingCpuScenario[] = [];
  for (const rawEntry of raw.split(',')) {
    const entry = rawEntry.trim();
    if (!isCpuScenario(entry)) {
      throw new Error(`Unknown CPU benchmark scenario: ${entry}`);
    }
    result.push(entry);
  }
  return result;
}

function isCpuScenario(value: string): value is SegmentSpoolingCpuScenario {
  switch (value) {
    case 'S1':
    case 'S2':
    case 'S3':
    case 'S4':
    case 'S5':
      return true;
    default:
      return false;
  }
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  const report = await runSegmentSpoolingCpuBenchmark({
    totalBytes: envPositiveInteger(
      'AIOSTREAMS_CPU_BENCHMARK_TOTAL_BYTES',
      DEFAULT_TOTAL_BYTES
    ),
    segmentBytes: envPositiveInteger(
      'AIOSTREAMS_CPU_BENCHMARK_SEGMENT_BYTES',
      DEFAULT_SEGMENT_BYTES
    ),
    runs: envPositiveInteger('AIOSTREAMS_CPU_BENCHMARK_RUNS', DEFAULT_RUNS),
    warmupBytes: envPositiveInteger(
      'AIOSTREAMS_CPU_BENCHMARK_WARMUP_BYTES',
      DEFAULT_WARMUP_BYTES
    ),
    slowPauseMs: envNonNegativeInteger(
      'AIOSTREAMS_CPU_BENCHMARK_SLOW_PAUSE_MS',
      DEFAULT_SLOW_PAUSE_MS
    ),
    slowBytesPerSecond: envPositiveInteger(
      'AIOSTREAMS_CPU_BENCHMARK_SLOW_BYTES_PER_SECOND',
      DEFAULT_SLOW_BYTES_PER_SECOND
    ),
    runCorrectness: process.env.AIOSTREAMS_CPU_BENCHMARK_CORRECTNESS !== '0',
    scenarios: envScenarios(),
  });
  process.stdout.write(`${JSON.stringify(report, undefined, 2)}\n`);
}
