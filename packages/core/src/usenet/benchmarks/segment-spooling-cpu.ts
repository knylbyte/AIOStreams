import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { promises as fs } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import {
  monitorEventLoopDelay,
  PerformanceObserver,
  performance,
} from 'node:perf_hooks';
import tls from 'node:tls';
import { pathToFileURL } from 'node:url';
import yencode from 'yencode';
import '../../config/index.js';
import { DEFAULT_ENGINE_OPTIONS, type ProviderConfig } from '../types.js';
import { resolveSegmentSpoolingPlan } from '../resource-plan.js';
import { SegmentSpoolingHotpathCounters } from '../pool/hotpath-counters.js';
import type { SegmentSpoolingHotpathSnapshot } from '../pool/hotpath-counters.js';
import { SegmentSpoolingRuntime } from '../pool/segment-spooling-runtime.js';
import { SegmentCache } from '../pool/segment-cache.js';
import { MultiProviderPool } from '../pool/multi-provider-pool.js';
import { StatsAccumulator } from '../stats/accumulator.js';
import { FileStream } from '../pool/file-stream.js';
import type { NzbSegmentRef } from '../types.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;
const GIBIBYTE_BYTES = MEBIBYTE_BYTES * KIBIBYTE_BYTES;
const DEFAULT_TOTAL_BYTES = 512 * MEBIBYTE_BYTES;
const DEFAULT_SEGMENT_BYTES = MEBIBYTE_BYTES;
const DEFAULT_RUNS = 5;
const DEFAULT_WARMUP_BYTES = 32 * MEBIBYTE_BYTES;

export type SegmentSpoolingCpuScenario = 'S1' | 'S2' | 'S3' | 'S4' | 'S5';

export interface SegmentSpoolingCpuBenchmarkOptions {
  readonly totalBytes?: number;
  readonly segmentBytes?: number;
  readonly runs?: number;
  readonly warmupBytes?: number;
  readonly scenarios?: readonly SegmentSpoolingCpuScenario[];
}

export interface SegmentSpoolingCpuRunResult {
  readonly scenario: SegmentSpoolingCpuScenario;
  readonly decodedBytes: number;
  readonly deliveredBytes: number;
  readonly checksum: string;
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
  };
  readonly spool: {
    readonly peakReserved: number;
    readonly peakActual: number;
    readonly finalReserved: number;
    readonly finalActual: number;
    readonly finalArtifacts: number;
    readonly finalOpenFiles: number;
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
  };
  readonly scenarios: readonly SegmentSpoolingCpuScenarioSummary[];
}

interface BenchmarkStreamSpec {
  readonly key: string;
  readonly totalBytes: number;
  readonly segmentBytes: number;
  readonly fill: number;
}

interface EncodedBody {
  readonly decodedBytes: number;
  readonly tail: Buffer;
}

class BenchmarkTlsNntpServer {
  private readonly server: tls.Server;
  private readonly clients = new Set<tls.TLSSocket>();
  private readonly streams = new Map<string, BenchmarkStreamSpec>();
  private readonly encoded = new Map<string, EncodedBody>();

  private constructor(credentials: {
    readonly key: Buffer;
    readonly cert: Buffer;
  }) {
    this.server = tls.createServer(credentials, (socket) => {
      this.clients.add(socket);
      socket.setNoDelay(true);
      socket.on('error', () => undefined);
      socket.on('close', () => this.clients.delete(socket));
      socket.write('200 benchmark nntp ready\r\n', 'latin1');
      let pending = '';
      let responses = Promise.resolve();
      socket.on('data', (chunk: Buffer) => {
        pending += chunk.toString('latin1');
        for (;;) {
          const end = pending.indexOf('\r\n');
          if (end < 0) return;
          const command = pending.slice(0, end);
          pending = pending.slice(end + 2);
          if (command.startsWith('BODY <') && command.endsWith('>')) {
            const messageId = command.slice(6, -1);
            responses = responses.then(() =>
              this.respondBody(socket, messageId)
            );
          } else if (command === 'DATE') {
            responses = responses.then(() =>
              this.write(socket, Buffer.from('111 20260818120000\r\n'))
            );
          } else {
            responses = responses.then(() =>
              this.write(socket, Buffer.from('500 unsupported\r\n'))
            );
          }
        }
      });
    });
  }

  static async create(): Promise<BenchmarkTlsNntpServer> {
    const [key, cert] = await Promise.all([
      readFile(
        new URL('../../../test/fixtures/nntp-test-key.pem', import.meta.url)
      ),
      readFile(
        new URL('../../../test/fixtures/nntp-test-cert.pem', import.meta.url)
      ),
    ]);
    const result = new BenchmarkTlsNntpServer({ key, cert });
    await new Promise<void>((resolve, reject) => {
      result.server.once('error', reject);
      result.server.listen(0, '127.0.0.1', () => {
        result.server.removeListener('error', reject);
        resolve();
      });
    });
    return result;
  }

  get port(): number {
    const address = this.server.address();
    assert(address && typeof address !== 'string');
    return address.port;
  }

  register(spec: BenchmarkStreamSpec): void {
    this.streams.set(spec.key, spec);
    const full = Math.min(spec.segmentBytes, spec.totalBytes);
    this.ensureEncoded(spec.fill, full);
    const final = spec.totalBytes % spec.segmentBytes;
    if (final > 0) this.ensureEncoded(spec.fill, final);
  }

  async close(): Promise<void> {
    for (const client of this.clients) client.destroy();
    await new Promise<void>((resolve, reject) => {
      this.server.close((error) => (error ? reject(error) : resolve()));
    });
  }

  private ensureEncoded(fill: number, bytes: number): void {
    const key = `${fill}:${bytes}`;
    if (this.encoded.has(key)) return;
    const post = yencode.post('benchmark.bin', Buffer.alloc(bytes, fill), 128);
    const firstLineEnd = post.indexOf('\r\n');
    assert(firstLineEnd >= 0);
    this.encoded.set(key, {
      decodedBytes: bytes,
      tail: post.subarray(firstLineEnd + 2),
    });
  }

  private async respondBody(
    socket: tls.TLSSocket,
    messageId: string
  ): Promise<void> {
    const separator = messageId.lastIndexOf('-');
    const stream = this.streams.get(messageId.slice(0, separator));
    const index = Number.parseInt(messageId.slice(separator + 1), 10);
    if (!stream || !Number.isSafeInteger(index) || index < 0) {
      await this.write(socket, Buffer.from('430 no such article\r\n'));
      return;
    }
    const begin = index * stream.segmentBytes;
    const decodedBytes = Math.min(
      stream.segmentBytes,
      stream.totalBytes - begin
    );
    const encoded = this.encoded.get(`${stream.fill}:${decodedBytes}`);
    assert(encoded);
    const parts = Math.ceil(stream.totalBytes / stream.segmentBytes);
    const header = Buffer.from(
      [
        '222 article follows',
        `=ybegin part=${index + 1} total=${parts} line=128 size=${stream.totalBytes} name=benchmark.bin`,
        `=ypart begin=${begin + 1} end=${begin + encoded.decodedBytes}`,
        '',
      ].join('\r\n'),
      'latin1'
    );
    await this.write(socket, header);
    await this.write(socket, encoded.tail);
    await this.write(socket, Buffer.from('\r\n.\r\n', 'latin1'));
  }

  private async write(socket: tls.TLSSocket, chunk: Buffer): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error): void => {
        socket.removeListener('error', onError);
        reject(error);
      };
      socket.once('error', onError);
      socket.write(chunk, () => {
        socket.removeListener('error', onError);
        resolve();
      });
    });
  }
}

function positiveInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a safe positive integer`);
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

async function immediate(): Promise<void> {
  await new Promise<void>((resolve) => setImmediate(resolve));
}

async function runScenario(
  scenario: SegmentSpoolingCpuScenario,
  totalBytes: number,
  segmentBytes: number
): Promise<SegmentSpoolingCpuRunResult> {
  const root = await fs.mkdtemp(path.join(tmpdir(), 'spooling-cpu-'));
  const server = await BenchmarkTlsNntpServer.create();
  const hotpath = new SegmentSpoolingHotpathCounters();
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
    hotpathCounters: hotpath,
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
    [provider(server.port)],
    engineOptions,
    cache,
    new StatsAccumulator(),
    { spooling: runtime }
  );

  const perStreamBytes =
    scenario === 'S3' || scenario === 'S4'
      ? Math.max(segmentBytes, Math.floor(totalBytes / 2))
      : totalBytes;
  const first: BenchmarkStreamSpec = {
    key: 'cpu-stream-a',
    totalBytes: perStreamBytes,
    segmentBytes,
    fill: 0x31,
  };
  const second: BenchmarkStreamSpec = {
    key: 'cpu-stream-b',
    totalBytes: perStreamBytes,
    segmentBytes,
    fill: 0x52,
  };
  server.register(first);
  if (scenario === 'S3') server.register(second);

  const files = [makeFile(pool, first, engineOptions, plan)];
  if (scenario === 'S3')
    files.push(makeFile(pool, second, engineOptions, plan));
  if (scenario === 'S4') files.push(makeFile(pool, first, engineOptions, plan));
  await Promise.all(files.map((file) => file.open()));

  let deliveredBytes = 0;
  let firstByteMs = Number.POSITIVE_INFINITY;
  let memoryPeak = 0;
  let spoolReservedPeak = 0;
  let spoolActualPeak = 0;
  const checksums: string[] = [];
  const memoryBefore = process.memoryUsage();
  let arrayBuffersPeak = memoryBefore.arrayBuffers;
  let externalPeak = memoryBefore.external;
  let gcCount = 0;
  let gcDurationMs = 0;
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
  const cpuStarted = process.cpuUsage();

  const sample = (): void => {
    const stats = runtime.stats();
    memoryPeak = Math.max(memoryPeak, stats.memory.peakBytes);
    spoolReservedPeak = Math.max(
      spoolReservedPeak,
      stats.spool.budget.peakReservedBytes
    );
    spoolActualPeak = Math.max(
      spoolActualPeak,
      stats.spool.budget.peakActualBytes
    );
    const memory = process.memoryUsage();
    arrayBuffersPeak = Math.max(arrayBuffersPeak, memory.arrayBuffers);
    externalPeak = Math.max(externalPeak, memory.external);
  };

  const consume = async (file: FileStream, slow: boolean): Promise<void> => {
    const hash = createHash('sha256');
    let localBytes = 0;
    for await (const chunk of file.createReadStream()) {
      assert(Buffer.isBuffer(chunk));
      if (!Number.isFinite(firstByteMs)) {
        firstByteMs = performance.now() - startedAt;
      }
      hash.update(chunk);
      localBytes += chunk.length;
      deliveredBytes += chunk.length;
      sample();
      if (slow) await immediate();
    }
    assert.equal(localBytes, file.size());
    checksums.push(hash.digest('hex'));
  };

  try {
    await Promise.all(
      files.map((file, index) =>
        consume(file, scenario === 'S5' && index === 0)
      )
    );
    await cache.close();
    await pool.close();
  } finally {
    eventLoop.disable();
    gcObserver.disconnect();
    await Promise.allSettled([pool.close(), cache.close(), server.close()]);
    await fs.rm(root, { recursive: true, force: true });
  }
  const cpu = process.cpuUsage(cpuStarted);
  const wallTimeMs = performance.now() - startedAt;
  sample();
  const final = runtime.stats();
  const memoryAfter = process.memoryUsage();
  const hotpathSnapshot = hotpath.snapshot();
  const decodedBytes = hotpathSnapshot.yencDecodedBytes;
  assert(decodedBytes > 0);
  assert.equal(hotpathSnapshot.activeDownloads, 0);
  assert.equal(final.memory.usedBytes, 0);
  assert.equal(final.spool.budget.reservedBytes, 0);
  assert.equal(final.spool.budget.actualBytes, 0);
  assert.equal(final.spool.artifacts, 0);
  assert.equal(final.spool.files.openFiles, 0);
  assert.equal(resourceEvents, hotpathSnapshot.resourceEventsObserved);
  const cpuUserMs = cpu.user / 1000;
  const cpuSystemMs = cpu.system / 1000;
  return {
    scenario,
    decodedBytes,
    deliveredBytes,
    checksum: checksums.sort().join(':'),
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
    internalMemory: { peak: memoryPeak, final: final.memory.usedBytes },
    spool: {
      peakReserved: spoolReservedPeak,
      peakActual: spoolActualPeak,
      finalReserved: final.spool.budget.reservedBytes,
      finalActual: final.spool.budget.actualBytes,
      finalArtifacts: final.spool.artifacts,
      finalOpenFiles: final.spool.files.openFiles,
    },
    hotpath: hotpathSnapshot,
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
  const scenarios = options.scenarios ?? ['S1', 'S2', 'S3', 'S4', 'S5'];

  await runScenario('S1', warmupBytes, Math.min(segmentBytes, warmupBytes));
  const summaries: SegmentSpoolingCpuScenarioSummary[] = [];
  for (const scenario of scenarios) {
    const measured: SegmentSpoolingCpuRunResult[] = [];
    for (let index = 0; index < runs; index++) {
      measured.push(await runScenario(scenario, totalBytes, segmentBytes));
    }
    summaries.push(aggregate(scenario, measured));
  }
  return {
    configuration: { totalBytes, segmentBytes, runs, warmupBytes, scenarios },
    scenarios: summaries,
  };
}

function envPositiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined) return fallback;
  return positiveInteger(Number(raw), name);
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
    scenarios: envScenarios(),
  });
  process.stdout.write(`${JSON.stringify(report, undefined, 2)}\n`);
}
