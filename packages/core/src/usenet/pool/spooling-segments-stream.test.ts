import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { addAbortSignal, PassThrough, Readable } from 'node:stream';
import { getEventListeners, once } from 'node:events';
import test from 'node:test';
import '../../config/index.js';
import { ArticleNotFoundError } from '../nntp/errors.js';
import { StatsAccumulator } from '../stats/accumulator.js';
import {
  CommandPriority,
  DEFAULT_ENGINE_OPTIONS,
  type NzbSegmentRef,
  type SegmentData,
} from '../types.js';
import type { EngineResourcePlan } from '../resource-plan.js';
import type { ByteLease } from './byte-budget.js';
import { ByteBudget } from './byte-budget.js';
import { FileStream, type FileStreamPool } from './file-stream.js';
import type { SharedSegment } from './segment-arena.js';
import type {
  SegmentArtifact,
  SegmentArtifactFetchOptions,
  SegmentArtifactReadOptions,
  SegmentRangeMetadataFetchOptions,
  SegmentRangeMetadata,
} from './segment-artifact.js';
import { createSegmentReadStream } from './segment-read-stream-factory.js';
import { SegmentsStream } from './segments-stream.js';
import {
  SpoolingSegmentsStream,
  type SpoolingSegmentArtifactSource,
} from './spooling-segments-stream.js';
import { reapIdleStreams } from './tracked-stream.js';
import { YencMetadataError } from './yenc.js';
import type { DecodedSegmentMetadata } from './streaming-yenc-article-decoder.js';

interface FetchCall {
  readonly segment: NzbSegmentRef;
  readonly signal: AbortSignal | undefined;
  readonly expectedLength: number | undefined;
  readonly expectedByteRange: readonly [number, number] | undefined;
  readonly allowGrowing: boolean;
}

interface MetadataCall {
  readonly segment: NzbSegmentRef;
  readonly signal: AbortSignal | undefined;
  readonly requireByteRange: boolean;
  readonly allowStandalonePart: boolean;
}

type FetchHandler = (call: FetchCall) => Promise<SegmentArtifact>;

class TestArtifactSource implements FileStreamPool {
  readonly calls: FetchCall[] = [];
  readonly metadataRequests: MetadataCall[] = [];
  readonly bufferingBodies = new Map<string, Buffer>();
  readonly sharedBodies = new Map<string, Buffer>();
  bufferingCalls = 0;
  sharedCalls = 0;
  sharedReleases = 0;
  metadataCalls = 0;
  activeStreamLeases = 0;
  peakStreamLeases = 0;
  streamLeaseReleases = 0;
  readonly streamLeaseRequests: number[] = [];

  private readonly callWaiters = new Set<{
    readonly count: number;
    readonly resolve: () => void;
  }>();

  constructor(
    private readonly handler: FetchHandler,
    private readonly metadataHandler?: (
      segment: NzbSegmentRef,
      signal: AbortSignal | undefined,
      options: SegmentRangeMetadataFetchOptions
    ) => Promise<SegmentRangeMetadata>
  ) {}

  fetchSegmentArtifact(
    segment: NzbSegmentRef,
    _nzbHash: string,
    signal: AbortSignal | undefined,
    _priority: CommandPriority,
    options?: SegmentArtifactFetchOptions
  ): Promise<SegmentArtifact> {
    const call = {
      segment,
      signal,
      expectedLength: options?.expectedLength,
      expectedByteRange: options?.expectedByteRange,
      allowGrowing: options?.allowGrowing ?? false,
    };
    this.calls.push(call);
    for (const waiter of [...this.callWaiters]) {
      if (this.calls.length < waiter.count) continue;
      this.callWaiters.delete(waiter);
      waiter.resolve();
    }
    return this.handler(call);
  }

  acquireSegmentStreamMemory(bytes: number): Promise<ByteLease> {
    this.streamLeaseRequests.push(bytes);
    this.activeStreamLeases++;
    this.peakStreamLeases = Math.max(
      this.peakStreamLeases,
      this.activeStreamLeases
    );
    let released = false;
    return Promise.resolve({
      bytes,
      release: () => {
        if (released) return;
        released = true;
        this.streamLeaseReleases++;
        this.activeStreamLeases--;
      },
    });
  }

  fetchSegmentInto(
    segment: NzbSegmentRef,
    _nzbHash: string,
    _signal: AbortSignal | undefined,
    _priority: CommandPriority,
    out: () => Buffer
  ): Promise<SegmentData> {
    this.bufferingCalls++;
    const body = this.bufferingBodies.get(segment.messageId);
    if (!body) return Promise.reject(new Error('missing buffering fixture'));
    const target = out();
    if (target.length < body.length) {
      return Promise.resolve({ body: Buffer.from(body), size: body.length });
    }
    body.copy(target);
    return Promise.resolve({
      body: target.subarray(0, body.length),
      size: body.length,
    });
  }

  fetchSegmentShared(segment: NzbSegmentRef): Promise<SharedSegment> {
    this.sharedCalls++;
    const body = this.sharedBodies.get(segment.messageId);
    if (!body) {
      return Promise.reject(new Error('unexpected shared-segment lookup'));
    }
    const data: SegmentData = {
      body,
      byteRange: [0, body.length],
      fileSize: body.length,
      size: body.length,
    };
    let released = false;
    return Promise.resolve({
      data,
      owned: true,
      release: () => {
        if (released) return;
        released = true;
        this.sharedReleases++;
      },
    });
  }

  fetchSegmentRangeMetadata(
    segment: NzbSegmentRef,
    _nzbHash: string,
    signal: AbortSignal | undefined,
    _priority: CommandPriority,
    options: SegmentRangeMetadataFetchOptions = {}
  ): Promise<SegmentRangeMetadata> {
    this.metadataCalls++;
    this.metadataRequests.push({
      segment,
      signal,
      requireByteRange: options.requireByteRange ?? false,
      allowStandalonePart: options.allowStandalonePart ?? false,
    });
    if (this.metadataHandler) {
      return this.metadataHandler(segment, signal, options);
    }
    const body =
      this.sharedBodies.get(segment.messageId) ??
      this.bufferingBodies.get(segment.messageId);
    if (!body) {
      return Promise.reject(new Error('unexpected segment metadata lookup'));
    }
    return Promise.resolve({
      byteRange: [0, body.length],
      fileSize: body.length,
      totalParts: 1,
      decodedSize: body.length,
    });
  }

  waitForCalls(count: number): Promise<void> {
    if (this.calls.length >= count) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.callWaiters.add({ count, resolve });
    });
  }
}

class BufferArtifact implements SegmentArtifact {
  readonly metadata: DecodedSegmentMetadata;
  readonly length: number;
  readonly storage: 'spool' = 'spool';
  releaseCalls = 0;
  readerHighWaterMark: number | undefined;

  private reader: Readable | undefined;
  private released = false;

  constructor(
    private readonly body: Buffer,
    metadata: Omit<DecodedSegmentMetadata, 'size'> = {}
  ) {
    this.length = body.length;
    this.metadata = { ...metadata, size: body.length };
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    assert.equal(this.reader, undefined);
    const start = options.start ?? 0;
    const end = options.endExclusive ?? this.length;
    const reader = Readable.from(
      [Buffer.from(this.body.subarray(start, end))],
      {
        highWaterMark: options.highWaterMark,
      }
    );
    this.readerHighWaterMark = options.highWaterMark;
    if (options.signal) addAbortSignal(options.signal, reader);
    this.reader = reader;
    return reader;
  }

  async release(): Promise<void> {
    if (this.released) return;
    this.released = true;
    this.releaseCalls++;
    const reader = this.reader;
    if (reader && !reader.destroyed) reader.destroy();
    if (reader && !reader.closed) await once(reader, 'close');
  }
}

class ControlledArtifact implements SegmentArtifact {
  readonly metadata;
  readonly storage: 'spool' = 'spool';
  readonly readerReady = Promise.withResolvers<PassThrough>();
  releaseCalls = 0;

  private reader: PassThrough | undefined;
  private released = false;

  constructor(readonly length: number) {
    this.metadata = { size: length };
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    assert.equal(options.start ?? 0, 0);
    assert.equal(options.endExclusive ?? this.length, this.length);
    assert.equal(this.reader, undefined);
    const reader = new PassThrough({
      highWaterMark: options.highWaterMark,
    });
    if (options.signal) addAbortSignal(options.signal, reader);
    this.reader = reader;
    this.readerReady.resolve(reader);
    return reader;
  }

  write(chunk: Buffer): boolean {
    assert(this.reader);
    return this.reader.write(chunk);
  }

  end(): void {
    assert(this.reader);
    this.reader.end();
  }

  fail(error: Error): void {
    assert(this.reader);
    this.reader.destroy(error);
  }

  async release(): Promise<void> {
    if (this.released) return;
    this.released = true;
    this.releaseCalls++;
    const reader = this.reader;
    if (reader && !reader.destroyed) reader.destroy();
    if (reader && !reader.closed) await once(reader, 'close');
  }
}

/** Test artifact that exposes range bytes before a controlled validation gate. */
class GatedRangeArtifact implements SegmentArtifact {
  readonly metadata;
  readonly storage: 'spool' = 'spool';
  readonly readerReady = Promise.withResolvers<PassThrough>();
  releaseCalls = 0;

  private reader: PassThrough | undefined;
  private released = false;
  private committedBytes = 0;
  private rangeStart = 0;
  private rangeEnd: number;

  constructor(readonly length: number) {
    this.metadata = { size: length };
    this.rangeEnd = length;
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    assert.equal(this.reader, undefined);
    this.rangeStart = options.start ?? 0;
    this.rangeEnd = options.endExclusive ?? this.length;
    const reader = new PassThrough({ highWaterMark: options.highWaterMark });
    if (options.signal) addAbortSignal(options.signal, reader);
    this.reader = reader;
    this.readerReady.resolve(reader);
    return reader;
  }

  commit(chunk: Buffer): boolean {
    assert(this.reader);
    const begin = this.committedBytes;
    const end = begin + chunk.length;
    this.committedBytes = end;
    const overlapBegin = Math.max(begin, this.rangeStart);
    const overlapEnd = Math.min(end, this.rangeEnd);
    if (overlapEnd <= overlapBegin) return true;
    return this.reader.write(
      chunk.subarray(overlapBegin - begin, overlapEnd - begin)
    );
  }

  complete(): void {
    assert(this.reader);
    this.reader.end();
  }

  fail(error: Error): void {
    assert(this.reader);
    this.reader.destroy(error);
  }

  async release(): Promise<void> {
    if (this.released) return;
    this.released = true;
    this.releaseCalls++;
    const reader = this.reader;
    if (reader && !reader.destroyed) reader.destroy();
    if (reader && !reader.closed) await once(reader, 'close');
  }
}

class BudgetedArtifactSource extends TestArtifactSource {
  constructor(
    handler: FetchHandler,
    readonly budget: ByteBudget,
    private readonly leaseBytes: number
  ) {
    super(handler);
  }

  override acquireSegmentStreamMemory(
    bytes: number,
    priority: CommandPriority,
    signal?: AbortSignal
  ): Promise<ByteLease> {
    assert.equal(bytes, this.leaseBytes);
    this.streamLeaseRequests.push(bytes);
    return this.budget.acquire(bytes, { priority, signal });
  }
}

function spoolingResourcePlan(
  readerHighWaterMarkBytes: number
): EngineResourcePlan {
  return {
    mode: 'segment_spooling',
    arenaBytes: 1,
    segmentSpooling: {
      memoryBudgetBytes: 24 * readerHighWaterMarkBytes,
      perStreamBufferBytes: 4 * readerHighWaterMarkBytes,
      spoolBytes: 1024,
      minFreeDiskBytes: 0,
      decoderChunkBytes: 1,
      writerQueueBytes: readerHighWaterMarkBytes,
      readerHighWaterMarkBytes,
      perDownloadBaseLeaseBytes: 2,
      maxOpenSpoolFiles: 8,
      orphanTtlMs: 60_000,
    },
  };
}

function segment(index: number): NzbSegmentRef {
  return { messageId: `segment-${index}`, bytes: 4 };
}

function streamOptions(
  source: SpoolingSegmentArtifactSource,
  count: number,
  overrides: Partial<{
    readonly maxPrefetchSegments: number;
    readonly skipBytes: number;
    readonly limitBytes: number;
    readonly sizeForSegment: (idx: number) => number | undefined;
    readonly knownHoles: ReadonlySet<number>;
    readonly onHole: (
      idx: number,
      bytes: number,
      kind: 'missing' | 'undecodable'
    ) => 'pad' | 'fail';
  }> = {}
): ConstructorParameters<typeof SpoolingSegmentsStream>[0] {
  return {
    pool: source,
    segments: Array.from({ length: count }, (_, index) => segment(index)),
    nzbHash: 'spooling-stream-test',
    maxPrefetchSegments: overrides.maxPrefetchSegments ?? 3,
    readerHighWaterMarkBytes: 4,
    skipBytes: overrides.skipBytes,
    limitBytes: overrides.limitBytes,
    priority: CommandPriority.High,
    sizeForSegment: overrides.sizeForSegment,
    knownHoles: overrides.knownHoles,
    onHole: overrides.onHole,
  };
}

async function collect(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    assert(Buffer.isBuffer(chunk));
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function abortableArtifact(
  promise: Promise<SegmentArtifact>,
  signal: AbortSignal | undefined
): Promise<SegmentArtifact> {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<SegmentArtifact>((resolve, reject) => {
    const onAbort = (): void => {
      signal.removeEventListener('abort', onAbort);
      reject(signal.reason);
    };
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      (artifact) => {
        signal.removeEventListener('abort', onAbort);
        resolve(artifact);
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      }
    );
  });
}

function abortablePromise<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined
): Promise<T> {
  if (!signal) return promise;
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => {
      signal.removeEventListener('abort', onAbort);
      reject(signal.reason);
    };
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener('abort', onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener('abort', onAbort);
        reject(error);
      }
    );
  });
}

function immediate(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

async function closeEvent(stream: Readable): Promise<void> {
  if (stream.closed) return;
  await new Promise<void>((resolve) => stream.once('close', resolve));
}

test('segment read factory selects buffering and spooling without fallback', async () => {
  const pool = new TestArtifactSource(() =>
    Promise.reject(new Error('factory fixture must not fetch'))
  );
  const segments: NzbSegmentRef[] = [];
  const common = {
    pool,
    segments,
    nzbHash: 'factory',
    maxPrefetchSegments: 2,
    bufferingBufferSizeBytes: 1024,
    spoolingReaderHighWaterMarkBytes: 512,
    priority: CommandPriority.High,
  };
  const buffering = createSegmentReadStream(common, 'segment_buffering');
  const spooling = createSegmentReadStream(common, 'segment_spooling');
  assert(buffering instanceof SegmentsStream);
  assert(spooling instanceof SpoolingSegmentsStream);
  buffering.destroy();
  spooling.destroy();
  await Promise.all([closeEvent(buffering), closeEvent(spooling)]);
});

test('FileStream routes only the selected direct mode through the central factory', async () => {
  const body = Buffer.from('mode');
  const bufferingSource = new TestArtifactSource(() =>
    Promise.reject(new Error('buffering mode reached artifact fetch'))
  );
  bufferingSource.bufferingBodies.set('mode-segment', body);
  const buffering = new FileStream(
    bufferingSource,
    {
      segments: [{ messageId: 'mode-segment', bytes: body.length }],
      knownSize: body.length,
    },
    'buffering-mode',
    { ...DEFAULT_ENGINE_OPTIONS, prefetchSegments: 1 }
  );
  await buffering.open();
  assert.deepEqual(await collect(buffering.createReadStream()), body);
  assert.equal(bufferingSource.bufferingCalls, 1);
  assert.equal(bufferingSource.calls.length, 0);

  const artifact = new BufferArtifact(body);
  const spoolingSource = new TestArtifactSource(() =>
    Promise.resolve(artifact)
  );
  const spooling = new FileStream(
    spoolingSource,
    {
      segments: [{ messageId: 'mode-segment', bytes: body.length }],
      knownSize: body.length,
    },
    'spooling-mode',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 1,
    }
  );
  await spooling.open();
  assert.deepEqual(await collect(spooling.createReadStream()), body);
  assert.equal(spoolingSource.bufferingCalls, 0);
  assert.equal(spoolingSource.calls.length, 1);
  assert.equal(spoolingSource.calls[0].expectedLength, body.length);
  assert.equal(artifact.releaseCalls, 1);
  assert.equal(spoolingSource.activeStreamLeases, 0);
});

test('spooling mode leaves FileStream readAt on the shared buffering API', async () => {
  const body = Buffer.from('mode');
  const source = new TestArtifactSource(() =>
    Promise.reject(new Error('readAt reached artifact fetch'))
  );
  source.sharedBodies.set('read-at-segment', body);
  const file = new FileStream(
    source,
    {
      segments: [{ messageId: 'read-at-segment', bytes: body.length }],
      knownSize: body.length,
    },
    'read-at-mode',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 1,
    }
  );
  await file.open();
  assert.equal((await file.readAt(1, 2)).toString(), 'od');
  assert.equal(source.sharedCalls, 1);
  assert.equal(source.sharedReleases, 1);
  assert.equal(source.calls.length, 0);
});

test('spooling FileStream starts at byte zero without a buffering locator and tails segment zero', async () => {
  const first = new ControlledArtifact(4);
  const second = new BufferArtifact(Buffer.from('bbbb'));
  const source = new TestArtifactSource((call) =>
    Promise.resolve(call.segment.messageId === 'segment-0' ? first : second)
  );
  const file = new FileStream(
    source,
    {
      segments: [segment(0), segment(1)],
      knownSize: 8,
    },
    'no-buffering-locator',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 2,
    }
  );
  await file.open();
  const stream = file.createReadStream();
  const firstOutput = once(stream, 'data');
  const output = collect(stream);
  const firstReader = await first.readerReady.promise;
  assert.equal(firstReader.writableEnded, false);
  first.write(Buffer.from('aa'));
  await firstOutput;

  assert.equal(source.sharedCalls, 0);
  assert.equal(source.bufferingCalls, 0);
  assert.equal(source.metadataCalls, 0);
  assert.equal(source.calls.length, 2);
  assert.equal(source.calls[0].allowGrowing, true);
  assert.equal(source.calls[1].allowGrowing, false);

  first.write(Buffer.from('aa'));
  first.end();
  assert.equal((await output).toString(), 'aaaabbbb');
  assert.equal(first.releaseCalls, 1);
  assert.equal(second.releaseCalls, 1);
  assert.equal(source.activeStreamLeases, 0);
});

test('spooling FileStream open without knownSize uses only bounded scalar metadata', async () => {
  const bodies = new Map([
    ['segment-0', Buffer.from('aaaa')],
    ['segment-1', Buffer.from('bbbb')],
  ]);
  const source = new TestArtifactSource(
    (call) => {
      const body = bodies.get(call.segment.messageId);
      if (!body) throw new Error('missing bounded metadata test body');
      return Promise.resolve(new BufferArtifact(body));
    },
    (_segment, signal) => {
      assert.notEqual(signal?.aborted, true);
      return Promise.resolve({
        byteRange: [0, 4],
        fileSize: 8,
        totalParts: 2,
        decodedSize: 4,
      });
    }
  );
  const file = new FileStream(
    source,
    { segments: [segment(0), segment(1)] },
    'bounded-open-metadata',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 2,
    }
  );

  await file.open();
  assert.equal(file.size(), 8);
  assert.equal(source.metadataCalls, 1);
  assert.equal(source.sharedCalls, 0);
  assert.equal(source.bufferingCalls, 0);
  assert.equal(source.calls.length, 0);

  assert.equal((await collect(file.createReadStream())).toString(), 'aaaabbbb');
  assert.equal(source.sharedCalls, 0);
  assert.equal(source.bufferingCalls, 0);
  assert.equal(source.metadataCalls, 1);
  assert.equal(source.calls.length, 2);
  assert.deepEqual(
    source.calls.map((call) => call.expectedByteRange),
    [
      [0, 4],
      [4, 8],
    ]
  );
});

test('standalone yEnc parts build an exact bounded prefix map and stream byte-identically', async () => {
  const bodies = [Buffer.from('abcd'), Buffer.from('efgh'), Buffer.from('ij')];
  const source = new TestArtifactSource(
    (call) => {
      const index = Number(call.segment.messageId.split('-')[1]);
      const body = bodies[index];
      assert(body);
      return Promise.resolve(
        new BufferArtifact(body, {
          fileSize: body.length,
          totalParts: 1,
        })
      );
    },
    (candidate, signal, options) => {
      assert.notEqual(signal?.aborted, true);
      assert.equal(options.requireByteRange, true);
      assert.equal(options.allowStandalonePart, true);
      const index = Number(candidate.messageId.split('-')[1]);
      const body = bodies[index];
      assert(body);
      return Promise.resolve({
        fileSize: body.length,
        totalParts: 1,
        decodedSize: body.length,
        layout: 'standalone-part',
      });
    }
  );
  const file = new FileStream(
    source,
    { segments: bodies.map((_, index) => segment(index)) },
    'standalone-prefix-map',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 1,
    }
  );

  await file.open();
  assert.equal(file.size(), 10);
  assert.equal(source.metadataCalls, bodies.length);
  assert.equal(source.calls.length, 0);
  assert.equal(source.sharedCalls, 0);
  assert.equal(source.bufferingCalls, 0);

  assert.deepEqual(
    await collect(file.createReadStream()),
    Buffer.concat(bodies)
  );
  assert.deepEqual(
    source.calls.map((call) => call.expectedLength),
    [4, 4, 2]
  );
  assert(source.calls.every((call) => call.expectedByteRange === undefined));

  assert.equal(
    (await collect(file.createReadStream({ start: 8, end: 10 }))).toString(),
    'ij'
  );
  assert.equal(source.calls.length, 4);
  assert.equal(source.calls[3].segment.messageId, 'segment-2');
  assert.equal(source.calls[3].expectedLength, 2);
  assert.equal(source.calls[3].expectedByteRange, undefined);
  assert.equal(source.metadataCalls, bodies.length);
  assert.equal(source.activeStreamLeases, 0);
});

test('mixed global-range and standalone-part metadata is rejected deterministically', async () => {
  const source = new TestArtifactSource(
    () => Promise.reject(new Error('mixed layout must not fetch an artifact')),
    (candidate) => {
      if (candidate.messageId === 'segment-0') {
        return Promise.resolve({
          fileSize: 4,
          totalParts: 1,
          decodedSize: 4,
          layout: 'standalone-part',
        });
      }
      return Promise.resolve({
        byteRange: [4, 8],
        fileSize: 8,
        totalParts: 2,
        decodedSize: 4,
        layout: 'global-range',
      });
    }
  );
  const file = new FileStream(
    source,
    { segments: [segment(0), segment(1)] },
    'mixed-layout',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 1,
    }
  );

  await assert.rejects(file.open(), (error: unknown) => {
    assert(error instanceof YencMetadataError);
    assert.equal(error.code, 'inconsistent_layout');
    return true;
  });
  assert.equal(source.metadataCalls, 2);
  assert.equal(source.calls.length, 0);
});

test('nonzero spooling seek probes only scalar metadata and reuses one target artifact', async () => {
  const ranges = new Map<string, readonly [number, number]>([
    ['segment-0', [0, 2]],
    ['segment-1', [2, 5]],
    ['segment-2', [5, 11]],
    ['segment-3', [11, 20]],
  ]);
  const located = new BufferArtifact(Buffer.from('DDDDDDDDD'), {
    byteRange: [11, 20],
    fileSize: 20,
    totalParts: 4,
  });
  const source = new TestArtifactSource(
    (call) => {
      assert.equal(call.segment.messageId, 'segment-3');
      return Promise.resolve(located);
    },
    (candidate, signal, options) => {
      assert.equal(signal?.aborted, false);
      assert.equal(options.requireByteRange, true);
      const range = ranges.get(candidate.messageId);
      assert(range);
      return Promise.resolve({
        byteRange: range,
        fileSize: 20,
        totalParts: 4,
        decodedSize: range[1] - range[0],
        layout: 'global-range',
      });
    }
  );
  const file = new FileStream(
    source,
    {
      segments: [segment(0), segment(1), segment(2), segment(3)],
      knownSize: 20,
    },
    'reuse-locator-artifact',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 1,
    }
  );
  await file.open();

  assert.equal(
    (await collect(file.createReadStream({ start: 12, end: 15 }))).toString(),
    'DDD'
  );
  assert.equal(source.calls.length, 1);
  assert.equal(source.calls[0].segment.messageId, 'segment-3');
  assert.equal(source.calls[0].allowGrowing, true);
  assert.equal(source.calls[0].expectedLength, 9);
  assert.deepEqual(source.calls[0].expectedByteRange, [11, 20]);
  assert.equal(source.sharedCalls, 0);
  assert.equal(source.bufferingCalls, 0);
  assert.deepEqual(
    source.metadataRequests.map((request) => request.segment.messageId),
    ['segment-2', 'segment-3']
  );
  assert.equal(located.releaseCalls, 1);
  assert.equal(source.activeStreamLeases, 0);
});

test('destroy during bounded metadata search aborts before target-artifact admission', async () => {
  const pending = Promise.withResolvers<SegmentRangeMetadata>();
  const metadataStarted = Promise.withResolvers<void>();
  const source = new TestArtifactSource(
    () => Promise.reject(new Error('target artifact must not be admitted')),
    (_candidate, signal) => {
      metadataStarted.resolve();
      return abortablePromise(pending.promise, signal);
    }
  );
  const file = new FileStream(
    source,
    {
      segments: [segment(0), segment(1)],
      knownSize: 8,
    },
    'abort-locator',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 1,
    }
  );
  await file.open();
  const stream = file.createReadStream({ start: 5, end: 7 });
  stream.resume();
  await metadataStarted.promise;
  const locatorSignal = source.metadataRequests[0].signal;
  assert(locatorSignal);
  stream.destroy();
  await closeEvent(stream);

  assert.equal(locatorSignal.aborted, true);
  assert.equal(getEventListeners(locatorSignal, 'abort').length, 0);
  assert.equal(source.metadataCalls, 1);
  assert.equal(source.calls.length, 0);
  assert.equal(source.activeStreamLeases, 0);
});

test('unusable locator metadata fails typed before creating a target artifact', async () => {
  const metadataFailure = new YencMetadataError(
    'invalid_header',
    'article metadata unusable on all providers (invalid_header)'
  );
  const source = new TestArtifactSource(
    () => Promise.reject(new Error('target artifact must not be admitted')),
    () => Promise.reject(metadataFailure)
  );
  const file = new FileStream(
    source,
    {
      segments: [segment(0), segment(1), segment(2)],
      knownSize: 12,
    },
    'invalid-locator-metadata',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 1,
    }
  );
  await file.open();
  const stream = file.createReadStream({ start: 5, end: 7 });
  const streamError = Promise.withResolvers<Error>();
  stream.once('error', streamError.resolve);
  stream.resume();

  assert.equal(await streamError.promise, metadataFailure);
  await closeEvent(stream);
  assert.equal(source.metadataCalls, 1);
  assert.equal(source.calls.length, 0);
  assert.equal(source.activeStreamLeases, 0);
});

test('out-of-order artifacts wait on disk references and emit strictly in order', async () => {
  const gates = Array.from({ length: 3 }, () =>
    Promise.withResolvers<SegmentArtifact>()
  );
  const artifacts = ['aaaa', 'bbbb', 'cccc'].map(
    (body) => new BufferArtifact(Buffer.from(body))
  );
  const source = new TestArtifactSource((call) => {
    const index = Number(call.segment.messageId.split('-')[1]);
    return abortableArtifact(gates[index].promise, call.signal);
  });
  const output = collect(new SpoolingSegmentsStream(streamOptions(source, 3)));
  await source.waitForCalls(3);
  gates[2].resolve(artifacts[2]);
  gates[1].resolve(artifacts[1]);
  gates[0].resolve(artifacts[0]);

  assert.equal((await output).toString(), 'aaaabbbbcccc');
  assert.deepEqual(
    artifacts.map((artifact) => artifact.releaseCalls),
    [1, 1, 1]
  );
  assert.equal(source.activeStreamLeases, 0);
  assert.equal(source.calls.length, 3);
});

test('planned fetch tasks never exceed the configured prefetch window', async () => {
  const gates = Array.from({ length: 5 }, () =>
    Promise.withResolvers<SegmentArtifact>()
  );
  const settled = Array.from({ length: 5 }, () =>
    Promise.withResolvers<void>()
  );
  const artifacts = Array.from(
    { length: 5 },
    (_, index) => new BufferArtifact(Buffer.from(String(index)))
  );
  const source = new TestArtifactSource((call) => {
    const index = Number(call.segment.messageId.split('-')[1]);
    return abortableArtifact(gates[index].promise, call.signal).finally(
      settled[index].resolve
    );
  });
  const output = collect(
    new SpoolingSegmentsStream(
      streamOptions(source, 5, { maxPrefetchSegments: 2 })
    )
  );
  await source.waitForCalls(2);
  gates[1].resolve(artifacts[1]);
  await settled[1].promise;
  assert.equal(
    source.calls.length,
    2,
    'a completed future artifact still occupies its bounded plan slot'
  );

  for (let index = 2; index < gates.length; index++) {
    gates[index].resolve(artifacts[index]);
  }
  gates[0].resolve(artifacts[0]);
  await source.waitForCalls(5);
  assert.equal((await output).toString(), '01234');
  assert(artifacts.every((artifact) => artifact.releaseCalls === 1));
  assert.equal(source.activeStreamLeases, 0);
});

test('the first artifact emits committed bytes before its writer completes', async () => {
  const artifact = new ControlledArtifact(6);
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const stream = new SpoolingSegmentsStream(streamOptions(source, 1));
  const firstData = Promise.withResolvers<Buffer>();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    if (chunks.length === 1) firstData.resolve(chunk);
  });
  const ended = Promise.withResolvers<void>();
  stream.once('end', ended.resolve);
  await artifact.readerReady.promise;
  artifact.write(Buffer.from('abc'));
  assert.equal((await firstData.promise).toString(), 'abc');
  assert.equal(stream.readableEnded, false);
  artifact.write(Buffer.from('def'));
  artifact.end();
  await ended.promise;
  assert.equal(Buffer.concat(chunks).toString(), 'abcdef');
  await closeEvent(stream);
  assert.equal(artifact.releaseCalls, 1);
  assert.equal(source.activeStreamLeases, 0);
});

test('the next segment cannot advance before the growing first reader validates EOF', async () => {
  const first = new ControlledArtifact(3);
  const second = new BufferArtifact(Buffer.from('def'));
  const source = new TestArtifactSource((call) =>
    Promise.resolve(call.segment.messageId === 'segment-0' ? first : second)
  );
  const stream = new SpoolingSegmentsStream(streamOptions(source, 2));
  const chunks: Buffer[] = [];
  const firstData = Promise.withResolvers<void>();
  stream.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    firstData.resolve();
  });
  const ended = Promise.withResolvers<void>();
  stream.once('end', ended.resolve);
  await first.readerReady.promise;
  first.write(Buffer.from('abc'));
  await firstData.promise;
  await Promise.resolve();
  await Promise.resolve();
  assert.equal(Buffer.concat(chunks).toString(), 'abc');
  assert.equal(second.releaseCalls, 0);

  first.end();
  await ended.promise;
  assert.equal(Buffer.concat(chunks).toString(), 'abcdef');
  await closeEvent(stream);
  assert.equal(first.releaseCalls, 1);
  assert.equal(second.releaseCalls, 1);
  assert.equal(source.activeStreamLeases, 0);
});

test('a satisfied partial range emits bytes but waits for producer-validated EOF', async () => {
  const artifact = new GatedRangeArtifact(6);
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const stream = new SpoolingSegmentsStream(
    streamOptions(source, 1, { limitBytes: 3 })
  );
  const chunks: Buffer[] = [];
  const firstData = Promise.withResolvers<void>();
  let ended = false;
  stream.on('data', (chunk: Buffer) => {
    chunks.push(chunk);
    firstData.resolve();
  });
  stream.once('end', () => {
    ended = true;
  });

  await artifact.readerReady.promise;
  artifact.commit(Buffer.from('abc'));
  await firstData.promise;
  await immediate();
  assert.equal(Buffer.concat(chunks).toString(), 'abc');
  assert.equal(ended, false);
  assert.equal(stream.closed, false);
  assert.equal(artifact.releaseCalls, 0);
  assert.equal(source.calls.length, 1);

  const completed = once(stream, 'end');
  artifact.complete();
  await completed;
  await closeEvent(stream);
  assert.equal(artifact.releaseCalls, 1);
  assert.equal(source.activeStreamLeases, 0);
});

test('a late producer failure rejects a partial range after its bytes were emitted', async () => {
  const artifact = new GatedRangeArtifact(6);
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const stream = new SpoolingSegmentsStream(
    streamOptions(source, 1, { limitBytes: 3 })
  );
  const chunks: Buffer[] = [];
  let ended = false;
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));
  stream.once('end', () => {
    ended = true;
  });
  const streamError = Promise.withResolvers<Error>();
  stream.once('error', streamError.resolve);

  await artifact.readerReady.promise;
  artifact.commit(Buffer.from('abc'));
  await immediate();
  const failure = new Error('producer validation failed after range bytes');
  artifact.fail(failure);
  assert.equal(await streamError.promise, failure);
  await closeEvent(stream);
  assert.equal(Buffer.concat(chunks).toString(), 'abc');
  assert.equal(ended, false);
  assert.equal(artifact.releaseCalls, 1);
  assert.equal(source.activeStreamLeases, 0);
});

test('an exact final-segment range cannot hide a late yend validation failure', async () => {
  const artifact = new GatedRangeArtifact(3);
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const stream = new SpoolingSegmentsStream(
    streamOptions(source, 1, { limitBytes: 3 })
  );
  const chunks: Buffer[] = [];
  let ended = false;
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));
  stream.once('end', () => {
    ended = true;
  });
  const streamError = Promise.withResolvers<Error>();
  stream.once('error', streamError.resolve);

  await artifact.readerReady.promise;
  artifact.commit(Buffer.from('xyz'));
  await immediate();
  assert.equal(ended, false);
  const failure = new Error('yEnc decode failed: no_end_found');
  artifact.fail(failure);
  assert.equal(await streamError.promise, failure);
  await closeEvent(stream);
  assert.equal(Buffer.concat(chunks).toString(), 'xyz');
  assert.equal(ended, false);
  assert.equal(artifact.releaseCalls, 1);
});

test('a range ending in a later segment waits for that segment validation', async () => {
  const first = new BufferArtifact(Buffer.from('aaaa'));
  const second = new GatedRangeArtifact(6);
  const source = new TestArtifactSource((call) =>
    Promise.resolve(call.segment.messageId === 'segment-0' ? first : second)
  );
  const stream = new SpoolingSegmentsStream(
    streamOptions(source, 2, { limitBytes: 7 })
  );
  const chunks: Buffer[] = [];
  let ended = false;
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));
  stream.once('end', () => {
    ended = true;
  });

  await second.readerReady.promise;
  second.commit(Buffer.from('bbbbbb'));
  await immediate();
  assert.equal(Buffer.concat(chunks).toString(), 'aaaabbb');
  assert.equal(ended, false);
  assert.equal(second.releaseCalls, 0);

  const completed = once(stream, 'end');
  second.complete();
  await completed;
  await closeEvent(stream);
  assert.equal(first.releaseCalls, 1);
  assert.equal(second.releaseCalls, 1);
  assert.equal(source.activeStreamLeases, 0);
});

test('skip and exact byte limit span segment boundaries without over-read', async () => {
  const bodies = ['abcd', 'efgh', 'ijkl'].map((value) => Buffer.from(value));
  const artifacts = bodies.map((body) => new BufferArtifact(body));
  const source = new TestArtifactSource((call) => {
    const index = Number(call.segment.messageId.split('-')[1]);
    return Promise.resolve(artifacts[index]);
  });
  const stream = new SpoolingSegmentsStream(
    streamOptions(source, 3, { skipBytes: 2, limitBytes: 7 })
  );
  assert.equal((await collect(stream)).toString(), 'cdefghi');
  assert.equal(source.activeStreamLeases, 0);
  assert(artifacts.every((artifact) => artifact.releaseCalls === 1));
});

test('exact EOF aborts unneeded prefetched waiters and releases the stream lease', async () => {
  const future = [
    Promise.withResolvers<SegmentArtifact>(),
    Promise.withResolvers<SegmentArtifact>(),
  ];
  const first = new BufferArtifact(Buffer.from('abcdef'));
  const source = new TestArtifactSource((call) => {
    const index = Number(call.segment.messageId.split('-')[1]);
    return index === 0
      ? Promise.resolve(first)
      : abortableArtifact(future[index - 1].promise, call.signal);
  });
  const stream = new SpoolingSegmentsStream(
    streamOptions(source, 3, { limitBytes: 3 })
  );
  const output = collect(stream);
  await source.waitForCalls(3);
  assert.equal((await output).toString(), 'abc');
  assert.equal(source.calls[1].signal?.aborted, true);
  assert.equal(source.calls[2].signal?.aborted, true);
  assert.equal(source.activeStreamLeases, 0);
  assert.equal(first.releaseCalls, 1);
});

test('outer backpressure and explicit pause/resume control the active reader', async () => {
  const artifact = new ControlledArtifact(8);
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const stream = new SpoolingSegmentsStream(streamOptions(source, 1));
  const readable = Promise.withResolvers<void>();
  stream.once('readable', readable.resolve);
  const reader = await artifact.readerReady.promise;
  artifact.write(Buffer.from('abcd'));
  await readable.promise;
  assert.equal(reader.isPaused(), true, 'outer HWM must pause the artifact');

  assert.equal(stream.read(4).toString(), 'abcd');
  stream.pause();
  assert.equal(reader.isPaused(), true);
  stream.resume();
  artifact.write(Buffer.from('efgh'));
  artifact.end();
  assert.equal((await collect(stream)).toString(), 'efgh');
  assert.equal(source.activeStreamLeases, 0);
});

test('stream memory lease remains held while unread output is queued', async () => {
  const artifact = new BufferArtifact(Buffer.from('unread!!'));
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const stream = new SpoolingSegmentsStream(streamOptions(source, 1));
  stream.read(0);
  await once(stream, 'readable');
  await immediate();

  assert(stream.readableLength > 0);
  assert.deepEqual(source.streamLeaseRequests, [8]);
  assert.equal(source.activeStreamLeases, 1);
  assert.equal(source.streamLeaseReleases, 0);

  assert.equal((await collect(stream)).toString(), 'unread!!');
  await closeEvent(stream);
  assert.equal(source.activeStreamLeases, 0);
  assert.equal(source.streamLeaseReleases, 1);
});

test('stream memory lease survives producer completion during backpressure', async () => {
  const artifact = new ControlledArtifact(8);
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const stream = new SpoolingSegmentsStream(streamOptions(source, 1));
  stream.read(0);
  const readable = once(stream, 'readable');
  await artifact.readerReady.promise;
  artifact.write(Buffer.from('abcdefgh'));
  artifact.end();
  await readable;
  await immediate();

  assert(stream.readableLength > 0);
  assert.equal(source.activeStreamLeases, 1);
  assert.equal(source.streamLeaseReleases, 0);

  assert.equal((await collect(stream)).toString(), 'abcdefgh');
  await closeEvent(stream);
  assert.equal(source.activeStreamLeases, 0);
  assert.equal(source.streamLeaseReleases, 1);
});

test('client destroy discards queued output before releasing its lease once', async () => {
  const artifact = new BufferArtifact(Buffer.from('discard'));
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const stream = new SpoolingSegmentsStream(streamOptions(source, 1));
  stream.read(0);
  await once(stream, 'readable');
  assert(stream.readableLength > 0);
  assert.equal(source.activeStreamLeases, 1);

  stream.destroy();
  await closeEvent(stream);
  assert.equal(stream.readableLength, 0);
  assert.equal(source.activeStreamLeases, 0);
  assert.equal(source.streamLeaseReleases, 1);
  stream.destroy();
  assert.equal(source.streamLeaseReleases, 1);
});

test('two queued streams cannot overbook the shared byte budget', async () => {
  const budget = new ByteBudget(8, { maxWaiters: 2 });
  const source = new BudgetedArtifactSource(
    () => Promise.resolve(new BufferArtifact(Buffer.from('data'))),
    budget,
    8
  );
  const first = new SpoolingSegmentsStream(streamOptions(source, 1));
  first.read(0);
  await once(first, 'readable');
  assert(first.readableLength > 0);

  const second = new SpoolingSegmentsStream(streamOptions(source, 1));
  second.read(0);
  assert.deepEqual(budget.stats(), {
    maxBytes: 8,
    usedBytes: 8,
    waiting: 1,
    peakBytes: 8,
  });

  const secondReadable = once(second, 'readable');
  assert.equal((await collect(first)).toString(), 'data');
  await closeEvent(first);
  await secondReadable;
  assert.equal(budget.stats().usedBytes, 8);
  assert.equal((await collect(second)).toString(), 'data');
  await closeEvent(second);
  assert.equal(budget.stats().usedBytes, 0);
  assert.equal(budget.stats().peakBytes, 8);
  budget.close();
});

test('FileStream relay retains the stream lease until its outer queue drains', async () => {
  const body = Buffer.from('relay-buffer');
  const artifact = new BufferArtifact(body);
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const file = new FileStream(
    source,
    { segments: [segment(0)], knownSize: body.length },
    'relay-lease',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 1,
    }
  );
  await file.open();
  const outer = file.createReadStream();
  const outerReadable = once(outer, 'readable');
  outer.read(0);
  await outerReadable;
  await immediate();

  assert(outer.readableLength > 0);
  assert.equal(source.streamLeaseRequests[0], 3 * outer.readableHighWaterMark);
  assert.equal(source.activeStreamLeases, 1);
  assert.equal(source.streamLeaseReleases, 0);

  assert.deepEqual(await collect(outer), body);
  await closeEvent(outer);
  assert.equal(source.activeStreamLeases, 0);
  assert.equal(source.streamLeaseReleases, 1);
  assert.equal(artifact.releaseCalls, 1);
});

test('FileStream reserves one atomic 3H window for reader, stream, and relay', async () => {
  const highWaterMark = 4;
  const body = Buffer.from('data');
  const artifact = new BufferArtifact(body);
  const source = new TestArtifactSource(() => Promise.resolve(artifact));
  const file = new FileStream(
    source,
    { segments: [segment(0)], knownSize: body.length },
    'three-queue-budget',
    {
      ...DEFAULT_ENGINE_OPTIONS,
      streamingMode: 'segment_spooling',
      prefetchSegments: 1,
    },
    undefined,
    undefined,
    spoolingResourcePlan(highWaterMark)
  );
  await file.open();
  const outer = file.createReadStream();
  const outerReadable = once(outer, 'readable');
  outer.read(0);
  await outerReadable;

  assert.equal(outer.readableHighWaterMark, highWaterMark);
  assert.equal(outer.readableLength, highWaterMark);
  assert.equal(artifact.readerHighWaterMark, highWaterMark);
  assert.deepEqual(source.streamLeaseRequests, [3 * highWaterMark]);

  assert.deepEqual(await collect(outer), body);
  await closeEvent(outer);
  assert.equal(source.streamLeaseReleases, 1);
});

test('a second FileStream cannot overbook an atomic 3H relay window', async () => {
  const highWaterMark = 4;
  const budget = new ByteBudget(3 * highWaterMark, { maxWaiters: 2 });
  const source = new BudgetedArtifactSource(
    () => Promise.resolve(new BufferArtifact(Buffer.from('data'))),
    budget,
    3 * highWaterMark
  );
  const createFile = (id: string): FileStream =>
    new FileStream(
      source,
      { segments: [{ messageId: id, bytes: 4 }], knownSize: 4 },
      id,
      {
        ...DEFAULT_ENGINE_OPTIONS,
        streamingMode: 'segment_spooling',
        prefetchSegments: 1,
      },
      undefined,
      undefined,
      spoolingResourcePlan(highWaterMark)
    );
  const firstFile = createFile('first-relay-budget');
  const secondFile = createFile('second-relay-budget');
  await Promise.all([firstFile.open(), secondFile.open()]);
  const first = firstFile.createReadStream();
  const firstReadable = once(first, 'readable');
  first.read(0);
  await firstReadable;
  const second = secondFile.createReadStream();
  second.read(0);
  await immediate();

  assert.deepEqual(budget.stats(), {
    maxBytes: 3 * highWaterMark,
    usedBytes: 3 * highWaterMark,
    waiting: 1,
    peakBytes: 3 * highWaterMark,
  });

  assert.equal((await collect(first)).toString(), 'data');
  await closeEvent(first);
  await once(second, 'readable');
  assert.equal(budget.stats().usedBytes, 3 * highWaterMark);
  assert.equal((await collect(second)).toString(), 'data');
  await closeEvent(second);
  assert.equal(budget.stats().usedBytes, 0);
  assert.deepEqual(source.streamLeaseRequests, [12, 12]);
  budget.close();
});

test('client destruction aborts every planned fetch and leaves no lease', async () => {
  const gates = Array.from({ length: 2 }, () =>
    Promise.withResolvers<SegmentArtifact>()
  );
  const source = new TestArtifactSource((call) => {
    const index = Number(call.segment.messageId.split('-')[1]);
    return abortableArtifact(gates[index].promise, call.signal);
  });
  const stream = new SpoolingSegmentsStream(
    streamOptions(source, 2, { maxPrefetchSegments: 2 })
  );
  stream.on('error', () => undefined);
  stream.resume();
  await source.waitForCalls(2);
  stream.destroy(new Error('client closed'));
  await closeEvent(stream);
  assert(source.calls.every((call) => call.signal?.aborted === true));
  assert(
    source.calls.every(
      (call) =>
        call.signal === undefined ||
        getEventListeners(call.signal, 'abort').length === 0
    )
  );
  assert.equal(source.activeStreamLeases, 0);
});

test('the idle reaper destroys a pending spooling stream and drains its resources', async () => {
  const pending = Promise.withResolvers<SegmentArtifact>();
  const source = new TestArtifactSource((call) =>
    abortableArtifact(pending.promise, call.signal)
  );
  const stream = new SpoolingSegmentsStream(streamOptions(source, 1));
  stream.on('error', () => undefined);
  stream.resume();
  await source.waitForCalls(1);

  const stats = new StatsAccumulator();
  const id = stats.streamOpened({
    nzbHash: 'idle-spooling',
    filename: 'idle.bin',
    size: 4,
    start: 0,
  });
  const live = new Map([[id, stream]]);
  assert.equal(reapIdleStreams(stats, live, 1, Date.now() + 1_000), 1);
  await closeEvent(stream);
  stats.streamClosed(id);
  assert.equal(source.calls[0].signal?.aborted, true);
  assert.equal(source.activeStreamLeases, 0);
});

test('a future segment failure is observed only after earlier output', async () => {
  const firstGate = Promise.withResolvers<SegmentArtifact>();
  const first = new BufferArtifact(Buffer.from('first'));
  const failure = new Error('future spool failed');
  const source = new TestArtifactSource((call) =>
    call.segment.messageId === 'segment-0'
      ? abortableArtifact(firstGate.promise, call.signal)
      : Promise.reject(failure)
  );
  const stream = new SpoolingSegmentsStream(
    streamOptions(source, 2, { maxPrefetchSegments: 2 })
  );
  const chunks: Buffer[] = [];
  const streamError = Promise.withResolvers<Error>();
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));
  stream.once('error', streamError.resolve);
  await source.waitForCalls(2);
  assert.equal(chunks.length, 0);
  firstGate.resolve(first);
  assert.equal(await streamError.promise, failure);
  await closeEvent(stream);
  assert.equal(Buffer.concat(chunks).toString(), 'first');
  assert.equal(first.releaseCalls, 1);
  assert.equal(source.activeStreamLeases, 0);
});

test('known and newly discovered holes use bounded zero artifacts', async () => {
  const source = new TestArtifactSource((call) =>
    Promise.reject(
      new ArticleNotFoundError('missing on every provider', {
        messageId: call.segment.messageId,
        allProviders: true,
      })
    )
  );
  const decisions: number[] = [];
  const stream = new SpoolingSegmentsStream(
    streamOptions(source, 2, {
      knownHoles: new Set([0]),
      sizeForSegment: () => 4,
      onHole: (idx) => {
        decisions.push(idx);
        return 'pad';
      },
    })
  );
  assert.deepEqual(await collect(stream), Buffer.alloc(8));
  assert.deepEqual(
    source.calls.map((call) => call.segment.messageId),
    ['segment-1']
  );
  assert.deepEqual(decisions, [0, 1]);
  assert.equal(source.activeStreamLeases, 0);
});

test('two simultaneous range streams own and release independent resources', async () => {
  const artifacts: BufferArtifact[] = [];
  const source = new TestArtifactSource(() => {
    const artifact = new BufferArtifact(Buffer.from('parallel'));
    artifacts.push(artifact);
    return Promise.resolve(artifact);
  });
  const first = collect(
    new SpoolingSegmentsStream(streamOptions(source, 1, { limitBytes: 5 }))
  );
  const second = collect(
    new SpoolingSegmentsStream(streamOptions(source, 1, { skipBytes: 3 }))
  );
  assert.deepEqual(
    (await Promise.all([first, second])).map((body) => body.toString()),
    ['paral', 'allel']
  );
  assert.equal(source.peakStreamLeases, 2);
  assert.equal(source.activeStreamLeases, 0);
  assert.equal(artifacts.length, 2);
  assert(artifacts.every((artifact) => artifact.releaseCalls === 1));
});

test('segment-buffering factory path preserves the legacy byte checksum', async () => {
  const bodies = [
    Buffer.from('alpha'),
    Buffer.from('beta'),
    Buffer.from('gamma'),
  ];
  const pool = new TestArtifactSource(() =>
    Promise.reject(new Error('buffering fixture used artifact path'))
  );
  const segments = bodies.map((body, index) => {
    const messageId = `buffered-${index}`;
    pool.bufferingBodies.set(messageId, body);
    return { messageId, bytes: body.length };
  });
  const stream = createSegmentReadStream(
    {
      pool,
      segments,
      nzbHash: 'buffering-checksum',
      maxPrefetchSegments: 3,
      bufferingBufferSizeBytes: 1024,
      priority: CommandPriority.High,
    },
    'segment_buffering'
  );
  const actual = await collect(stream);
  const expected = Buffer.concat(bodies);
  assert.equal(
    createHash('sha256').update(actual).digest('hex'),
    createHash('sha256').update(expected).digest('hex')
  );
});
