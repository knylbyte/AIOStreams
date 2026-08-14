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
import type { ByteLease } from './byte-budget.js';
import { FileStream, type FileStreamPool } from './file-stream.js';
import type { SharedSegment } from './segment-arena.js';
import type {
  SegmentArtifact,
  SegmentArtifactReadOptions,
} from './segment-artifact.js';
import { createSegmentReadStream } from './segment-read-stream-factory.js';
import { SegmentsStream } from './segments-stream.js';
import {
  SpoolingSegmentsStream,
  type SpoolingSegmentArtifactSource,
} from './spooling-segments-stream.js';
import { reapIdleStreams } from './tracked-stream.js';

interface FetchCall {
  readonly segment: NzbSegmentRef;
  readonly signal: AbortSignal | undefined;
  readonly expectedLength: number | undefined;
}

type FetchHandler = (call: FetchCall) => Promise<SegmentArtifact>;

class TestArtifactSource implements FileStreamPool {
  readonly calls: FetchCall[] = [];
  readonly bufferingBodies = new Map<string, Buffer>();
  readonly sharedBodies = new Map<string, Buffer>();
  bufferingCalls = 0;
  sharedCalls = 0;
  sharedReleases = 0;
  activeStreamLeases = 0;
  peakStreamLeases = 0;

  private readonly callWaiters = new Set<{
    readonly count: number;
    readonly resolve: () => void;
  }>();

  constructor(private readonly handler: FetchHandler) {}

  fetchSegmentArtifact(
    segment: NzbSegmentRef,
    _nzbHash: string,
    signal: AbortSignal | undefined,
    _priority: CommandPriority,
    options?: { readonly expectedLength?: number }
  ): Promise<SegmentArtifact> {
    const call = {
      segment,
      signal,
      expectedLength: options?.expectedLength,
    };
    this.calls.push(call);
    for (const waiter of [...this.callWaiters]) {
      if (this.calls.length < waiter.count) continue;
      this.callWaiters.delete(waiter);
      waiter.resolve();
    }
    return this.handler(call);
  }

  acquireSegmentStreamMemory(): Promise<ByteLease> {
    this.activeStreamLeases++;
    this.peakStreamLeases = Math.max(
      this.peakStreamLeases,
      this.activeStreamLeases
    );
    let released = false;
    return Promise.resolve({
      bytes: 64,
      release: () => {
        if (released) return;
        released = true;
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

  waitForCalls(count: number): Promise<void> {
    if (this.calls.length >= count) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.callWaiters.add({ count, resolve });
    });
  }
}

class BufferArtifact implements SegmentArtifact {
  readonly metadata;
  readonly length: number;
  readonly storage: 'spool' = 'spool';
  releaseCalls = 0;

  private reader: Readable | undefined;
  private released = false;

  constructor(private readonly body: Buffer) {
    this.length = body.length;
    this.metadata = { size: body.length };
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
