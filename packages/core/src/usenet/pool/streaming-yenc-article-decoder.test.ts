import assert from 'node:assert/strict';
import test from 'node:test';
import yencode from 'yencode';
import {
  StreamingYencArticleDecoder,
  type BackpressuredByteSink,
  type DirectDecodeByteSink,
  type DecodedSegmentMetadata,
} from './streaming-yenc-article-decoder.js';
import { decodeArticle, YencDecodeError } from './yenc.js';
import { SegmentSpoolingHotpathCounters } from './hotpath-counters.js';

class CollectingSink implements BackpressuredByteSink {
  readonly chunks: Buffer[] = [];
  endCalls = 0;
  failure: Error | undefined;
  private drainListener: (() => void) | undefined;

  constructor(private readonly writeResults: boolean[] = []) {}

  write(chunk: Buffer): boolean {
    // Test double accepts ownership exactly as a real pre-leased sink would.
    this.chunks.push(chunk);
    return this.writeResults.shift() ?? true;
  }

  onceDrain(listener: () => void): void {
    assert.equal(this.drainListener, undefined);
    this.drainListener = listener;
  }

  emitDrain(): void {
    const listener = this.drainListener;
    assert(listener, 'expected one pending drain listener');
    this.drainListener = undefined;
    listener();
  }

  async end(): Promise<void> {
    this.endCalls++;
  }

  fail(error: Error): void {
    this.failure ??= error;
    this.drainListener = undefined;
  }

  decoded(): Buffer {
    return Buffer.concat(this.chunks);
  }
}

class DirectCollectingSink implements DirectDecodeByteSink {
  readonly directDecode = true as const;
  readonly maxDecodeInputBytes = 4096;
  readonly backing = Buffer.allocUnsafeSlow(64 * 1024);
  readonly targetBackings = new Set<ArrayBufferLike>();
  cursor = 0;
  endCalls = 0;
  failure: Error | undefined;

  write(): boolean {
    throw new Error('legacy write must not be used by a direct sink');
  }

  acquireDecodeTarget(maxDecodedBytes: number): Buffer {
    const target = this.backing.subarray(
      this.cursor,
      this.cursor + maxDecodedBytes
    );
    this.targetBackings.add(target.buffer);
    return target;
  }

  commitDecoded(bytes: number): boolean {
    this.cursor += bytes;
    return true;
  }

  onceDrain(): void {
    throw new Error('direct collecting sink never backpressures');
  }

  end(): Promise<void> {
    this.endCalls++;
    return Promise.resolve();
  }

  fail(error: Error): void {
    this.failure ??= error;
  }

  decoded(): Buffer {
    return Buffer.from(this.backing.subarray(0, this.cursor));
  }
}

function multipartArticle(data: Buffer): Buffer {
  const single = yencode.post('ignored.bin', data, 7);
  const firstLineEnd = single.indexOf('\r\n');
  assert(firstLineEnd >= 0);
  const headers = Buffer.from(
    [
      'transport preamble',
      '=ybegin part=2 total=3 line=7 size=1234 name=name with spaces.bin',
      `=ypart begin=101 end=${100 + data.length}`,
      '',
    ].join('\r\n'),
    'latin1'
  );
  return Buffer.concat([headers, single.subarray(firstLineEnd + 2)]);
}

function expectedMetadata(raw: Buffer): DecodedSegmentMetadata {
  const decoded = decodeArticle(raw);
  return {
    byteRange: decoded.byteRange,
    fileSize: decoded.fileSize,
    totalParts: decoded.totalParts,
    name: decoded.name,
    size: decoded.size,
  };
}

async function decodeChunks(
  raw: Buffer,
  chunks: readonly Buffer[]
): Promise<{
  readonly body: Buffer;
  readonly metadata: DecodedSegmentMetadata;
  readonly sink: CollectingSink;
}> {
  const sink = new CollectingSink();
  const decoder = new StreamingYencArticleDecoder(sink);
  for (const chunk of chunks) assert.equal(decoder.push(chunk), true);
  const metadata = await decoder.finish();
  return { body: sink.decoded(), metadata, sink };
}

test('matches decodeArticle at every possible article split', async () => {
  const data = Buffer.from([
    4, 19, 214, 224, 227, 0, 1, 2, 3, 4, 5, 42, 61, 127, 128, 200, 255,
  ]);
  const raw = multipartArticle(data);
  const expected = decodeArticle(raw);
  const metadata = expectedMetadata(raw);

  for (let split = 0; split <= raw.length; split++) {
    const result = await decodeChunks(raw, [
      raw.subarray(0, split),
      raw.subarray(split),
    ]);
    assert.deepEqual(result.body, expected.body, `body split ${split}`);
    assert.deepEqual(result.metadata, metadata, `metadata split ${split}`);
    assert.equal(result.sink.endCalls, 1, `sink end split ${split}`);
    assert.equal(result.sink.failure, undefined, `sink failure split ${split}`);
  }
});

test('publishes exact bounded yEnc header metadata before the first sink write', async () => {
  const body = Buffer.from('header-before-payload');
  const raw = multipartArticle(body);
  const sink = new CollectingSink();
  let headerCalls = 0;
  const decoder = new StreamingYencArticleDecoder(sink, (metadata) => {
    headerCalls++;
    assert.equal(sink.chunks.length, 0);
    assert.deepEqual(metadata.byteRange, [100, 100 + body.length]);
    assert.equal(metadata.fileSize, 1234);
    assert.equal(metadata.totalParts, 3);
    assert.equal(metadata.name, 'name with spaces.bin');
    assert.equal(metadata.expectedSize, body.length);
  });

  assert.equal(decoder.push(raw), true);
  const metadata = await decoder.finish();
  assert.equal(headerCalls, 1);
  assert.equal(metadata.size, body.length);
  assert.deepEqual(sink.decoded(), body);
});

test('reports NNTP status and the first decoded payload exactly once', async () => {
  const raw = yencode.post(
    'lifecycle.bin',
    Buffer.from('decoder lifecycle payload'),
    7
  );
  const sink = new CollectingSink();
  const phases: string[] = [];
  const decoder = new StreamingYencArticleDecoder(sink, undefined, undefined, {
    onNntpStatus: () => phases.push('status'),
    onFirstDecodedPayload: () => phases.push('decoded'),
  });

  decoder.onStatus();
  for (let offset = 0; offset < raw.length; offset += 3) {
    assert.equal(decoder.push(raw.subarray(offset, offset + 3)), true);
  }
  await decoder.finish();

  assert.deepEqual(phases, ['status', 'decoded']);
});

test('preserves escape and NNTP dot-unstuffing state across byte boundaries', async () => {
  const raw = Buffer.from(
    [
      '=ybegin line=128 size=5 name=dot-stuffed.bin',
      // A doubled leading dot is NNTP dot-stuffing. The split-by-byte decode
      // also cuts every possible yEnc escape and CRLF boundary.
      '..',
      '=}',
      '=J',
      '=M',
      '+',
      '=yend size=5',
    ].join('\r\n'),
    'latin1'
  );
  const expected = decodeArticle(raw);
  const chunks = Array.from(raw, (_value, index) =>
    raw.subarray(index, index + 1)
  );
  const result = await decodeChunks(raw, chunks);

  assert.deepEqual(result.body, expected.body);
  assert.deepEqual(result.metadata, expectedMetadata(raw));
  assert.equal(result.metadata.size, 5);
});

test('propagates sink backpressure and delegates one drain notification', async () => {
  const raw = yencode.post('backpressure.bin', Buffer.from('payload'), 128);
  const sink = new CollectingSink([false]);
  const decoder = new StreamingYencArticleDecoder(sink);

  assert.equal(decoder.write(raw), false);
  let drained = 0;
  decoder.onceDrain(() => drained++);
  assert.equal(drained, 0);
  sink.emitDrain();
  assert.equal(drained, 1);

  const metadata = await decoder.finish();
  assert.deepEqual(sink.decoded(), Buffer.from('payload'));
  assert.equal(metadata.size, 7);
});

test('decodes many native calls into one caller-owned output backing', async () => {
  const body = Buffer.alloc(32 * 1024, 0x31);
  const raw = yencode.post('direct-output.bin', body, 128);
  const sink = new DirectCollectingSink();
  const counters = new SegmentSpoolingHotpathCounters();
  const decoder = new StreamingYencArticleDecoder(sink, undefined, counters);

  for (let offset = 0; offset < raw.length; offset += 97) {
    assert.equal(decoder.write(raw.subarray(offset, offset + 97)), true);
  }
  await decoder.finish();

  assert.deepEqual(sink.decoded(), body);
  assert.equal(sink.targetBackings.size, 1);
  assert(counters.yencDecodeCalls > 100);
  assert.equal(counters.yencOutputBackingReuses, counters.yencDecodeCalls);
  assert.equal(counters.yencOutputBackingAllocations, 0);
});

test('direct header transition avoids the legacy combined transition buffer', async () => {
  const body = Buffer.from('header transition data');
  const raw = yencode.post('transition.bin', body, 128);
  const directCounters = new SegmentSpoolingHotpathCounters();
  const directSink = new DirectCollectingSink();
  const direct = new StreamingYencArticleDecoder(
    directSink,
    undefined,
    directCounters
  );
  assert.equal(direct.write(raw), true);
  await direct.finish();
  assert.deepEqual(directSink.decoded(), body);
  assert.equal(directCounters.headerTransitionCopies, 0);

  const legacyCounters = new SegmentSpoolingHotpathCounters();
  const legacySink = new CollectingSink();
  const legacy = new StreamingYencArticleDecoder(
    legacySink,
    undefined,
    legacyCounters
  );
  assert.equal(legacy.write(raw), true);
  await legacy.finish();
  assert.deepEqual(legacySink.decoded(), body);
  assert.equal(legacyCounters.headerTransitionCopies, 1);
});

test('classifies missing ybegin and missing yend and fails the sink', async () => {
  const noStartSink = new CollectingSink();
  const noStart = new StreamingYencArticleDecoder(noStartSink);
  assert.equal(noStart.write(Buffer.from('ordinary article text')), true);
  await assert.rejects(noStart.finish(), (error: unknown) => {
    assert(error instanceof YencDecodeError);
    assert.equal(error.code, 'no_start_found');
    assert.equal(error.terminal, true);
    return true;
  });
  assert.equal(noStartSink.failure, await rejectedError(noStart.finish()));

  const noEndSink = new CollectingSink();
  const noEnd = new StreamingYencArticleDecoder(noEndSink);
  noEnd.write(Buffer.from('=ybegin line=128 size=1 name=x\r\n+', 'latin1'));
  await assert.rejects(noEnd.finish(), (error: unknown) => {
    assert(error instanceof YencDecodeError);
    assert.equal(error.code, 'no_end_found');
    assert.equal(error.terminal, true);
    return true;
  });
  assert.equal(noEndSink.failure, await rejectedError(noEnd.finish()));
});

test('does not let a throwing sink cleanup replace the decoder failure', async () => {
  const structural = new YencDecodeError('no_end_found', 'structural failure', {
    terminal: true,
  });
  const sink: BackpressuredByteSink = {
    write: () => true,
    onceDrain: () => undefined,
    end: async () => undefined,
    fail: () => {
      throw new Error('cleanup failure');
    },
  };
  const decoder = new StreamingYencArticleDecoder(sink);

  assert.doesNotThrow(() => decoder.fail(structural));
  assert.throws(
    () => decoder.write(Buffer.from('x')),
    (error: unknown) => {
      assert.equal(error, structural);
      return true;
    }
  );
  await assert.rejects(
    decoder.finish(),
    (error: unknown) => error === structural
  );
});

async function rejectedError(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise;
    assert.fail('expected rejection');
  } catch (error) {
    return error;
  }
}
