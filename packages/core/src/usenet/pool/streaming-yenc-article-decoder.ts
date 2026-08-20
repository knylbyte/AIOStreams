import { StreamingYencDecoder, YencDecodeError } from './yenc.js';
import type { SegmentSpoolingHotpathCounters } from './hotpath-counters.js';

const LF = 0x0a;
const CR = 0x0d;
const CONTROL_LINE_CAP_BYTES = 4096;
const PREAMBLE_CAP_BYTES = 64 * 1024;
const YBEGIN_PREFIX = Buffer.from('=ybegin ', 'latin1');
const YPART_PREFIX = Buffer.from('=ypart ', 'latin1');

interface ControlAttributes {
  readonly size?: number;
  readonly total?: number;
  readonly part?: number;
  readonly begin?: number;
  readonly end?: number;
  readonly name?: string;
}

type DecoderState =
  | 'seeking_begin'
  | 'after_begin'
  | 'data'
  | 'complete'
  | 'failed';

/** Metadata parsed without retaining the complete article or decoded body. */
export interface DecodedSegmentMetadata {
  readonly byteRange?: readonly [number, number];
  readonly fileSize?: number;
  readonly totalParts?: number;
  readonly name?: string;
  readonly size: number;
}

/**
 * Scalar yEnc header fields available before the first decoded payload byte.
 * `expectedSize` is present only when `=ypart` or a single-part `=ybegin`
 * supplies an exact decoded length; it is not a substitute for final decoder
 * validation.
 */
export interface DecodedSegmentHeaderMetadata {
  readonly byteRange?: readonly [number, number];
  readonly fileSize?: number;
  readonly totalParts?: number;
  readonly name?: string;
  readonly expectedSize?: number;
}

/**
 * Bounded ownership boundary for decoded bytes. Before retaining or copying a
 * chunk, an implementation must synchronously acquire its exact memory lease
 * (for example with `ByteBudget.tryAcquire`). `write(false)` still means the
 * chunk was accepted, but the queue is now full. The decoder transfers each
 * output Buffer exactly once and does not touch it after `write` returns.
 */
export interface BackpressuredByteSink {
  write(chunk: Buffer): boolean;
  onceDrain(listener: () => void): void;
  end(): Promise<void>;
  fail(error: Error): void;
}

/**
 * Direct native-decode destination used by segment spooling. `commitDecoded`
 * publishes only a written count; the sink already owns the backing. A false
 * result means the complete raw input was accepted but the NNTP connection
 * must wait before presenting another input window.
 */
export interface DirectDecodeByteSink extends BackpressuredByteSink {
  readonly directDecode: true;
  readonly maxDecodeInputBytes: number;
  acquireDecodeTarget(maxDecodedBytes: number): Buffer;
  commitDecoded(bytes: number, options: DirectDecodeCommitOptions): boolean;
}

/**
 * Synchronous terminal metadata for one caller-owned decode target. The yEnc
 * decoder is the only authority for `articleEnded`; sinks must not infer it
 * from byte counts, batch shape, or the later BODY `end()` callback.
 */
export interface DirectDecodeCommitOptions {
  readonly inputBoundary: boolean;
  readonly articleEnded: boolean;
}

/** Fixed-size lifecycle telemetry; it never receives article identifiers. */
export interface StreamingYencLifecycleObserver {
  onNntpStatus?(): void;
  onFirstDecodedPayload?(): void;
}

function isDirectDecodeSink(
  sink: BackpressuredByteSink
): sink is DirectDecodeByteSink {
  return 'directDecode' in sink && sink.directDecode === true;
}

/**
 * Incrementally parses one complete NNTP BODY and streams decoded yEnc bytes.
 *
 * Invariants:
 *
 * - raw input and decoded output are never accumulated into an article-sized
 *   Buffer;
 * - preamble/header state is bounded by fixed byte caps;
 * - each raw `push` performs at most one sink write, so false propagates
 *   synchronously without retaining the caller's raw Buffer;
 * - yEnc and NNTP dot-escape state may cross arbitrary input boundaries;
 * - `finish`, `end`, and `fail` are idempotent.
 */
export class StreamingYencArticleDecoder {
  private readonly decoder: StreamingYencDecoder;
  private readonly controlLine = Buffer.allocUnsafe(CONTROL_LINE_CAP_BYTES);
  private controlLineLength = 0;
  private preambleBytes = 0;
  private state: DecoderState = 'seeking_begin';
  private decodedBytes = 0;
  private failure: Error | undefined;
  private finishPromise: Promise<DecodedSegmentMetadata> | undefined;
  private finishDrain: PromiseWithResolvers<void> | undefined;

  private byteRangeValue: readonly [number, number] | undefined;
  private fileSizeValue: number | undefined;
  private totalPartsValue: number | undefined;
  private nameValue: string | undefined;
  private multipartDeclared = false;
  private headerNotified = false;
  private decodedPayloadNotified = false;
  private readonly directSink: DirectDecodeByteSink | undefined;

  constructor(
    private readonly sink: BackpressuredByteSink,
    private readonly onHeader?: (
      metadata: DecodedSegmentHeaderMetadata
    ) => void,
    private readonly hotpathCounters?: SegmentSpoolingHotpathCounters,
    private readonly lifecycleObserver?: StreamingYencLifecycleObserver
  ) {
    this.decoder = new StreamingYencDecoder(hotpathCounters);
    this.directSink = isDirectDecodeSink(sink) ? sink : undefined;
  }

  /** NNTP BODY-consumer hook invoked after a successful response status. */
  onStatus(): void {
    this.lifecycleObserver?.onNntpStatus?.();
  }

  /**
   * Consume a raw, still dot-stuffed BODY chunk synchronously. A false result
   * must pause the NNTP connection until {@link onceDrain} fires.
   */
  push(raw: Buffer): boolean {
    if (this.failure) throw this.failure;
    if (this.state === 'complete') return true;
    if (raw.length === 0) return true;
    try {
      if (this.state === 'data') return this.decodeData(raw);
      return this.consumeControlInput(raw);
    } catch (error) {
      const failure = this.asError(error);
      this.fail(failure);
      throw failure;
    }
  }

  /** BODY-consumer adapter; identical to {@link push}. */
  write(raw: Buffer): boolean {
    return this.push(raw);
  }

  /** Delegate the sink's one-shot drain notification to the NNTP connection. */
  onceDrain(listener: () => void): void {
    this.sink.onceDrain(listener);
  }

  /** Backpressured BODY lifecycle hook; metadata remains available via finish. */
  async end(): Promise<void> {
    await this.finish();
  }

  /**
   * Validate structural completion, flush the sink, and return parsed metadata.
   * Repeated calls share one Promise and never end the sink twice.
   */
  finish(): Promise<DecodedSegmentMetadata> {
    this.finishPromise ??= this.finishOnce();
    return this.finishPromise;
  }

  /** Fail this decoder and its sink exactly once. */
  fail(error: Error): void {
    if (this.failure || this.state === 'complete') return;
    this.failure = error;
    this.state = 'failed';
    this.finishDrain?.reject(error);
    this.finishDrain = undefined;
    try {
      this.sink.fail(error);
    } catch {
      // Cleanup callbacks must never replace the structural/transport failure
      // that caused the decoder to enter its terminal state.
    }
  }

  private consumeControlInput(raw: Buffer): boolean {
    let off = 0;
    while (off < raw.length && this.state !== 'data') {
      let lf = raw.indexOf(LF, off);
      if (lf < 0) lf = raw.length;
      const end = lf < raw.length ? lf + 1 : raw.length;
      this.appendControlBytes(raw, off, end);
      off = end;
      if (lf >= raw.length) return true;

      if (this.state === 'seeking_begin') {
        if (this.consumeSeekingLine()) {
          this.state = 'after_begin';
        }
        this.controlLineLength = 0;
        continue;
      }

      if (this.isPartHeader()) {
        this.parsePartHeader();
        this.controlLineLength = 0;
        this.state = 'data';
        this.notifyHeader();
        return off < raw.length ? this.decodeData(raw.subarray(off)) : true;
      }

      // The line immediately after =ybegin is data when =ypart is absent. A
      // direct sink consumes both bounded slices into the same decode window;
      // legacy sinks keep their compatibility copy.
      const pendingLength = this.controlLineLength;
      this.controlLineLength = 0;
      this.state = 'data';
      this.notifyHeader();
      if (this.directSink) {
        const first = this.decodeData(
          this.controlLine.subarray(0, pendingLength),
          off >= raw.length
        );
        if (!first) return false;
        return off < raw.length
          ? this.decodeData(raw.subarray(off), true)
          : true;
      }
      const transitionLength = pendingLength + raw.length - off;
      if (this.hotpathCounters) this.hotpathCounters.headerTransitionCopies++;
      const transition = Buffer.allocUnsafe(transitionLength);
      this.controlLine.copy(transition, 0, 0, pendingLength);
      raw.copy(transition, pendingLength, off);
      return this.decodeData(transition);
    }
    return this.state === 'data' && off < raw.length
      ? this.decodeData(raw.subarray(off))
      : true;
  }

  private appendControlBytes(raw: Buffer, start: number, end: number): void {
    const length = end - start;
    if (this.controlLineLength + length > this.controlLine.length) {
      throw new YencDecodeError(
        this.state === 'seeking_begin' ? 'no_start_found' : 'invalid_header',
        this.state === 'seeking_begin'
          ? 'yEnc decode failed: no_start_found'
          : 'yEnc decode failed: header line exceeds fixed limit',
        { terminal: true }
      );
    }
    raw.copy(this.controlLine, this.controlLineLength, start, end);
    this.controlLineLength += length;
  }

  private consumeSeekingLine(): boolean {
    const line = this.controlLineText();
    if (!line.startsWith('=ybegin ')) {
      this.preambleBytes += this.controlLineLength;
      if (this.preambleBytes > PREAMBLE_CAP_BYTES) {
        throw new YencDecodeError(
          'no_start_found',
          'yEnc decode failed: no_start_found',
          { terminal: true }
        );
      }
      return false;
    }
    const attrs = parseControlAttributes(line, YBEGIN_PREFIX.length);
    this.fileSizeValue = attrs.size;
    this.totalPartsValue = attrs.total;
    this.nameValue = attrs.name;
    this.multipartDeclared =
      attrs.part !== undefined ||
      (this.totalPartsValue !== undefined && this.totalPartsValue > 1);
    return true;
  }

  private isPartHeader(): boolean {
    return (
      this.controlLineLength >= YPART_PREFIX.length &&
      this.controlLine.compare(
        YPART_PREFIX,
        0,
        YPART_PREFIX.length,
        0,
        YPART_PREFIX.length
      ) === 0
    );
  }

  private parsePartHeader(): void {
    const attrs = parseControlAttributes(
      this.controlLineText(),
      YPART_PREFIX.length
    );
    const begin = attrs.begin;
    const end = attrs.end;
    if (begin !== undefined && end !== undefined) {
      this.byteRangeValue = [begin - 1, end];
    }
  }

  private notifyHeader(): void {
    if (this.headerNotified) return;
    this.headerNotified = true;
    const byteRange = this.byteRangeValue;
    const expectedSize = byteRange
      ? byteRange[1] - byteRange[0]
      : !this.multipartDeclared &&
          this.fileSizeValue !== undefined &&
          this.fileSizeValue > 0
        ? this.fileSizeValue
        : undefined;
    this.onHeader?.({
      byteRange,
      fileSize: this.fileSizeValue,
      totalParts: this.totalPartsValue,
      name: this.nameValue,
      expectedSize:
        expectedSize !== undefined && expectedSize > 0
          ? expectedSize
          : undefined,
    });
  }

  private controlLineText(): string {
    if (this.hotpathCounters) this.hotpathCounters.headerLinesParsed++;
    let end = this.controlLineLength;
    if (end > 0 && this.controlLine[end - 1] === LF) end--;
    if (end > 0 && this.controlLine[end - 1] === CR) end--;
    return this.controlLine.toString('latin1', 0, end);
  }

  private decodeData(raw: Buffer, inputBoundary = true): boolean {
    if (this.decoder.ended || raw.length === 0) return true;
    if (this.directSink) {
      if (raw.length > this.directSink.maxDecodeInputBytes) {
        throw new YencDecodeError(
          'invalid_chunk_size',
          'Streaming yEnc input exceeds the direct decode window',
          { terminal: true }
        );
      }
      const target = this.directSink.acquireDecodeTarget(raw.length);
      const written = this.decoder.pushInto(raw, target);
      this.decodedBytes += written;
      this.notifyDecodedPayload(written);
      return this.directSink.commitDecoded(written, {
        inputBoundary,
        articleEnded: this.decoder.ended,
      });
    }
    const decoded = this.decoder.push(raw);
    if (decoded.length === 0) return true;
    const acceptsMore = this.sink.write(decoded);
    this.decodedBytes += decoded.length;
    this.notifyDecodedPayload(decoded.length);
    return acceptsMore;
  }

  private notifyDecodedPayload(bytes: number): void {
    if (bytes <= 0 || this.decodedPayloadNotified) return;
    this.decodedPayloadNotified = true;
    this.lifecycleObserver?.onFirstDecodedPayload?.();
  }

  private async finishOnce(): Promise<DecodedSegmentMetadata> {
    try {
      if (this.failure) throw this.failure;
      if (this.state === 'seeking_begin') {
        throw new YencDecodeError(
          'no_start_found',
          'yEnc decode failed: no_start_found',
          { terminal: true }
        );
      }
      if (this.state === 'after_begin' && this.controlLineLength > 0) {
        const pending = Buffer.allocUnsafe(this.controlLineLength);
        this.controlLine.copy(pending, 0, 0, this.controlLineLength);
        this.controlLineLength = 0;
        this.state = 'data';
        if (!this.decodeData(pending)) await this.waitForFinishDrain();
      }
      if (!this.decoder.ended) {
        throw new YencDecodeError(
          'no_end_found',
          'yEnc decode failed: no_end_found',
          { terminal: true }
        );
      }
      await this.sink.end();
      if (this.failure) throw this.failure;
      this.state = 'complete';
      return {
        byteRange: this.byteRangeValue,
        fileSize: this.fileSizeValue,
        totalParts: this.totalPartsValue,
        name: this.nameValue,
        size: this.decodedBytes,
      };
    } catch (error) {
      const failure = this.asError(error);
      this.fail(failure);
      throw failure;
    }
  }

  private waitForFinishDrain(): Promise<void> {
    this.finishDrain ??= Promise.withResolvers<void>();
    const pending = this.finishDrain;
    this.sink.onceDrain(() => {
      if (this.finishDrain !== pending) return;
      this.finishDrain = undefined;
      pending.resolve();
    });
    return pending.promise;
  }

  private asError(error: unknown): Error {
    return error instanceof Error
      ? error
      : new YencDecodeError(undefined, 'yEnc streaming decoder failed', {
          cause: error,
        });
  }
}

function parseControlAttributes(
  line: string,
  attributesOffset: number
): ControlAttributes {
  let size: number | undefined;
  let total: number | undefined;
  let part: number | undefined;
  let begin: number | undefined;
  let end: number | undefined;
  let name: string | undefined;
  let cursor = attributesOffset;
  while (cursor < line.length) {
    while (line.charCodeAt(cursor) === 0x20) cursor++;
    if (cursor >= line.length) break;
    const equals = line.indexOf('=', cursor);
    if (equals < 0) break;
    const key = line.slice(cursor, equals);
    const valueStart = equals + 1;
    if (key === 'name') {
      name = line.slice(valueStart);
      break;
    }
    const space = line.indexOf(' ', valueStart);
    const valueEnd = space < 0 ? line.length : space;
    const parsed = Number.parseInt(line.slice(valueStart, valueEnd), 10);
    if (Number.isSafeInteger(parsed)) {
      switch (key) {
        case 'size':
          size = parsed;
          break;
        case 'total':
          total = parsed;
          break;
        case 'part':
          part = parsed;
          break;
        case 'begin':
          begin = parsed;
          break;
        case 'end':
          end = parsed;
          break;
      }
    }
    cursor = valueEnd + 1;
  }
  return { size, total, part, begin, end, name };
}
