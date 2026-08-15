import { addAbortSignal, Readable } from 'node:stream';
import { createLogger } from '../../logging/logger.js';
import type { DecodedSegmentMetadata } from './streaming-yenc-article-decoder.js';
import type { SharedSegment } from './segment-arena.js';
import type { GrowingSpoolArtifact } from '../spool/growing-artifact.js';
import { UsenetSpoolError } from '../spool/errors.js';

const ARTIFACT_CHUNK_BYTES = 64 * 1024;
const MEBIBYTE_BYTES = 1024 * 1024;
/** Matches the largest planned Block-1 spool reader HWM and bounds owned data. */
const MAX_ARTIFACT_READER_HIGH_WATER_MARK_BYTES = 2 * MEBIBYTE_BYTES;
const logger = createLogger('usenet/segment-artifact');

/** Bounded range options shared by every decoded-segment storage variant. */
export interface SegmentArtifactReadOptions {
  readonly start?: number;
  readonly endExclusive?: number;
  readonly signal?: AbortSignal;
  readonly highWaterMark?: number;
}

/** Where the decoded bytes backing a {@link SegmentArtifact} currently live. */
export type SegmentArtifactStorage = 'arena' | 'disk-cache' | 'spool' | 'zero';

/** Scalar-only metadata used by the buffer-free direct spooling locator. */
export interface SegmentRangeMetadata {
  readonly byteRange?: readonly [number, number];
  readonly fileSize?: number;
  readonly totalParts?: number;
  readonly name?: string;
  readonly decodedSize?: number;
}

/** Strictness required by a file-offset locator using scalar yEnc metadata. */
export interface SegmentRangeMetadataFetchOptions {
  /** Multipart files require an exact `=ypart` range for offset arithmetic. */
  readonly requireByteRange?: boolean;
}

/** Delivery policy for one independently validated artifact waiter. */
export interface SegmentArtifactFetchOptions {
  /** Exact file-grid length asserted independently after producer completion. */
  readonly expectedLength?: number;
  /**
   * Permit resolution after the first committed byte. Successful reader EOF
   * still waits for BODY, decoder, sink and final-length validation.
   */
  readonly allowGrowing?: boolean;
}

/**
 * One independently owned reference to decoded segment bytes.
 *
 * Each handle creates at most one reader. Reader close releases the handle;
 * callers that never open a reader must call {@link release}. Release is
 * idempotent and resolves only after storage owned by the final reference has
 * been cleaned up.
 */
export interface SegmentArtifact {
  readonly metadata: DecodedSegmentMetadata;
  readonly length: number;
  readonly storage: SegmentArtifactStorage;
  createReadStream(options?: SegmentArtifactReadOptions): Readable;
  release(): Promise<void>;
}

/**
 * Block-8 extension point for file-backed persistent L2 hits. Implementations
 * must return an independently releasable file lease and must not materialize
 * the decoded body in a Buffer.
 */
export interface SegmentArtifactCacheLookup {
  acquire(
    messageId: string,
    signal?: AbortSignal
  ): Promise<SegmentArtifact | undefined>;
}

interface ValidatedReadRange {
  readonly start: number;
  readonly endExclusive: number;
  readonly highWaterMark: number;
}

function validateReadRange(
  length: number,
  options: SegmentArtifactReadOptions
): ValidatedReadRange {
  const start = options.start ?? 0;
  const endExclusive = options.endExclusive ?? length;
  const highWaterMark = options.highWaterMark ?? ARTIFACT_CHUNK_BYTES;
  if (
    !Number.isSafeInteger(start) ||
    !Number.isSafeInteger(endExclusive) ||
    start < 0 ||
    endExclusive < start ||
    endExclusive > length
  ) {
    throw new RangeError('Segment artifact range is outside decoded bytes');
  }
  if (
    !Number.isSafeInteger(highWaterMark) ||
    highWaterMark <= 0 ||
    highWaterMark > MAX_ARTIFACT_READER_HIGH_WATER_MARK_BYTES
  ) {
    throw new RangeError(
      'Segment artifact highWaterMark must be a safe positive integer no larger than 2 MiB'
    );
  }
  return { start, endExclusive, highWaterMark };
}

class BufferRangeReadable extends Readable {
  private position: number;

  constructor(
    private readonly body: Buffer,
    start: number,
    private readonly endExclusive: number,
    highWaterMark: number
  ) {
    super({ highWaterMark });
    this.position = start;
  }

  override _read(size: number): void {
    if (this.position >= this.endExclusive) {
      this.push(null);
      return;
    }
    const bytes = Math.min(
      Math.max(1, size),
      ARTIFACT_CHUNK_BYTES,
      this.endExclusive - this.position
    );
    const chunk = Buffer.allocUnsafe(bytes);
    this.body.copy(chunk, 0, this.position, this.position + bytes);
    this.position += bytes;
    this.push(chunk);
  }
}

function metadataFromShared(shared: SharedSegment): DecodedSegmentMetadata {
  return {
    byteRange: shared.data.byteRange,
    fileSize: shared.data.fileSize,
    totalParts: shared.data.totalParts,
    name: shared.data.name,
    size: shared.data.size,
  };
}

/**
 * A single-reader artifact that keeps one SegmentArena pin alive while copying
 * each emitted chunk into independent owned memory. No view into the recyclable
 * arena slot escapes `_read`; chunks are at most 64 KiB and the internal
 * Readable high-water mark is capped at 2 MiB.
 */
export class ArenaSegmentArtifact implements SegmentArtifact {
  readonly metadata: DecodedSegmentMetadata;
  readonly length: number;
  readonly storage = 'arena' as const;

  private reader: Readable | undefined;
  private released = false;
  private pinReleased = false;
  private releasePromise: Promise<void> | undefined;
  private releaseDone: (() => void) | undefined;

  constructor(private readonly shared: SharedSegment) {
    this.metadata = metadataFromShared(shared);
    this.length = shared.data.body.length;
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    if (this.released) {
      throw new Error('Arena segment artifact has been released');
    }
    if (this.reader) {
      throw new Error('Segment artifact handles support one reader');
    }
    const range = validateReadRange(this.length, options);
    const reader = new BufferRangeReadable(
      this.shared.data.body,
      range.start,
      range.endExclusive,
      range.highWaterMark
    );
    this.reader = reader;
    reader.once('close', () => this.finishRelease());
    if (options.signal) addAbortSignal(options.signal, reader);
    return reader;
  }

  release(): Promise<void> {
    if (this.releasePromise) return this.releasePromise;
    const done = Promise.withResolvers<void>();
    this.releasePromise = done.promise;
    this.releaseDone = done.resolve;
    this.released = true;
    if (this.reader && !this.reader.destroyed) this.reader.destroy();
    else this.finishRelease();
    return this.releasePromise;
  }

  private finishRelease(): void {
    if (!this.released) this.released = true;
    if (!this.pinReleased) {
      this.pinReleased = true;
      this.shared.release();
    }
    this.releaseDone?.();
    this.releaseDone = undefined;
    this.releasePromise ??= Promise.resolve();
  }
}

/**
 * SegmentArtifact adapter over one growing or complete transient spool file.
 * `releaseReference` belongs to the single-flight owner and disposes the file
 * only after its final independently delivered handle is released.
 */
export class GrowingSpoolArtifactAdapter implements SegmentArtifact {
  readonly storage = 'spool' as const;

  private reader: Readable | undefined;
  private releasePromise: Promise<void> | undefined;
  private released = false;

  constructor(
    private readonly artifact: GrowingSpoolArtifact,
    private readonly initialMetadata: DecodedSegmentMetadata,
    private readonly releaseReference: () => Promise<void>,
    private readonly producerCompletion?: Promise<DecodedSegmentMetadata>,
    private readonly currentMetadata?: () => DecodedSegmentMetadata
  ) {}

  get metadata(): DecodedSegmentMetadata {
    return this.currentMetadata?.() ?? this.initialMetadata;
  }

  get length(): number {
    return this.metadata.size;
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    if (this.released) {
      throw new Error('Spool segment artifact has been released');
    }
    if (this.reader) {
      throw new Error('Segment artifact handles support one reader');
    }
    const lengthAtOpen = this.length;
    validateReadRange(lengthAtOpen, options);
    const completion = this.producerCompletion?.then((metadata) => {
      if (metadata.size !== lengthAtOpen) {
        throw new UsenetSpoolError(
          'USENET_SPOOL_IO',
          'Decoded segment length differs from the exact file range'
        );
      }
    });
    void completion?.catch(() => undefined);
    const reader = this.artifact.createReadStream({
      ...options,
      completion,
    });
    this.reader = reader;
    reader.once('close', () => {
      void this.release().catch((error: unknown) => {
        logger.warn({ err: error }, 'failed to release spool artifact reader');
      });
    });
    return reader;
  }

  release(): Promise<void> {
    if (this.releasePromise) return this.releasePromise;
    this.released = true;
    if (this.reader && !this.reader.destroyed) this.reader.destroy();
    this.releasePromise = this.releaseReference();
    return this.releasePromise;
  }
}

class ZeroReadable extends Readable {
  private remaining: number;

  constructor(length: number, highWaterMark: number) {
    super({ highWaterMark });
    this.remaining = length;
  }

  override _read(size: number): void {
    if (this.remaining === 0) {
      this.push(null);
      return;
    }
    const bytes = Math.min(
      this.remaining,
      ARTIFACT_CHUNK_BYTES,
      Math.max(1, size)
    );
    this.remaining -= bytes;
    this.push(Buffer.alloc(bytes));
  }
}

/**
 * A sparse logical segment that emits independently owned zero chunks of at
 * most 64 KiB. Its Readable queue is bounded by the 2 MiB HWM cap and no
 * allocation scales with the logical hole length.
 */
export class ZeroSegmentArtifact implements SegmentArtifact {
  readonly metadata: DecodedSegmentMetadata;
  readonly storage = 'zero' as const;

  private reader: Readable | undefined;
  private released = false;

  constructor(readonly length: number) {
    if (!Number.isSafeInteger(length) || length < 0) {
      throw new RangeError(
        'Zero segment artifact length must be a safe non-negative integer'
      );
    }
    this.metadata = { size: length };
  }

  createReadStream(options: SegmentArtifactReadOptions = {}): Readable {
    if (this.released) {
      throw new Error('Zero segment artifact has been released');
    }
    if (this.reader) {
      throw new Error('Segment artifact handles support one reader');
    }
    const range = validateReadRange(this.length, options);
    const reader = new ZeroReadable(
      range.endExclusive - range.start,
      range.highWaterMark
    );
    this.reader = reader;
    reader.once('close', () => {
      this.released = true;
    });
    if (options.signal) addAbortSignal(options.signal, reader);
    return reader;
  }

  release(): Promise<void> {
    if (this.released) return Promise.resolve();
    this.released = true;
    if (this.reader && !this.reader.destroyed) this.reader.destroy();
    return Promise.resolve();
  }
}
