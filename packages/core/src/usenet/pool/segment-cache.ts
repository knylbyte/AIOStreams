import { createReadStream, createWriteStream, promises as fs } from 'node:fs';
import type { FileHandle } from 'node:fs/promises';
import { pipeline } from 'node:stream/promises';
import { createLogger } from '../../logging/logger.js';
import type { SegmentData } from '../types.js';
import { DiskBackedCache } from '../../utils/disk-backed-cache.js';
import { SegmentArena } from './segment-arena.js';
import {
  DiskSegmentArtifact,
  type SegmentArtifact,
  type SegmentArtifactCacheLookup,
} from './segment-artifact.js';
import type { DecodedSegmentMetadata } from './streaming-yenc-article-decoder.js';

const logger = createLogger('usenet/segment-cache');
const MAX_SEGMENT_METADATA_BYTES = 64 * 1024;
const PROMOTION_COPY_CHUNK_BYTES = 64 * 1024;
const DEFAULT_MAX_PROMOTIONS = 4;

/** Point-in-time cache stats for the dashboard. */
export interface CacheStats {
  hits: number;
  misses: number;
  /** hits / (hits + misses); 0 when never queried. */
  hitRate: number;
  /** On-disk cache bytes. */
  diskBytes: number;
  /** On-disk cache entry count. */
  diskCount: number;
  /** Subset of hits served from the disk cache. */
  diskHits: number;
  /** Allocated in-RAM arena bytes (pinned decoded bodies, serve-path tier). */
  arenaBytes?: number;
  /** Resident arena entries. */
  arenaEntries?: number;
  /** Arena entries currently pinned by in-flight reads (hovers near 0). */
  arenaPinned?: number;
  arenaEvictions?: number;
}

export interface SegmentCacheOptions {
  /**
   * In-RAM byte budget for the pinned segment arena (see {@link SegmentArena}).
   * `0` disables it.
   */
  arenaBytes?: number;
  /**
   * In-RAM byte budget for owned bodies in the generic cache's mem tier.
   * Superseded by the arena; kept for rollback.
   */
  memBytes?: number;
  /** On-disk byte budget. `0` (default) disables the cache. */
  diskBytes?: number;
  /** Base directory for the disk cache. */
  diskPath?: string;
  /** Subdirectory namespace (e.g. per provider-set) under {@link diskPath}. */
  namespace?: string;
  /** Hard concurrent promotion cap; excess best-effort work is skipped. */
  maxPromotions?: number;
}

/** JSON metadata buffer (shared by serialize / size / serialize-into). */
function metaBufOf(s: SegmentData): Buffer {
  return Buffer.from(
    JSON.stringify({
      byteRange: s.byteRange,
      fileSize: s.fileSize,
      totalParts: s.totalParts,
      name: s.name,
      size: s.size,
    }),
    'utf8'
  );
}

/** Length-prefixed metadata header + raw body. */
function serializeSegment(s: SegmentData): Buffer {
  const meta = metaBufOf(s);
  const header = Buffer.allocUnsafe(4);
  header.writeUInt32LE(meta.length, 0);
  return Buffer.concat([header, meta, s.body]);
}

/** Exact serialized byte length (drives the pooled write-buffer slot size). */
function serializedSegmentSize(s: SegmentData): number {
  return 4 + metaBufOf(s).length + s.body.length;
}

/**
 * Zero-alloc serializer: write `[u32 metaLen][meta][body]` straight into `dst`
 * (the cache's pooled write slot) instead of allocating via `Buffer.concat`. Runs
 * synchronously at `set()` time, capturing the (pooled ring-slot or leased
 * arena-slot) body before it can be reused. Returns the number of bytes written.
 */
function serializeSegmentInto(s: SegmentData, dst: Buffer): number {
  const meta = metaBufOf(s);
  dst.writeUInt32LE(meta.length, 0);
  meta.copy(dst, 4);
  s.body.copy(dst, 4 + meta.length);
  return 4 + meta.length + s.body.length;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function optionalSafeInteger(
  value: unknown,
  field: string,
  minimum: number
): number | undefined {
  if (value === undefined) return undefined;
  if (
    typeof value !== 'number' ||
    !Number.isSafeInteger(value) ||
    value < minimum
  ) {
    throw new Error(`Segment cache metadata ${field} is invalid`);
  }
  return value;
}

function parseByteRange(value: unknown): readonly [number, number] | undefined {
  if (value === undefined) return undefined;
  const begin: unknown = Array.isArray(value) ? value[0] : undefined;
  const end: unknown = Array.isArray(value) ? value[1] : undefined;
  if (
    !Array.isArray(value) ||
    value.length !== 2 ||
    typeof begin !== 'number' ||
    typeof end !== 'number' ||
    !Number.isSafeInteger(begin) ||
    !Number.isSafeInteger(end) ||
    begin < 0 ||
    end <= begin
  ) {
    throw new Error('Segment cache metadata byteRange is invalid');
  }
  return [begin, end];
}

function parseSegmentMetadata(
  encoded: Buffer,
  bodyLength: number
): DecodedSegmentMetadata {
  const value: unknown = JSON.parse(encoded.toString('utf8'));
  if (!isRecord(value)) {
    throw new Error('Segment cache metadata must be an object');
  }
  const byteRange = parseByteRange(value.byteRange);
  const fileSize = optionalSafeInteger(value.fileSize, 'fileSize', 0);
  const totalParts = optionalSafeInteger(value.totalParts, 'totalParts', 1);
  const declaredSize = optionalSafeInteger(value.size, 'size', 0);
  const name = value.name;
  if (name !== undefined && typeof name !== 'string') {
    throw new Error('Segment cache metadata name is invalid');
  }
  if (declaredSize !== undefined && declaredSize !== bodyLength) {
    throw new Error('Segment cache metadata size differs from its body');
  }
  if (byteRange && byteRange[1] - byteRange[0] !== bodyLength) {
    throw new Error('Segment cache metadata byteRange differs from its body');
  }
  if (byteRange && fileSize !== undefined && byteRange[1] > fileSize) {
    throw new Error('Segment cache metadata byteRange exceeds fileSize');
  }
  return {
    byteRange,
    fileSize,
    totalParts,
    name,
    size: bodyLength,
  };
}

function deserializeSegment(buf: Buffer): SegmentData {
  if (buf.length < 4) throw new Error('Segment cache entry is truncated');
  const metaLen = buf.readUInt32LE(0);
  if (
    metaLen <= 0 ||
    metaLen > MAX_SEGMENT_METADATA_BYTES ||
    4 + metaLen > buf.length
  ) {
    throw new Error('Segment cache metadata header is invalid');
  }
  const body = buf.subarray(4 + metaLen);
  const metadata = parseSegmentMetadata(
    buf.subarray(4, 4 + metaLen),
    body.length
  );
  return {
    body,
    byteRange: metadata.byteRange
      ? [metadata.byteRange[0], metadata.byteRange[1]]
      : undefined,
    fileSize: metadata.fileSize,
    totalParts: metadata.totalParts,
    name: metadata.name,
    size: metadata.size,
  };
}

async function readExactly(
  handle: FileHandle,
  buffer: Buffer,
  position: number,
  signal?: AbortSignal
): Promise<void> {
  let offset = 0;
  while (offset < buffer.length) {
    signal?.throwIfAborted();
    const result = await handle.read(
      buffer,
      offset,
      buffer.length - offset,
      position + offset
    );
    if (result.bytesRead === 0) {
      throw new Error('Segment cache entry is truncated');
    }
    offset += result.bytesRead;
  }
}

async function readDiskSegmentMetadata(
  filePath: string,
  serializedBytes: number,
  signal?: AbortSignal
): Promise<{ metadata: DecodedSegmentMetadata; bodyOffset: number }> {
  const handle = await fs.open(filePath, 'r');
  try {
    const prefix = Buffer.allocUnsafe(4);
    await readExactly(handle, prefix, 0, signal);
    const metaLen = prefix.readUInt32LE(0);
    if (
      metaLen <= 0 ||
      metaLen > MAX_SEGMENT_METADATA_BYTES ||
      4 + metaLen > serializedBytes
    ) {
      throw new Error('Segment cache metadata header is invalid');
    }
    const encoded = Buffer.allocUnsafe(metaLen);
    await readExactly(handle, encoded, 4, signal);
    const bodyOffset = 4 + metaLen;
    return {
      metadata: parseSegmentMetadata(encoded, serializedBytes - bodyOffset),
      bodyOffset,
    };
  } finally {
    await handle.close();
  }
}

/**
 * Cache for decoded segment payloads: a pinned in-RAM {@link SegmentArena}
 * (populated by the pool's shared fetch coordinator, not by {@link set}) in
 * front of an on-disk tier that survives restarts. Keyed by message-id.
 * {@link getAsync} retains the buffering-mode full deserialize path;
 * {@link acquire} exposes the same serialized entry as a file-backed artifact.
 */
export class SegmentCache implements SegmentArtifactCacheLookup {
  private readonly cache: DiskBackedCache<SegmentData>;
  private readonly maxPromotions: number;
  private readonly promotions = new Set<Promise<boolean>>();
  private closed = false;
  /** Pinned decoded-body tier; owned here, driven by MultiProviderPool. */
  readonly arena: SegmentArena;

  constructor(opts: SegmentCacheOptions) {
    const maxPromotions = opts.maxPromotions ?? DEFAULT_MAX_PROMOTIONS;
    if (!Number.isSafeInteger(maxPromotions) || maxPromotions <= 0) {
      throw new RangeError(
        'Segment cache maxPromotions must be a safe positive integer'
      );
    }
    this.maxPromotions = maxPromotions;
    this.arena = new SegmentArena({ budgetBytes: opts.arenaBytes ?? 0 });
    this.cache = new DiskBackedCache<SegmentData>({
      name: opts.namespace ?? 'segments',
      dir: opts.diskPath ?? '',
      maxMemBytes: opts.memBytes ?? 0,
      maxDiskBytes: opts.diskBytes ?? 0,
      serialize: serializeSegment,
      serializeInto: serializeSegmentInto,
      serializedSize: serializedSegmentSize,
      deserialize: deserializeSegment,
      sizeOf: (s) => s.body.length,
    });
  }

  get promotionEnabled(): boolean {
    return !this.closed && this.cache.maxDiskBytes > 0;
  }

  /** Synchronous lookup for the hot path (in-process; no network or disk read). */
  get(messageId: string): SegmentData | undefined {
    return this.cache.get(messageId);
  }

  /** Disk lookup, consulted before a network fetch. */
  getAsync(messageId: string): Promise<SegmentData | undefined> {
    return this.cache.getAsync(messageId);
  }

  /**
   * File-backed L2 lookup for segment spooling. Only the bounded metadata
   * header is parsed; the decoded body remains behind a counted file lease.
   */
  async acquire(
    messageId: string,
    signal?: AbortSignal
  ): Promise<SegmentArtifact | undefined> {
    signal?.throwIfAborted();
    const lease = await this.cache.acquireDiskFile(messageId);
    if (!lease) return undefined;
    try {
      signal?.throwIfAborted();
      const parsed = await readDiskSegmentMetadata(
        lease.path,
        lease.serializedBytes,
        signal
      );
      signal?.throwIfAborted();
      return new DiskSegmentArtifact(
        lease,
        parsed.metadata,
        parsed.bodyOffset,
        parsed.metadata.size
      );
    } catch (error) {
      await lease.release();
      if (signal?.aborted) throw error;
      await this.cache.delete(messageId);
      logger.debug(
        { err: error },
        'discarded an invalid persistent segment cache entry'
      );
      return undefined;
    }
  }

  /**
   * Best-effort file promotion. The admission set is a hard cap with no queue:
   * playback continues immediately when promotion is disabled or saturated.
   */
  promote(
    messageId: string,
    metadata: DecodedSegmentMetadata,
    sourcePath: string
  ): Promise<boolean> {
    if (
      this.closed ||
      this.cache.maxDiskBytes === 0 ||
      this.promotions.size >= this.maxPromotions
    ) {
      return Promise.resolve(false);
    }
    let promotion: Promise<boolean>;
    const run = async (): Promise<boolean> => {
      try {
        return await this.promoteOnce(messageId, metadata, sourcePath);
      } catch (error) {
        logger.debug({ err: error }, 'segment cache promotion failed');
        return false;
      } finally {
        this.promotions.delete(promotion);
      }
    };
    promotion = run();
    this.promotions.add(promotion);
    return promotion;
  }

  private async promoteOnce(
    messageId: string,
    metadata: DecodedSegmentMetadata,
    sourcePath: string
  ): Promise<boolean> {
    if (!Number.isSafeInteger(metadata.size) || metadata.size <= 0)
      return false;
    const sourceStats = await fs.lstat(sourcePath);
    if (!sourceStats.isFile() || sourceStats.size !== metadata.size)
      return false;
    const headerMetadata = metaBufOf({
      body: Buffer.alloc(0),
      byteRange: metadata.byteRange
        ? [metadata.byteRange[0], metadata.byteRange[1]]
        : undefined,
      fileSize: metadata.fileSize,
      totalParts: metadata.totalParts,
      name: metadata.name,
      size: metadata.size,
    });
    if (
      headerMetadata.length <= 0 ||
      headerMetadata.length > MAX_SEGMENT_METADATA_BYTES
    ) {
      return false;
    }
    const prefix = Buffer.allocUnsafe(4);
    prefix.writeUInt32LE(headerMetadata.length, 0);
    const header = Buffer.concat([prefix, headerMetadata]);
    const serializedBytes = header.length + metadata.size;
    if (
      !Number.isSafeInteger(serializedBytes) ||
      serializedBytes > this.cache.maxDiskBytes
    ) {
      return false;
    }

    const prepared = await this.cache.createPreparedFile();
    try {
      await fs.writeFile(prepared.path, header, { flag: 'w', mode: 0o600 });
      await pipeline(
        createReadStream(sourcePath, {
          highWaterMark: PROMOTION_COPY_CHUNK_BYTES,
        }),
        createWriteStream(prepared.path, {
          flags: 'a',
          mode: 0o600,
          highWaterMark: PROMOTION_COPY_CHUNK_BYTES,
        })
      );
      return await this.cache.installPreparedFile(
        messageId,
        prepared,
        serializedBytes
      );
    } finally {
      await prepared.release();
    }
  }

  /**
   * Insert a decoded segment, written through to disk. `skipMem` must be set
   * when `data.body` is a view into a recycled decode slot: the disk tier
   * copies it out synchronously, but the mem tier would retain the view past
   * the slot's recycle. The arena is never populated via set(); the
   * coordinator commits leased slots directly.
   */
  set(
    messageId: string,
    data: SegmentData,
    opts?: { skipMem?: boolean }
  ): void {
    this.cache.set(messageId, data, opts);
  }

  stats(): CacheStats {
    const s = this.cache.stats();
    const a = this.arena.stats();
    // Arena misses always fall through to the disk tier (which counts its own
    // hit-or-miss), so merged misses = disk misses; merged hits add arena hits.
    const hits = s.hits + a.hits;
    const misses = s.misses;
    return {
      hits,
      misses,
      hitRate: hits + misses > 0 ? hits / (hits + misses) : 0,
      diskBytes: s.diskBytes,
      diskCount: s.diskCount,
      diskHits: s.diskHits,
      arenaBytes: a.bytes,
      arenaEntries: a.entries,
      arenaPinned: a.pinned,
      arenaEvictions: a.evictions,
    };
  }

  clear(): void {
    this.arena.clear();
    void this.cache.clear();
  }

  /** Flush the disk index + drain pending writes (called on engine close). */
  async close(): Promise<void> {
    this.closed = true;
    this.arena.clear();
    await Promise.allSettled([...this.promotions]);
    await this.cache.close();
  }
}
