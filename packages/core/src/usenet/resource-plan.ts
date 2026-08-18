import type { EngineOptions, UsenetStreamingMode } from './types.js';
import { NNTP_READ_CARRY_MAX_BYTES } from './nntp/read-carry.js';

const KIBIBYTE_BYTES = 1024;
const MEBIBYTE_BYTES = KIBIBYTE_BYTES * KIBIBYTE_BYTES;

/**
 * Pinned segment-arena budget bounds. Slot demand tracks concurrent decodes
 * (pins on already-resident entries consume no new slots), so the buffering
 * budget scales with the connection budget, is floored to cover the archive
 * re-touch set, and is capped because a large pinned live-set has its own
 * major-GC marking cost. These three buffering constants preserve the
 * pre-resource-plan behaviour byte-for-byte.
 */
const SEGMENT_BUFFERING_ARENA_MIN_BYTES = 64 * MEBIBYTE_BYTES;
const SEGMENT_BUFFERING_ARENA_MAX_BYTES = 160 * MEBIBYTE_BYTES;
const SEGMENT_BUFFERING_ARENA_PER_DOWNLOAD_BYTES = 1.5 * MEBIBYTE_BYTES;

const SEGMENT_SPOOLING_ARENA_MIN_BYTES = 16 * MEBIBYTE_BYTES;
const SEGMENT_SPOOLING_ARENA_MAX_BYTES = 48 * MEBIBYTE_BYTES;
const SEGMENT_SPOOLING_ARENA_PER_DOWNLOAD_BYTES = 0.5 * MEBIBYTE_BYTES;

const SEGMENT_SPOOLING_MEMORY_MIN_BYTES = 16 * MEBIBYTE_BYTES;
const SEGMENT_SPOOLING_STREAM_BUFFER_MIN_BYTES = 2 * MEBIBYTE_BYTES;
const SEGMENT_SPOOLING_SPOOL_MIN_BYTES = 64 * MEBIBYTE_BYTES;

const DECODER_CHUNK_BYTES = 256 * KIBIBYTE_BYTES;
const WRITER_QUEUE_MIN_BYTES = 1 * MEBIBYTE_BYTES;
const WRITER_QUEUE_MAX_BYTES = 4 * MEBIBYTE_BYTES;
const READER_HIGH_WATER_MARK_MIN_BYTES = 256 * KIBIBYTE_BYTES;
const READER_HIGH_WATER_MARK_MAX_BYTES = 2 * MEBIBYTE_BYTES;
const ESTIMATED_SEGMENT_MIN_BYTES = 1 * MEBIBYTE_BYTES;
const ORPHAN_TTL_MS = 24 * 60 * 60_000;

/** Resource-plan inputs taken directly from resolved engine options. */
export type EngineResourcePlanOptions = Pick<
  EngineOptions,
  | 'streamingMode'
  | 'maxConcurrentDownloads'
  | 'segmentMemoryCacheBytes'
  | 'segmentSpoolingMemoryBudgetBytes'
  | 'segmentSpoolingStreamBufferBytes'
  | 'segmentSpoolingSpoolBytes'
  | 'segmentSpoolingMinFreeDiskBytes'
>;

/** Engine-wide, deterministic resource limits for the selected mode. */
export interface EngineResourcePlan {
  readonly mode: UsenetStreamingMode;
  readonly arenaBytes: number;
  /** Present only for `segment_spooling`. */
  readonly segmentSpooling?: SegmentSpoolingPlan;
}

/** Derived hard limits and internal queue sizes for segment spooling. */
export interface SegmentSpoolingPlan {
  readonly memoryBudgetBytes: number;
  readonly perStreamBufferBytes: number;
  readonly spoolBytes: number;
  readonly minFreeDiskBytes: number;
  readonly decoderChunkBytes: number;
  readonly writerQueueBytes: number;
  readonly readerHighWaterMarkBytes: number;
  /**
   * Atomic on-wire admission: two decoder/sink chunks plus the lazily allocated
   * but guaranteed NNTP TLS/onread carry window.
   */
  readonly perDownloadBaseLeaseBytes: number;
  readonly maxOpenSpoolFiles: number;
  readonly orphanTtlMs: number;
}

/** Derived estimates and queue sizes for one segment-spooling stream. */
export interface StreamResourcePlan {
  readonly prefetchSegments: number;
  readonly avgSegmentBytes: number;
  readonly estimatedSpoolWindowBytes: number;
  readonly readerHighWaterMarkBytes: number;
  readonly writerQueueBytes: number;
}

/** Inputs used to estimate one segment-spooling stream's resource window. */
export interface StreamResourcePlanOptions {
  readonly prefetchSegments: number;
  readonly avgDecodedSegmentBytes?: number;
  readonly segmentSpooling: SegmentSpoolingPlan;
}

/** Inputs used for a conservative per-segment spool reservation. */
export interface SegmentReservationOptions {
  readonly segmentBytes?: number;
  readonly avgDecodedSegmentBytes?: number;
}

export type UsenetResourcePlanConfigField =
  | 'usenet.segmentMemoryCacheBytes'
  | 'usenet.segmentSpoolingMemoryBudgetBytes'
  | 'usenet.segmentSpoolingStreamBufferBytes'
  | 'usenet.segmentSpoolingSpoolBytes'
  | 'usenet.segmentSpoolingMinFreeDiskBytes';

export type UsenetResourcePlanConfigIssueCode =
  | 'invalid_byte_value'
  | 'segment_spooling_memory_budget_too_small'
  | 'segment_spooling_stream_buffer_too_small'
  | 'segment_spooling_stream_buffer_exceeds_half_memory_budget'
  | 'segment_spooling_spool_budget_too_small';

/** One actionable cross-field or byte-value configuration problem. */
export interface UsenetResourcePlanConfigIssue {
  readonly code: UsenetResourcePlanConfigIssueCode;
  readonly field: UsenetResourcePlanConfigField;
  readonly message: string;
}

/**
 * Typed failure raised when the selected mode cannot produce a valid resource
 * plan. `issues` contains stable codes and exact runtime-config field names.
 */
export class UsenetResourcePlanConfigError extends Error {
  readonly code = 'USENET_RESOURCE_PLAN_CONFIG_INVALID' as const;
  readonly issues: readonly UsenetResourcePlanConfigIssue[];
  override readonly cause?: unknown;

  constructor(
    issues: readonly UsenetResourcePlanConfigIssue[],
    options: { cause?: unknown } = {}
  ) {
    super(
      `Invalid Usenet resource configuration: ${issues
        .map((issue) => issue.message)
        .join(' ')}`
    );
    this.name = 'UsenetResourcePlanConfigError';
    this.issues = [...issues];
    if (options.cause !== undefined) this.cause = options.cause;
    Error.captureStackTrace?.(this, UsenetResourcePlanConfigError);
  }
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function isNonNegativeSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value >= 0;
}

function validateByteValue(
  issues: UsenetResourcePlanConfigIssue[],
  field: UsenetResourcePlanConfigField,
  value: number
): boolean {
  if (isNonNegativeSafeInteger(value)) return true;
  issues.push({
    code: 'invalid_byte_value',
    field,
    message: `${field} must be a safe non-negative integer number of bytes; received ${String(value)}.`,
  });
  return false;
}

/**
 * Resolve `clamp(floor(downloads * 1.5 MiB), 64 MiB, 160 MiB)`, exactly the
 * automatic arena formula used before the resource-plan layer existed.
 */
export function resolveSegmentBufferingArenaBytes(
  maxConcurrentDownloads: number
): number {
  return clamp(
    Math.floor(
      maxConcurrentDownloads * SEGMENT_BUFFERING_ARENA_PER_DOWNLOAD_BYTES
    ),
    SEGMENT_BUFFERING_ARENA_MIN_BYTES,
    SEGMENT_BUFFERING_ARENA_MAX_BYTES
  );
}

/**
 * Resolve the segment-spooling auto formula:
 * `clamp(floor(downloads * 0.5 MiB), 16 MiB, 48 MiB)`.
 */
export function resolveSegmentSpoolingArenaBytes(
  maxConcurrentDownloads: number
): number {
  return clamp(
    Math.floor(
      maxConcurrentDownloads * SEGMENT_SPOOLING_ARENA_PER_DOWNLOAD_BYTES
    ),
    SEGMENT_SPOOLING_ARENA_MIN_BYTES,
    SEGMENT_SPOOLING_ARENA_MAX_BYTES
  );
}

/**
 * Resolve the arena budget. A positive explicit value is never clamped; zero
 * means automatic sizing for the selected mode.
 */
export function resolveSegmentArenaBytes(
  options: Pick<
    EngineResourcePlanOptions,
    'streamingMode' | 'maxConcurrentDownloads' | 'segmentMemoryCacheBytes'
  >
): number {
  if (!isNonNegativeSafeInteger(options.segmentMemoryCacheBytes)) {
    throw new UsenetResourcePlanConfigError([
      {
        code: 'invalid_byte_value',
        field: 'usenet.segmentMemoryCacheBytes',
        message:
          'usenet.segmentMemoryCacheBytes must be a safe non-negative integer number of bytes; ' +
          `received ${String(options.segmentMemoryCacheBytes)}.`,
      },
    ]);
  }
  if (options.segmentMemoryCacheBytes > 0) {
    return options.segmentMemoryCacheBytes;
  }
  return options.streamingMode === 'segment_buffering'
    ? resolveSegmentBufferingArenaBytes(options.maxConcurrentDownloads)
    : resolveSegmentSpoolingArenaBytes(options.maxConcurrentDownloads);
}

/**
 * Resolve `clamp(floor(perStreamBufferBytes / 2), 1 MiB, 4 MiB)` for the
 * asynchronous writer queue.
 */
export function resolveSegmentSpoolingWriterQueueBytes(
  perStreamBufferBytes: number
): number {
  return clamp(
    Math.floor(perStreamBufferBytes / 2),
    WRITER_QUEUE_MIN_BYTES,
    WRITER_QUEUE_MAX_BYTES
  );
}

/**
 * Resolve `clamp(floor(perStreamBufferBytes / 4), 256 KiB, 2 MiB)` for a
 * growing-file reader. The result is a queue setting, not a hard budget.
 */
export function resolveSegmentSpoolingReaderHighWaterMarkBytes(
  perStreamBufferBytes: number
): number {
  return clamp(
    Math.floor(perStreamBufferBytes / 4),
    READER_HIGH_WATER_MARK_MIN_BYTES,
    READER_HIGH_WATER_MARK_MAX_BYTES
  );
}

/** Resolve `clamp(maxConcurrentDownloads * 2, 32, 256)` open spool files. */
export function resolveSegmentSpoolingMaxOpenFiles(
  maxConcurrentDownloads: number
): number {
  return clamp(maxConcurrentDownloads * 2, 32, 256);
}

function validateSegmentSpoolingOptions(
  options: EngineResourcePlanOptions
): void {
  const issues: UsenetResourcePlanConfigIssue[] = [];
  const memoryValid = validateByteValue(
    issues,
    'usenet.segmentSpoolingMemoryBudgetBytes',
    options.segmentSpoolingMemoryBudgetBytes
  );
  const streamBufferValid = validateByteValue(
    issues,
    'usenet.segmentSpoolingStreamBufferBytes',
    options.segmentSpoolingStreamBufferBytes
  );
  const spoolValid = validateByteValue(
    issues,
    'usenet.segmentSpoolingSpoolBytes',
    options.segmentSpoolingSpoolBytes
  );
  validateByteValue(
    issues,
    'usenet.segmentSpoolingMinFreeDiskBytes',
    options.segmentSpoolingMinFreeDiskBytes
  );

  if (
    memoryValid &&
    options.segmentSpoolingMemoryBudgetBytes < SEGMENT_SPOOLING_MEMORY_MIN_BYTES
  ) {
    issues.push({
      code: 'segment_spooling_memory_budget_too_small',
      field: 'usenet.segmentSpoolingMemoryBudgetBytes',
      message: `usenet.segmentSpoolingMemoryBudgetBytes must be at least ${SEGMENT_SPOOLING_MEMORY_MIN_BYTES} bytes (16 MiB).`,
    });
  }
  if (
    streamBufferValid &&
    options.segmentSpoolingStreamBufferBytes <
      SEGMENT_SPOOLING_STREAM_BUFFER_MIN_BYTES
  ) {
    issues.push({
      code: 'segment_spooling_stream_buffer_too_small',
      field: 'usenet.segmentSpoolingStreamBufferBytes',
      message: `usenet.segmentSpoolingStreamBufferBytes must be at least ${SEGMENT_SPOOLING_STREAM_BUFFER_MIN_BYTES} bytes (2 MiB).`,
    });
  }
  if (
    memoryValid &&
    streamBufferValid &&
    options.segmentSpoolingStreamBufferBytes >
      Math.floor(options.segmentSpoolingMemoryBudgetBytes / 2)
  ) {
    issues.push({
      code: 'segment_spooling_stream_buffer_exceeds_half_memory_budget',
      field: 'usenet.segmentSpoolingStreamBufferBytes',
      message:
        'usenet.segmentSpoolingStreamBufferBytes must not exceed half of ' +
        `usenet.segmentSpoolingMemoryBudgetBytes (${Math.floor(
          options.segmentSpoolingMemoryBudgetBytes / 2
        )} bytes for the configured budget).`,
    });
  }
  if (
    spoolValid &&
    options.segmentSpoolingSpoolBytes < SEGMENT_SPOOLING_SPOOL_MIN_BYTES
  ) {
    issues.push({
      code: 'segment_spooling_spool_budget_too_small',
      field: 'usenet.segmentSpoolingSpoolBytes',
      message: `usenet.segmentSpoolingSpoolBytes must be at least ${SEGMENT_SPOOLING_SPOOL_MIN_BYTES} bytes (64 MiB).`,
    });
  }

  if (issues.length > 0) {
    throw new UsenetResourcePlanConfigError(issues);
  }
}

/**
 * Resolve all segment-spooling limits. Invalid combinations fail before any
 * runtime resource owner is constructed.
 */
export function resolveSegmentSpoolingPlan(
  options: EngineResourcePlanOptions
): SegmentSpoolingPlan {
  validateSegmentSpoolingOptions(options);
  return {
    memoryBudgetBytes: options.segmentSpoolingMemoryBudgetBytes,
    perStreamBufferBytes: options.segmentSpoolingStreamBufferBytes,
    spoolBytes: options.segmentSpoolingSpoolBytes,
    minFreeDiskBytes: options.segmentSpoolingMinFreeDiskBytes,
    decoderChunkBytes: DECODER_CHUNK_BYTES,
    writerQueueBytes: resolveSegmentSpoolingWriterQueueBytes(
      options.segmentSpoolingStreamBufferBytes
    ),
    readerHighWaterMarkBytes: resolveSegmentSpoolingReaderHighWaterMarkBytes(
      options.segmentSpoolingStreamBufferBytes
    ),
    // One atomic admission avoids an on-wire download ever waiting for carry
    // headroom after the rest of the memory budget has already been consumed.
    perDownloadBaseLeaseBytes:
      2 * DECODER_CHUNK_BYTES + NNTP_READ_CARRY_MAX_BYTES,
    maxOpenSpoolFiles: resolveSegmentSpoolingMaxOpenFiles(
      options.maxConcurrentDownloads
    ),
    orphanTtlMs: ORPHAN_TTL_MS,
  };
}

/**
 * Produce the complete deterministic engine resource plan. Segment-spooling
 * fields are deliberately not validated while segment buffering is selected.
 */
export function resolveEngineResourcePlan(
  options: EngineResourcePlanOptions
): EngineResourcePlan {
  const arenaBytes = resolveSegmentArenaBytes(options);
  if (options.streamingMode === 'segment_buffering') {
    return { mode: options.streamingMode, arenaBytes };
  }
  return {
    mode: options.streamingMode,
    arenaBytes,
    segmentSpooling: resolveSegmentSpoolingPlan(options),
  };
}

/** Resolve the initial conservative spool reservation for one segment. */
export function resolveEstimatedDecodedSegmentBytes(
  options: SegmentReservationOptions
): number {
  return Math.max(
    ESTIMATED_SEGMENT_MIN_BYTES,
    options.segmentBytes ??
      options.avgDecodedSegmentBytes ??
      ESTIMATED_SEGMENT_MIN_BYTES
  );
}

/** Resolve estimates and queue sizes shared by one spooling HTTP stream. */
export function resolveStreamResourcePlan(
  options: StreamResourcePlanOptions
): StreamResourcePlan {
  const avgSegmentBytes = resolveEstimatedDecodedSegmentBytes({
    avgDecodedSegmentBytes: options.avgDecodedSegmentBytes,
  });
  return {
    prefetchSegments: options.prefetchSegments,
    avgSegmentBytes,
    estimatedSpoolWindowBytes: options.prefetchSegments * avgSegmentBytes,
    readerHighWaterMarkBytes: options.segmentSpooling.readerHighWaterMarkBytes,
    writerQueueBytes: options.segmentSpooling.writerQueueBytes,
  };
}
