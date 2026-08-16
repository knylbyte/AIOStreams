const KIBIBYTE_BYTES = 1024;

/**
 * Largest Buffer any segment artifact reader may transfer in one `push()`.
 * This is a binary unit and is shared by arena, zero and growing-file readers.
 */
export const SEGMENT_STREAM_MAX_CHUNK_BYTES = 64 * KIBIBYTE_BYTES;

/** Hard ownership plan for one Node Readable queue. */
export interface SegmentStreamQueuePlan {
  readonly highWaterMarkBytes: number;
  readonly maxChunkBytes: number;
  readonly capacityBytes: number;
}

/**
 * Resolve the hard byte capacity behind a Readable high-water mark.
 *
 * Node requests another read while the current queue is below its HWM, so a
 * final push may leave at most `HWM + maxChunk - 1` owned bytes. Unlike an HWM
 * rounding formula, this bound remains valid after an arbitrary partial read
 * followed by a refill.
 */
export function resolveSegmentStreamQueuePlan(
  highWaterMarkBytes: number
): SegmentStreamQueuePlan {
  if (!Number.isSafeInteger(highWaterMarkBytes) || highWaterMarkBytes <= 0) {
    throw new RangeError(
      'Segment stream high-water mark must be a safe positive integer'
    );
  }
  const maxChunkBytes = SEGMENT_STREAM_MAX_CHUNK_BYTES;
  const capacityBytes = highWaterMarkBytes + maxChunkBytes - 1;
  if (!Number.isSafeInteger(capacityBytes)) {
    throw new RangeError(
      'Segment stream queue capacity must be a safe positive integer'
    );
  }
  return { highWaterMarkBytes, maxChunkBytes, capacityBytes };
}

/**
 * Resolve one atomic lease for the artifact reader, ordered stream and
 * optional FileStream relay. No partial queue lease is acquired separately.
 */
export function resolveSegmentStreamMemoryBytes(
  readerHighWaterMarkBytes: number,
  relayHighWaterMarkBytes?: number
): number {
  const readerQueue = resolveSegmentStreamQueuePlan(readerHighWaterMarkBytes);
  const relayQueue =
    relayHighWaterMarkBytes === undefined
      ? undefined
      : resolveSegmentStreamQueuePlan(relayHighWaterMarkBytes);
  const bytes =
    2 * readerQueue.capacityBytes + (relayQueue?.capacityBytes ?? 0);
  if (!Number.isSafeInteger(bytes) || bytes <= 0) {
    throw new RangeError(
      'Segment stream memory window must be a safe positive integer'
    );
  }
  return bytes;
}
