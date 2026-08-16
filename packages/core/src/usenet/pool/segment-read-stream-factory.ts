import type { Readable } from 'node:stream';
import type { HoleDecision, HoleKind } from '../holes.js';
import type {
  CommandPriority,
  NzbSegmentRef,
  UsenetStreamingMode,
} from '../types.js';
import { UsenetSpoolError } from '../spool/errors.js';
import {
  SegmentsStream,
  type SegmentBufferingSource,
} from './segments-stream.js';
import {
  SpoolingSegmentsStream,
  type SpoolingSegmentArtifactSource,
} from './spooling-segments-stream.js';
import type { SegmentArtifact } from './segment-artifact.js';

/** Mode-neutral inputs for the direct ordered segment read path. */
export interface CommonSegmentReadOptions {
  readonly pool: SegmentBufferingSource & SpoolingSegmentArtifactSource;
  readonly segments: NzbSegmentRef[];
  readonly nzbHash: string;
  readonly maxPrefetchSegments: number;
  /** Existing RAM reorder-buffer size, used only by segment buffering. */
  readonly bufferingBufferSizeBytes: number;
  /** Resource-plan HWM, required and used only by segment spooling. */
  readonly spoolingReaderHighWaterMarkBytes?: number;
  /** FileStream relay HWM; omitted for a direct two-queue spool stream. */
  readonly spoolingRelayHighWaterMarkBytes?: number;
  readonly skipBytes?: number;
  readonly limitBytes?: number;
  readonly priority: CommandPriority;
  readonly signal?: AbortSignal;
  readonly sizeForSegment?: (idx: number) => number | undefined;
  readonly byteRangeForSegment?: (
    idx: number
  ) => readonly [number, number] | undefined;
  readonly onHole?: (
    idx: number,
    bytes: number,
    kind: HoleKind
  ) => HoleDecision;
  readonly knownHoles?: ReadonlySet<number>;
  /** Complete or growing first artifact retained by the bounded locator. */
  readonly initialSpoolingArtifact?: SegmentArtifact;
}

/**
 * The single mode boundary for direct `FileStream` range reads. Buffering is
 * intentionally constructed with its legacy options, while spooling refuses
 * to fall back when its resource plan is absent.
 */
export function createSegmentReadStream(
  options: CommonSegmentReadOptions,
  mode: UsenetStreamingMode
): Readable {
  if (mode === 'segment_buffering') {
    return new SegmentsStream({
      pool: options.pool,
      segments: options.segments,
      nzbHash: options.nzbHash,
      maxWorkers: options.maxPrefetchSegments,
      bufferSizeBytes: options.bufferingBufferSizeBytes,
      skipBytes: options.skipBytes,
      limitBytes: options.limitBytes,
      priority: options.priority,
      signal: options.signal,
      sizeForSegment: options.sizeForSegment,
      onHole: options.onHole,
      knownHoles: options.knownHoles,
    });
  }

  const readerHighWaterMarkBytes = options.spoolingReaderHighWaterMarkBytes;
  if (readerHighWaterMarkBytes === undefined) {
    throw new UsenetSpoolError(
      'USENET_SPOOL_UNAVAILABLE',
      'Segment-spooling stream requires a resolved resource plan'
    );
  }
  return new SpoolingSegmentsStream({
    pool: options.pool,
    segments: options.segments,
    nzbHash: options.nzbHash,
    maxPrefetchSegments: options.maxPrefetchSegments,
    readerHighWaterMarkBytes,
    relayHighWaterMarkBytes: options.spoolingRelayHighWaterMarkBytes,
    skipBytes: options.skipBytes,
    limitBytes: options.limitBytes,
    priority: options.priority,
    signal: options.signal,
    sizeForSegment: options.sizeForSegment,
    byteRangeForSegment: options.byteRangeForSegment,
    onHole: options.onHole,
    knownHoles: options.knownHoles,
    initialArtifact: options.initialSpoolingArtifact,
  });
}
