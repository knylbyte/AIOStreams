import { SegmentCache } from './segment-cache.js';
import { PrioritySemaphore } from './priority-semaphore.js';
import { StatsAccumulator } from '../stats/accumulator.js';
import { createLogger } from '../../logging/logger.js';
import {
  SegmentArena,
  SharedSegment,
  ArenaLease,
  ownedShared,
} from './segment-arena.js';
import {
  SegmentFetcher,
  SegmentHeadData,
  StatDetail,
  LocalSegmentFetcher,
  awaitAbortable,
} from '../nntp/segment-fetcher.js';
import {
  ArticleNotFoundError,
  NntpError,
  definitiveLossKind,
} from '../nntp/errors.js';
import { YencDecodeError, YencMetadataError } from './yenc.js';
import type { HoleKind } from '../holes.js';
import {
  CommandPriority,
  EngineOptions,
  NzbSegmentRef,
  PoolInfo,
  ProviderConfig,
  SegmentData,
} from '../types.js';
import {
  ArenaSegmentArtifact,
  GrowingSpoolArtifactAdapter,
  type SegmentArtifactFetchOptions,
  type SegmentRangeMetadataFetchOptions,
  type SegmentRangeMetadata,
  type SegmentRangeLayout,
  type SegmentArtifact,
} from './segment-artifact.js';
import { SegmentSpoolingRuntime } from './segment-spooling-runtime.js';
import type { SegmentSpoolingRuntimeStats } from './segment-spooling-runtime.js';
import { SpoolingSegmentSink } from './spooling-segment-sink.js';
import type {
  DecodedSegmentHeaderMetadata,
  DecodedSegmentMetadata,
} from './streaming-yenc-article-decoder.js';
import type { GrowingSpoolArtifact } from '../spool/growing-artifact.js';
import { UsenetSpoolError } from '../spool/errors.js';
import { resolveEstimatedDecodedSegmentBytes } from '../resource-plan.js';
import type { ByteLease } from './byte-budget.js';
import type { SegmentStreamCleanupCause } from './resource-events.js';

const logger = createLogger('usenet/multi-provider-pool');

function inferSegmentRangeLayout(
  metadata: SegmentRangeMetadata
): SegmentRangeLayout | undefined {
  if (metadata.layout !== undefined) return metadata.layout;
  if (metadata.byteRange !== undefined) return 'global-range';
  if (metadata.totalParts !== undefined && metadata.totalParts > 1) {
    return undefined;
  }
  return 'standalone-part';
}

interface ArtifactExpectations {
  readonly expectedLength: number | undefined;
  readonly expectedByteRange: readonly [number, number] | undefined;
}

function artifactMetadataMismatch(prefix = 'Decoded'): UsenetSpoolError {
  return new UsenetSpoolError(
    'USENET_SPOOL_METADATA_MISMATCH',
    `${prefix} segment metadata differs from the exact file range`
  );
}

function validateArtifactExpectations(
  options: SegmentArtifactFetchOptions
): ArtifactExpectations {
  let expectedLength = options.expectedLength;
  if (
    expectedLength !== undefined &&
    (!Number.isSafeInteger(expectedLength) || expectedLength <= 0)
  ) {
    throw new UsenetSpoolError(
      'USENET_SPOOL_INVALID_ARGUMENT',
      'Expected segment length must be a safe positive integer'
    );
  }
  const range = options.expectedByteRange;
  if (range === undefined) {
    return { expectedLength, expectedByteRange: undefined };
  }
  const begin = range[0];
  const end = range[1];
  if (
    range.length !== 2 ||
    !Number.isSafeInteger(begin) ||
    !Number.isSafeInteger(end) ||
    begin < 0 ||
    end <= begin
  ) {
    throw new UsenetSpoolError(
      'USENET_SPOOL_INVALID_ARGUMENT',
      'Expected segment byte range must contain safe increasing offsets'
    );
  }
  const rangeLength = end - begin;
  if (expectedLength !== undefined && expectedLength !== rangeLength) {
    throw new UsenetSpoolError(
      'USENET_SPOOL_INVALID_ARGUMENT',
      'Expected segment length must equal the expected byte-range length'
    );
  }
  expectedLength ??= rangeLength;
  const expectedByteRange: readonly [number, number] = [begin, end];
  return { expectedLength, expectedByteRange };
}

function assertMetadataExpectations(
  metadata: DecodedSegmentMetadata,
  expectations: ArtifactExpectations,
  prefix = 'Decoded'
): void {
  if (
    expectations.expectedLength !== undefined &&
    metadata.size !== expectations.expectedLength
  ) {
    throw artifactMetadataMismatch(prefix);
  }
  const expectedRange = expectations.expectedByteRange;
  if (expectedRange === undefined) return;
  const actualRange = metadata.byteRange;
  if (
    actualRange === undefined ||
    actualRange[0] !== expectedRange[0] ||
    actualRange[1] !== expectedRange[1]
  ) {
    throw artifactMetadataMismatch(prefix);
  }
}

export type { SegmentHeadData } from '../nntp/segment-fetcher.js';
export type { SharedSegment } from './segment-arena.js';

/** One registered waiter of a shared single-flight fetch. */
interface SharedWaiter {
  deliver(h: SharedSegment): void;
  fail(err: unknown): void;
}

/** A shared fetch in flight; aborting waiters deregister themselves. */
interface SharedFlight {
  waiters: Set<SharedWaiter>;
  /**
   * Aborted when the last waiter deregisters: cancels a still-queued
   * semaphore acquire; a granted (on-the-wire) fetch always completes and
   * warms the cache.
   */
  ctl: AbortController;
  task?: Promise<void>;
}

/** One caller waiting for an independently releasable file-backed handle. */
interface ArtifactWaiter {
  readonly expectedLength: number | undefined;
  readonly expectedByteRange: readonly [number, number] | undefined;
  readonly allowGrowing: boolean;
  deliver(artifact: SegmentArtifact): void;
  fail(error: unknown): void;
}

/** One message-id fetch shared by every currently registered artifact waiter. */
interface ArtifactFlight {
  readonly waiters: Set<ArtifactWaiter>;
  readonly ctl: AbortController;
  onWire: boolean;
  growingOwner?: SharedSpoolArtifactOwner;
  growingOwnerPublished: boolean;
  task?: Promise<void>;
}

/** Optional construction seams used by the engine and deterministic tests. */
export interface MultiProviderPoolDependencies {
  readonly fetcher?: SegmentFetcher;
  readonly spooling?: SegmentSpoolingRuntime;
  /** Deterministic lifecycle seam; receives only the bounded owner count. */
  readonly onActiveOperationCountChanged?: (active: number) => void;
}

class SharedSpoolArtifactOwner {
  private references = 0;
  private disposePromise: Promise<void> | undefined;
  private producerActive: boolean;
  private metadataValue: DecodedSegmentMetadata;
  private readonly producerCompletion =
    Promise.withResolvers<DecodedSegmentMetadata>();
  private producerSettled = false;
  private promotionActive = false;

  constructor(
    private readonly artifact: GrowingSpoolArtifact,
    metadata: DecodedSegmentMetadata,
    producerActive = false
  ) {
    this.metadataValue = metadata;
    this.producerActive = producerActive;
    if (!producerActive) {
      this.producerSettled = true;
      this.producerCompletion.resolve(metadata);
    }
    void this.producerCompletion.promise.catch(() => undefined);
  }

  owns(artifact: GrowingSpoolArtifact): boolean {
    return this.artifact === artifact;
  }

  matches(expectations: ArtifactExpectations): boolean {
    try {
      assertMetadataExpectations(this.metadataValue, expectations, 'Published');
      return true;
    } catch {
      return false;
    }
  }

  acquire(expectations: ArtifactExpectations): SegmentArtifact {
    if (this.disposePromise) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Cannot acquire a disposed shared spool artifact'
      );
    }
    assertMetadataExpectations(this.metadataValue, expectations, 'Published');
    const validatedCompletion = this.producerCompletion.promise.then(
      (metadata) => {
        assertMetadataExpectations(metadata, expectations);
        return metadata;
      }
    );
    void validatedCompletion.catch(() => undefined);
    this.references++;
    let released = false;
    return new GrowingSpoolArtifactAdapter(
      this.artifact,
      this.metadataValue,
      () => {
        if (released) return this.disposePromise ?? Promise.resolve();
        released = true;
        this.references--;
        return this.disposeIfUnused();
      },
      validatedCompletion,
      () => this.metadataValue
    );
  }

  producerCompleted(metadata: DecodedSegmentMetadata): void {
    if (this.producerSettled) return;
    this.producerSettled = true;
    this.metadataValue = metadata;
    this.producerActive = false;
    this.producerCompletion.resolve(metadata);
  }

  producerFailed(error: Error): void {
    if (this.producerSettled) return;
    this.producerSettled = true;
    this.producerActive = false;
    this.producerCompletion.reject(error);
  }

  /**
   * A promotion lease keeps the spool file alive independently. Final reader
   * release may therefore initiate disposal without awaiting the best-effort
   * cache copy; GrowingSpoolArtifact performs the physical delete afterwards.
   */
  trackPromotion(promotion: Promise<unknown>): void {
    this.promotionActive = true;
    void promotion
      .finally(() => {
        this.promotionActive = false;
      })
      .catch(() => undefined);
  }

  disposeIfUnused(): Promise<void> {
    if (this.producerActive || this.references !== 0) return Promise.resolve();
    const disposal = this.dispose();
    if (!this.promotionActive) return disposal;
    void disposal.catch((error: unknown) => {
      logger.warn({ err: error }, 'deferred spool artifact disposal failed');
    });
    return Promise.resolve();
  }

  private dispose(): Promise<void> {
    this.disposePromise ??= this.artifact.dispose();
    return this.disposePromise;
  }
}

/**
 * TTL for the negative-miss cache
 */
const MISS_TTL_MS = 60_000;
/** Max distinct missing message-ids remembered (insertion-order eviction). */
const MISS_CACHE_MAX = 16_384;
/** Hard bounds for callbacks retained by the Block-6 single-flight layer. */
const ARTIFACT_FLIGHT_MAX = 16_384;
const ARTIFACT_WAITERS_PER_FLIGHT_MAX = 1024;
const SHARED_FLIGHT_MAX = 16_384;
const SHARED_WAITERS_PER_FLIGHT_MAX = 1024;
const HEAD_FLIGHT_MAX = 16_384;
const ACTIVE_OPERATION_MAX = 65_536;

/**
 * Coordinates segment fetches: owns the segment cache, single-flight de-dupe and
 * the global (prioritised) download budget, and delegates the actual
 * connection-owning work (provider failover + yEnc decode) to a
 * {@link SegmentFetcher} (the in-process {@link LocalSegmentFetcher}).
 */
export class MultiProviderPool {
  private fetcher: SegmentFetcher;
  private globalDownloads: PrioritySemaphore;
  private readonly spooling: SegmentSpoolingRuntime | undefined;
  private closePromise: Promise<void> | undefined;
  private closedError: UsenetSpoolError | undefined;
  private readonly closeController = new AbortController();
  /** Bounded by the sum of the finite flight/admission limits above. */
  private readonly activeOperations = new Set<Promise<void>>();
  private readonly onActiveOperationCountChanged:
    | ((active: number) => void)
    | undefined;
  /** Single-flight coordinator for shared (arena-backed) segment fetches. */
  private sharedInflight = new Map<string, SharedFlight>();
  /** Single-flight coordinator for file-backed segment artifacts. */
  private artifactInflight = new Map<string, ArtifactFlight>();
  /**
   * Single-flight for head-only probe fetches. Fill/repost NZBs list the SAME
   * articles under multiple `<file>` entries, and head fetches don't populate
   * the segment cache; without this, every duplicate probe re-downloads the
   * article.
   */
  private inflightHeads = new Map<string, Promise<SegmentHeadData>>();
  /**
   * Budget permits whose transfer has actually started on a connection.
   */
  private onWireCount = 0;

  private assertOpen(): void {
    if (this.closedError) throw this.closedError;
  }

  private operationSignal(signal?: AbortSignal): AbortSignal {
    return signal
      ? AbortSignal.any([signal, this.closeController.signal])
      : this.closeController.signal;
  }

  private trackOperation<T>(operation: () => Promise<T>): Promise<T> {
    this.assertOpen();
    if (this.activeOperations.size >= ACTIVE_OPERATION_MAX) {
      return Promise.reject(
        new UsenetSpoolError(
          'USENET_SPOOL_CAPACITY',
          'Usenet fetch operation capacity reached'
        )
      );
    }
    const task = operation();
    const settled = task.then(
      () => undefined,
      () => undefined
    );
    this.activeOperations.add(settled);
    this.notifyActiveOperationCount();
    void settled.finally(() => {
      this.activeOperations.delete(settled);
      this.notifyActiveOperationCount();
    });
    return task;
  }

  private notifyActiveOperationCount(): void {
    try {
      this.onActiveOperationCountChanged?.(this.activeOperations.size);
    } catch {
      // Test/diagnostic observers never participate in resource ownership.
    }
  }

  /**
   * Negative cache of definitive all-providers verdicts
   */
  private missCache = new Map<string, { exp: number; kind: HoleKind }>();

  /** The pinned decoded-body tier (owned by the segment cache). */
  private get arena(): SegmentArena {
    return this.cache.arena;
  }

  /** Tracks one permit-holding fetch's transition onto the wire. */
  private wireTracker(): { start: () => void; end: () => void } {
    let onWire = false;
    return {
      // Failover may invoke `run` once per candidate; count the fetch once.
      start: () => {
        if (onWire) return;
        onWire = true;
        this.onWireCount++;
      },
      end: () => {
        if (!onWire) return;
        onWire = false;
        this.onWireCount--;
      },
    };
  }

  /** The non-expired cached verdict for `messageId`, if any. */
  private cachedMiss(messageId: string): HoleKind | undefined {
    const hit = this.missCache.get(messageId);
    if (hit === undefined) return undefined;
    if (hit.exp <= Date.now()) {
      this.missCache.delete(messageId);
      return undefined;
    }
    return hit.kind;
  }

  /** Record `messageId` as unservable by every provider (bounded, TTL'd). */
  private recordMiss(messageId: string, kind: HoleKind): void {
    if (
      this.missCache.size >= MISS_CACHE_MAX &&
      !this.missCache.has(messageId)
    ) {
      const oldest = this.missCache.keys().next().value;
      if (oldest !== undefined) this.missCache.delete(oldest);
    }
    this.missCache.set(messageId, { exp: Date.now() + MISS_TTL_MS, kind });
  }

  /** A fresh definitive error replaying a cached verdict. */
  private cachedMissError(messageId: string, kind: HoleKind): Error {
    return kind === 'undecodable'
      ? new YencDecodeError(
          undefined,
          `article undecodable on all providers (cached): ${messageId}`,
          { terminal: true }
        )
      : new ArticleNotFoundError(
          `article not found on any provider (cached): ${messageId}`,
          { messageId, allProviders: true }
        );
  }

  constructor(
    providers: ProviderConfig[],
    opts: EngineOptions,
    private cache: SegmentCache,
    stats: StatsAccumulator,
    dependencies: MultiProviderPoolDependencies = {}
  ) {
    // The fetcher owns the connection pools + failover + decode; the engine's
    // StatsAccumulator is its (in-process) stats sink.
    this.fetcher =
      dependencies.fetcher ?? new LocalSegmentFetcher(providers, opts, stats);
    this.spooling = dependencies.spooling;
    this.onActiveOperationCountChanged =
      dependencies.onActiveOperationCountChanged;

    // The global download budget is a HARD ceiling on concurrent in-flight
    // BODY/ARTICLE downloads. It is auto-sized (in buildUsenetEngineOptions) to
    // Σ maxConnections × depth so the default never throttles pipelining; an
    // explicit `maxConcurrentDownloads` lower than that is honoured as a real
    // cap (the per-provider connection pools still bound sockets per account).
    // The per-stream priority reservation rides on this semaphore.
    this.globalDownloads = new PrioritySemaphore(
      Math.max(1, opts.maxConcurrentDownloads),
      opts.streamingPriority
    );
  }

  /**
   * Fetch + decode one segment, trying providers in priority/availability order
   * with per-segment 430 failover and backup escalation. Throws
   * `ArticleNotFoundError` when every provider reports the article missing, or
   * the last transient `NntpError` when all attempts failed transiently.
   *
   * Thin wrapper over {@link fetchSegmentShared}: the returned body is always
   * owned, so callers may retain it freely.
   */
  async fetchSegment(
    segment: NzbSegmentRef,
    nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority = CommandPriority.High
  ): Promise<SegmentData> {
    this.assertOpen();
    const h = await this.fetchSegmentShared(segment, nzbHash, signal, priority);
    try {
      return h.owned ? h.data : { ...h.data, body: Buffer.from(h.data.body) };
    } finally {
      h.release();
    }
  }

  /**
   * Fetch + decode one segment as a pinned view into the shared segment arena
   * ({@link SegmentArena} documents the pin/release contract). Single-flighted:
   * concurrent callers of the same message-id share one fetch, each receiving
   * its own pin.
   */
  fetchSegmentShared(
    segment: NzbSegmentRef,
    nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority = CommandPriority.High
  ): Promise<SharedSegment> {
    try {
      this.assertOpen();
    } catch (error) {
      return Promise.reject(error);
    }
    const id = segment.messageId;
    const hit = this.arena.acquire(id);
    if (hit) return Promise.resolve(hit);
    if (signal?.aborted) {
      return Promise.reject(new NntpError('connection', 'aborted'));
    }
    const cached = this.cachedMiss(id);
    if (cached !== undefined) {
      return Promise.reject(this.cachedMissError(id, cached));
    }

    let flight = this.sharedInflight.get(id);
    const isNew = !flight;
    if (!flight) {
      if (this.sharedInflight.size >= SHARED_FLIGHT_MAX) {
        return Promise.reject(
          new UsenetSpoolError(
            'USENET_SPOOL_CAPACITY',
            'Shared segment flight capacity reached'
          )
        );
      }
      flight = { waiters: new Set(), ctl: new AbortController() };
      this.sharedInflight.set(id, flight);
    }
    const joined = flight;
    if (joined.waiters.size >= SHARED_WAITERS_PER_FLIGHT_MAX) {
      return Promise.reject(
        new UsenetSpoolError(
          'USENET_SPOOL_CAPACITY',
          'Shared segment waiter capacity reached'
        )
      );
    }
    const p = new Promise<SharedSegment>((resolve, reject) => {
      // An aborting waiter deregisters itself before delivery, so pins are
      // granted only to waiters that will consume them. While any waiter
      // remains the fetch runs without its signal, so one abandoning caller
      // cannot poison the flight for the others; the last waiter to leave
      // cancels a still-queued acquire via the flight controller. Waiters
      // without a signal never deregister.
      let onAbort: (() => void) | undefined;
      let settled = false;
      const done = (): void => {
        if (onAbort) signal!.removeEventListener('abort', onAbort);
      };
      const waiter: SharedWaiter = {
        deliver: (h) => {
          if (settled) {
            h.release();
            return;
          }
          settled = true;
          done();
          resolve(h);
        },
        fail: (e) => {
          if (settled) return;
          settled = true;
          done();
          reject(e);
        },
      };
      joined.waiters.add(waiter);
      if (signal) {
        onAbort = () => {
          if (settled) return;
          settled = true;
          joined.waiters.delete(waiter);
          if (joined.waiters.size === 0) {
            // Deregister BEFORE aborting so a new caller starts a fresh
            // flight instead of joining this dying one.
            if (this.sharedInflight.get(id) === joined) {
              this.sharedInflight.delete(id);
            }
            joined.ctl.abort();
          }
          reject(new NntpError('connection', 'aborted'));
        };
        signal.addEventListener('abort', onAbort, { once: true });
      }
    });
    if (isNew) {
      joined.task = this.trackOperation(() =>
        this.runShared(segment, nzbHash, priority, joined)
      );
      void joined.task.catch((error: unknown) => {
        if (this.sharedInflight.get(id) === joined) {
          this.sharedInflight.delete(id);
        }
        const waiters = [...joined.waiters];
        joined.waiters.clear();
        for (const waiter of waiters) waiter.fail(error);
      });
    }
    return p;
  }

  /**
   * Fetch one decoded segment as independently releasable storage. This is the
   * only network path used by segment spooling: it never calls the buffering
   * APIs and never asks the persistent cache to materialize a complete body.
   *
   * Lookup order is arena, optional file-backed L2, negative miss cache, then
   * a message-id single flight into the transient spool.
   */
  async fetchSegmentArtifact(
    segment: NzbSegmentRef,
    nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority = CommandPriority.High,
    options: SegmentArtifactFetchOptions = {}
  ): Promise<SegmentArtifact> {
    this.assertOpen();
    const expectations = validateArtifactExpectations(options);
    const allowGrowing = options.allowGrowing ?? false;
    if (typeof allowGrowing !== 'boolean') {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Growing artifact delivery must be a boolean'
      );
    }
    const id = segment.messageId;
    const pinned = this.arena.acquire(id);
    if (pinned) {
      const artifact = new ArenaSegmentArtifact(pinned);
      await this.assertArtifactMetadata(artifact, expectations);
      if (this.closedError) {
        await artifact.release();
        throw this.closedError;
      }
      return artifact;
    }
    if (signal?.aborted) throw new NntpError('connection', 'aborted');

    const runtime = this.spooling;
    if (!runtime) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_UNAVAILABLE',
        'Segment artifact fetching requires segment-spooling mode'
      );
    }

    if (runtime.artifactCache) {
      const persistent = await runtime.artifactCache.acquire(id, signal);
      if (persistent) {
        if (signal?.aborted || this.closedError) {
          await persistent.release();
          if (this.closedError) throw this.closedError;
          throw new NntpError('connection', 'aborted');
        }
        await this.assertArtifactMetadata(persistent, expectations);
        if (this.closedError) {
          await persistent.release();
          throw this.closedError;
        }
        return persistent;
      }
    }

    const cached = this.cachedMiss(id);
    if (cached !== undefined) throw this.cachedMissError(id, cached);
    return this.joinArtifactFlight(
      segment,
      nzbHash,
      priority,
      signal,
      expectations,
      allowGrowing
    );
  }

  /**
   * Fetch only bounded scalar yEnc metadata for the direct spooling locator.
   * Arena and file-backed hits reuse their already parsed metadata; a network
   * miss retains at most the bounded yEnc head parser state and never calls the
   * buffering cache/body APIs.
   */
  async fetchSegmentRangeMetadata(
    segment: NzbSegmentRef,
    nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority = CommandPriority.High,
    options: SegmentRangeMetadataFetchOptions = {}
  ): Promise<SegmentRangeMetadata> {
    this.assertOpen();
    const id = segment.messageId;
    const pinned = this.arena.acquire(id);
    if (pinned) {
      try {
        const metadata: SegmentRangeMetadata = {
          byteRange: pinned.data.byteRange,
          fileSize: pinned.data.fileSize,
          totalParts: pinned.data.totalParts,
          name: pinned.data.name,
          decodedSize: pinned.data.size,
          layout: pinned.data.byteRange
            ? 'global-range'
            : pinned.data.totalParts !== undefined && pinned.data.totalParts > 1
              ? undefined
              : 'standalone-part',
        };
        if (this.isUsableRangeMetadata(metadata, options)) return metadata;
      } finally {
        pinned.release();
      }
    }
    if (signal?.aborted) throw new NntpError('connection', 'aborted');

    const persistent = await this.spooling?.artifactCache?.acquire(id, signal);
    if (persistent) {
      try {
        this.assertOpen();
        const metadata: SegmentRangeMetadata = {
          byteRange: persistent.metadata.byteRange,
          fileSize: persistent.metadata.fileSize,
          totalParts: persistent.metadata.totalParts,
          name: persistent.metadata.name,
          decodedSize: persistent.metadata.size,
          layout: persistent.metadata.byteRange
            ? 'global-range'
            : persistent.metadata.totalParts !== undefined &&
                persistent.metadata.totalParts > 1
              ? undefined
              : 'standalone-part',
        };
        if (this.isUsableRangeMetadata(metadata, options)) return metadata;
      } finally {
        await persistent.release();
      }
    }

    const cached = this.cachedMiss(id);
    if (cached !== undefined) throw this.cachedMissError(id, cached);
    return this.trackOperation(async () => {
      const operationSignal = this.operationSignal(signal);
      const releaseGlobal = await this.globalDownloads.acquire(
        priority,
        operationSignal
      );
      const wire = this.wireTracker();
      try {
        const head = await this.fetcher.fetchHead(
          segment,
          nzbHash,
          priority,
          0,
          wire.start,
          operationSignal,
          {
            strictYencMetadata: true,
            requireByteRange: options.requireByteRange,
            allowStandalonePart: options.allowStandalonePart,
          }
        );
        const metadata = {
          byteRange: head.byteRange,
          fileSize: head.fileSize,
          totalParts: head.totalParts,
          name: head.name,
          decodedSize: head.size,
          layout: head.layout,
        };
        if (!this.isUsableRangeMetadata(metadata, options)) {
          throw new YencMetadataError(
            'invalid_header',
            'yEnc metadata probe returned unusable range metadata'
          );
        }
        return metadata;
      } catch (error) {
        const kind = definitiveLossKind(error);
        if (kind) this.recordMiss(id, kind);
        throw error;
      } finally {
        wire.end();
        releaseGlobal();
      }
    });
  }

  private isUsableRangeMetadata(
    metadata: SegmentRangeMetadata,
    options: SegmentRangeMetadataFetchOptions
  ): boolean {
    const fileSize = metadata.fileSize;
    if (
      fileSize === undefined ||
      !Number.isSafeInteger(fileSize) ||
      fileSize <= 0
    ) {
      return false;
    }
    const range = metadata.byteRange;
    const layout = inferSegmentRangeLayout(metadata);
    const validRange =
      layout === 'global-range' &&
      range !== undefined &&
      Number.isSafeInteger(range[0]) &&
      Number.isSafeInteger(range[1]) &&
      range[0] >= 0 &&
      range[1] > range[0] &&
      range[1] <= fileSize;
    const decodedSize = metadata.decodedSize;
    const validStandalone =
      layout === 'standalone-part' &&
      range === undefined &&
      decodedSize !== undefined &&
      Number.isSafeInteger(decodedSize) &&
      decodedSize > 0 &&
      decodedSize === fileSize;
    if (options.requireByteRange) {
      return (
        validRange || (options.allowStandalonePart === true && validStandalone)
      );
    }
    return validRange || validStandalone;
  }

  private joinArtifactFlight(
    segment: NzbSegmentRef,
    nzbHash: string,
    priority: CommandPriority,
    signal: AbortSignal | undefined,
    expectations: ArtifactExpectations,
    allowGrowing: boolean
  ): Promise<SegmentArtifact> {
    try {
      this.assertOpen();
    } catch (error) {
      return Promise.reject(error);
    }
    const id = segment.messageId;
    let flight = this.artifactInflight.get(id);
    const isNew = flight === undefined;
    if (!flight) {
      if (this.artifactInflight.size >= ARTIFACT_FLIGHT_MAX) {
        return Promise.reject(
          new UsenetSpoolError(
            'USENET_SPOOL_CAPACITY',
            'Segment artifact single-flight capacity reached'
          )
        );
      }
      flight = {
        waiters: new Set<ArtifactWaiter>(),
        ctl: new AbortController(),
        onWire: false,
        growingOwnerPublished: false,
      };
      this.artifactInflight.set(id, flight);
    }
    const joined = flight;
    if (joined.waiters.size >= ARTIFACT_WAITERS_PER_FLIGHT_MAX) {
      return Promise.reject(
        new UsenetSpoolError(
          'USENET_SPOOL_CAPACITY',
          'Segment artifact waiter capacity reached'
        )
      );
    }

    const promise = new Promise<SegmentArtifact>((resolve, reject) => {
      let settled = false;
      let onAbort: (() => void) | undefined;
      const finish = (): boolean => {
        if (settled) return false;
        settled = true;
        if (onAbort && signal) {
          signal.removeEventListener('abort', onAbort);
        }
        return true;
      };
      const waiter: ArtifactWaiter = {
        ...expectations,
        allowGrowing,
        deliver: (artifact) => {
          if (!finish()) {
            void artifact.release().catch((releaseError: unknown) => {
              logger.warn(
                { err: releaseError },
                'failed to release an abandoned segment artifact handle'
              );
            });
            return;
          }
          resolve(artifact);
        },
        fail: (error) => {
          if (finish()) reject(error);
        },
      };
      joined.waiters.add(waiter);
      if (signal) {
        onAbort = () => {
          if (!finish()) return;
          joined.waiters.delete(waiter);
          if (joined.waiters.size === 0 && !joined.onWire) {
            if (this.artifactInflight.get(id) === joined) {
              this.artifactInflight.delete(id);
            }
            joined.ctl.abort();
          }
          reject(new NntpError('connection', 'aborted'));
        };
        signal.addEventListener('abort', onAbort, { once: true });
        if (signal.aborted) onAbort();
      }
      const owner = joined.growingOwner;
      if (
        !settled &&
        joined.growingOwnerPublished &&
        owner !== undefined &&
        allowGrowing &&
        owner.matches(expectations)
      ) {
        joined.waiters.delete(waiter);
        waiter.deliver(owner.acquire(expectations));
      }
    });
    if (isNew) {
      joined.task = this.trackOperation(() =>
        this.runArtifactFlight(segment, nzbHash, priority, joined)
      );
      void joined.task.catch((error: unknown) => {
        if (this.artifactInflight.get(id) === joined) {
          this.artifactInflight.delete(id);
        }
        const waiters = [...joined.waiters];
        joined.waiters.clear();
        for (const waiter of waiters) waiter.fail(error);
      });
    }
    return promise;
  }

  /** Run one network/spool operation and fan out counted artifact handles. */
  private async runArtifactFlight(
    segment: NzbSegmentRef,
    nzbHash: string,
    priority: CommandPriority,
    flight: ArtifactFlight
  ): Promise<void> {
    const id = segment.messageId;
    const runtime = this.spooling;
    if (!runtime) return;

    let memoryLease:
      | Awaited<ReturnType<SegmentSpoolingRuntime['acquireDownloadMemory']>>
      | undefined;
    let releaseGlobal: (() => void) | undefined;
    const wire = this.wireTracker();
    let unownedArtifact: GrowingSpoolArtifact | undefined;
    try {
      const downloadMemoryLease = await runtime.acquireDownloadMemory(
        priority,
        flight.ctl.signal
      );
      memoryLease = downloadMemoryLease;
      try {
        releaseGlobal = await this.globalDownloads.acquire(
          priority,
          flight.ctl.signal
        );
      } catch (error) {
        if (flight.ctl.signal.aborted) {
          throw new NntpError('connection', 'aborted');
        }
        throw error;
      }

      const result = await this.fetcher.fetchBodyToSink(
        segment,
        nzbHash,
        priority,
        async () => {
          const artifact = await runtime.spoolManager.createArtifact({
            sessionId: nzbHash,
            segmentId: id,
            initialReservationBytes: resolveEstimatedDecodedSegmentBytes({
              segmentBytes: segment.bytes,
            }),
            signal: flight.ctl.signal,
            priority,
          });
          const sink = new SpoolingSegmentSink(
            artifact,
            2 * runtime.plan.decoderChunkBytes,
            runtime.plan.decoderChunkBytes
          );
          const prepareGrowingOwner = (
            header: DecodedSegmentHeaderMetadata
          ): void => {
            if (flight.growingOwner?.owns(artifact)) return;
            const metadata = this.resolveEarlyArtifactMetadata(flight, header);
            if (!metadata) return;
            const owner = new SharedSpoolArtifactOwner(
              artifact,
              metadata,
              true
            );
            flight.growingOwner = owner;
            flight.growingOwnerPublished = false;
            void artifact
              .waitForChange(0, flight.ctl.signal)
              .then(() => this.publishGrowingArtifact(flight, owner))
              .catch(() => {
                // Attempt disposal/provider failover owns the typed failure.
              });
          };
          return {
            sink,
            value: artifact,
            onHeader: prepareGrowingOwner,
            dispose: async (error: Error) => {
              sink.fail(error);
              const owner = flight.growingOwner;
              if (owner?.owns(artifact)) {
                flight.growingOwner = undefined;
                flight.growingOwnerPublished = false;
                owner.producerFailed(artifact.snapshot().error ?? error);
              }
              await artifact.dispose();
            },
          };
        },
        flight.ctl.signal,
        () => {
          flight.onWire = true;
          wire.start();
        }
      );
      this.assertOpen();
      unownedArtifact = result.value;
      let owner = flight.growingOwner;
      if (owner?.owns(result.value)) {
        owner.producerCompleted(result.metadata);
      } else {
        owner = new SharedSpoolArtifactOwner(result.value, result.metadata);
        flight.growingOwner = owner;
      }
      this.startArtifactPromotion(
        runtime,
        id,
        result.value,
        result.metadata,
        owner
      );
      unownedArtifact = undefined;
      if (this.artifactInflight.get(id) === flight) {
        this.artifactInflight.delete(id);
      }
      const waiters = [...flight.waiters];
      flight.waiters.clear();
      for (const waiter of waiters) {
        try {
          waiter.deliver(owner.acquire(waiter));
        } catch (error) {
          waiter.fail(error);
        }
      }
      try {
        await owner.disposeIfUnused();
      } catch (cleanupError) {
        logger.warn(
          { err: cleanupError },
          'failed to dispose an unclaimed segment spool artifact'
        );
      }
    } catch (error) {
      if (unownedArtifact) {
        try {
          await unownedArtifact.dispose();
        } catch (cleanupError) {
          logger.warn(
            { err: cleanupError },
            'failed to dispose an unowned segment spool artifact'
          );
        }
      }
      const normalized = flight.ctl.signal.aborted
        ? new NntpError('connection', 'aborted')
        : error instanceof Error
          ? error
          : new UsenetSpoolError(
              'USENET_SPOOL_IO',
              'Segment artifact fetch failed',
              { cause: error }
            );
      const owner = flight.growingOwner;
      if (owner) {
        flight.growingOwner = undefined;
        flight.growingOwnerPublished = false;
        try {
          owner.producerFailed(normalized);
          await owner.disposeIfUnused();
        } catch (cleanupError) {
          logger.warn(
            { err: cleanupError },
            'failed to dispose a failed shared spool artifact'
          );
        }
      }
      if (this.artifactInflight.get(id) === flight) {
        this.artifactInflight.delete(id);
      }
      const kind = definitiveLossKind(normalized);
      if (kind) this.recordMiss(id, kind);
      const waiters = [...flight.waiters];
      flight.waiters.clear();
      for (const waiter of waiters) waiter.fail(normalized);
    } finally {
      wire.end();
      releaseGlobal?.();
      memoryLease?.release();
    }
  }

  private startArtifactPromotion(
    runtime: SegmentSpoolingRuntime,
    messageId: string,
    artifact: GrowingSpoolArtifact,
    metadata: DecodedSegmentMetadata,
    owner: SharedSpoolArtifactOwner
  ): void {
    if (runtime.artifactCache?.promotionEnabled === false) {
      runtime.recordPromotion('skipped');
      return;
    }
    const promote = runtime.artifactCache?.promote;
    if (!promote) {
      runtime.recordPromotion('skipped');
      return;
    }
    let source: ReturnType<GrowingSpoolArtifact['acquirePromotion']>;
    try {
      source = artifact.acquirePromotion();
    } catch (error) {
      runtime.recordPromotion('skipped');
      logger.debug({ err: error }, 'segment cache promotion was skipped');
      return;
    }
    let promotion: Promise<boolean>;
    try {
      promotion = promote.call(
        runtime.artifactCache,
        messageId,
        metadata,
        source.path,
        (bytes) => runtime.tryAcquirePromotionMemory(bytes)
      );
    } catch (error) {
      source.release();
      runtime.recordPromotion('failed');
      logger.debug({ err: error }, 'segment cache promotion failed to start');
      return;
    }
    const protectedPromotion = promotion
      .then(
        (installed) => {
          runtime.recordPromotion(installed ? 'success' : 'skipped');
          return installed;
        },
        (error: unknown) => {
          runtime.recordPromotion('failed');
          logger.debug({ err: error }, 'segment cache promotion failed');
          return false;
        }
      )
      .catch((error: unknown) => {
        // The event observer is user-provided and isolated by the runtime, but
        // preserve best-effort playback even if a future hook changes shape.
        logger.debug({ err: error }, 'segment cache promotion failed');
        return false;
      })
      .finally(() => source.release());
    owner.trackPromotion(protectedPromotion);
  }

  /** Reserve one complete bounded output-stream memory window. */
  acquireSegmentStreamMemory(
    bytes: number,
    priority: CommandPriority,
    signal?: AbortSignal
  ): Promise<ByteLease> {
    try {
      this.assertOpen();
    } catch (error) {
      return Promise.reject(error);
    }
    const runtime = this.spooling;
    if (!runtime) {
      return Promise.reject(
        new UsenetSpoolError(
          'USENET_SPOOL_UNAVAILABLE',
          'Segment stream memory requires segment-spooling mode'
        )
      );
    }
    return runtime.acquireStreamMemory(bytes, priority, signal);
  }

  recordSegmentStreamCleanup(cause: SegmentStreamCleanupCause): void {
    this.spooling?.recordStreamCleanup(cause);
  }

  private resolveEarlyArtifactMetadata(
    flight: ArtifactFlight,
    header: DecodedSegmentHeaderMetadata
  ): DecodedSegmentMetadata | undefined {
    const growingWaiters = [...flight.waiters].filter(
      (waiter) => waiter.allowGrowing
    );
    if (growingWaiters.length === 0) return undefined;

    let expectedLength = header.expectedSize;
    if (expectedLength !== undefined) {
      const eligible = growingWaiters.some(
        (waiter) =>
          waiter.expectedLength === undefined ||
          waiter.expectedLength === expectedLength
      );
      if (!eligible) return undefined;
    } else {
      for (const waiter of growingWaiters) {
        if (waiter.expectedLength === undefined) continue;
        if (
          expectedLength !== undefined &&
          expectedLength !== waiter.expectedLength
        ) {
          return undefined;
        }
        expectedLength = waiter.expectedLength;
      }
    }
    if (expectedLength === undefined) return undefined;
    return {
      byteRange: header.byteRange,
      fileSize: header.fileSize,
      totalParts: header.totalParts,
      name: header.name,
      size: expectedLength,
    };
  }

  private publishGrowingArtifact(
    flight: ArtifactFlight,
    owner: SharedSpoolArtifactOwner
  ): void {
    if (flight.growingOwner !== owner || flight.ctl.signal.aborted) return;
    flight.growingOwnerPublished = true;
    const matching = [...flight.waiters].filter(
      (waiter) => waiter.allowGrowing && owner.matches(waiter)
    );
    for (const waiter of matching) {
      flight.waiters.delete(waiter);
      waiter.deliver(owner.acquire(waiter));
    }
  }

  private async assertArtifactMetadata(
    artifact: SegmentArtifact,
    expectations: ArtifactExpectations
  ): Promise<void> {
    try {
      assertMetadataExpectations(artifact.metadata, expectations, 'Stored');
      if (artifact.length !== artifact.metadata.size) {
        throw artifactMetadataMismatch('Stored');
      }
      return;
    } catch (error) {
      await artifact.release();
      throw error;
    }
  }

  /** The single flight behind {@link fetchSegmentShared}. */
  private async runShared(
    segment: NzbSegmentRef,
    nzbHash: string,
    priority: CommandPriority,
    flight: SharedFlight
  ): Promise<void> {
    const id = segment.messageId;
    let lease: ArenaLease | null | undefined;
    try {
      let data = await this.cache.getAsync(id);
      this.assertOpen();
      if (data) {
        // Disk hit: promote into the arena (one memcpy) when a slot is free,
        // so serve-path re-touches stop paying the disk round-trip.
        lease = this.arena.checkout(data.body.length);
        if (lease) {
          data.body.copy(lease.slot, 0);
          data = { ...data, body: lease.slot.subarray(0, data.body.length) };
        }
      } else {
        const need = Math.max(1 << 20, segment.bytes ?? 0);
        data = await this.runFetch(
          segment,
          nzbHash,
          priority,
          () =>
            (lease ??= this.arena.checkout(need))?.slot ??
            Buffer.allocUnsafe(need),
          flight.ctl.signal
        );
        if (lease && data.body.buffer !== lease.slot.buffer) {
          // Decode fell back to an owned buffer (oversized/undeclared bytes).
          this.arena.abandon(lease);
          lease = null;
        }
      }
      // Deliver in one synchronous block: unregister the flight, commit the
      // slot, and grant one pin per still-registered waiter, so no checkout
      // (hence no eviction) can interleave.
      if (this.sharedInflight.get(id) === flight) {
        this.sharedInflight.delete(id);
      }
      if (lease) {
        this.arena.commit(lease, id, data);
        for (const w of flight.waiters) {
          w.deliver(this.arena.acquireCommitted(id));
        }
      } else {
        const shared = ownedShared(data);
        for (const w of flight.waiters) w.deliver(shared);
      }
    } catch (err) {
      if (lease) this.arena.abandon(lease);
      if (this.sharedInflight.get(id) === flight) {
        this.sharedInflight.delete(id);
      }
      for (const w of flight.waiters) w.fail(err);
    } finally {
      if (this.sharedInflight.get(id) === flight) {
        this.sharedInflight.delete(id);
      }
      flight.waiters.clear();
    }
  }

  /**
   * Fetch + decode one segment into a caller-owned buffer (a per-stream decode
   * slot). `out` is invoked lazily at decode time: cache hits never check a
   * slot out, and no slot is held while waiting on the download semaphore.
   * Deliberately not single-flighted: the body may be a view into the slot,
   * whose recycle policy belongs to this one caller. Concurrent streams of the
   * same file may duplicate a fetch until the disk tier catches up.
   */
  async fetchSegmentInto(
    segment: NzbSegmentRef,
    nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority,
    out: () => Buffer
  ): Promise<SegmentData> {
    this.assertOpen();
    // Arena hit: copy into the caller's slot to keep the stream's
    // `body.buffer === slot.buffer` bookkeeping intact; oversized bodies fall
    // back to an owned copy.
    const pinned = this.arena.acquire(segment.messageId);
    if (pinned) {
      try {
        const body = pinned.data.body;
        const dst = out();
        if (dst.length >= body.length) {
          body.copy(dst, 0);
          return { ...pinned.data, body: dst.subarray(0, body.length) };
        }
        return { ...pinned.data, body: Buffer.from(body) };
      } finally {
        pinned.release();
      }
    }
    const cached = this.cachedMiss(segment.messageId);
    if (cached !== undefined) {
      throw this.cachedMissError(segment.messageId, cached);
    }
    // Disk hits return owned bodies (fresh deserialize) and ignore `out`.
    const fromDisk = await this.cache.getAsync(segment.messageId);
    this.assertOpen();
    if (fromDisk) return fromDisk;
    return awaitAbortable(
      this.runFetch(segment, nzbHash, priority, out, signal),
      signal
    );
  }

  private runFetch(
    segment: NzbSegmentRef,
    nzbHash: string,
    priority: CommandPriority,
    out?: () => Buffer,
    /**
     * Cancels the download while it is still queued at the global semaphore,
     * so an abandoned prefetch does not hold its FIFO position ahead of live
     * streams. Once granted, the fetch completes and warms the cache.
     */
    signal?: AbortSignal
  ): Promise<SegmentData> {
    return this.trackOperation(() =>
      this.runFetchOnce(segment, nzbHash, priority, out, signal)
    );
  }

  private async runFetchOnce(
    segment: NzbSegmentRef,
    nzbHash: string,
    priority: CommandPriority,
    out?: () => Buffer,
    signal?: AbortSignal
  ): Promise<SegmentData> {
    const operationSignal = this.operationSignal(signal);
    let releaseGlobal: () => void;
    try {
      releaseGlobal = await this.globalDownloads.acquire(
        priority,
        operationSignal
      );
    } catch (error) {
      if (this.closedError) throw this.closedError;
      throw new NntpError('connection', 'aborted');
    }
    const wire = this.wireTracker();
    try {
      const data = await this.fetcher.fetchBody(
        segment,
        nzbHash,
        priority,
        out,
        operationSignal,
        wire.start
      );
      this.assertOpen();
      // Write-through for ALL priorities, including import probes that still take
      // the full path (par2, mid-volume header reads). RAM is protected by the
      // bounded pending-write queue, not by skipping the writes. Slot-backed
      // bodies must skip the mem tier (see SegmentCache.set).
      this.cache.set(segment.messageId, data, { skipMem: out !== undefined });
      return data;
    } catch (err) {
      // Negatively cache only a definitive all-providers verdict
      const kind = definitiveLossKind(err);
      if (kind) this.recordMiss(segment.messageId, kind);
      throw err;
    } finally {
      wire.end();
      releaseGlobal();
    }
  }

  /**
   * Head-only probe fetch: stream the article's raw payload, decode just the
   * leading `want` bytes + yEnc header fields, and let the rest drain on the
   * wire; no full-article buffer, no decode of the remainder, no cache write.
   * Same provider failover semantics as {@link fetchSegment}. Single-flighted
   * (fill/repost NZBs probe the same article under multiple files); an
   * already-cached body is reused.
   */
  async fetchSegmentHead(
    segment: NzbSegmentRef,
    nzbHash: string,
    signal: AbortSignal | undefined,
    priority: CommandPriority,
    want: number
  ): Promise<SegmentHeadData> {
    this.assertOpen();
    const fromHit = (d: SegmentData): SegmentHeadData => ({
      head: Buffer.from(d.body.subarray(0, want)),
      byteRange: d.byteRange,
      fileSize: d.fileSize,
      totalParts: d.totalParts,
      name: d.name,
      size: d.size,
    });
    const pinned = this.arena.acquire(segment.messageId);
    if (pinned) {
      try {
        return fromHit(pinned.data); // head is copied while pinned
      } finally {
        pinned.release();
      }
    }

    let shared = this.inflightHeads.get(segment.messageId);
    if (!shared) {
      if (this.inflightHeads.size >= HEAD_FLIGHT_MAX) {
        throw new UsenetSpoolError(
          'USENET_SPOOL_CAPACITY',
          'Segment head flight capacity reached'
        );
      }
      const promise = this.trackOperation(
        async (): Promise<SegmentHeadData> => {
          const fromDisk = await this.cache.getAsync(segment.messageId);
          this.assertOpen();
          if (fromDisk) return fromHit(fromDisk);
          const operationSignal = this.operationSignal();
          const releaseGlobal = await this.globalDownloads.acquire(
            priority,
            operationSignal
          );
          const wire = this.wireTracker();
          try {
            return await this.fetcher.fetchHead(
              segment,
              nzbHash,
              priority,
              want,
              wire.start,
              operationSignal
            );
          } finally {
            wire.end();
            releaseGlobal();
          }
        }
      );
      shared = promise;
      this.inflightHeads.set(segment.messageId, promise);
      void promise
        .catch(() => undefined)
        .finally(() => {
          if (this.inflightHeads.get(segment.messageId) === promise) {
            this.inflightHeads.delete(segment.messageId);
          }
        });
    }
    return awaitAbortable(shared, this.operationSignal(signal));
  }

  /**
   * Cheap existence probe (STAT) across providers, used by health checks /
   * inspect. Does NOT consume the global download budget. Returns true if any
   * provider has the article.
   */
  async statSegment(
    messageId: string,
    signal: AbortSignal | undefined,
    nzbHash?: string
  ): Promise<boolean> {
    this.assertOpen();
    if (this.arena.has(messageId)) return true;
    return this.trackOperation(() =>
      this.fetcher.statSegment(
        messageId,
        nzbHash,
        CommandPriority.Low,
        this.operationSignal(signal)
      )
    );
  }

  /**
   * STAT probe that reports WHICH provider answered and can restrict the
   * candidate set (census evidence with per-provider STAT trust). An arena hit
   * is authoritative present with no `answeredBy` (no provider was asked, so
   * trust calibration must ignore it).
   */
  async statSegmentDetailed(
    messageId: string,
    signal: AbortSignal | undefined,
    nzbHash?: string,
    providerIds?: readonly string[]
  ): Promise<StatDetail> {
    this.assertOpen();
    if (this.arena.has(messageId)) return { present: true, answered: true };
    return this.trackOperation(() =>
      this.fetcher.statSegmentDetailed(
        messageId,
        nzbHash,
        CommandPriority.Low,
        this.operationSignal(signal),
        providerIds
      )
    );
  }

  /**
   * BODY probe on exactly one provider (STAT-trust calibration): transfer
   * discarded, no failover, no cache. Takes a Low-priority download slot so a
   * calibration burst can never oversubscribe the account's sockets.
   */
  async probeBodyOnProvider(
    segment: NzbSegmentRef,
    providerId: string,
    signal?: AbortSignal
  ): Promise<'ok' | 'not_found' | 'unreachable'> {
    this.assertOpen();
    return this.trackOperation(async () => {
      const operationSignal = this.operationSignal(signal);
      const releaseGlobal = await this.globalDownloads.acquire(
        CommandPriority.Low,
        operationSignal
      );
      const wire = this.wireTracker();
      try {
        return await this.fetcher.probeBodyOnProvider(
          segment,
          providerId,
          operationSignal,
          wire.start
        );
      } finally {
        wire.end();
        releaseGlobal();
      }
    });
  }

  /** Configured provider ids, in priority order. */
  providerIds(): string[] {
    return this.fetcher.providerIds();
  }

  /** Download slots currently leased (in-flight article fetches). */
  get downloadsInUse(): number {
    return this.globalDownloads.inUse;
  }

  /** Leased download slots whose transfer has actually started on a connection. */
  get downloadsOnWire(): number {
    return this.onWireCount;
  }

  poolInfo(): PoolInfo {
    return {
      providers: this.fetcher.info(),
      globalDownloadsInUse: this.globalDownloads.inUse,
      globalDownloadMax: this.globalDownloads.capacity,
      globalDownloadsOnWire: this.onWireCount,
      globalDownloadsWaiting: this.globalDownloads.waiting,
    };
  }

  /** Actual segment-spooling owners, absent in the compatible buffering mode. */
  spoolingStats(): SegmentSpoolingRuntimeStats | undefined {
    return this.spooling?.stats();
  }

  purgeStaleIdles(): void {
    this.fetcher.purgeStaleIdles();
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    const error = new UsenetSpoolError(
      'USENET_SPOOL_CLOSED',
      'Segment artifact pool is closed'
    );
    this.closedError = error;
    this.closeController.abort(error);
    this.globalDownloads.close(error);

    for (const flight of this.sharedInflight.values()) {
      flight.ctl.abort(error);
      const waiters = [...flight.waiters];
      flight.waiters.clear();
      for (const waiter of waiters) waiter.fail(error);
    }
    for (const flight of this.artifactInflight.values()) {
      flight.ctl.abort(error);
      const waiters = [...flight.waiters];
      flight.waiters.clear();
      for (const waiter of waiters) waiter.fail(error);
    }

    const errors: unknown[] = [];
    try {
      this.fetcher.close();
    } catch (fetcherError) {
      errors.push(fetcherError);
    }
    this.closePromise = this.closeOnce(errors);
    return this.closePromise;
  }

  private async closeOnce(errors: unknown[]): Promise<void> {
    // No operation can enter after closedError is published, so this snapshot
    // is the complete bounded flight set owned by this pool.
    await Promise.allSettled([...this.activeOperations]);
    this.sharedInflight.clear();
    this.artifactInflight.clear();
    this.inflightHeads.clear();
    this.missCache.clear();
    if (this.spooling) {
      try {
        await this.spooling.close();
      } catch (error) {
        errors.push(error);
      }
    }
    if (errors.length > 0) {
      throw new AggregateError(errors, 'Usenet provider pool close failed');
    }
  }
}
