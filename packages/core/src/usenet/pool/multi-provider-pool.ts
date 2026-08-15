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
import { YencDecodeError } from './yenc.js';
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
  type SegmentRangeMetadata,
  type SegmentArtifact,
} from './segment-artifact.js';
import { SegmentSpoolingRuntime } from './segment-spooling-runtime.js';
import { SpoolingSegmentSink } from './spooling-segment-sink.js';
import type {
  DecodedSegmentHeaderMetadata,
  DecodedSegmentMetadata,
} from './streaming-yenc-article-decoder.js';
import type { GrowingSpoolArtifact } from '../spool/growing-artifact.js';
import { UsenetSpoolError } from '../spool/errors.js';
import { resolveEstimatedDecodedSegmentBytes } from '../resource-plan.js';
import type { ByteLease } from './byte-budget.js';

const logger = createLogger('usenet/multi-provider-pool');

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
}

/** One caller waiting for an independently releasable file-backed handle. */
interface ArtifactWaiter {
  readonly expectedLength: number | undefined;
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
}

/** Optional construction seams used by the engine and deterministic tests. */
export interface MultiProviderPoolDependencies {
  readonly fetcher?: SegmentFetcher;
  readonly spooling?: SegmentSpoolingRuntime;
}

class SharedSpoolArtifactOwner {
  private references = 0;
  private disposePromise: Promise<void> | undefined;
  private producerActive: boolean;
  private metadataValue: DecodedSegmentMetadata;
  private readonly producerCompletion =
    Promise.withResolvers<DecodedSegmentMetadata>();
  private producerSettled = false;

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

  get expectedLength(): number {
    return this.metadataValue.size;
  }

  owns(artifact: GrowingSpoolArtifact): boolean {
    return this.artifact === artifact;
  }

  acquire(): SegmentArtifact {
    if (this.disposePromise) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CLOSED',
        'Cannot acquire a disposed shared spool artifact'
      );
    }
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
      this.producerCompletion.promise,
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

  disposeIfUnused(): Promise<void> {
    return !this.producerActive && this.references === 0
      ? this.dispose()
      : Promise.resolve();
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
      flight = { waiters: new Set(), ctl: new AbortController() };
      this.sharedInflight.set(id, flight);
    }
    const joined = flight;
    const p = new Promise<SharedSegment>((resolve, reject) => {
      // An aborting waiter deregisters itself before delivery, so pins are
      // granted only to waiters that will consume them. While any waiter
      // remains the fetch runs without its signal, so one abandoning caller
      // cannot poison the flight for the others; the last waiter to leave
      // cancels a still-queued acquire via the flight controller. Waiters
      // without a signal never deregister.
      let onAbort: (() => void) | undefined;
      const done = (): void => {
        if (onAbort) signal!.removeEventListener('abort', onAbort);
      };
      const waiter: SharedWaiter = {
        deliver: (h) => {
          done();
          resolve(h);
        },
        fail: (e) => {
          done();
          reject(e);
        },
      };
      joined.waiters.add(waiter);
      if (signal) {
        onAbort = () => {
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
    if (isNew) void this.runShared(segment, nzbHash, priority, joined);
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
    const expectedLength = options.expectedLength;
    const allowGrowing = options.allowGrowing ?? false;
    if (
      expectedLength !== undefined &&
      (!Number.isSafeInteger(expectedLength) || expectedLength <= 0)
    ) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Expected segment length must be a safe positive integer'
      );
    }
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
      await this.assertArtifactLength(artifact, expectedLength);
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
        if (signal?.aborted) {
          await persistent.release();
          throw new NntpError('connection', 'aborted');
        }
        await this.assertArtifactLength(persistent, expectedLength);
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
      expectedLength,
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
    priority: CommandPriority = CommandPriority.High
  ): Promise<SegmentRangeMetadata> {
    const id = segment.messageId;
    const pinned = this.arena.acquire(id);
    if (pinned) {
      try {
        return {
          byteRange: pinned.data.byteRange,
          fileSize: pinned.data.fileSize,
          totalParts: pinned.data.totalParts,
          name: pinned.data.name,
          decodedSize: pinned.data.size,
        };
      } finally {
        pinned.release();
      }
    }
    if (signal?.aborted) throw new NntpError('connection', 'aborted');

    const persistent = await this.spooling?.artifactCache?.acquire(id, signal);
    if (persistent) {
      try {
        return {
          byteRange: persistent.metadata.byteRange,
          fileSize: persistent.metadata.fileSize,
          totalParts: persistent.metadata.totalParts,
          name: persistent.metadata.name,
          decodedSize: persistent.metadata.size,
        };
      } finally {
        await persistent.release();
      }
    }

    const cached = this.cachedMiss(id);
    if (cached !== undefined) throw this.cachedMissError(id, cached);
    const releaseGlobal = await this.globalDownloads.acquire(priority, signal);
    const wire = this.wireTracker();
    try {
      const head = await this.fetcher.fetchHead(
        segment,
        nzbHash,
        priority,
        0,
        wire.start,
        signal
      );
      return {
        byteRange: head.byteRange,
        fileSize: head.fileSize,
        totalParts: head.totalParts,
        name: head.name,
        decodedSize: head.size,
      };
    } catch (error) {
      const kind = definitiveLossKind(error);
      if (kind) this.recordMiss(id, kind);
      throw error;
    } finally {
      wire.end();
      releaseGlobal();
    }
  }

  private joinArtifactFlight(
    segment: NzbSegmentRef,
    nzbHash: string,
    priority: CommandPriority,
    signal: AbortSignal | undefined,
    expectedLength: number | undefined,
    allowGrowing: boolean
  ): Promise<SegmentArtifact> {
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
        expectedLength,
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
        (expectedLength === undefined ||
          owner.expectedLength === expectedLength)
      ) {
        joined.waiters.delete(waiter);
        waiter.deliver(owner.acquire());
      }
    });
    if (isNew) {
      void this.runArtifactFlight(segment, nzbHash, priority, joined);
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
          });
          const sink = new SpoolingSegmentSink(
            artifact,
            downloadMemoryLease.bytes,
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
      unownedArtifact = result.value;
      let owner = flight.growingOwner;
      if (owner?.owns(result.value)) {
        owner.producerCompleted(result.metadata);
      } else {
        owner = new SharedSpoolArtifactOwner(result.value, result.metadata);
        flight.growingOwner = owner;
      }
      unownedArtifact = undefined;
      if (this.artifactInflight.get(id) === flight) {
        this.artifactInflight.delete(id);
      }
      const waiters = [...flight.waiters];
      flight.waiters.clear();
      for (const waiter of waiters) {
        if (
          waiter.expectedLength !== undefined &&
          waiter.expectedLength !== result.metadata.size
        ) {
          waiter.fail(this.segmentLengthMismatch());
          continue;
        }
        waiter.deliver(owner.acquire());
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

  /** Reserve one complete bounded output-stream memory window. */
  acquireSegmentStreamMemory(
    priority: CommandPriority,
    signal?: AbortSignal
  ): Promise<ByteLease> {
    const runtime = this.spooling;
    if (!runtime) {
      return Promise.reject(
        new UsenetSpoolError(
          'USENET_SPOOL_UNAVAILABLE',
          'Segment stream memory requires segment-spooling mode'
        )
      );
    }
    return runtime.acquireStreamMemory(priority, signal);
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
      (waiter) =>
        waiter.allowGrowing &&
        (waiter.expectedLength === undefined ||
          waiter.expectedLength === owner.expectedLength)
    );
    for (const waiter of matching) {
      flight.waiters.delete(waiter);
      waiter.deliver(owner.acquire());
    }
  }

  private async assertArtifactLength(
    artifact: SegmentArtifact,
    expectedLength: number | undefined
  ): Promise<void> {
    if (expectedLength === undefined || artifact.length === expectedLength) {
      return;
    }
    await artifact.release();
    throw this.segmentLengthMismatch('Stored');
  }

  private segmentLengthMismatch(prefix = 'Decoded'): UsenetSpoolError {
    return new UsenetSpoolError(
      'USENET_SPOOL_IO',
      `${prefix} segment length differs from the exact file range`
    );
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
    if (fromDisk) return fromDisk;
    return awaitAbortable(
      this.runFetch(segment, nzbHash, priority, out, signal),
      signal
    );
  }

  private async runFetch(
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
    let releaseGlobal: () => void;
    try {
      releaseGlobal = await this.globalDownloads.acquire(priority, signal);
    } catch {
      throw new NntpError('connection', 'aborted');
    }
    const wire = this.wireTracker();
    try {
      const data = await this.fetcher.fetchBody(
        segment,
        nzbHash,
        priority,
        out,
        signal,
        wire.start
      );
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
      const promise = (async (): Promise<SegmentHeadData> => {
        const fromDisk = await this.cache.getAsync(segment.messageId);
        if (fromDisk) return fromHit(fromDisk);
        const releaseGlobal = await this.globalDownloads.acquire(
          priority,
          undefined
        );
        const wire = this.wireTracker();
        try {
          return await this.fetcher.fetchHead(
            segment,
            nzbHash,
            priority,
            want,
            wire.start
          );
        } finally {
          wire.end();
          releaseGlobal();
        }
      })();
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
    return awaitAbortable(shared, signal);
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
    if (this.arena.has(messageId)) return true;
    return this.fetcher.statSegment(
      messageId,
      nzbHash,
      CommandPriority.Low,
      signal
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
    if (this.arena.has(messageId)) return { present: true, answered: true };
    return this.fetcher.statSegmentDetailed(
      messageId,
      nzbHash,
      CommandPriority.Low,
      signal,
      providerIds
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
    const releaseGlobal = await this.globalDownloads.acquire(
      CommandPriority.Low,
      signal
    );
    const wire = this.wireTracker();
    try {
      return await this.fetcher.probeBodyOnProvider(
        segment,
        providerId,
        signal,
        wire.start
      );
    } finally {
      wire.end();
      releaseGlobal();
    }
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

  purgeStaleIdles(): void {
    this.fetcher.purgeStaleIdles();
  }

  close(): void {
    const error = new UsenetSpoolError(
      'USENET_SPOOL_CLOSED',
      'Segment artifact pool is closed'
    );
    for (const flight of this.artifactInflight.values()) {
      flight.ctl.abort(error);
      const waiters = [...flight.waiters];
      flight.waiters.clear();
      for (const waiter of waiters) waiter.fail(error);
    }
    this.artifactInflight.clear();
    this.fetcher.close();
    if (this.spooling) {
      void this.spooling.close().catch((closeError: unknown) => {
        logger.warn(
          { err: closeError },
          'failed to close segment-spooling resources'
        );
      });
    }
  }
}
