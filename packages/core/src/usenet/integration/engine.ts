import { appConfig } from '../../utils/index.js';
import { getCacheFolder } from '../../utils/general.js';
import {
  UsenetEngineRegistry,
  EngineOptions,
  ProviderConfig,
} from '../index.js';
import { resolveEngineResourcePlan } from '../resource-plan.js';
import {
  PERFORMANCE_PROFILES,
  type PerformanceProfile,
} from '../../config/schema/usenet.js';

/**
 * Per-process registry of warm engines, keyed by provider-set fingerprint.
 * Shared between the service (`resolve`) and the byte-serving route so
 * connection pools and the segment cache stay warm across requests.
 */
export const usenetEngineRegistry = new UsenetEngineRegistry();

/** Await every engine-owned stream, spool, budget and cache writer on shutdown. */
export function shutdownUsenetEngines(): Promise<void> {
  return usenetEngineRegistry.closeAll();
}

/** Human-facing summary of the per-stream knobs a (speed) test exercises. */
export interface UsenetStreamConfigSummary {
  /** In-flight BODY commands per connection (NNTP pipelining). */
  pipelineDepth: number;
  /** Per-stream read-ahead window in segments (also the per-stream parallelism). */
  prefetchSegments: number;
}

/** Runtime-setting subset consumed when constructing {@link EngineOptions}. */
export type UsenetEngineRuntimeSettings = Pick<
  (typeof appConfig)['usenet'],
  | 'performanceProfile'
  | 'maxConcurrentDownloads'
  | 'prefetchSegments'
  | 'streamingMode'
  | 'streamingPriority'
  | 'segmentMemoryCacheBytes'
  | 'segmentSpoolingMemoryBudgetBytes'
  | 'segmentSpoolingStreamBufferBytes'
  | 'segmentSpoolingSpoolBytes'
  | 'segmentSpoolingMinFreeDiskBytes'
  | 'segmentDiskCacheBytes'
  | 'segmentTimeout'
  | 'segmentStallTimeout'
  | 'dialTimeout'
  | 'idleConnection'
  | 'streamIdleTimeout'
  | 'circuitBreakerThreshold'
  | 'circuitBreakerCooldown'
  | 'lazyRarResolution'
  | 'strictArchiveMembership'
  | 'verifyMode'
  | 'verifyBudgetMs'
  | 'censusShadowConcurrency'
  | 'censusMaxLifetime'
>;

/**
 * Build the engine {@link EngineOptions} for a given provider set from the
 * DB-backed settings store. Duration settings are stored in seconds
 * (human-friendly) but the engine's options are in milliseconds, so they are
 * scaled here. Read at call-time (never at module load) so live settings edits
 * and env overrides are observed.
 *
 * `providers` scopes the auto-computed download budget (`maxConcurrentDownloads`
 * auto = Σ provider connections × pipeline depth), so passing a single provider
 * yields an isolated config; used by the per-provider speed test.
 * `runtimeSettings` and `cacheFolder` are injectable deterministic inputs for
 * unit tests; production callers omit them and therefore always read the
 * current `appConfig.usenet` snapshot and shared cache root.
 */
export function buildUsenetEngineOptions(
  providers: ProviderConfig[],
  runtimeSettings: UsenetEngineRuntimeSettings = appConfig.usenet,
  cacheFolder: () => string = getCacheFolder
): Partial<EngineOptions> {
  const u = runtimeSettings;
  const depthOf = (p: ProviderConfig): number =>
    Math.max(1, p.pipelineDepth ?? 1);
  const sumPipelineSlots = providers.reduce(
    (n, p) => n + (p.maxConnections || 0) * depthOf(p),
    0
  );
  // A performance profile bundles the speed/resource knobs; `custom` falls back
  // to the individual fields. Resolved at call-time so a profile switch in the
  // dashboard takes effect on the next stream without a restart.
  const profile: PerformanceProfile = u.performanceProfile;
  const preset =
    profile !== 'custom' ? PERFORMANCE_PROFILES[profile] : undefined;
  const prefetchSegments = preset?.prefetchSegments ?? u.prefetchSegments;
  const diskCacheBytes =
    preset?.segmentDiskCacheBytes ?? u.segmentDiskCacheBytes;
  // `0` means auto: size the global in-flight download budget to the total
  // pipeline-slot count (Σ maxConnections × depth) so it never throttles
  // pipelining. An explicit value is a hard ceiling the pool gate clamps to
  // (each account's sockets are still bounded by its own maxConnections).
  const maxDownloadSetting =
    preset?.maxConcurrentDownloads ?? u.maxConcurrentDownloads;
  const maxConcurrentDownloads =
    maxDownloadSetting > 0 ? maxDownloadSetting : Math.max(1, sumPipelineSlots);
  // All disk-backed caches share the `<data>/cache` root; the engine adds its
  // own per-provider-set namespace subdirectory under it.
  const diskCachePath = diskCacheBytes > 0 ? cacheFolder() : undefined;
  const options: EngineOptions = {
    maxConcurrentDownloads,
    prefetchSegments,
    streamingMode: u.streamingMode,
    streamingPriority: u.streamingPriority,
    segmentMemoryCacheBytes: u.segmentMemoryCacheBytes,
    segmentSpoolingMemoryBudgetBytes: u.segmentSpoolingMemoryBudgetBytes,
    segmentSpoolingStreamBufferBytes: u.segmentSpoolingStreamBufferBytes,
    segmentSpoolingSpoolBytes: u.segmentSpoolingSpoolBytes,
    segmentSpoolingMinFreeDiskBytes: u.segmentSpoolingMinFreeDiskBytes,
    segmentDiskCacheBytes: diskCacheBytes,
    segmentDiskCachePath: diskCachePath,
    segmentTimeoutMs: u.segmentTimeout * 1000,
    segmentStallTimeoutMs: u.segmentStallTimeout * 1000,
    dialTimeoutMs: u.dialTimeout * 1000,
    idleConnectionMs: u.idleConnection * 1000,
    streamIdleTimeoutMs: u.streamIdleTimeout * 1000,
    circuitBreakerThreshold: u.circuitBreakerThreshold,
    circuitBreakerCooldownMs: u.circuitBreakerCooldown * 1000,
    lazyRarResolution: u.lazyRarResolution,
    strictArchiveMembership: u.strictArchiveMembership,
    verifyMode: u.verifyMode,
    verifyBudgetMs: u.verifyBudgetMs,
    censusShadowConcurrency: u.censusShadowConcurrency,
    censusMaxLifetimeMs: u.censusMaxLifetime * 1000,
  };
  // Cross-field checks live in the pure resource-plan layer. This rejects an
  // invalid spooling selection before an engine/resource owner is constructed,
  // while the resolver deliberately ignores dormant spooling fields in the
  // compatible buffering mode.
  resolveEngineResourcePlan(options);
  return options;
}

/**
 * Resolve the global usenet engine configuration (every enabled provider) from
 * the DB-backed settings store, for the warm streaming engine.
 */
export function getUsenetEngineConfig(): {
  providers: ProviderConfig[];
  options: Partial<EngineOptions>;
} {
  const providers = (appConfig.usenet.providers as ProviderConfig[]).filter(
    (p) => p.enabled !== false
  );
  return { providers, options: buildUsenetEngineOptions(providers) };
}

/**
 * Resolve the streaming config a SINGLE provider runs under, for an isolated
 * speed test that replicates a real playback: the same {@link EngineOptions} the
 * engine would build for that provider alone, plus a human-facing summary of the
 * per-stream knobs being exercised (read-ahead window + pipeline depth). Lets the
 * dashboard show "tested at read-ahead R × depth D" so the knobs are tunable by
 * re-running.
 */
export function getSpeedTestEngineConfig(provider: ProviderConfig): {
  options: Partial<EngineOptions>;
  summary: UsenetStreamConfigSummary;
} {
  const u = appConfig.usenet;
  const options = buildUsenetEngineOptions([provider]);
  const pipelineDepth = Math.max(1, provider.pipelineDepth ?? 1);
  const prefetchSegments = options.prefetchSegments ?? u.prefetchSegments;
  return {
    options,
    summary: { pipelineDepth, prefetchSegments },
  };
}
