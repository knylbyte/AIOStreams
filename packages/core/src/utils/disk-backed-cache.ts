import { createHash, randomUUID } from 'node:crypto';
import {
  createReadStream,
  createWriteStream,
  promises as fs,
  type Stats,
} from 'node:fs';
import path from 'path';
import { pipeline } from 'node:stream/promises';
import { createLogger } from '../logging/logger.js';

const logger = createLogger('disk-cache');

/** Maximum chunk retained by generic prepared-file copy pipelines. */
export const DISK_CACHE_COPY_CHUNK_BYTES = 64 * 1024;

/** Maximum unresolved delete participants for one physical cache path. */
export const DISK_CACHE_DELETE_PARTICIPANT_LIMIT = 64;

function nodeErrorCode(error: unknown): string | undefined {
  if (typeof error !== 'object' || error === null || !('code' in error)) {
    return undefined;
  }
  return typeof error.code === 'string' ? error.code : undefined;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

interface ParsedDiskIndex {
  readonly entries: Array<[string, DiskEntry]>;
  readonly discardedEntries: boolean;
}

function parseDiskIndex(raw: string): ParsedDiskIndex {
  const parsed: unknown = JSON.parse(raw);
  if (!isRecord(parsed)) return { entries: [], discardedEntries: true };
  const entries: Array<[string, DiskEntry]> = [];
  let discardedEntries = false;
  for (const [fileKey, value] of Object.entries(parsed)) {
    if (!/^[a-f0-9]{40}$/.test(fileKey) || !isRecord(value)) {
      discardedEntries = true;
      continue;
    }
    const size = value.size;
    if (typeof size !== 'number' || !Number.isSafeInteger(size) || size < 0) {
      discardedEntries = true;
      continue;
    }
    entries.push([fileKey, { size }]);
  }
  return { entries, discardedEntries };
}

export type DiskBackedCacheErrorCode =
  | 'DISK_CACHE_CLOSED'
  | 'DISK_CACHE_DELETE_PARTICIPANT_LIMIT'
  | 'DISK_CACHE_INDEX_IO'
  | 'DISK_CACHE_PREPARED_LIMIT'
  | 'DISK_CACHE_PREPARED_INVALID';

/** Stable lifecycle/admission failures from {@link DiskBackedCache}. */
export class DiskBackedCacheError extends Error {
  readonly code: DiskBackedCacheErrorCode;

  constructor(
    code: DiskBackedCacheErrorCode,
    message: string,
    options?: ErrorOptions
  ) {
    super(message, options);
    this.name = 'DiskBackedCacheError';
    this.code = code;
  }
}

export type DiskBackedCacheFileSystem = Pick<
  typeof fs,
  | 'mkdir'
  | 'open'
  | 'lstat'
  | 'rm'
  | 'writeFile'
  | 'readFile'
  | 'readdir'
  | 'rename'
>;

/**
 * Codecs + budgets for a {@link DiskBackedCache}. Value-generic: the caller
 * supplies (de)serialisers and a weigher so the same module can back hot binary
 * segment payloads today and torrent/NZB download managers later.
 */
export interface DiskBackedCacheOptions<V> {
  /** Namespace → subdirectory + index file name. Must be filesystem-safe. */
  name: string;
  /** Base directory; the cache lives under `${dir}/${name}/`. */
  dir: string;
  /** L1 (in-memory) byte budget. `0` disables the memory tier. */
  maxMemBytes: number;
  /** L2 (on-disk) byte budget. `0` disables the disk tier. */
  maxDiskBytes: number;
  serialize: (value: V) => Buffer;
  deserialize: (buf: Buffer) => V;
  /** Decoded byte weight of a value (drives both budgets). */
  sizeOf: (value: V) => number;
  /**
   * Optional zero-alloc serializer: write the full serialized form of `value`
   * into `dst` (≥ {@link serializedSize} bytes) and return the bytes written.
   * When provided (with {@link serializedSize}), disk writes serialize
   * synchronously in {@link DiskBackedCache.set} through a pooled, recycled
   * write-buffer ring, so a transient/pooled `value` body is captured before it
   * can be reused, with no per-write `Buffer.concat`. Falls back to
   * {@link serialize} when absent.
   */
  serializeInto?: (value: V, dst: Buffer) => number;
  /** Exact serialized byte length of `value`. Required iff {@link serializeInto} is set. */
  serializedSize?: (value: V) => number;
  /** Testable cross-device seam; production defaults to `fs.rename`. */
  renameFile?: (source: string, destination: string) => Promise<void>;
  /** Narrow generic filesystem seam for deterministic lifecycle/I/O tests. */
  fileSystem?: Partial<DiskBackedCacheFileSystem>;
}

/**
 * A counted reference to one immutable serialized cache file.
 *
 * Logical LRU eviction removes the entry from cache accounting immediately,
 * while physical deletion is deferred until the final lease is released. This
 * keeps open readers valid on Windows as well as POSIX. Format-aware callers
 * finalize exactly one provisional stats outcome before release; all three
 * lifecycle methods are idempotent.
 */
export interface DiskFileLease {
  readonly path: string;
  readonly serializedBytes: number;
  /** Finalize this provisional lookup as one successful disk hit. */
  confirmHit(): void;
  /** Finalize it as one miss and invalidate the structurally corrupt entry. */
  invalidateAsMiss(): void;
  /**
   * Release exactly once. A non-final process lease resolves immediately; the
   * final lease observes every bounded predecessor through the delete intent
   * bound to this local entry and propagates that required attempt's failure.
   */
  release(): Promise<void>;
}

/**
 * A bounded, cache-owned staging file for {@link installPreparedFile}.
 * Callers write a serialized entry to {@link path}; install or release then
 * consumes the handle exactly once.
 */
export interface DiskPreparedFile {
  readonly path: string;
  release(): Promise<void>;
}

interface MemEntry<V> {
  value: V;
  size: number;
}

interface DiskEntry {
  size: number;
}

interface FileLeaseState {
  leases: number;
  pendingDelete: boolean;
  deletePromise?: Promise<void>;
  deleteIntent?: ProcessPathDeleteIntent;
}

interface UnindexedDeleteState {
  readonly fileKey: string;
  readonly path: string;
  readonly logMessage: string;
  readonly candidateFingerprint?: DiskPathFingerprint;
  operation?: Promise<void>;
  deleteIntent?: ProcessPathDeleteIntent;
  /** Admission saturation is terminal for this unadmitted local cleanup target. */
  admissionError?: DiskBackedCacheError;
  lastError?: unknown;
}

interface PreparedFileState {
  readonly path: string;
  readonly generation: number;
  status: 'active' | 'installing' | 'released';
  slotReleased: boolean;
  operation?: Promise<unknown>;
  releasePromise?: Promise<void>;
}

export interface DiskBackedCacheStats {
  memBytes: number;
  memCount: number;
  diskBytes: number;
  diskCount: number;
  hits: number;
  misses: number;
  /** Subset of hits that were served from the disk tier. */
  diskHits: number;
  hitRate: number;
}

/**
 * Live registry of all {@link DiskBackedCache} instances so the dashboard can
 * surface them alongside the Redis/SQL/memory caches. Instances register on
 * construction and unregister on {@link DiskBackedCache.close}.
 */
interface RegisteredDiskCache {
  readonly name: string;
  readonly maxMemBytes: number;
  readonly maxDiskBytes: number;
  stats(): DiskBackedCacheStats;
  clear(): Promise<void>;
  flush(): Promise<void>;
}

const diskCacheRegistry = new Set<RegisteredDiskCache>();

type ProcessPathDeleteOutcome = 'deleted' | 'absent' | 'superseded';

type ProcessPathDeleteTarget =
  | {
      readonly kind: 'observed-incarnation';
      readonly fingerprint: DiskPathFingerprint;
    }
  | {
      readonly kind: 'current-incarnation';
    };

type DiskPathType =
  | 'file'
  | 'directory'
  | 'symbolic-link'
  | 'block-device'
  | 'character-device'
  | 'fifo'
  | 'socket'
  | 'other';

/**
 * Bounded scalar identity of one observed filesystem entry. It intentionally
 * excludes atime (ordinary reads may change it) and file contents. Device and
 * inode are combined with type, size and change timestamps so a recycled path
 * is conservatively recognized without materializing a cache body.
 */
interface DiskPathFingerprint {
  readonly type: DiskPathType;
  readonly dev: number;
  readonly ino: number;
  readonly mode: number;
  readonly nlink: number;
  readonly uid: number;
  readonly gid: number;
  readonly rdev: number;
  readonly size: number;
  readonly blksize: number;
  readonly blocks: number;
  readonly mtimeMs: number;
  readonly ctimeMs: number;
  readonly birthtimeMs: number;
}

function diskPathType(stats: Stats): DiskPathType {
  if (stats.isFile()) return 'file';
  if (stats.isDirectory()) return 'directory';
  if (stats.isSymbolicLink()) return 'symbolic-link';
  if (stats.isBlockDevice()) return 'block-device';
  if (stats.isCharacterDevice()) return 'character-device';
  if (stats.isFIFO()) return 'fifo';
  if (stats.isSocket()) return 'socket';
  return 'other';
}

function diskPathFingerprint(stats: Stats): DiskPathFingerprint {
  return {
    type: diskPathType(stats),
    dev: stats.dev,
    ino: stats.ino,
    mode: stats.mode,
    nlink: stats.nlink,
    uid: stats.uid,
    gid: stats.gid,
    rdev: stats.rdev,
    size: stats.size,
    blksize: stats.blksize,
    blocks: stats.blocks,
    mtimeMs: stats.mtimeMs,
    ctimeMs: stats.ctimeMs,
    birthtimeMs: stats.birthtimeMs,
  };
}

function sameDiskPathFingerprint(
  left: DiskPathFingerprint,
  right: DiskPathFingerprint
): boolean {
  return (
    left.type === right.type &&
    left.dev === right.dev &&
    left.ino === right.ino &&
    left.mode === right.mode &&
    left.nlink === right.nlink &&
    left.uid === right.uid &&
    left.gid === right.gid &&
    left.rdev === right.rdev &&
    left.size === right.size &&
    left.blksize === right.blksize &&
    left.blocks === right.blocks &&
    left.mtimeMs === right.mtimeMs &&
    left.ctimeMs === right.ctimeMs &&
    left.birthtimeMs === right.birthtimeMs
  );
}

interface ProcessPathDeleteRequest {
  readonly target: ProcessPathDeleteTarget;
  run(target: ProcessPathDeleteTarget): Promise<ProcessPathDeleteOutcome>;
  onStart(operation: Promise<void>): void;
}

interface ProcessPathDeleteIntent {
  /** Immutable incarnation token; object identity is never inferred from path. */
  readonly id: symbol;
  /** The exact delete contract shared only by compatible participants. */
  readonly target: ProcessPathDeleteTarget;
  /** Bounded number of local states observing this target completion. */
  participants: number;
  /** Resolves exactly once after delete, confirmed absence or supersession. */
  readonly completion: Promise<ProcessPathDeleteOutcome>;
  readonly resolveCompletion: (outcome: ProcessPathDeleteOutcome) => void;
  status: 'unresolved' | 'resolved';
  outcome?: ProcessPathDeleteOutcome;
  attempt?: Promise<void>;
  pendingAttempt?: ProcessPathDeleteRequest;
  lastError?: unknown;
}

interface ProcessPathOwnershipState {
  leases: number;
  mutationClaimed: boolean;
  mutationRelease?: PromiseWithResolvers<void>;
  deleteIntent?: ProcessPathDeleteIntent;
  /** FIFO target successors; total participants are bounded per path. */
  readonly deleteSuccessors: ProcessPathDeleteIntent[];
  deleteParticipants: number;
}

interface ProcessPathMutationClaim {
  release(): Promise<void>;
}

interface ProcessPathLeaseRelease {
  readonly wasFinalProcessLease: boolean;
  readonly completion: Promise<void>;
}

interface ProcessPathDeleteRegistration {
  readonly intent: ProcessPathDeleteIntent;
  /** Present only when this exact request started the process-wide attempt. */
  readonly startedOperation?: Promise<void>;
}

interface ProcessPathDeleteAttemptResult {
  readonly operation?: Promise<void>;
  readonly started: boolean;
}

type ProcessPathDeleteInspection =
  | { readonly status: 'resolved' }
  | {
      readonly status: 'blocked' | 'ready';
      readonly current: ProcessPathDeleteIntent;
    }
  | {
      readonly status: 'active';
      readonly current: ProcessPathDeleteIntent;
      readonly operation: Promise<void>;
    };

/**
 * Process-local ownership shared across cache re-instantiation. Synchronous
 * lease/mutation claims and delete-intent publication are the linearization
 * points. A transient filesystem failure retains the same incarnation token,
 * blocks every new lease/mutation, and is retried only by an explicit caller.
 * Target-compatible participants share completion only after their own final
 * success/ENOENT/supersession. Incompatible targets advance through a bounded
 * FIFO successor chain without ever reopening the path between turns.
 */
const processPathOwnership = new Map<string, ProcessPathOwnershipState>();

function resolvedFilePath(filePath: string): string {
  return path.resolve(filePath);
}

function getProcessPathState(filePath: string): ProcessPathOwnershipState {
  const resolved = resolvedFilePath(filePath);
  const existing = processPathOwnership.get(resolved);
  if (existing) return existing;
  const created: ProcessPathOwnershipState = {
    leases: 0,
    mutationClaimed: false,
    deleteSuccessors: [],
    deleteParticipants: 0,
  };
  processPathOwnership.set(resolved, created);
  return created;
}

function cleanupProcessPathState(
  resolved: string,
  state: ProcessPathOwnershipState
): void {
  if (
    state.leases === 0 &&
    !state.mutationClaimed &&
    !state.mutationRelease &&
    !state.deleteIntent &&
    state.deleteSuccessors.length === 0 &&
    state.deleteParticipants === 0 &&
    processPathOwnership.get(resolved) === state
  ) {
    processPathOwnership.delete(resolved);
  }
}

function sameProcessPathDeleteTarget(
  left: ProcessPathDeleteTarget,
  right: ProcessPathDeleteTarget
): boolean {
  if (left.kind !== right.kind) return false;
  if (left.kind === 'current-incarnation') return true;
  return (
    right.kind === 'observed-incarnation' &&
    sameDiskPathFingerprint(left.fingerprint, right.fingerprint)
  );
}

function findProcessPathDeleteIntent(
  state: ProcessPathOwnershipState,
  target: ProcessPathDeleteTarget
): ProcessPathDeleteIntent | undefined {
  if (
    state.deleteIntent &&
    sameProcessPathDeleteTarget(state.deleteIntent.target, target)
  ) {
    return state.deleteIntent;
  }
  return state.deleteSuccessors.find((intent) =>
    sameProcessPathDeleteTarget(intent.target, target)
  );
}

function hasProcessPathDeleteIntent(
  state: ProcessPathOwnershipState,
  intent: ProcessPathDeleteIntent
): boolean {
  return (
    state.deleteIntent?.id === intent.id ||
    state.deleteSuccessors.some((candidate) => candidate.id === intent.id)
  );
}

function inspectProcessPathDelete(
  filePath: string,
  participant: ProcessPathDeleteIntent
): ProcessPathDeleteInspection {
  if (participant.status === 'resolved') return { status: 'resolved' };
  const resolved = resolvedFilePath(filePath);
  const state = processPathOwnership.get(resolved);
  if (!state || !hasProcessPathDeleteIntent(state, participant)) {
    throw new Error('Disk cache delete-intent invariant violated');
  }
  const current = state.deleteIntent;
  if (!current) {
    throw new Error('Disk cache delete-successor invariant violated');
  }
  if (current.attempt) {
    return {
      status: 'active',
      current,
      operation: current.attempt,
    };
  }
  if (state.leases > 0 || state.mutationClaimed) {
    return { status: 'blocked', current };
  }
  return { status: 'ready', current };
}

function startProcessPathDeleteAttempt(
  resolved: string,
  state: ProcessPathOwnershipState,
  intent: ProcessPathDeleteIntent,
  request?: ProcessPathDeleteRequest
): ProcessPathDeleteAttemptResult {
  if (state.deleteIntent?.id !== intent.id || intent.status === 'resolved') {
    return { started: false };
  }
  if (intent.attempt) {
    return { operation: intent.attempt, started: false };
  }
  if (state.leases > 0 || state.mutationClaimed) {
    return { started: false };
  }
  if (request && intent.lastError !== undefined) {
    // A caller explicitly retrying a failed intent supplies the next bounded
    // attempt. Before the first attempt, the original publisher remains owner
    // so a later lease release has a deterministic executor.
    intent.pendingAttempt = request;
  }
  const selectedRequest = intent.pendingAttempt;
  if (!selectedRequest) {
    throw new Error('Disk cache delete-attempt invariant violated');
  }
  // `lastError` describes a settled failed attempt only. Clearing it before
  // dispatch distinguishes live contention from the narrow rejection/finally
  // handoff that an explicit retry may safely bridge once.
  intent.lastError = undefined;
  let operation: Promise<void>;
  operation = Promise.resolve()
    .then(() => selectedRequest.run(intent.target))
    .then(
      (outcome) => {
        if (
          state.deleteIntent?.id !== intent.id ||
          intent.status === 'resolved'
        ) {
          return;
        }
        // Install the next incompatible target before publishing this target's
        // completion. This synchronous handoff is the no-gap linearization
        // point: leases and mutations remain fenced across successor turns.
        intent.status = 'resolved';
        intent.outcome = outcome;
        intent.lastError = undefined;
        intent.pendingAttempt = undefined;
        state.deleteParticipants -= intent.participants;
        if (state.deleteParticipants < 0) {
          throw new Error(
            'Disk cache delete-participant accounting invariant violated'
          );
        }
        state.deleteIntent = state.deleteSuccessors.shift();
        intent.resolveCompletion(outcome);
        if (state.deleteIntent) {
          // The successor starts under the same still-published path fence.
          // Its executor installs its own rejection observer synchronously.
          startProcessPathDeleteAttempt(resolved, state, state.deleteIntent);
        }
      },
      (error: unknown) => {
        intent.lastError = error;
        throw error;
      }
    )
    .finally(() => {
      if (intent.attempt === operation) intent.attempt = undefined;
      cleanupProcessPathState(resolved, state);
    });
  intent.attempt = operation;
  selectedRequest.onStart(operation);
  return { operation, started: true };
}

function tryAcquireProcessPathLease(filePath: string): boolean {
  const state = getProcessPathState(filePath);
  if (state.mutationClaimed || state.deleteIntent) return false;
  state.leases++;
  return true;
}

async function waitForProcessPathDeleteIntent(
  resolved: string,
  state: ProcessPathOwnershipState,
  participant: ProcessPathDeleteIntent
): Promise<void> {
  // A path can contain at most one intent per registered participant. Two
  // observations per participant cover an active/predecessor handoff plus one
  // bounded mutation-release transition. No polling or retry is involved.
  const maxTurns = DISK_CACHE_DELETE_PARTICIPANT_LIMIT * 2 + 2;
  for (let turn = 0; turn < maxTurns; turn++) {
    if (participant.status === 'resolved') return;
    if (!hasProcessPathDeleteIntent(state, participant)) {
      throw new Error('Disk cache delete-intent invariant violated');
    }
    const current = state.deleteIntent;
    if (!current) {
      throw new Error('Disk cache delete-successor invariant violated');
    }
    if (current.attempt) {
      await current.attempt;
      continue;
    }
    if (state.leases > 0) {
      throw new Error('Disk cache final-lease invariant violated');
    }
    if (state.mutationClaimed) {
      const mutationRelease = state.mutationRelease;
      if (!mutationRelease) {
        throw new Error('Disk cache mutation-release invariant violated');
      }
      await mutationRelease.promise;
      continue;
    }
    const attempt = startProcessPathDeleteAttempt(resolved, state, current);
    if (!attempt.operation) {
      throw new Error('Disk cache delete-attempt invariant violated');
    }
    await attempt.operation;
  }
  throw new Error('Disk cache delete completion progress limit exceeded');
}

function releaseProcessPathLease(
  filePath: string,
  participant?: ProcessPathDeleteIntent
): ProcessPathLeaseRelease {
  const resolved = resolvedFilePath(filePath);
  const state = processPathOwnership.get(resolved);
  if (!state || state.leases <= 0) {
    throw new Error('Disk cache process lease accounting invariant violated');
  }
  state.leases--;
  const wasFinalProcessLease = state.leases === 0;
  if (!wasFinalProcessLease) {
    return { wasFinalProcessLease, completion: Promise.resolve() };
  }
  const awaitedIntent = participant ?? state.deleteIntent;
  if (awaitedIntent) {
    return {
      wasFinalProcessLease,
      completion: waitForProcessPathDeleteIntent(
        resolved,
        state,
        awaitedIntent
      ),
    };
  }
  cleanupProcessPathState(resolved, state);
  return { wasFinalProcessLease, completion: Promise.resolve() };
}

function tryClaimProcessPathMutation(
  filePath: string
): ProcessPathMutationClaim | undefined {
  const resolved = resolvedFilePath(filePath);
  const state = getProcessPathState(resolved);
  if (state.leases > 0 || state.mutationClaimed || state.deleteIntent) {
    cleanupProcessPathState(resolved, state);
    return undefined;
  }
  state.mutationClaimed = true;
  const mutationRelease = Promise.withResolvers<void>();
  state.mutationRelease = mutationRelease;
  let releasePromise: Promise<void> | undefined;
  return {
    release: () => {
      if (releasePromise) return releasePromise;
      state.mutationClaimed = false;
      state.mutationRelease = undefined;
      try {
        const attempt = state.deleteIntent
          ? startProcessPathDeleteAttempt(resolved, state, state.deleteIntent)
          : undefined;
        releasePromise = attempt?.operation ?? Promise.resolve();
      } catch (error) {
        releasePromise = Promise.reject(error);
      }
      // Publish progress after a queued delete has been started under the same
      // synchronous fence. Final lease releasers can now observe that attempt.
      mutationRelease.resolve();
      cleanupProcessPathState(resolved, state);
      return releasePromise;
    },
  };
}

function requestProcessPathDelete(
  filePath: string,
  request: ProcessPathDeleteRequest
): ProcessPathDeleteRegistration {
  const resolved = resolvedFilePath(filePath);
  const state = getProcessPathState(resolved);
  if (
    !Number.isSafeInteger(state.deleteParticipants) ||
    state.deleteParticipants >= DISK_CACHE_DELETE_PARTICIPANT_LIMIT
  ) {
    throw new DiskBackedCacheError(
      'DISK_CACHE_DELETE_PARTICIPANT_LIMIT',
      'Disk cache delete participant limit reached'
    );
  }
  let intent = findProcessPathDeleteIntent(state, request.target);
  if (!intent) {
    const completion = Promise.withResolvers<ProcessPathDeleteOutcome>();
    intent = {
      id: Symbol('disk-cache-delete-intent'),
      target: request.target,
      participants: 0,
      completion: completion.promise,
      resolveCompletion: completion.resolve,
      status: 'unresolved',
      pendingAttempt: request,
    };
    if (state.deleteIntent) {
      state.deleteSuccessors.push(intent);
    } else {
      // Synchronous publication is the delete-intent linearization point. From
      // here through every successor both lease and mutation claims fail closed.
      state.deleteIntent = intent;
    }
  }
  intent.participants++;
  state.deleteParticipants++;
  const current = state.deleteIntent;
  if (!current) {
    throw new Error('Disk cache delete-successor invariant violated');
  }
  const result = startProcessPathDeleteAttempt(
    resolved,
    state,
    current,
    current.id === intent.id || current.lastError !== undefined
      ? request
      : undefined
  );
  return {
    intent,
    startedOperation: result.started ? result.operation : undefined,
  };
}

function retryProcessPathDelete(
  filePath: string,
  intent: ProcessPathDeleteIntent,
  request: ProcessPathDeleteRequest
): Promise<void> | undefined {
  if (intent.status === 'resolved') {
    return intent.completion.then(() => undefined);
  }
  const resolved = resolvedFilePath(filePath);
  const state = processPathOwnership.get(resolved);
  if (!state || !hasProcessPathDeleteIntent(state, intent)) {
    // An unresolved token must remain installed until its final transition.
    throw new Error('Disk cache delete-intent invariant violated');
  }
  const current = state.deleteIntent;
  if (!current) {
    throw new Error('Disk cache delete-successor invariant violated');
  }
  const result = startProcessPathDeleteAttempt(
    resolved,
    state,
    current,
    current.id === intent.id || current.lastError !== undefined
      ? request
      : undefined
  );
  // A participant joining an existing attempt observes the same operation.
  // An incompatible successor observes the active predecessor first; explicit
  // cleanup can then advance through the bounded successor chain.
  return result.operation;
}

/** Snapshot every live disk-backed cache for the dashboard cache page. */
export function describeDiskCaches(): {
  name: string;
  maxMemBytes: number;
  maxDiskBytes: number;
  stats: DiskBackedCacheStats;
}[] {
  return [...diskCacheRegistry].map((c) => ({
    name: c.name,
    maxMemBytes: c.maxMemBytes,
    maxDiskBytes: c.maxDiskBytes,
    stats: c.stats(),
  }));
}

/** Clear one registered disk cache by name. Returns false if not found. */
export async function clearDiskCacheByName(name: string): Promise<boolean> {
  const cache = [...diskCacheRegistry].find((c) => c.name === name);
  if (!cache) return false;
  await cache.clear();
  return true;
}

/**
 * Drain in-flight writes and persist every registered disk cache's index.
 */
export async function flushAllDiskCaches(): Promise<void> {
  await Promise.allSettled([...diskCacheRegistry].map((c) => c.flush()));
}

/**
 * Two-tier, byte-bounded, restart-surviving cache: a hot in-memory LRU (L1) in
 * front of an on-disk LRU overflow (L2). One file per key under a namespaced
 * directory, plus a persisted index so the disk budget + LRU survive restarts.
 *
 * Modelled on StremThru's `internal/cache/disk_backed.go` but value-generic via
 * injected codecs. Deliberately independent of the Redis/SQL {@link Cache}: the
 * semantics differ (sync hot-path `get`, byte-bounded eviction, binary values).
 *
 * Writes are write-through but non-blocking: `set` updates L1 synchronously and
 * persists to disk in the background, so the hot path never awaits disk I/O.
 */
export class DiskBackedCache<V> {
  private static readonly MAX_PENDING_WRITES = 64;
  private static readonly MAX_PENDING_WRITE_BYTES = 128 * 1024 * 1024;

  private mem = new Map<string, MemEntry<V>>();
  private memBytes = 0;
  /** L2 index, insertion-order = LRU order (re-inserted on access). */
  private disk = new Map<string, DiskEntry>();
  private diskBytes = 0;

  private hits = 0;
  private misses = 0;
  private diskHits = 0;

  private readonly dir: string;
  private readonly indexPath: string;
  private readonly opts: DiskBackedCacheOptions<V>;

  /** In-flight disk writes keyed by file key, so reads can await consistency. */
  private pendingWrites = new Map<string, Promise<void>>();
  /** Approximate bytes held by in-flight disk writes (serialized payloads). */
  private pendingWriteBytes = 0;
  /** Serialises index persistence. */
  private indexFlush: Promise<void> = Promise.resolve();
  private indexDirty = false;
  /** Independent durability signal retained after a failed background flush. */
  private indexFlushFailed = false;
  /** Pending debounced index-persist timer (see {@link scheduleIndexFlush}). */
  private flushTimer?: NodeJS.Timeout;
  private ready: Promise<void>;
  private closed = false;
  /** Monotone destructive epoch; only clear invalidates admitted mutations. */
  private generation = 0;
  /** Fail-safe startup state: memory stays usable, this disk namespace does not. */
  private diskUnavailable = false;
  private clearing = false;
  private clearPromise?: Promise<void>;
  private closePromise?: Promise<void>;
  /** File ownership states remain only while leased or awaiting deletion. */
  private readonly fileLeases = new Map<string, FileLeaseState>();
  /** Physical deletes currently running; bounded by known namespace paths. */
  private readonly pendingDeletes = new Set<Promise<void>>();
  /**
   * Retryable orphan/stale-file deletes, coalesced by absolute path. The map is
   * bounded by the finite files discovered in this namespace plus bounded
   * in-flight mutation destinations; successful/ENOENT cleanup removes entries.
   */
  private readonly unindexedDeletes = new Map<string, UnindexedDeleteState>();
  /** Staging handles are bounded by the same admission cap as writes. */
  private readonly preparedFiles = new Map<
    DiskPreparedFile,
    PreparedFileState
  >();
  /** Reserved before async creation and held through active/installing state. */
  private preparedSlots = 0;
  private readonly pendingPreparedCreations = new Set<
    Promise<DiskPreparedFile>
  >();
  private readonly fileSystem: DiskBackedCacheFileSystem;
  private readonly renameFile: (
    source: string,
    destination: string
  ) => Promise<void>;

  constructor(opts: DiskBackedCacheOptions<V>) {
    this.opts = opts;
    this.dir = path.join(opts.dir, opts.name);
    this.indexPath = path.join(opts.dir, `${opts.name}.index.json`);
    this.fileSystem = {
      mkdir: opts.fileSystem?.mkdir ?? fs.mkdir,
      open: opts.fileSystem?.open ?? fs.open,
      lstat: opts.fileSystem?.lstat ?? fs.lstat,
      rm: opts.fileSystem?.rm ?? fs.rm,
      writeFile: opts.fileSystem?.writeFile ?? fs.writeFile,
      readFile: opts.fileSystem?.readFile ?? fs.readFile,
      readdir: opts.fileSystem?.readdir ?? fs.readdir,
      rename: opts.fileSystem?.rename ?? fs.rename,
    };
    this.renameFile = opts.renameFile ?? this.fileSystem.rename;
    this.ready = this.load();
    diskCacheRegistry.add(this);
  }

  /** Namespace of this cache (drives the on-disk subdirectory + index file). */
  get name(): string {
    return this.opts.name;
  }

  /** L1 (memory) byte budget. */
  get maxMemBytes(): number {
    return this.opts.maxMemBytes;
  }

  /** L2 (disk) byte budget. */
  get maxDiskBytes(): number {
    return this.opts.maxDiskBytes;
  }

  /** Resolves once the on-disk index has been loaded + reconciled. */
  whenReady(): Promise<void> {
    return this.ready;
  }

  private diskEnabled(): boolean {
    return this.opts.maxDiskBytes > 0 && !this.diskUnavailable;
  }

  private fileKey(key: string): string {
    return createHash('sha1').update(key).digest('hex');
  }

  private filePath(fileKey: string): string {
    return path.join(this.dir, fileKey);
  }

  /** Load the persisted index, then reconcile index ↔ files (drop orphans). */
  private async load(): Promise<void> {
    if (!this.diskEnabled()) return;
    try {
      await this.fileSystem.mkdir(this.dir, { recursive: true });
      let entries: Array<[string, DiskEntry]> = [];
      let rawIndex: string | undefined;
      let reconciled = false;
      try {
        rawIndex = await this.fileSystem.readFile(this.indexPath, 'utf8');
      } catch (error) {
        // No index yet — first run or it was removed.
        if (nodeErrorCode(error) !== 'ENOENT') throw error;
      }
      if (rawIndex !== undefined) {
        try {
          const parsed = parseDiskIndex(rawIndex);
          entries = parsed.entries;
          if (parsed.discardedEntries) reconciled = true;
        } catch {
          // Invalid JSON is reconciled to a clean empty index.
          reconciled = true;
        }
      }
      // Build one complete, fail-safe startup observation before publishing
      // logical state or scheduling destructive cleanup. Orphan fingerprints
      // bind later deletes to the exact entry observed by this scan.
      const present = new Set(await this.fileSystem.readdir(this.dir));
      const observed = new Map<
        string,
        { readonly stats: Stats; readonly fingerprint: DiskPathFingerprint }
      >();
      for (const fileKey of present) {
        try {
          const stats = await this.fileSystem.lstat(this.filePath(fileKey));
          observed.set(fileKey, {
            stats,
            fingerprint: diskPathFingerprint(stats),
          });
        } catch (error) {
          if (nodeErrorCode(error) !== 'ENOENT') throw error;
          // A directory entry that disappeared during the snapshot is absent,
          // not a candidate that may be deleted by a later stale observation.
        }
      }

      // Index → keep only safe regular files and reconcile legacy index sizes
      // against their actual serialized byte length.
      const stagedDisk = new Map<string, DiskEntry>();
      let stagedDiskBytes = 0;
      for (const [fileKey, entry] of entries) {
        const observation = observed.get(fileKey);
        if (!observation || typeof entry?.size !== 'number') {
          reconciled = true;
          continue;
        }
        const { stats } = observation;
        if (
          !stats.isFile() ||
          !Number.isSafeInteger(stats.size) ||
          stats.size < 0
        ) {
          reconciled = true;
          continue;
        }
        stagedDisk.set(fileKey, { size: stats.size });
        stagedDiskBytes += stats.size;
        if (entry.size !== stats.size) reconciled = true;
      }
      // Commit the logical scan only after index read, directory read and all
      // indexed-file checks completed. Operational uncertainty must never turn
      // a partial startup scan into a destructive reconciliation.
      this.disk = stagedDisk;
      this.diskBytes = stagedDiskBytes;
      this.indexDirty = reconciled;
      // Files → delete any not referenced by the index (StremThru cleanOrphaned).
      for (const [fileKey, observation] of observed) {
        if (!this.disk.has(fileKey)) {
          await this.scheduleUnindexedPathDelete(
            fileKey,
            'disk cache orphan cleanup was deferred',
            observation.fingerprint
          );
        }
      }
      // The index may have shrunk; trim to budget.
      this.evictDisk();
      if (this.indexDirty) this.scheduleIndexFlush();
      logger.debug(
        {
          name: this.opts.name,
          diskCount: this.disk.size,
          diskBytes: this.diskBytes,
        },
        'disk cache loaded'
      );
    } catch (err) {
      this.diskUnavailable = true;
      this.disk.clear();
      this.diskBytes = 0;
      this.indexDirty = false;
      this.indexFlushFailed = false;
      this.cancelIndexFlushTimer();
      logger.warn(
        { name: this.opts.name, err: errorMessage(err) },
        'disk cache startup scan failed; preserving disk namespace and continuing memory-only'
      );
    }
  }

  /** Sync L1 lookup for the hot path. Does NOT count a miss (disk may hold it). */
  get(key: string): V | undefined {
    const entry = this.mem.get(key);
    if (!entry) return undefined;
    this.hits++;
    // Refresh LRU recency.
    this.mem.delete(key);
    this.mem.set(key, entry);
    return entry.value;
  }

  /** L1 peek without touching hit/miss counters. */
  private peek(key: string): V | undefined {
    const entry = this.mem.get(key);
    if (!entry) return undefined;
    this.mem.delete(key);
    this.mem.set(key, entry);
    return entry.value;
  }

  /**
   * Acquire an immutable file-backed L2 hit without reading its payload.
   * Acquisition touches LRU recency but returns a provisional stats outcome:
   * format-aware callers confirm the hit only after validating their header.
   * Logical eviction may proceed while leased, but physical unlink waits for
   * release.
   */
  async acquireDiskFile(
    key: string,
    signal?: AbortSignal
  ): Promise<DiskFileLease | undefined> {
    if (this.diskUnavailable || this.closed || this.clearing) {
      return undefined;
    }
    if (!this.diskEnabled()) {
      this.misses++;
      return undefined;
    }
    const generation = this.generation;
    signal?.throwIfAborted();
    await this.ready.catch(() => undefined);
    signal?.throwIfAborted();
    if (!this.isCurrentGeneration(generation)) return undefined;
    const fileKey = this.fileKey(key);
    const pending = this.pendingWrites.get(fileKey);
    if (pending) await pending.catch(() => undefined);
    signal?.throwIfAborted();
    if (!this.isCurrentGeneration(generation)) return undefined;

    const entry = this.disk.get(fileKey);
    const existingState = this.fileLeases.get(fileKey);
    if (!entry || existingState?.pendingDelete) {
      this.misses++;
      return undefined;
    }

    const dataPath = this.filePath(fileKey);
    // No await between the logical index check and both reference increments.
    // This process-wide claim is the lease linearization point: a destructive
    // mutation claimed before it makes this lookup fall through without stats;
    // a lease claimed first keeps the immutable path alive through release.
    if (!tryAcquireProcessPathLease(dataPath)) return undefined;
    const state = existingState ?? { leases: 0, pendingDelete: false };
    state.leases++;
    this.fileLeases.set(fileKey, state);

    let serializedBytes: number;
    try {
      const stats = await this.fileSystem.lstat(dataPath);
      if (
        !stats.isFile() ||
        !Number.isSafeInteger(stats.size) ||
        stats.size < 0
      ) {
        // Publish logical invalidation while this lookup still owns its
        // process-wide lease. The resulting intent closes the path before the
        // final old lease can be released and a newer incarnation admitted.
        this.dropDisk(fileKey);
        await this.releaseInvalidatedFileLease(fileKey, state);
        this.misses++;
        return undefined;
      }
      serializedBytes = stats.size;
    } catch (error) {
      if (nodeErrorCode(error) === 'ENOENT') {
        // There must be no await between the confirmed stale observation and
        // intent publication. Physical ENOENT confirmation remains tied to
        // this intent and may safely run after the lease is released.
        try {
          this.dropDisk(fileKey);
        } catch (deleteError) {
          await this.releaseFileLease(fileKey, state);
          throw deleteError;
        }
        await this.releaseInvalidatedFileLease(fileKey, state);
        if (signal?.aborted) throw signal.reason;
        this.misses++;
        return undefined;
      }
      await this.releaseFileLease(fileKey, state);
      if (signal?.aborted) throw signal.reason;
      throw error;
    }
    try {
      signal?.throwIfAborted();
    } catch (error) {
      await this.releaseFileLease(fileKey, state);
      throw error;
    }
    if (!this.isCurrentGeneration(generation)) {
      await this.releaseFileLease(fileKey, state);
      return undefined;
    }

    const current = this.disk.get(fileKey);
    if (!current || state.pendingDelete) {
      await this.releaseFileLease(fileKey, state);
      this.misses++;
      return undefined;
    }
    if (current.size !== serializedBytes) {
      this.diskBytes += serializedBytes - current.size;
      this.assertDiskAccounting();
    }
    this.disk.delete(fileKey);
    this.disk.set(fileKey, { size: serializedBytes });
    this.indexDirty = true;
    this.evictDisk();
    this.scheduleIndexFlush();
    if (!this.disk.has(fileKey)) {
      await this.releaseFileLease(fileKey, state);
      this.misses++;
      return undefined;
    }

    let outcome: 'hit' | 'miss' | undefined;
    let released = false;
    let releasePromise: Promise<void> | undefined;
    return {
      path: dataPath,
      serializedBytes,
      confirmHit: () => {
        if (outcome) return;
        outcome = 'hit';
        this.hits++;
        this.diskHits++;
      },
      invalidateAsMiss: () => {
        if (outcome) return;
        outcome = 'miss';
        this.misses++;
        this.dropDisk(fileKey);
      },
      release: () => {
        if (releasePromise) return releasePromise;
        if (released) return Promise.resolve();
        released = true;
        releasePromise = this.releaseFileLease(fileKey, state);
        return releasePromise;
      },
    };
  }

  /** L1 → L2 lookup; promotes disk hits back into memory. */
  async getAsync(key: string): Promise<V | undefined> {
    const hot = this.peek(key);
    if (hot !== undefined) {
      this.hits++;
      return hot;
    }
    // Capture before the first async disk operation. Clear increments this
    // token synchronously; close is covered by isCurrentGeneration().
    const generation = this.generation;
    const lease = await this.acquireDiskFile(key);
    if (!lease) return undefined;
    try {
      if (!this.isCurrentGeneration(generation)) return undefined;
      let buf: Buffer;
      try {
        buf = await this.fileSystem.readFile(lease.path);
      } catch (error) {
        if (
          this.isCurrentGeneration(generation) &&
          nodeErrorCode(error) === 'ENOENT'
        ) {
          lease.invalidateAsMiss();
        }
        return undefined;
      }
      if (!this.isCurrentGeneration(generation)) return undefined;
      let value: V;
      let decodedSize: number;
      try {
        value = this.opts.deserialize(buf);
        if (!this.isCurrentGeneration(generation)) return undefined;
        decodedSize = this.opts.sizeOf(value);
      } catch {
        if (this.isCurrentGeneration(generation)) lease.invalidateAsMiss();
        return undefined;
      }
      // Deserialization may be caller-supplied code. Re-check immediately
      // before every publish action so an older lookup can never cross clear
      // or close and repopulate L1 / finalize a hit in the new lifecycle.
      if (!this.isCurrentGeneration(generation)) return undefined;
      this.addToMem(key, value, decodedSize);
      if (!this.isCurrentGeneration(generation)) return undefined;
      lease.confirmHit();
      if (!this.isCurrentGeneration(generation)) return undefined;
      return value;
    } finally {
      await lease.release();
    }
  }

  /**
   * Insert into L1 and (unless `skipDisk`) write-through to the disk tier in
   * the background. `skipDisk` is for transient payloads (e.g. import-probe
   * article bodies) that benefit from the hot L1 but would only churn the disk.
   * `skipMem` is for values whose backing memory the caller recycles: the
   * disk serialize copies them out synchronously, but the mem tier must not
   * retain the view.
   */
  set(
    key: string,
    value: V,
    opts?: { skipDisk?: boolean; skipMem?: boolean }
  ): void {
    if (this.closed || this.clearing) return;
    const size = this.opts.sizeOf(value);
    if (size <= 0) return;
    const fitsMem = this.opts.maxMemBytes > 0 && size <= this.opts.maxMemBytes;
    const fitsDisk = this.diskEnabled() && size <= this.opts.maxDiskBytes;
    if (!fitsMem && !fitsDisk) return; // larger than every budget

    if (fitsMem && !opts?.skipMem) this.addToMem(key, value, size);
    if (fitsDisk && !opts?.skipDisk) this.persistToDisk(key, value, size);
  }

  private addToMem(key: string, value: V, size: number): void {
    if (this.opts.maxMemBytes <= 0) return;
    const existing = this.mem.get(key);
    if (existing) {
      this.memBytes -= existing.size;
      this.mem.delete(key);
    }
    this.mem.set(key, { value, size });
    this.memBytes += size;
    while (this.memBytes > this.opts.maxMemBytes && this.mem.size > 0) {
      const oldestEntry = this.mem.keys().next();
      if (oldestEntry.done) break;
      const oldest = oldestEntry.value;
      const e = this.mem.get(oldest);
      this.mem.delete(oldest);
      if (e) this.memBytes -= e.size;
    }
  }

  /**
   * Recycled write-buffer ring for the zero-alloc serialize path. Bounded by the
   * same {@link MAX_PENDING_WRITES} backpressure; a slot returns to the pool once
   * its `fs.writeFile` settles.
   */
  private writePool: Buffer[] = [];

  private acquireWriteBuf(size: number): Buffer {
    const slot = this.writePool.pop();
    if (slot && slot.length >= size) return slot;
    return Buffer.allocUnsafe(Math.max(size, 1 << 20));
  }

  private releaseWriteBuf(buf: Buffer): void {
    if (this.writePool.length < DiskBackedCache.MAX_PENDING_WRITES) {
      this.writePool.push(buf);
    }
  }

  /** Serialize synchronously, then atomically persist in a bounded background slot. */
  private persistToDisk(key: string, value: V, _decodedSize: number): void {
    const generation = this.generation;
    const fileKey = this.fileKey(key);
    if (
      this.pendingWrites.has(fileKey) ||
      this.pendingWrites.size >= DiskBackedCache.MAX_PENDING_WRITES ||
      this.fileLeases.has(fileKey)
    ) {
      return;
    }
    // Zero-alloc path: serialize SYNCHRONOUSLY into a pooled slot (capturing a
    // transient/pooled `value` body before it can be reused), then write the
    // slot's bytes and recycle it. Falls back to the allocating `serialize` when
    // the codec doesn't provide the into-form.
    const into = this.opts.serializeInto;
    const sizer = this.opts.serializedSize;
    let slot: Buffer | undefined;
    let payload: Buffer;
    if (into && sizer) {
      slot = this.acquireWriteBuf(sizer(value));
      payload = slot.subarray(0, into(value, slot));
    } else {
      payload = this.opts.serialize(value);
    }
    const serializedBytes = payload.length;
    if (
      serializedBytes <= 0 ||
      serializedBytes > this.opts.maxDiskBytes ||
      this.pendingWriteBytes + serializedBytes >
        DiskBackedCache.MAX_PENDING_WRITE_BYTES
    ) {
      if (slot) this.releaseWriteBuf(slot);
      return;
    }

    this.pendingWriteBytes += serializedBytes;
    const tempPath = path.join(this.dir, `.write-${randomUUID()}`);
    let write: Promise<void>;
    const run = async (): Promise<void> => {
      try {
        await this.ready.catch(() => undefined);
        if (!this.isCurrentMutationGeneration(generation)) return;
        await this.fileSystem.mkdir(this.dir, { recursive: true });
        if (!this.isCurrentMutationGeneration(generation)) return;
        await this.fileSystem.writeFile(tempPath, payload, {
          flag: 'wx',
          mode: 0o600,
        });
        if (!this.isCurrentMutationGeneration(generation)) return;
        const destination = this.filePath(fileKey);
        const mutation = tryClaimProcessPathMutation(destination);
        if (!mutation) return;
        try {
          if (!this.isCurrentMutationGeneration(generation)) return;
          await this.replaceBackgroundFile(tempPath, destination);
          if (!this.isCurrentMutationGeneration(generation)) {
            await this.scheduleUnindexedPathDelete(
              fileKey,
              'disk cache stale background destination cleanup was deferred'
            );
            return;
          }
          this.commitDiskEntry(fileKey, serializedBytes);
        } finally {
          await mutation.release();
        }
      } catch (err) {
        logger.debug(
          { name: this.opts.name, err: errorMessage(err) },
          'disk cache write failed'
        );
      } finally {
        await this.fileSystem
          .rm(tempPath, { force: true })
          .catch(() => undefined);
        if (slot) this.releaseWriteBuf(slot);
        this.releasePendingWriteBytes(serializedBytes);
        if (this.pendingWrites.get(fileKey) === write) {
          this.pendingWrites.delete(fileKey);
        }
        this.evictDisk();
      }
    };
    write = run();
    this.pendingWrites.set(fileKey, write);
  }

  private commitDiskEntry(fileKey: string, serializedBytes: number): void {
    const existing = this.disk.get(fileKey);
    if (existing) this.diskBytes -= existing.size;
    this.disk.delete(fileKey);
    this.disk.set(fileKey, { size: serializedBytes });
    this.diskBytes += serializedBytes;
    this.assertDiskAccounting();
    this.indexDirty = true;
    this.evictDisk();
    this.scheduleIndexFlush();
  }

  private async replaceBackgroundFile(
    source: string,
    destination: string
  ): Promise<void> {
    try {
      await this.renameFile(source, destination);
    } catch (error) {
      const code = nodeErrorCode(error);
      if (code !== 'EEXIST' && code !== 'EPERM') throw error;
      await this.fileSystem.rm(destination, { force: true });
      await this.renameFile(source, destination);
    }
  }

  /**
   * Create one secure, bounded staging file inside the cache namespace.
   */
  createPreparedFile(): Promise<DiskPreparedFile> {
    const generation = this.generation;
    if (!this.isCurrentGeneration(generation) || !this.diskEnabled()) {
      return Promise.reject(this.cacheClosedError());
    }
    if (this.preparedSlots >= DiskBackedCache.MAX_PENDING_WRITES) {
      return Promise.reject(
        new DiskBackedCacheError(
          'DISK_CACHE_PREPARED_LIMIT',
          'Disk cache prepared-file capacity reached'
        )
      );
    }
    // Reservation is synchronous: concurrent calls can never pass the cap.
    this.preparedSlots++;
    this.assertPreparedAccounting();
    let creation: Promise<DiskPreparedFile>;
    creation = this.createPreparedFileOnce(generation).finally(() => {
      this.pendingPreparedCreations.delete(creation);
    });
    this.pendingPreparedCreations.add(creation);
    return creation;
  }

  private async createPreparedFileOnce(
    generation: number
  ): Promise<DiskPreparedFile> {
    let preparedPath: string | undefined;
    let registered = false;
    try {
      await this.ready.catch(() => undefined);
      this.assertAcceptingGeneration(generation);
      await this.fileSystem.mkdir(this.dir, { recursive: true });
      this.assertAcceptingGeneration(generation);
      preparedPath = path.join(this.dir, `.prepared-${randomUUID()}`);
      const handle = await this.fileSystem.open(preparedPath, 'wx', 0o600);
      try {
        await handle.close();
      } catch (error) {
        await this.fileSystem
          .rm(preparedPath, { force: true })
          .catch(() => undefined);
        throw error;
      }
      this.assertAcceptingGeneration(generation);

      const state: PreparedFileState = {
        path: preparedPath,
        generation,
        status: 'active',
        slotReleased: false,
      };
      const prepared: DiskPreparedFile = {
        path: preparedPath,
        release: () => this.releasePreparedFile(prepared, state),
      };
      this.preparedFiles.set(prepared, state);
      registered = true;
      return prepared;
    } catch (error) {
      if (preparedPath) {
        await this.fileSystem
          .rm(preparedPath, { force: true })
          .catch(() => undefined);
      }
      if (!registered) this.releasePreparedSlot();
      throw error;
    }
  }

  /**
   * Atomically install a fully serialized prepared file under `key`.
   * Existing immutable entries win and merely receive an LRU touch. The
   * prepared handle is consumed on every outcome.
   */
  async installPreparedFile(
    key: string,
    prepared: DiskPreparedFile,
    serializedBytes: number
  ): Promise<boolean> {
    if (!Number.isSafeInteger(serializedBytes) || serializedBytes <= 0) {
      throw new RangeError(
        'Prepared disk cache size must be a safe positive integer'
      );
    }
    const state = this.preparedFiles.get(prepared);
    if (!state || state.status !== 'active') {
      throw new DiskBackedCacheError(
        'DISK_CACHE_PREPARED_INVALID',
        'Prepared disk cache file is not active'
      );
    }
    state.status = 'installing';
    const operation = this.installPreparedFileOnce(
      key,
      state.path,
      serializedBytes,
      state.generation
    );
    state.operation = operation;
    try {
      return await operation;
    } finally {
      state.status = 'released';
      await this.removePreparedFile(prepared, state);
    }
  }

  private async installPreparedFileOnce(
    key: string,
    preparedPath: string,
    serializedBytes: number,
    generation: number
  ): Promise<boolean> {
    await this.ready.catch(() => undefined);
    if (
      !this.isCurrentGeneration(generation) ||
      !this.diskEnabled() ||
      serializedBytes > this.opts.maxDiskBytes
    ) {
      return false;
    }
    const stats = await this.fileSystem.lstat(preparedPath);
    if (!stats.isFile() || stats.size !== serializedBytes) {
      throw new Error('Prepared disk cache file size is invalid');
    }

    const fileKey = this.fileKey(key);
    const previous = this.pendingWrites.get(fileKey);
    if (previous) await previous.catch(() => undefined);
    if (!this.isCurrentGeneration(generation) || !this.diskEnabled()) {
      return false;
    }
    const existing = this.disk.get(fileKey);
    if (existing) {
      this.disk.delete(fileKey);
      this.disk.set(fileKey, existing);
      this.indexDirty = true;
      this.scheduleIndexFlush();
      return false;
    }
    const leaseState = this.fileLeases.get(fileKey);
    if (leaseState?.pendingDelete && leaseState.leases === 0) {
      if (leaseState.deleteIntent?.lastError !== undefined) return false;
      try {
        await this.startPhysicalDelete(fileKey, leaseState);
      } catch (error) {
        logger.debug(
          { name: this.opts.name, err: errorMessage(error) },
          'disk cache stale destination cleanup failed'
        );
        return false;
      }
    }
    if (!this.isCurrentGeneration(generation)) return false;
    if (
      this.pendingWrites.has(fileKey) ||
      this.pendingWrites.size >= DiskBackedCache.MAX_PENDING_WRITES ||
      this.pendingWriteBytes + serializedBytes >
        DiskBackedCache.MAX_PENDING_WRITE_BYTES ||
      this.fileLeases.has(fileKey)
    ) {
      return false;
    }

    const destination = this.filePath(fileKey);
    const mutation = tryClaimProcessPathMutation(destination);
    if (!mutation) return false;
    this.pendingWriteBytes += serializedBytes;
    let install: Promise<void>;
    const run = async (): Promise<void> => {
      let operationFailed = false;
      let operationError: unknown;
      try {
        await this.movePreparedFile(
          preparedPath,
          destination,
          serializedBytes,
          fileKey
        );
        if (!this.isCurrentGeneration(generation)) {
          await this.scheduleUnindexedPathDelete(
            fileKey,
            'disk cache stale prepared destination cleanup was deferred'
          );
          return;
        }
        this.commitDiskEntry(fileKey, serializedBytes);
      } catch (error) {
        operationFailed = true;
        operationError = error;
        throw error;
      } finally {
        let releaseError: unknown;
        try {
          await mutation.release();
        } catch (error) {
          releaseError = error;
        } finally {
          this.releasePendingWriteBytes(serializedBytes);
          if (this.pendingWrites.get(fileKey) === install) {
            this.pendingWrites.delete(fileKey);
          }
          this.evictDisk();
        }
        if (releaseError) {
          if (operationFailed) {
            throw new AggregateError(
              [operationError, releaseError],
              'Prepared disk cache install cleanup failed'
            );
          }
          throw releaseError;
        }
      }
    };
    install = run();
    this.pendingWrites.set(fileKey, install);
    await install;
    return this.isCurrentGeneration(generation) && this.disk.has(fileKey);
  }

  private async movePreparedFile(
    source: string,
    destination: string,
    serializedBytes: number,
    fileKey: string
  ): Promise<void> {
    try {
      await this.renameFile(source, destination);
      return;
    } catch (error) {
      const code = nodeErrorCode(error);
      if (code === 'EEXIST' || code === 'EPERM') {
        await this.replaceUnindexedPreparedDestination(
          source,
          destination,
          fileKey
        );
        return;
      }
      if (code !== 'EXDEV') throw error;
    }

    const copyPath = path.join(this.dir, `.install-${randomUUID()}`);
    try {
      await pipeline(
        createReadStream(source, {
          highWaterMark: DISK_CACHE_COPY_CHUNK_BYTES,
        }),
        createWriteStream(copyPath, {
          flags: 'wx',
          mode: 0o600,
          highWaterMark: DISK_CACHE_COPY_CHUNK_BYTES,
        })
      );
      const copied = await this.fileSystem.lstat(copyPath);
      if (!copied.isFile() || copied.size !== serializedBytes) {
        throw new Error('Cross-device prepared-file copy was incomplete');
      }
      try {
        await this.renameFile(copyPath, destination);
      } catch (error) {
        const code = nodeErrorCode(error);
        if (code !== 'EEXIST' && code !== 'EPERM') throw error;
        await this.replaceUnindexedPreparedDestination(
          copyPath,
          destination,
          fileKey
        );
      }
    } finally {
      await Promise.allSettled([
        this.fileSystem.rm(copyPath, { force: true }),
        this.fileSystem.rm(source, { force: true }),
      ]);
    }
  }

  private async replaceUnindexedPreparedDestination(
    source: string,
    destination: string,
    fileKey: string
  ): Promise<void> {
    if (this.disk.has(fileKey) || this.fileLeases.has(fileKey)) {
      throw new DiskBackedCacheError(
        'DISK_CACHE_PREPARED_INVALID',
        'Prepared cache destination is still owned'
      );
    }
    try {
      const stats = await this.fileSystem.lstat(destination);
      if (!stats.isFile()) {
        throw new DiskBackedCacheError(
          'DISK_CACHE_PREPARED_INVALID',
          'Prepared cache destination is not a safe regular file'
        );
      }
      if (this.disk.has(fileKey) || this.fileLeases.has(fileKey)) {
        throw new DiskBackedCacheError(
          'DISK_CACHE_PREPARED_INVALID',
          'Prepared cache destination became owned'
        );
      }
      await this.fileSystem.rm(destination, { force: true });
    } catch (error) {
      if (nodeErrorCode(error) !== 'ENOENT') throw error;
    }
    // Exactly one retry; a second Windows sharing violation remains visible.
    await this.renameFile(source, destination);
  }

  private releasePreparedFile(
    prepared: DiskPreparedFile,
    state: PreparedFileState
  ): Promise<void> {
    if (state.releasePromise) return state.releasePromise;
    if (state.status === 'installing') {
      return Promise.resolve(state.operation).then(
        () => this.removePreparedFile(prepared, state),
        () => this.removePreparedFile(prepared, state)
      );
    }
    if (state.status === 'active') state.status = 'released';
    return this.removePreparedFile(prepared, state);
  }

  private removePreparedFile(
    prepared: DiskPreparedFile,
    state: PreparedFileState
  ): Promise<void> {
    if (state.slotReleased) return Promise.resolve();
    if (state.releasePromise) return state.releasePromise;
    let cleanup: Promise<void>;
    cleanup = this.fileSystem
      .rm(state.path, { force: true })
      .then(() => {
        this.preparedFiles.delete(prepared);
        this.releasePreparedSlot(state);
      })
      .finally(() => {
        if (state.releasePromise === cleanup) state.releasePromise = undefined;
      });
    state.releasePromise = cleanup;
    return cleanup;
  }

  /** Evict least-recently-used disk entries until within budget. */
  private evictDisk(): void {
    while (this.diskBytes > this.opts.maxDiskBytes && this.disk.size > 0) {
      let oldest: string | undefined;
      for (const fileKey of this.disk.keys()) {
        if (!this.pendingWrites.has(fileKey)) {
          oldest = fileKey;
          break;
        }
      }
      if (oldest === undefined) break;
      try {
        this.dropDisk(oldest);
      } catch (error) {
        if (
          error instanceof DiskBackedCacheError &&
          error.code === 'DISK_CACHE_DELETE_PARTICIPANT_LIMIT'
        ) {
          // Admission saturation keeps the immutable LRU entry and accounting
          // intact. A later explicit eviction may retry after the bounded
          // process-wide participant chain has completed.
          break;
        }
        throw error;
      }
    }
  }

  private dropDisk(fileKey: string): void {
    const entry = this.disk.get(fileKey);
    if (!entry) return;
    const state = this.fileLeases.get(fileKey) ?? {
      leases: 0,
      pendingDelete: false,
    };
    // Reserve and bind the bounded process-wide participant before publishing
    // any local logical deletion. `requestProcessPathDelete()` is synchronous;
    // filesystem I/O begins in its following microtask. Capacity rejection
    // therefore leaves disk/index/accounting and local retry state untouched.
    const deletion = this.startPhysicalDelete(fileKey, state);
    state.pendingDelete = true;
    this.fileLeases.set(fileKey, state);
    this.disk.delete(fileKey);
    this.diskBytes -= entry.size;
    this.assertDiskAccounting();
    this.indexDirty = true;
    this.scheduleIndexFlush();
    void deletion.catch(() => undefined);
  }

  private async releaseFileLease(
    fileKey: string,
    state: FileLeaseState
  ): Promise<void> {
    if (state.leases <= 0) {
      if (state.pendingDelete) return this.startPhysicalDelete(fileKey, state);
      return state.deletePromise ?? Promise.resolve();
    }
    state.leases--;
    let processReleaseError: unknown;
    try {
      const processRelease = releaseProcessPathLease(
        this.filePath(fileKey),
        state.pendingDelete ? state.deleteIntent : undefined
      );
      await processRelease.completion;
    } catch (error) {
      processReleaseError = error;
    }
    if (state.leases === 0) {
      if (state.pendingDelete) {
        const intent = state.deleteIntent;
        if (intent?.status === 'resolved') {
          this.completeFileDeleteIntent(fileKey, state, intent);
        }
      } else if (this.fileLeases.get(fileKey) === state) {
        this.fileLeases.delete(fileKey);
      }
    }
    if (processReleaseError) throw processReleaseError;
  }

  /**
   * Release a lookup lease after its logical entry was synchronously
   * invalidated. A physical cleanup failure is already retained by the shared
   * delete intent; it must not turn a definitive stale lookup into an
   * unhandled background rejection or accidentally reopen the path.
   */
  private async releaseInvalidatedFileLease(
    fileKey: string,
    state: FileLeaseState
  ): Promise<void> {
    try {
      await this.releaseFileLease(fileKey, state);
    } catch (error) {
      logger.debug(
        { name: this.opts.name, err: errorMessage(error) },
        'disk cache stale lookup cleanup remains pending'
      );
    }
  }

  private bindFileDeleteIntent(
    fileKey: string,
    state: FileLeaseState,
    intent: ProcessPathDeleteIntent
  ): void {
    if (state.deleteIntent === intent) return;
    if (state.deleteIntent && state.deleteIntent.status === 'unresolved') {
      throw new Error('Disk cache local delete-intent invariant violated');
    }
    state.deleteIntent = intent;
    void intent.completion.then(() => {
      this.completeFileDeleteIntent(fileKey, state, intent);
    });
  }

  private completeFileDeleteIntent(
    fileKey: string,
    state: FileLeaseState,
    intent: ProcessPathDeleteIntent
  ): void {
    if (state.deleteIntent !== intent || intent.status !== 'resolved') return;
    state.deleteIntent = undefined;
    state.deletePromise = undefined;
    state.pendingDelete = false;
    if (state.leases === 0 && this.fileLeases.get(fileKey) === state) {
      this.fileLeases.delete(fileKey);
    }
  }

  /**
   * Remove one path under its already-published process intent. Startup
   * candidates carry a fingerprint captured by the complete fail-safe scan;
   * indexed/stale-destination deletes intentionally omit it because their
   * logical delete itself owns the current incarnation.
   */
  private async removePathForDeleteIntent(
    dataPath: string,
    target: ProcessPathDeleteTarget
  ): Promise<ProcessPathDeleteOutcome> {
    if (target.kind === 'observed-incarnation') {
      let current: Stats;
      try {
        current = await this.fileSystem.lstat(dataPath);
      } catch (error) {
        if (nodeErrorCode(error) === 'ENOENT') return 'absent';
        throw error;
      }
      if (
        !sameDiskPathFingerprint(
          target.fingerprint,
          diskPathFingerprint(current)
        )
      ) {
        return 'superseded';
      }
    }

    try {
      await this.fileSystem.rm(dataPath, { force: true });
      return 'deleted';
    } catch (error) {
      if (nodeErrorCode(error) === 'ENOENT') return 'absent';
      throw error;
    }
  }

  private startPhysicalDelete(
    fileKey: string,
    state: FileLeaseState
  ): Promise<void> {
    if (state.deletePromise) return state.deletePromise;
    const dataPath = this.filePath(fileKey);
    let deletion: Promise<void> | undefined;
    const request: ProcessPathDeleteRequest = {
      target: { kind: 'current-incarnation' },
      run: async (target) => {
        try {
          return await this.removePathForDeleteIntent(dataPath, target);
        } catch (error) {
          logger.debug(
            { name: this.opts.name, err: errorMessage(error) },
            'disk cache deferred delete failed'
          );
          throw error;
        }
      },
      onStart: (operation) => {
        deletion = operation;
        state.deletePromise = operation;
        this.pendingDeletes.add(operation);
        const finishAttempt = (): void => {
          if (state.deletePromise === deletion) {
            state.deletePromise = undefined;
          }
          if (deletion) this.pendingDeletes.delete(deletion);
        };
        // Keep the attempt registered through the process intent's rejection
        // and finally transition. Explicit flush/close can then deterministically
        // await it before issuing their one bounded retry.
        void operation.then(finishAttempt, finishAttempt);
        // File-lease callers still observe the original rejection. This
        // additional observer prevents a deferred cleanup started by a caller
        // that intentionally ignores release() from becoming unhandled.
        void operation.catch(() => undefined);
      },
    };
    const currentIntent = state.deleteIntent;
    if (currentIntent?.status === 'resolved') {
      this.completeFileDeleteIntent(fileKey, state, currentIntent);
      return this.startPhysicalDelete(fileKey, state);
    }
    if (currentIntent) {
      return (
        retryProcessPathDelete(dataPath, currentIntent, request) ??
        Promise.resolve()
      );
    }
    const registration = requestProcessPathDelete(dataPath, request);
    this.bindFileDeleteIntent(fileKey, state, registration.intent);
    return registration.startedOperation ?? Promise.resolve();
  }

  private scheduleUnindexedPathDelete(
    fileKey: string,
    logMessage: string,
    candidateFingerprint?: DiskPathFingerprint
  ): Promise<void> {
    const dataPath = this.filePath(fileKey);
    const resolvedPath = resolvedFilePath(dataPath);
    let state = this.unindexedDeletes.get(resolvedPath);
    if (state?.deleteIntent?.status === 'resolved') {
      this.completeUnindexedDeleteIntent(state, state.deleteIntent);
      state = undefined;
    }
    if (!state) {
      state = {
        fileKey,
        path: resolvedPath,
        logMessage,
        candidateFingerprint,
      };
      this.unindexedDeletes.set(resolvedPath, state);
    }
    return this.startUnindexedPathDelete(state).catch(() => undefined);
  }

  private startUnindexedPathDelete(state: UnindexedDeleteState): Promise<void> {
    if (state.admissionError) {
      if (this.unindexedDeletes.get(state.path) === state) {
        this.unindexedDeletes.delete(state.path);
      }
      return Promise.reject(state.admissionError);
    }
    if (state.operation) return state.operation;
    let deletion: Promise<void> | undefined;
    const target: ProcessPathDeleteTarget = state.candidateFingerprint
      ? {
          kind: 'observed-incarnation',
          fingerprint: state.candidateFingerprint,
        }
      : { kind: 'current-incarnation' };
    const request: ProcessPathDeleteRequest = {
      target,
      run: async (deleteTarget) => {
        try {
          return await this.removePathForDeleteIntent(state.path, deleteTarget);
        } catch (error) {
          state.lastError = error;
          throw error;
        }
      },
      onStart: (operation) => {
        deletion = operation;
        state.operation = operation;
        this.pendingDeletes.add(operation);
        const finishAttempt = (): void => {
          if (state.operation === deletion) state.operation = undefined;
          if (deletion) this.pendingDeletes.delete(deletion);
        };
        void operation.then(finishAttempt, finishAttempt);
        void operation.catch((error: unknown) => {
          logger.debug(
            { name: this.opts.name, err: errorMessage(error) },
            state.logMessage
          );
        });
      },
    };
    const currentIntent = state.deleteIntent;
    if (currentIntent?.status === 'resolved') {
      this.completeUnindexedDeleteIntent(state, currentIntent);
      return currentIntent.completion.then(() => undefined);
    }
    if (currentIntent) {
      return (
        retryProcessPathDelete(state.path, currentIntent, request) ??
        Promise.resolve()
      );
    }
    let registration: ProcessPathDeleteRegistration;
    try {
      registration = requestProcessPathDelete(state.path, request);
    } catch (error) {
      if (
        error instanceof DiskBackedCacheError &&
        error.code === 'DISK_CACHE_DELETE_PARTICIPANT_LIMIT'
      ) {
        // This local cleanup target was never admitted and must never become a
        // later current-incarnation retry. Startup reconciliation in a future
        // cache generation may observe the path again under a fresh target.
        state.admissionError = error;
      }
      return Promise.reject(error);
    }
    this.bindUnindexedDeleteIntent(state, registration.intent);
    return registration.startedOperation ?? Promise.resolve();
  }

  private bindUnindexedDeleteIntent(
    state: UnindexedDeleteState,
    intent: ProcessPathDeleteIntent
  ): void {
    if (state.deleteIntent === intent) return;
    if (state.deleteIntent && state.deleteIntent.status === 'unresolved') {
      throw new Error('Disk cache orphan delete-intent invariant violated');
    }
    state.deleteIntent = intent;
    void intent.completion.then(() => {
      this.completeUnindexedDeleteIntent(state, intent);
    });
  }

  private completeUnindexedDeleteIntent(
    state: UnindexedDeleteState,
    intent: ProcessPathDeleteIntent
  ): void {
    if (state.deleteIntent !== intent || intent.status !== 'resolved') return;
    state.deleteIntent = undefined;
    state.operation = undefined;
    state.lastError = undefined;
    if (this.unindexedDeletes.get(state.path) === state) {
      this.unindexedDeletes.delete(state.path);
    }
  }

  private isCurrentGeneration(generation: number): boolean {
    return generation === this.generation && !this.closed && !this.clearing;
  }

  /**
   * Background writes admitted before close may finish, while clear remains a
   * destructive barrier. Admission itself is synchronously closed by set().
   */
  private isCurrentMutationGeneration(generation: number): boolean {
    return (
      generation === this.generation && !this.clearing && this.diskEnabled()
    );
  }

  private assertAcceptingGeneration(generation: number): void {
    if (!this.isCurrentGeneration(generation) || !this.diskEnabled()) {
      throw this.cacheClosedError();
    }
  }

  private cacheClosedError(): DiskBackedCacheError {
    return new DiskBackedCacheError(
      'DISK_CACHE_CLOSED',
      'Disk cache is not accepting new file operations'
    );
  }

  private releasePreparedSlot(state?: PreparedFileState): void {
    if (state?.slotReleased) return;
    if (state) state.slotReleased = true;
    if (this.preparedSlots <= 0) {
      throw new Error('Disk cache prepared-slot accounting invariant violated');
    }
    this.preparedSlots--;
    this.assertPreparedAccounting();
  }

  private assertPreparedAccounting(): void {
    if (
      !Number.isSafeInteger(this.preparedSlots) ||
      this.preparedSlots < 0 ||
      this.preparedSlots > DiskBackedCache.MAX_PENDING_WRITES
    ) {
      throw new Error('Disk cache prepared-slot accounting invariant violated');
    }
  }

  private assertDiskAccounting(): void {
    if (!Number.isSafeInteger(this.diskBytes) || this.diskBytes < 0) {
      throw new Error('Disk cache byte accounting invariant violated');
    }
  }

  private releasePendingWriteBytes(bytes: number): void {
    this.pendingWriteBytes -= bytes;
    if (
      !Number.isSafeInteger(this.pendingWriteBytes) ||
      this.pendingWriteBytes < 0
    ) {
      throw new Error('Disk cache pending-write accounting invariant violated');
    }
  }

  stats(): DiskBackedCacheStats {
    const total = this.hits + this.misses;
    return {
      memBytes: this.memBytes,
      memCount: this.mem.size,
      diskBytes: this.diskBytes,
      diskCount: this.disk.size,
      hits: this.hits,
      misses: this.misses,
      diskHits: this.diskHits,
      hitRate: total > 0 ? this.hits / total : 0,
    };
  }

  async delete(key: string): Promise<boolean> {
    const memEntry = this.mem.get(key);
    const fileKey = this.fileKey(key);
    const pending = this.pendingWrites.get(fileKey);
    if (pending) await pending.catch(() => undefined);
    const diskEntry = this.disk.has(fileKey);
    // Disk-delete admission precedes every logical tier mutation. If the
    // bounded participant registry is saturated, the explicit delete rejects
    // without leaving either tier partially removed or retryable by accident.
    if (diskEntry) {
      this.dropDisk(fileKey);
    }
    if (memEntry && this.mem.get(key) === memEntry) {
      this.mem.delete(key);
      this.memBytes -= memEntry.size;
    }
    return memEntry !== undefined || diskEntry;
  }

  clear(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    if (this.clearPromise) return this.clearPromise;
    // Linearization point: every older async mutation is stale before waiting.
    this.generation++;
    this.clearing = true;
    this.mem.clear();
    this.memBytes = 0;
    this.cancelIndexFlushTimer();
    // Cleanup states created by mutations that cross this clear are retained
    // for a later explicit retry. Existing orphan retries are part of this
    // clear operation, while a newly stale destination gets one mutation-
    // release attempt without an immediate duplicate retry in the same clear.
    const unindexedDeletesAtStart = [...this.unindexedDeletes.values()];
    let operation: Promise<void>;
    operation = this.clearOnce(unindexedDeletesAtStart).finally(() => {
      if (this.clearPromise === operation) {
        this.clearPromise = undefined;
        this.clearing = false;
      }
    });
    this.clearPromise = operation;
    return operation;
  }

  private async clearOnce(
    unindexedDeletesAtStart: readonly UnindexedDeleteState[]
  ): Promise<void> {
    await this.ready.catch(() => undefined);
    this.cancelIndexFlushTimer();
    await Promise.allSettled([...this.pendingPreparedCreations]);
    // Active prepared handles are invalidated by the generation change but
    // remain caller-owned until their mandatory release. Removing their path
    // here could let an in-flight external writer recreate it after clear.
    const cleanupFailures: unknown[] = [];
    await Promise.allSettled([...this.pendingWrites.values()]);
    // Any old snapshot already inside writeFile must settle before the final rm.
    await this.indexFlush.catch(() => undefined);
    for (const fileKey of [...this.disk.keys()]) this.dropDisk(fileKey);
    let deleteError: unknown;
    try {
      await this.retryPendingPhysicalDeletes(unindexedDeletesAtStart);
    } catch (error) {
      deleteError = error;
    }
    if (this.diskEnabled()) {
      await this.fileSystem.rm(this.indexPath, { force: true });
      this.indexDirty = false;
      this.indexFlushFailed = false;
    }
    if (deleteError) cleanupFailures.push(deleteError);
    if (cleanupFailures.length > 0) {
      throw new AggregateError(
        cleanupFailures,
        'Disk cache clear cleanup did not complete'
      );
    }
  }

  /** Adjust budgets (e.g. after a settings change) and evict to fit. */
  resize(maxMemBytes: number, maxDiskBytes?: number): void {
    this.opts.maxMemBytes = maxMemBytes;
    if (maxDiskBytes !== undefined) this.opts.maxDiskBytes = maxDiskBytes;
    while (this.memBytes > this.opts.maxMemBytes && this.mem.size > 0) {
      const oldestEntry = this.mem.keys().next();
      if (oldestEntry.done) break;
      const oldest = oldestEntry.value;
      const e = this.mem.get(oldest);
      this.mem.delete(oldest);
      if (e) this.memBytes -= e.size;
    }
    this.evictDisk();
  }

  /**
   * Debounce window for the self-scheduled index persist.
   */
  private static readonly INDEX_FLUSH_DEBOUNCE_MS = 5_000;

  /**
   * Ensure a dirty index is persisted soon, coalescing a burst of writes into a
   * single flush. Unref'd so the timer never keeps the process alive.
   */
  private scheduleIndexFlush(): void {
    if (
      this.flushTimer ||
      this.closed ||
      this.clearing ||
      !this.diskEnabled()
    ) {
      return;
    }
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined;
      void this.flushIndex().catch((error: unknown) => {
        logger.debug(
          { name: this.opts.name, err: errorMessage(error) },
          'disk cache background index flush failed'
        );
      });
    }, DiskBackedCache.INDEX_FLUSH_DEBOUNCE_MS);
    this.flushTimer.unref?.();
  }

  /** Persist the disk index. Coalesces concurrent callers. */
  async flushIndex(): Promise<void> {
    if (
      !this.diskEnabled() ||
      !this.indexDirty ||
      this.closed ||
      this.clearing
    ) {
      return;
    }
    return this.flushIndexForGeneration(this.generation, false, false);
  }

  private flushIndexForGeneration(
    generation: number,
    allowClosed: boolean,
    force: boolean
  ): Promise<void> {
    const operation = this.indexFlush.then(async () => {
      if (
        !this.diskEnabled() ||
        (!force && !this.indexDirty) ||
        generation !== this.generation ||
        this.clearing ||
        (this.closed && !allowClosed)
      ) {
        return;
      }
      this.indexDirty = false;
      const snapshot: Record<string, DiskEntry> = {};
      for (const [k, v] of this.disk) snapshot[k] = v;
      try {
        await this.fileSystem.writeFile(
          this.indexPath,
          JSON.stringify(snapshot)
        );
        this.indexFlushFailed = false;
      } catch (err) {
        if (generation === this.generation) this.indexDirty = true;
        this.indexFlushFailed = true;
        throw new DiskBackedCacheError(
          'DISK_CACHE_INDEX_IO',
          'Disk cache index could not be persisted',
          { cause: err }
        );
      }
    });
    // Keep the serialization chain usable after a failed attempt while the
    // explicit caller still observes the typed durability failure.
    this.indexFlush = operation.catch(() => undefined);
    return operation;
  }

  /**
   * Drain in-flight writes and persist the index, without closing the cache.
   */
  async flush(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    if (this.clearPromise) await this.clearPromise;
    return this.flushOnce(this.generation, false, false);
  }

  private async flushOnce(
    generation: number,
    allowClosed: boolean,
    forceIndex: boolean
  ): Promise<void> {
    await this.ready.catch(() => undefined);
    await Promise.allSettled([...this.pendingWrites.values()]);
    await this.retryPendingPhysicalDeletes();
    await this.flushIndexForGeneration(generation, allowClosed, forceIndex);
  }

  /** Drain in-flight writes, persist the index, and stop accepting writes. */
  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    // Linearization point: no operation crossing an await may return a new
    // prepared/file lease after this synchronous state change.
    this.closed = true;
    this.cancelIndexFlushTimer();
    diskCacheRegistry.delete(this);
    const generation = this.generation;
    const activeClear = this.clearPromise;
    this.closePromise = this.closeOnce(generation, activeClear);
    return this.closePromise;
  }

  private async closeOnce(
    generation: number,
    activeClear: Promise<void> | undefined
  ): Promise<void> {
    const cleanupFailures: unknown[] = [];
    if (activeClear) {
      try {
        await activeClear;
      } catch (error) {
        cleanupFailures.push(error);
      }
    }
    await this.ready.catch(() => undefined);
    await Promise.allSettled([...this.pendingPreparedCreations]);
    cleanupFailures.push(...(await this.releaseAllPreparedFiles()));
    await Promise.allSettled([...this.pendingWrites.values()]);
    await this.indexFlush.catch(() => undefined);
    try {
      await this.retryPendingPhysicalDeletes();
    } catch (error) {
      cleanupFailures.push(error);
    }
    try {
      // Persist a final current snapshot after every admitted write. The
      // independent failure bit forces a retry even if an older attempt had
      // temporarily cleared indexDirty before failing.
      await this.flushIndexForGeneration(
        generation,
        true,
        this.indexFlushFailed
      );
    } catch (error) {
      cleanupFailures.push(error);
    }
    if (cleanupFailures.length === 1) throw cleanupFailures[0];
    if (cleanupFailures.length > 0) {
      throw new AggregateError(
        cleanupFailures,
        'Disk cache close cleanup did not complete'
      );
    }
  }

  private cancelIndexFlushTimer(): void {
    if (!this.flushTimer) return;
    clearTimeout(this.flushTimer);
    this.flushTimer = undefined;
  }

  private async releaseAllPreparedFiles(): Promise<unknown[]> {
    const results = await Promise.allSettled(
      [...this.preparedFiles].map(([prepared, state]) =>
        this.releasePreparedFile(prepared, state)
      )
    );
    return results.flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : []
    );
  }

  private async retryPendingPhysicalDeletes(
    unindexedStates: readonly UnindexedDeleteState[] = [
      ...this.unindexedDeletes.values(),
    ]
  ): Promise<void> {
    // Settle every attempt that was already admitted before this explicit
    // cleanup barrier. Its completion observers clear the local operation
    // slots first; failed unresolved intents are then eligible for exactly one
    // fresh attempt below.
    await Promise.allSettled([...this.pendingDeletes]);

    type CleanupParticipant = {
      readonly path: string;
      readonly intent: () => ProcessPathDeleteIntent | undefined;
      readonly start: () => Promise<void>;
    };

    // Each target can contribute one already-running attempt and at most one
    // explicit retry to this barrier. The process-wide participant limit makes
    // both this progress set and the number of successor turns hard-bounded.
    const attempted = new Set<symbol>();
    const maxRounds = DISK_CACHE_DELETE_PARTICIPANT_LIMIT * 2 + 2;
    for (let round = 0; round < maxRounds; round++) {
      const participants: CleanupParticipant[] = [];
      for (const [fileKey, state] of this.fileLeases) {
        if (state.pendingDelete && state.leases === 0) {
          participants.push({
            path: this.filePath(fileKey),
            intent: () => state.deleteIntent,
            start: () => this.startPhysicalDelete(fileKey, state),
          });
        }
      }
      for (const state of unindexedStates) {
        if (this.unindexedDeletes.get(state.path) === state) {
          participants.push({
            path: state.path,
            intent: () => state.deleteIntent,
            start: () => this.startUnindexedPathDelete(state),
          });
        }
      }
      if (participants.length === 0) return;

      const unboundOperations = new Set<Promise<void>>();
      // Register every local state synchronously before inspecting the shared
      // chain. This lets a later incompatible target become a fenced successor
      // without waiting for the current attempt to settle.
      for (const participant of participants) {
        if (participant.intent()) continue;
        const before = processPathOwnership.get(
          resolvedFilePath(participant.path)
        )?.deleteIntent?.attempt;
        const operation = participant.start();
        const bound = participant.intent();
        if (!bound) {
          unboundOperations.add(operation);
          continue;
        }
        const inspection = inspectProcessPathDelete(participant.path, bound);
        if (
          inspection.status === 'active' &&
          inspection.operation !== before &&
          operation === inspection.operation
        ) {
          // This barrier dispatched the first attempt for the newly registered
          // target; a rejection is reported rather than retried again here.
          attempted.add(inspection.current.id);
        }
      }

      const operations = new Set<Promise<void>>();
      const failureIntentIds = new Set<symbol>();
      const failures: unknown[] = [];
      for (const participant of participants) {
        const intent = participant.intent();
        if (!intent) continue;
        const inspection = inspectProcessPathDelete(participant.path, intent);
        if (inspection.status === 'resolved') continue;
        if (inspection.status === 'blocked') continue;
        if (inspection.status === 'active') {
          operations.add(inspection.operation);
          continue;
        }
        const current = inspection.current;
        if (attempted.has(current.id)) {
          if (!failureIntentIds.has(current.id)) {
            failureIntentIds.add(current.id);
            failures.push(
              current.lastError ??
                new Error('Disk cache delete attempt made no progress')
            );
          }
          continue;
        }
        attempted.add(current.id);
        const operation = participant.start();
        const after = inspectProcessPathDelete(participant.path, intent);
        if (after.status === 'active') {
          operations.add(after.operation);
        } else {
          operations.add(operation);
        }
      }

      if (failures.length > 0) {
        throw new AggregateError(
          failures,
          'Disk cache physical cleanup did not complete'
        );
      }
      if (unboundOperations.size > 0) {
        const unboundResults = await Promise.allSettled(unboundOperations);
        const unboundFailures = unboundResults.flatMap((result) =>
          result.status === 'rejected' ? [result.reason] : []
        );
        if (unboundFailures.length > 0) {
          throw new AggregateError(
            unboundFailures,
            'Disk cache physical cleanup could not register a participant'
          );
        }
      }
      if (operations.size === 0) {
        // No filesystem attempt is active: every remaining intent is blocked
        // exclusively by an already-issued lease (or a bounded mutation).
        return;
      }
      await Promise.allSettled(operations);
    }
    throw new Error('Disk cache delete cleanup progress limit exceeded');
  }
}
