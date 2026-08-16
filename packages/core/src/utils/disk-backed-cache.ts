import { createHash, randomUUID } from 'node:crypto';
import { createReadStream, createWriteStream, promises as fs } from 'node:fs';
import path from 'path';
import { pipeline } from 'node:stream/promises';
import { createLogger } from '../logging/logger.js';

const logger = createLogger('disk-cache');

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

function parseDiskIndex(raw: string): Array<[string, DiskEntry]> {
  const parsed: unknown = JSON.parse(raw);
  if (!isRecord(parsed)) return [];
  const entries: Array<[string, DiskEntry]> = [];
  for (const [fileKey, value] of Object.entries(parsed)) {
    if (!/^[a-f0-9]{40}$/.test(fileKey) || !isRecord(value)) continue;
    const size = value.size;
    if (typeof size !== 'number' || !Number.isSafeInteger(size) || size < 0) {
      continue;
    }
    entries.push([fileKey, { size }]);
  }
  return entries;
}

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
}

/**
 * A counted reference to one immutable serialized cache file.
 *
 * Logical LRU eviction removes the entry from cache accounting immediately,
 * while physical deletion is deferred until the final lease is released. This
 * keeps open readers valid on Windows as well as POSIX. Release is idempotent.
 */
export interface DiskFileLease {
  readonly path: string;
  readonly serializedBytes: number;
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
}

interface PreparedFileState {
  readonly path: string;
  status: 'active' | 'installing' | 'released';
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
  private static readonly COPY_CHUNK_BYTES = 64 * 1024;

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
  /** Pending debounced index-persist timer (see {@link scheduleIndexFlush}). */
  private flushTimer?: NodeJS.Timeout;
  private ready: Promise<void>;
  private closed = false;
  /** File ownership states remain only while leased or awaiting deletion. */
  private readonly fileLeases = new Map<string, FileLeaseState>();
  /** Physical deletes currently running; bounded by the disk index/lease set. */
  private readonly pendingDeletes = new Set<Promise<void>>();
  /** Staging handles are bounded by the same admission cap as writes. */
  private readonly preparedFiles = new Map<
    DiskPreparedFile,
    PreparedFileState
  >();
  private readonly renameFile: (
    source: string,
    destination: string
  ) => Promise<void>;

  constructor(opts: DiskBackedCacheOptions<V>) {
    this.opts = opts;
    this.dir = path.join(opts.dir, opts.name);
    this.indexPath = path.join(opts.dir, `${opts.name}.index.json`);
    this.renameFile = opts.renameFile ?? fs.rename;
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
    return this.opts.maxDiskBytes > 0;
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
      await fs.mkdir(this.dir, { recursive: true });
      let entries: Array<[string, DiskEntry]> = [];
      try {
        const raw = await fs.readFile(this.indexPath, 'utf8');
        entries = parseDiskIndex(raw);
      } catch {
        // No index yet — first run or it was removed.
      }
      // Index → keep only safe regular files and reconcile legacy index sizes
      // against their actual serialized byte length.
      const present = new Set(await fs.readdir(this.dir).catch(() => []));
      for (const [fileKey, entry] of entries) {
        if (!present.has(fileKey) || typeof entry?.size !== 'number') continue;
        try {
          const stats = await fs.lstat(this.filePath(fileKey));
          if (!stats.isFile() || !Number.isSafeInteger(stats.size)) continue;
          this.disk.set(fileKey, { size: stats.size });
          this.diskBytes += stats.size;
          if (entry.size !== stats.size) this.indexDirty = true;
        } catch {
          // Missing or unsafe entries are omitted from the reconciled index.
        }
      }
      // Files → delete any not referenced by the index (StremThru cleanOrphaned).
      for (const fileKey of present) {
        if (!this.disk.has(fileKey)) {
          await fs.rm(this.filePath(fileKey), { force: true }).catch(() => {});
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
      logger.warn(
        { name: this.opts.name, err: errorMessage(err) },
        'disk cache load failed; continuing memory-only'
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
   * Acquisition touches LRU recency and increments hit counters. Logical
   * eviction may proceed while leased, but physical unlink waits for release.
   */
  async acquireDiskFile(key: string): Promise<DiskFileLease | undefined> {
    if (!this.diskEnabled() || this.closed) {
      this.misses++;
      return undefined;
    }
    await this.ready.catch(() => undefined);
    const fileKey = this.fileKey(key);
    const pending = this.pendingWrites.get(fileKey);
    if (pending) await pending.catch(() => undefined);

    const entry = this.disk.get(fileKey);
    const existingState = this.fileLeases.get(fileKey);
    if (!entry || existingState?.pendingDelete) {
      this.misses++;
      return undefined;
    }

    // No await between the logical index check and the reference increment.
    const state = existingState ?? { leases: 0, pendingDelete: false };
    state.leases++;
    this.fileLeases.set(fileKey, state);

    let serializedBytes: number;
    try {
      const stats = await fs.lstat(this.filePath(fileKey));
      if (!stats.isFile() || !Number.isSafeInteger(stats.size)) {
        throw new Error('Disk cache entry is not a safe regular file');
      }
      serializedBytes = stats.size;
    } catch {
      await this.releaseFileLease(fileKey, state);
      this.dropDisk(fileKey);
      this.misses++;
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

    this.hits++;
    this.diskHits++;
    let released = false;
    let releasePromise: Promise<void> | undefined;
    return {
      path: this.filePath(fileKey),
      serializedBytes,
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
    const lease = await this.acquireDiskFile(key);
    if (!lease) return undefined;
    try {
      const buf = await fs.readFile(lease.path);
      const value = this.opts.deserialize(buf);
      this.addToMem(key, value, this.opts.sizeOf(value));
      return value;
    } catch {
      // File vanished or is corrupt. Convert the provisional file hit into a
      // miss, then logically evict; unlink waits for this lease to release.
      this.hits--;
      this.diskHits--;
      this.misses++;
      await this.delete(key);
      return undefined;
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
    if (this.closed) return;
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
        if (this.closed || !this.diskEnabled()) return;
        await fs.mkdir(this.dir, { recursive: true });
        await fs.writeFile(tempPath, payload, { flag: 'wx', mode: 0o600 });
        await this.replaceFile(tempPath, this.filePath(fileKey));
        this.commitDiskEntry(fileKey, serializedBytes);
      } catch (err) {
        logger.debug(
          { name: this.opts.name, err: errorMessage(err) },
          'disk cache write failed'
        );
      } finally {
        await fs.rm(tempPath, { force: true }).catch(() => undefined);
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

  private async replaceFile(
    source: string,
    destination: string
  ): Promise<void> {
    try {
      await fs.rename(source, destination);
    } catch (error) {
      const code = nodeErrorCode(error);
      if (code !== 'EEXIST' && code !== 'EPERM') throw error;
      await fs.rm(destination, { force: true });
      await fs.rename(source, destination);
    }
  }

  /**
   * Create one secure, bounded staging file inside the cache namespace.
   */
  async createPreparedFile(): Promise<DiskPreparedFile> {
    await this.ready.catch(() => undefined);
    if (this.closed || !this.diskEnabled()) {
      throw new Error('Disk cache is not accepting prepared files');
    }
    if (this.preparedFiles.size >= DiskBackedCache.MAX_PENDING_WRITES) {
      throw new Error('Disk cache prepared-file limit reached');
    }
    await fs.mkdir(this.dir, { recursive: true });
    const preparedPath = path.join(this.dir, `.prepared-${randomUUID()}`);
    const handle = await fs.open(preparedPath, 'wx', 0o600);
    try {
      await handle.close();
    } catch (error) {
      await fs.rm(preparedPath, { force: true }).catch(() => undefined);
      throw error;
    }

    const state: PreparedFileState = {
      path: preparedPath,
      status: 'active',
    };
    const prepared: DiskPreparedFile = {
      path: preparedPath,
      release: () => this.releasePreparedFile(prepared, state),
    };
    this.preparedFiles.set(prepared, state);
    return prepared;
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
      throw new Error('Prepared disk cache file is not active');
    }
    state.status = 'installing';
    const operation = this.installPreparedFileOnce(
      key,
      state.path,
      serializedBytes
    );
    state.operation = operation;
    try {
      return await operation;
    } finally {
      state.status = 'released';
      this.preparedFiles.delete(prepared);
      await fs.rm(state.path, { force: true }).catch(() => undefined);
    }
  }

  private async installPreparedFileOnce(
    key: string,
    preparedPath: string,
    serializedBytes: number
  ): Promise<boolean> {
    await this.ready.catch(() => undefined);
    if (
      this.closed ||
      !this.diskEnabled() ||
      serializedBytes > this.opts.maxDiskBytes
    ) {
      return false;
    }
    const stats = await fs.lstat(preparedPath);
    if (!stats.isFile() || stats.size !== serializedBytes) {
      throw new Error('Prepared disk cache file size is invalid');
    }

    const fileKey = this.fileKey(key);
    const previous = this.pendingWrites.get(fileKey);
    if (previous) await previous.catch(() => undefined);
    if (this.closed || !this.diskEnabled()) return false;
    const existing = this.disk.get(fileKey);
    if (existing) {
      this.disk.delete(fileKey);
      this.disk.set(fileKey, existing);
      this.indexDirty = true;
      this.scheduleIndexFlush();
      return false;
    }
    if (
      this.pendingWrites.has(fileKey) ||
      this.pendingWrites.size >= DiskBackedCache.MAX_PENDING_WRITES ||
      this.pendingWriteBytes + serializedBytes >
        DiskBackedCache.MAX_PENDING_WRITE_BYTES ||
      this.fileLeases.has(fileKey)
    ) {
      return false;
    }

    this.pendingWriteBytes += serializedBytes;
    let install: Promise<void>;
    const run = async (): Promise<void> => {
      try {
        await this.movePreparedFile(
          preparedPath,
          this.filePath(fileKey),
          serializedBytes
        );
        this.commitDiskEntry(fileKey, serializedBytes);
      } finally {
        this.releasePendingWriteBytes(serializedBytes);
        if (this.pendingWrites.get(fileKey) === install) {
          this.pendingWrites.delete(fileKey);
        }
        this.evictDisk();
      }
    };
    install = run();
    this.pendingWrites.set(fileKey, install);
    await install;
    return this.disk.has(fileKey);
  }

  private async movePreparedFile(
    source: string,
    destination: string,
    serializedBytes: number
  ): Promise<void> {
    try {
      await this.renameFile(source, destination);
      return;
    } catch (error) {
      if (nodeErrorCode(error) !== 'EXDEV') throw error;
    }

    const copyPath = path.join(this.dir, `.install-${randomUUID()}`);
    try {
      await pipeline(
        createReadStream(source, {
          highWaterMark: DiskBackedCache.COPY_CHUNK_BYTES,
        }),
        createWriteStream(copyPath, {
          flags: 'wx',
          mode: 0o600,
          highWaterMark: DiskBackedCache.COPY_CHUNK_BYTES,
        })
      );
      const copied = await fs.lstat(copyPath);
      if (!copied.isFile() || copied.size !== serializedBytes) {
        throw new Error('Cross-device prepared-file copy was incomplete');
      }
      await this.replaceFile(copyPath, destination);
    } finally {
      await Promise.allSettled([
        fs.rm(copyPath, { force: true }),
        fs.rm(source, { force: true }),
      ]);
    }
  }

  private releasePreparedFile(
    prepared: DiskPreparedFile,
    state: PreparedFileState
  ): Promise<void> {
    if (state.releasePromise) return state.releasePromise;
    if (state.status === 'installing') {
      state.releasePromise = Promise.resolve(state.operation).then(
        () => undefined,
        () => undefined
      );
      return state.releasePromise;
    }
    if (state.status === 'released') return Promise.resolve();
    state.status = 'released';
    this.preparedFiles.delete(prepared);
    state.releasePromise = fs
      .rm(state.path, { force: true })
      .then(() => undefined);
    return state.releasePromise;
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
      this.dropDisk(oldest);
    }
  }

  private dropDisk(fileKey: string): void {
    const entry = this.disk.get(fileKey);
    if (!entry) return;
    this.disk.delete(fileKey);
    this.diskBytes -= entry.size;
    this.assertDiskAccounting();
    this.indexDirty = true;
    this.scheduleIndexFlush();
    const state = this.fileLeases.get(fileKey) ?? {
      leases: 0,
      pendingDelete: false,
    };
    state.pendingDelete = true;
    this.fileLeases.set(fileKey, state);
    if (state.leases === 0) this.startPhysicalDelete(fileKey, state);
  }

  private releaseFileLease(
    fileKey: string,
    state: FileLeaseState
  ): Promise<void> {
    if (state.leases <= 0) return state.deletePromise ?? Promise.resolve();
    state.leases--;
    if (state.leases === 0) {
      if (state.pendingDelete) return this.startPhysicalDelete(fileKey, state);
      this.fileLeases.delete(fileKey);
    }
    return Promise.resolve();
  }

  private startPhysicalDelete(
    fileKey: string,
    state: FileLeaseState
  ): Promise<void> {
    if (state.deletePromise) return state.deletePromise;
    let deletion: Promise<void>;
    const run = async (): Promise<void> => {
      try {
        await fs.rm(this.filePath(fileKey), { force: true });
      } catch (error) {
        logger.debug(
          { name: this.opts.name, err: errorMessage(error) },
          'disk cache deferred delete failed'
        );
      } finally {
        if (this.fileLeases.get(fileKey) === state && state.leases === 0) {
          this.fileLeases.delete(fileKey);
        }
        this.pendingDeletes.delete(deletion);
      }
    };
    deletion = run();
    state.deletePromise = deletion;
    this.pendingDeletes.add(deletion);
    return deletion;
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
    let removed = false;
    const memEntry = this.mem.get(key);
    if (memEntry) {
      this.mem.delete(key);
      this.memBytes -= memEntry.size;
      removed = true;
    }
    const fileKey = this.fileKey(key);
    const pending = this.pendingWrites.get(fileKey);
    if (pending) await pending.catch(() => undefined);
    if (this.disk.has(fileKey)) {
      this.dropDisk(fileKey);
      removed = true;
    }
    return removed;
  }

  async clear(): Promise<void> {
    this.mem.clear();
    this.memBytes = 0;
    await this.ready.catch(() => undefined);
    await Promise.allSettled([...this.pendingWrites.values()]);
    for (const fileKey of [...this.disk.keys()]) this.dropDisk(fileKey);
    await Promise.allSettled([...this.pendingDeletes]);
    if (this.diskEnabled()) {
      await fs.rm(this.indexPath, { force: true }).catch(() => {});
      this.indexDirty = false;
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
    if (this.flushTimer || this.closed || !this.diskEnabled()) return;
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined;
      void this.flushIndex();
    }, DiskBackedCache.INDEX_FLUSH_DEBOUNCE_MS);
    this.flushTimer.unref?.();
  }

  /** Persist the disk index. Coalesces concurrent callers. */
  async flushIndex(): Promise<void> {
    if (!this.diskEnabled() || !this.indexDirty) return;
    this.indexFlush = this.indexFlush.then(async () => {
      if (!this.indexDirty) return;
      this.indexDirty = false;
      const snapshot: Record<string, DiskEntry> = {};
      for (const [k, v] of this.disk) snapshot[k] = v;
      try {
        await fs.writeFile(this.indexPath, JSON.stringify(snapshot));
      } catch (err) {
        this.indexDirty = true;
        logger.debug(
          { name: this.opts.name, err: errorMessage(err) },
          'disk cache index flush failed'
        );
      }
    });
    return this.indexFlush;
  }

  /**
   * Drain in-flight writes and persist the index, without closing the cache.
   */
  async flush(): Promise<void> {
    await this.ready.catch(() => {});
    await Promise.allSettled([...this.pendingWrites.values()]);
    await Promise.allSettled([...this.pendingDeletes]);
    await this.flushIndex();
  }

  /** Drain in-flight writes, persist the index, and stop accepting writes. */
  async close(): Promise<void> {
    this.closed = true;
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = undefined;
    }
    diskCacheRegistry.delete(this);
    await Promise.allSettled(
      [...this.preparedFiles].map(([prepared, state]) =>
        this.releasePreparedFile(prepared, state)
      )
    );
    await this.flush();
  }
}
