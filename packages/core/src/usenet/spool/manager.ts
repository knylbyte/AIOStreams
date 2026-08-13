import { createHash, randomUUID } from 'node:crypto';
import { promises as nodeFs } from 'node:fs';
import path from 'node:path';
import type { SegmentSpoolingPlan } from '../resource-plan.js';
import { getCacheFolder } from '../../utils/general.js';
import {
  ByteBudget,
  ByteBudgetError,
  type ByteLease,
} from '../pool/byte-budget.js';
import { SpoolBudget } from './budget.js';
import {
  classifySpoolFileError,
  isExistingSpoolError,
  isMissingSpoolError,
  spoolAbortError,
  UsenetSpoolError,
} from './errors.js';
import {
  GrowingSpoolArtifact,
  type GrowingSpoolArtifactOptions,
} from './growing-artifact.js';
import type {
  ManagedSpoolFile,
  OpenFileStats,
  SpoolClock,
  SpoolBudgetLease,
  SpoolFileHandle,
  SpoolFileSystem,
  SpoolIdGenerator,
  SpoolManagerStats,
  SpoolScheduledTask,
  SpoolScheduler,
} from './types.js';

const DIRECTORY_MODE = 0o700;
const FILE_MODE = 0o600;
const LIVENESS_MARKER = '.alive';
const LIVENESS_LOCK_DIRECTORY = '.liveness-locks';
const LIVENESS_LOCK_SUFFIX = '.lock';
const HASHED_NAMESPACE = /^[a-f0-9]{64}$/;
const HASHED_ARTIFACT = /^[a-f0-9]{64}\.(?:partial|ready)$/;

export interface SpoolManagerOptions {
  readonly plan: SegmentSpoolingPlan;
  readonly engineId: string;
  readonly cacheRoot?: string;
  readonly fileSystem?: Partial<SpoolFileSystem>;
  readonly clock?: SpoolClock;
  readonly idGenerator?: SpoolIdGenerator;
  readonly scheduler?: SpoolScheduler;
  readonly maxArtifacts?: number;
}

export interface CreateSpoolArtifactOptions {
  readonly sessionId: string;
  readonly segmentId: string;
  readonly initialReservationBytes: number;
  readonly signal?: AbortSignal;
}

interface PendingArtifactCleanup {
  readonly partialPath: string;
  readonly readyPath: string;
  readonly reservation: SpoolBudgetLease;
}

interface NamespaceControlLease {
  release(): Promise<void>;
}

function hashId(kind: string, value: string): string {
  return createHash('sha256')
    .update(kind)
    .update('\0')
    .update(value)
    .digest('hex');
}

function isPositiveSafeInteger(value: number): boolean {
  return Number.isSafeInteger(value) && value > 0;
}

function createNodeFileSystem(): SpoolFileSystem {
  return {
    mkdir: async (directory, options) => {
      await nodeFs.mkdir(directory, options);
    },
    open: async (filePath, flags, mode) => {
      const handle = await nodeFs.open(filePath, flags, mode);
      return {
        read: async (buffer, offset, length, position) => {
          const result = await handle.read(buffer, offset, length, position);
          return { bytesRead: result.bytesRead };
        },
        write: async (buffer, offset, length, position) => {
          const result = await handle.write(buffer, offset, length, position);
          return { bytesWritten: result.bytesWritten };
        },
        close: () => handle.close(),
      };
    },
    rename: (oldPath, newPath) => nodeFs.rename(oldPath, newPath),
    rm: (target, options) => nodeFs.rm(target, options),
    readdir: (directory) => nodeFs.readdir(directory),
    lstat: async (target) => {
      const stats = await nodeFs.lstat(target);
      return {
        mtimeMs: stats.mtimeMs,
        isDirectory: () => stats.isDirectory(),
        isFile: () => stats.isFile(),
        isSymbolicLink: () => stats.isSymbolicLink(),
      };
    },
    statfs: async (target) => {
      const stats = await nodeFs.statfs(target);
      return { bavail: stats.bavail, bsize: stats.bsize };
    },
    utimes: (target, atimeMs, mtimeMs) =>
      nodeFs.utimes(target, new Date(atimeMs), new Date(mtimeMs)),
  };
}

function createNodeScheduler(): SpoolScheduler {
  return (callback, delayMs) => {
    const timer = setTimeout(() => {
      void callback().catch(() => {
        process.emitWarning('Unexpected spool heartbeat scheduler failure');
      });
    }, delayMs);
    timer.unref();
    return { cancel: () => clearTimeout(timer) };
  };
}

class OpenFilePool {
  private readonly permits: ByteBudget;

  constructor(
    maxFiles: number,
    maxWaiters: number,
    private readonly fileSystem: SpoolFileSystem
  ) {
    this.permits = new ByteBudget(maxFiles, { maxWaiters });
  }

  async open(
    filePath: string,
    flags: string,
    mode: number | undefined,
    signal: AbortSignal | undefined
  ): Promise<ManagedSpoolFile> {
    let permit: ByteLease;
    try {
      permit = await this.permits.acquire(1, { signal });
    } catch (error) {
      if (signal?.aborted) throw spoolAbortError(signal.reason);
      if (error instanceof UsenetSpoolError) throw error;
      if (error instanceof ByteBudgetError) {
        throw new UsenetSpoolError(
          error.code === 'BYTE_BUDGET_CLOSED'
            ? 'USENET_SPOOL_CLOSED'
            : 'USENET_SPOOL_OPEN_FILE_LIMIT',
          error.code === 'BYTE_BUDGET_CLOSED'
            ? 'Spool file pool is closed'
            : 'Spool open-file waiter limit reached',
          { cause: error }
        );
      }
      throw error;
    }

    let handle: SpoolFileHandle;
    try {
      handle = await this.fileSystem.open(filePath, flags, mode);
      if (signal?.aborted) {
        await handle.close();
        throw spoolAbortError(signal.reason);
      }
    } catch (error) {
      permit.release();
      if (signal?.aborted) throw spoolAbortError(signal.reason);
      throw classifySpoolFileError(error, 'opening a spool file');
    }

    let closePromise: Promise<void> | undefined;
    return {
      handle,
      close: () => {
        closePromise ??= (async () => {
          try {
            await handle.close();
          } finally {
            permit.release();
          }
        })();
        return closePromise;
      },
    };
  }

  close(error: Error): void {
    this.permits.close(error);
  }

  stats(): OpenFileStats {
    const stats = this.permits.stats();
    return {
      maxFiles: stats.maxBytes,
      openFiles: stats.usedBytes,
      waiting: stats.waiting,
      peakOpenFiles: stats.peakBytes,
    };
  }
}

/**
 * Owns one process/engine spool namespace below the configured cache root.
 * Every externally supplied identifier is SHA-256 hashed before becoming a
 * path component. The manager tracks a finite artifact set, one global disk
 * budget, and one shared writer/reader file-descriptor cap.
 */
export class SpoolManager {
  readonly spoolRoot: string;
  readonly processRoot: string;
  readonly engineRoot: string;

  private readonly livenessLockRoot: string;
  private readonly plan: SegmentSpoolingPlan;
  private readonly fileSystem: SpoolFileSystem;
  private readonly clock: SpoolClock;
  private readonly idGenerator: SpoolIdGenerator;
  private readonly scheduler: SpoolScheduler;
  private readonly heartbeatIntervalMs: number;
  private readonly maxArtifacts: number;
  private readonly budget: SpoolBudget;
  private readonly filePool: OpenFilePool;
  private readonly artifacts = new Set<GrowingSpoolArtifact>();
  private readonly artifactReservations = new Map<
    GrowingSpoolArtifact,
    SpoolBudgetLease
  >();
  private readonly pendingArtifactCleanups = new Set<PendingArtifactCleanup>();
  private readonly closeController = new AbortController();
  private globalInitialization: Promise<void> | undefined;
  private namespaceEnsuring: Promise<boolean> | undefined;
  private heartbeatTask: SpoolScheduledTask | undefined;
  private heartbeatInFlight: Promise<void> | undefined;
  private closePromise: Promise<void> | undefined;
  private creationDrain: PromiseWithResolvers<void> | undefined;
  private creatingArtifacts = 0;
  private artifactSequence = 0;
  private processNamespaceOwned = false;
  private closed = false;

  constructor(options: SpoolManagerOptions) {
    if (!options.engineId) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Spool manager engineId cannot be empty'
      );
    }
    if (
      !isPositiveSafeInteger(options.plan.maxOpenSpoolFiles) ||
      !isPositiveSafeInteger(options.plan.writerQueueBytes) ||
      !isPositiveSafeInteger(options.plan.readerHighWaterMarkBytes) ||
      !isPositiveSafeInteger(options.plan.orphanTtlMs)
    ) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Spool plan contains invalid runtime limits'
      );
    }
    this.plan = options.plan;
    this.fileSystem = {
      ...createNodeFileSystem(),
      ...options.fileSystem,
    };
    this.clock = options.clock ?? Date.now;
    this.idGenerator = options.idGenerator ?? randomUUID;
    this.scheduler = options.scheduler ?? createNodeScheduler();
    this.heartbeatIntervalMs = Math.max(
      1,
      Math.floor(options.plan.orphanTtlMs / 3)
    );
    this.maxArtifacts =
      options.maxArtifacts ?? options.plan.maxOpenSpoolFiles * 4;
    if (!isPositiveSafeInteger(this.maxArtifacts)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Spool manager maxArtifacts must be a safe positive integer'
      );
    }

    const cacheRoot = path.resolve(options.cacheRoot ?? getCacheFolder());
    const processName = hashId('process', this.idGenerator());
    const engineName = hashId('engine', options.engineId);
    this.spoolRoot = path.join(cacheRoot, 'usenet-spool');
    this.livenessLockRoot = path.join(this.spoolRoot, LIVENESS_LOCK_DIRECTORY);
    this.processRoot = path.join(this.spoolRoot, processName);
    this.engineRoot = path.join(this.processRoot, engineName);
    this.budget = new SpoolBudget({
      maxBytes: options.plan.spoolBytes,
      minFreeDiskBytes: options.plan.minFreeDiskBytes,
      statfs: () => this.fileSystem.statfs(this.spoolRoot),
    });
    this.filePool = new OpenFilePool(
      options.plan.maxOpenSpoolFiles,
      this.maxArtifacts,
      this.fileSystem
    );
  }

  /**
   * Initialize the shared root once, then revalidate and recover this manager's
   * own namespace on every call. A low-frequency liveness heartbeat starts
   * only after the namespace and marker have both been verified.
   */
  async initialize(): Promise<void> {
    if (this.closed) throw this.closedError();
    await this.ensureGlobalInitialization();
    if (!(await this.ensureOwnNamespace())) {
      throw this.namespaceControlContentionError();
    }
    if (this.closed) throw this.closedError();
    this.scheduleHeartbeat();
  }

  /**
   * Reserve disk, create a secure partial file, and return an unintegrated
   * growing artifact. No NNTP or stream-routing behavior is attached here.
   */
  async createArtifact(
    options: CreateSpoolArtifactOptions
  ): Promise<GrowingSpoolArtifact> {
    this.assertArtifactId(options.sessionId, 'sessionId');
    this.assertArtifactId(options.segmentId, 'segmentId');
    if (this.closed) throw this.closedError();
    if (
      this.artifacts.size +
        this.pendingArtifactCleanups.size +
        this.creatingArtifacts >=
      this.maxArtifacts
    ) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CAPACITY',
        'Spool manager artifact limit reached'
      );
    }
    this.creatingArtifacts++;
    try {
      await this.initialize();
      const signal = options.signal
        ? AbortSignal.any([options.signal, this.closeController.signal])
        : this.closeController.signal;
      const reservation = await this.budget.reserve(
        options.initialReservationBytes,
        { signal }
      );
      let partialPath: string | undefined;
      let readyPath: string | undefined;
      try {
        const fileName = hashId(
          'artifact',
          `${options.sessionId}\0${options.segmentId}\0${this.idGenerator()}\0${this.nextArtifactSequence()}`
        );
        partialPath = path.join(this.engineRoot, `${fileName}.partial`);
        readyPath = path.join(this.engineRoot, `${fileName}.ready`);
        let artifact: GrowingSpoolArtifact | undefined;
        const artifactOptions: GrowingSpoolArtifactOptions = {
          partialPath,
          readyPath,
          reservation,
          writerQueueBytes: this.plan.writerQueueBytes,
          readerHighWaterMarkBytes: this.plan.readerHighWaterMarkBytes,
          maxReaders: this.plan.maxOpenSpoolFiles,
          fileSystem: this.fileSystem,
          openFile: (filePath, flags, mode, openSignal) =>
            this.filePool.open(filePath, flags, mode, openSignal),
          signal,
          onDisposed: () => {
            if (artifact) {
              this.artifacts.delete(artifact);
              this.artifactReservations.delete(artifact);
            }
          },
        };
        try {
          artifact = await GrowingSpoolArtifact.create(artifactOptions);
        } catch (error) {
          if (!isMissingSpoolError(error)) throw error;
          if (!(await this.ensureOwnNamespace())) {
            throw this.namespaceControlContentionError();
          }
          artifact = await GrowingSpoolArtifact.create(artifactOptions);
        }
        if (this.closed) {
          await artifact.dispose();
          throw this.closedError();
        }
        this.artifacts.add(artifact);
        this.artifactReservations.set(artifact, reservation);
        return artifact;
      } catch (error) {
        if (partialPath && readyPath) {
          try {
            await this.removeUntrackedArtifactFiles(partialPath, readyPath);
          } catch (cleanupError) {
            this.pendingArtifactCleanups.add({
              partialPath,
              readyPath,
              reservation,
            });
            throw cleanupError;
          }
        }
        reservation.release();
        throw error;
      }
    } finally {
      this.creatingArtifacts--;
      if (this.creatingArtifacts === 0 && this.creationDrain) {
        this.creationDrain.resolve();
        this.creationDrain = undefined;
      }
    }
  }

  stats(): SpoolManagerStats {
    return {
      budget: this.budget.stats(),
      files: this.filePool.stats(),
      artifacts: this.artifacts.size + this.pendingArtifactCleanups.size,
    };
  }

  /** Idempotently stop new work, dispose artifacts, then remove our namespace. */
  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closed = true;
    this.stopHeartbeat();
    const error = this.closedError();
    this.closeController.abort(error);
    this.budget.close(error);
    this.filePool.close(error);
    this.closePromise = this.closeOnce();
    return this.closePromise;
  }

  private ensureGlobalInitialization(): Promise<void> {
    if (this.globalInitialization) return this.globalInitialization;
    const operation = this.initializeGlobalOnce();
    this.globalInitialization = operation;
    void operation.catch(() => {
      if (this.globalInitialization === operation) {
        this.globalInitialization = undefined;
      }
    });
    return operation;
  }

  private async initializeGlobalOnce(): Promise<void> {
    try {
      await this.fileSystem.mkdir(this.spoolRoot, {
        recursive: true,
        mode: DIRECTORY_MODE,
      });
      await this.assertSafeDirectory(this.spoolRoot);
      await this.ensureLivenessLockRoot();
      await this.cleanupOrphans();
    } catch (error) {
      throw classifySpoolFileError(error, 'initializing spool storage');
    }
  }

  private ensureOwnNamespace(): Promise<boolean> {
    if (this.closed) return Promise.reject(this.closedError());
    if (this.namespaceEnsuring) return this.namespaceEnsuring;
    const operation = this.ensureOwnNamespaceOnce();
    this.namespaceEnsuring = operation;
    const clear = () => {
      if (this.namespaceEnsuring === operation) {
        this.namespaceEnsuring = undefined;
      }
    };
    void operation.then(clear, clear);
    return operation;
  }

  private async ensureOwnNamespaceOnce(): Promise<boolean> {
    const control = await this.tryAcquireNamespaceControl(
      path.basename(this.processRoot)
    );
    if (!control) return false;
    try {
      await this.fileSystem.mkdir(this.spoolRoot, {
        recursive: true,
        mode: DIRECTORY_MODE,
      });
      await this.assertSafeDirectory(this.spoolRoot);
      await this.fileSystem.mkdir(this.processRoot, {
        recursive: true,
        mode: DIRECTORY_MODE,
      });
      await this.assertSafeDirectory(this.processRoot);
      this.processNamespaceOwned = true;
      await this.fileSystem.mkdir(this.engineRoot, {
        recursive: true,
        mode: DIRECTORY_MODE,
      });
      await this.assertSafeDirectory(this.engineRoot);
      await this.refreshLivenessMarker();
      return true;
    } catch (error) {
      throw classifySpoolFileError(error, 'recovering the spool namespace');
    } finally {
      await control.release();
    }
  }

  private async cleanupOrphans(): Promise<void> {
    let entries: readonly string[];
    try {
      entries = await this.fileSystem.readdir(this.spoolRoot);
    } catch (error) {
      if (isMissingSpoolError(error)) return;
      throw error;
    }
    const ownName = path.basename(this.processRoot);
    const now = this.clock();
    if (!Number.isFinite(now)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_IO',
        'Spool clock returned an invalid timestamp'
      );
    }
    const oldestAllowed = now - this.plan.orphanTtlMs;
    for (const entry of entries) {
      if (entry === ownName || !HASHED_NAMESPACE.test(entry)) continue;
      const candidate = path.join(this.spoolRoot, entry);
      if (!(await this.isSafelyExpiredNamespace(candidate, oldestAllowed))) {
        continue;
      }

      // The first scan is only a cheap eligibility hint. Successful `wx`
      // acquisition is the cross-process linearization point; the second scan
      // is authoritative and deletion remains fenced until `rm` completes.
      const control = await this.tryAcquireNamespaceControl(entry);
      if (!control) continue;
      try {
        if (!(await this.isSafelyExpiredNamespace(candidate, oldestAllowed))) {
          continue;
        }
        await this.fileSystem.rm(candidate, { recursive: true, force: true });
      } finally {
        await control.release();
      }
    }
  }

  /**
   * Validate the complete, deliberately shallow namespace shape before an
   * orphan delete. Recent engine-directory or artifact mtimes protect a live
   * long-running process even when its process-root mtime itself is old.
   */
  private async isSafelyExpiredNamespace(
    processRoot: string,
    oldestAllowed: number
  ): Promise<boolean> {
    let processStats;
    try {
      processStats = await this.fileSystem.lstat(processRoot);
    } catch (error) {
      if (isMissingSpoolError(error)) return false;
      throw error;
    }
    if (processStats.isSymbolicLink() || !processStats.isDirectory()) {
      return false;
    }
    const processMtimeMs = processStats.mtimeMs;
    if (!this.isPastOrphanTtl(processMtimeMs, oldestAllowed)) return false;
    let entries: readonly string[];
    try {
      entries = await this.fileSystem.readdir(processRoot);
    } catch (error) {
      if (isMissingSpoolError(error)) return false;
      throw error;
    }

    if (!entries.includes(LIVENESS_MARKER)) return false;
    let markerStats;
    try {
      markerStats = await this.fileSystem.lstat(
        path.join(processRoot, LIVENESS_MARKER)
      );
    } catch (error) {
      if (isMissingSpoolError(error)) return false;
      throw error;
    }
    if (
      markerStats.isSymbolicLink() ||
      !markerStats.isFile() ||
      !this.isPastOrphanTtl(markerStats.mtimeMs, oldestAllowed)
    ) {
      return false;
    }

    for (const engineName of entries) {
      if (engineName === LIVENESS_MARKER) continue;
      if (!HASHED_NAMESPACE.test(engineName)) return false;
      const engineRoot = path.join(processRoot, engineName);
      let engineStats;
      try {
        engineStats = await this.fileSystem.lstat(engineRoot);
      } catch (error) {
        if (isMissingSpoolError(error)) return false;
        throw error;
      }
      if (
        engineStats.isSymbolicLink() ||
        !engineStats.isDirectory() ||
        !this.isPastOrphanTtl(engineStats.mtimeMs, oldestAllowed)
      ) {
        return false;
      }

      let artifactNames: readonly string[];
      try {
        artifactNames = await this.fileSystem.readdir(engineRoot);
      } catch (error) {
        if (isMissingSpoolError(error)) return false;
        throw error;
      }
      for (const artifactName of artifactNames) {
        if (!HASHED_ARTIFACT.test(artifactName)) return false;
        let artifactStats;
        try {
          artifactStats = await this.fileSystem.lstat(
            path.join(engineRoot, artifactName)
          );
        } catch (error) {
          if (isMissingSpoolError(error)) return false;
          throw error;
        }
        if (
          artifactStats.isSymbolicLink() ||
          !artifactStats.isFile() ||
          !this.isPastOrphanTtl(artifactStats.mtimeMs, oldestAllowed)
        ) {
          return false;
        }
      }
    }
    return true;
  }

  private isPastOrphanTtl(mtimeMs: number, oldestAllowed: number): boolean {
    return Number.isFinite(mtimeMs) && mtimeMs < oldestAllowed;
  }

  private async closeOnce(): Promise<void> {
    const lifecycleOperations: Promise<unknown>[] = [];
    if (this.globalInitialization) {
      lifecycleOperations.push(this.globalInitialization);
    }
    if (this.namespaceEnsuring) {
      lifecycleOperations.push(this.namespaceEnsuring);
    }
    if (this.heartbeatInFlight) {
      lifecycleOperations.push(this.heartbeatInFlight);
    }
    await Promise.allSettled(lifecycleOperations);
    await this.waitForCreations();

    let namespaceControl: NamespaceControlLease | undefined;
    let firstError: Error | undefined;
    if (this.processNamespaceOwned) {
      try {
        namespaceControl = await this.tryAcquireNamespaceControl(
          path.basename(this.processRoot)
        );
        if (!namespaceControl) {
          firstError = this.namespaceControlContentionError();
        }
      } catch (error) {
        firstError = error instanceof Error ? error : new Error(String(error));
      }
    }

    const trackedArtifacts = [...this.artifactReservations];
    const results = await Promise.allSettled(
      namespaceControl
        ? trackedArtifacts.map(([artifact]) => artifact.dispose())
        : []
    );
    for (const result of results) {
      if (result.status === 'rejected' && !firstError) {
        firstError =
          result.reason instanceof Error
            ? result.reason
            : new Error(String(result.reason));
      }
    }
    let namespaceRemoved = !this.processNamespaceOwned;
    if (this.processNamespaceOwned && namespaceControl) {
      try {
        await this.fileSystem.rm(this.processRoot, {
          recursive: true,
          force: true,
        });
        namespaceRemoved = true;
      } catch (error) {
        if (isMissingSpoolError(error)) {
          namespaceRemoved = true;
        } else if (!firstError) {
          firstError = classifySpoolFileError(
            error,
            'removing the process spool namespace'
          );
        }
      }
    }
    if (namespaceControl) {
      try {
        await namespaceControl.release();
      } catch (error) {
        if (!firstError) {
          firstError =
            error instanceof Error ? error : new Error(String(error));
        }
      }
    }
    if (namespaceRemoved) {
      this.processNamespaceOwned = false;
      for (const [artifact, reservation] of trackedArtifacts) {
        reservation.release();
        this.artifacts.delete(artifact);
        this.artifactReservations.delete(artifact);
      }
      for (const cleanup of this.pendingArtifactCleanups) {
        cleanup.reservation.release();
        this.pendingArtifactCleanups.delete(cleanup);
      }
    }
    if (firstError) throw firstError;
  }

  private async refreshLivenessMarker(): Promise<void> {
    const markerPath = path.join(this.processRoot, LIVENESS_MARKER);
    try {
      const marker = await this.fileSystem.open(markerPath, 'wx', FILE_MODE);
      await marker.close();
    } catch (error) {
      if (!isExistingSpoolError(error)) {
        throw classifySpoolFileError(error, 'creating spool liveness marker');
      }
    }

    const markerStats = await this.fileSystem.lstat(markerPath);
    if (markerStats.isSymbolicLink() || !markerStats.isFile()) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_UNAVAILABLE',
        'Spool liveness marker is not a safe file'
      );
    }
    const now = this.clock();
    if (!Number.isFinite(now)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_IO',
        'Spool clock returned an invalid timestamp'
      );
    }
    await this.fileSystem.utimes(markerPath, now, now);
  }

  /**
   * Try to acquire the stable cross-process fence for one hashed namespace.
   * The lock lives outside the recursively deleted namespace so its atomic
   * `wx` claim remains visible until destructive cleanup has fully finished.
   * Existing or crash-left locks are never stolen: callers conservatively
   * receive `undefined` without polling or retrying.
   */
  private async tryAcquireNamespaceControl(
    namespaceName: string
  ): Promise<NamespaceControlLease | undefined> {
    if (!HASHED_NAMESPACE.test(namespaceName)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        'Spool namespace control requires a hashed identifier'
      );
    }
    await this.ensureLivenessLockRoot();
    const lockPath = path.join(
      this.livenessLockRoot,
      `${namespaceName}${LIVENESS_LOCK_SUFFIX}`
    );
    let handle: SpoolFileHandle;
    try {
      handle = await this.fileSystem.open(lockPath, 'wx', FILE_MODE);
    } catch (error) {
      if (isExistingSpoolError(error)) return undefined;
      throw classifySpoolFileError(error, 'acquiring spool namespace control');
    }

    try {
      const stats = await this.fileSystem.lstat(lockPath);
      if (stats.isSymbolicLink() || !stats.isFile()) {
        throw new UsenetSpoolError(
          'USENET_SPOOL_UNAVAILABLE',
          'Spool namespace control is not a safe file'
        );
      }
    } catch (error) {
      await Promise.allSettled([handle.close()]);
      throw classifySpoolFileError(error, 'validating spool namespace control');
    }

    let releasePromise: Promise<void> | undefined;
    return {
      release: () => {
        releasePromise ??= this.releaseNamespaceControl(handle, lockPath);
        return releasePromise;
      },
    };
  }

  private async releaseNamespaceControl(
    handle: SpoolFileHandle,
    lockPath: string
  ): Promise<void> {
    try {
      await handle.close();
    } catch (error) {
      throw classifySpoolFileError(error, 'closing spool namespace control');
    }
    try {
      await this.fileSystem.rm(lockPath, { force: false });
    } catch (error) {
      if (isMissingSpoolError(error)) return;
      throw classifySpoolFileError(error, 'releasing spool namespace control');
    }
  }

  private async ensureLivenessLockRoot(): Promise<void> {
    await this.fileSystem.mkdir(this.livenessLockRoot, {
      recursive: true,
      mode: DIRECTORY_MODE,
    });
    await this.assertSafeDirectory(this.livenessLockRoot);
  }

  private scheduleHeartbeat(): void {
    if (this.closed || this.heartbeatTask || this.heartbeatInFlight) return;
    try {
      this.heartbeatTask = this.scheduler(async () => {
        this.heartbeatTask = undefined;
        const operation = this.runHeartbeatOnce();
        this.heartbeatInFlight = operation;
        try {
          await operation;
        } finally {
          if (this.heartbeatInFlight === operation) {
            this.heartbeatInFlight = undefined;
          }
          this.scheduleHeartbeat();
        }
      }, this.heartbeatIntervalMs);
    } catch (error) {
      throw classifySpoolFileError(error, 'scheduling spool heartbeat');
    }
  }

  private async runHeartbeatOnce(): Promise<void> {
    if (this.closed) return;
    try {
      await this.ensureOwnNamespace();
    } catch (error) {
      if (!this.closed) {
        throw classifySpoolFileError(error, 'refreshing spool heartbeat');
      }
    }
  }

  private stopHeartbeat(): void {
    this.heartbeatTask?.cancel();
    this.heartbeatTask = undefined;
  }

  private async assertSafeDirectory(directory: string): Promise<void> {
    const stats = await this.fileSystem.lstat(directory);
    if (stats.isSymbolicLink() || !stats.isDirectory()) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_UNAVAILABLE',
        'Spool namespace is not a safe directory'
      );
    }
  }

  private waitForCreations(): Promise<void> {
    if (this.creatingArtifacts === 0) return Promise.resolve();
    this.creationDrain ??= Promise.withResolvers<void>();
    return this.creationDrain.promise;
  }

  private async removeUntrackedArtifactFiles(
    partialPath: string,
    readyPath: string
  ): Promise<void> {
    for (const filePath of [partialPath, readyPath]) {
      try {
        await this.fileSystem.rm(filePath, { force: true });
      } catch (error) {
        if (!isMissingSpoolError(error)) {
          throw classifySpoolFileError(
            error,
            'cleaning up an untracked spool file'
          );
        }
      }
    }
  }

  private assertArtifactId(value: string, field: string): void {
    if (!value) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_INVALID_ARGUMENT',
        `Spool artifact ${field} cannot be empty`
      );
    }
  }

  private nextArtifactSequence(): number {
    if (!Number.isSafeInteger(this.artifactSequence)) {
      throw new UsenetSpoolError(
        'USENET_SPOOL_CAPACITY',
        'Spool artifact sequence is exhausted'
      );
    }
    return this.artifactSequence++;
  }

  private closedError(): UsenetSpoolError {
    return new UsenetSpoolError(
      'USENET_SPOOL_CLOSED',
      'Spool manager is closed'
    );
  }

  private namespaceControlContentionError(): UsenetSpoolError {
    return new UsenetSpoolError(
      'USENET_SPOOL_UNAVAILABLE',
      'Spool namespace is fenced by another process'
    );
  }
}
