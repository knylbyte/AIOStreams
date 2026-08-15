/** Lifecycle of one transient, file-backed segment artifact. */
export type SpoolArtifactState =
  | 'created'
  | 'writing'
  | 'complete'
  | 'failed'
  | 'disposed';

/** Minimal filesystem-stat shape needed for the free-space guard. */
export interface SpoolStatFs {
  readonly bavail: number | bigint;
  readonly bsize: number | bigint;
}

/** Minimal lstat shape used by safe orphan cleanup. */
export interface SpoolPathStats {
  readonly mtimeMs: number;
  isDirectory(): boolean;
  isFile(): boolean;
  isSymbolicLink(): boolean;
}

/** Narrow async file handle used by the spool hot path. */
export interface SpoolFileHandle {
  read(
    buffer: Buffer,
    offset: number,
    length: number,
    position: number
  ): Promise<{ readonly bytesRead: number }>;
  write(
    buffer: Buffer,
    offset: number,
    length: number,
    position: number
  ): Promise<{ readonly bytesWritten: number }>;
  close(): Promise<void>;
}

/** Narrow, fully asynchronous filesystem boundary used by {@link SpoolManager}. */
export interface SpoolFileSystem {
  mkdir(
    path: string,
    options: { readonly recursive: boolean; readonly mode: number }
  ): Promise<void>;
  open(path: string, flags: string, mode?: number): Promise<SpoolFileHandle>;
  rename(oldPath: string, newPath: string): Promise<void>;
  rm(
    path: string,
    options: { readonly force: boolean; readonly recursive?: boolean }
  ): Promise<void>;
  readdir(path: string): Promise<readonly string[]>;
  lstat(path: string): Promise<SpoolPathStats>;
  statfs(path: string): Promise<SpoolStatFs>;
  /** Update one control-file timestamp; arguments are Unix epoch milliseconds. */
  utimes(path: string, atimeMs: number, mtimeMs: number): Promise<void>;
}

/** A globally counted open file whose close operation is idempotent. */
export interface ManagedSpoolFile {
  readonly handle: SpoolFileHandle;
  close(): Promise<void>;
}

/** Shared, abortable open operation owned by the manager's global file cap. */
export type OpenManagedSpoolFile = (
  path: string,
  flags: string,
  mode: number | undefined,
  signal: AbortSignal | undefined
) => Promise<ManagedSpoolFile>;

/** Point-in-time disk-budget accounting. */
export interface SpoolBudgetStats {
  readonly maxBytes: number;
  readonly reservedBytes: number;
  readonly actualBytes: number;
  readonly peakReservedBytes: number;
  readonly peakActualBytes: number;
  readonly waiting: number;
}

/**
 * Ownership token for one spool file's reservation. `recordWritten` may only
 * advance within the reserved range; `release` is idempotent and is called
 * only after the corresponding file has actually been removed.
 */
export interface SpoolBudgetLease {
  readonly reservedBytes: number;
  readonly writtenBytes: number;
  grow(
    bytes: number,
    options?: { readonly signal?: AbortSignal }
  ): Promise<void>;
  recordWritten(bytes: number): void;
  release(): void;
}

/** Open-file limiter accounting shared by spool writers and readers. */
export interface OpenFileStats {
  readonly maxFiles: number;
  readonly openFiles: number;
  readonly waiting: number;
  readonly peakOpenFiles: number;
}

/** Manager-level resource snapshot. */
export interface SpoolManagerStats {
  readonly budget: SpoolBudgetStats;
  readonly files: OpenFileStats;
  readonly artifacts: number;
}

/** Byte range for a reader; `endExclusive` follows standard slice semantics. */
export interface GrowingFileReadOptions {
  readonly start?: number;
  readonly endExclusive?: number;
  readonly signal?: AbortSignal;
  readonly highWaterMark?: number;
  /** Internal producer/decoder validation that must precede successful EOF. */
  readonly completion?: Promise<void>;
}

/** Keeps a complete spool file alive while cache promotion is in progress. */
export interface SpoolPromotionLease {
  readonly path: string;
  release(): void;
}

/** Immutable state observed by a growing reader. */
export interface GrowingArtifactSnapshot {
  readonly state: SpoolArtifactState;
  readonly committedBytes: number;
  readonly error?: Error;
}

/** Internal source contract consumed by {@link GrowingFileReader}. */
export interface GrowingReadableSource {
  snapshot(): GrowingArtifactSnapshot;
  waitForChange(position: number, signal: AbortSignal): Promise<void>;
  openReadableFile(signal: AbortSignal): Promise<ManagedSpoolFile>;
}

/** Injectable wall clock used only for orphan-age decisions. */
export type SpoolClock = () => number;

/** Injectable source of process/artifact uniqueness; outputs are always hashed. */
export type SpoolIdGenerator = () => string;

/** One cancellable, already-unrefed manager-control timer. */
export interface SpoolScheduledTask {
  cancel(): void;
}

/** Injectable single-shot scheduler used only by the process heartbeat. */
export type SpoolScheduler = (
  callback: () => Promise<void>,
  delayMs: number
) => SpoolScheduledTask;
