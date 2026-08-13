export type UsenetSpoolErrorCode =
  | 'USENET_SPOOL_UNAVAILABLE'
  | 'USENET_SPOOL_CAPACITY'
  | 'USENET_SPOOL_DISK_FULL'
  | 'USENET_SPOOL_IO'
  | 'USENET_SPOOL_NOT_FOUND'
  | 'USENET_SPOOL_OPEN_FILE_LIMIT'
  | 'USENET_SPOOL_CLOSED'
  | 'USENET_SPOOL_ABORTED'
  | 'USENET_SPOOL_INVALID_ARGUMENT'
  | 'USENET_MEMORY_BUDGET';

/** Stable, credential-free error taxonomy for transient spool operations. */
export class UsenetSpoolError extends Error {
  override readonly cause?: unknown;

  constructor(
    readonly code: UsenetSpoolErrorCode,
    message: string,
    options: { readonly cause?: unknown } = {}
  ) {
    super(message);
    this.name =
      code === 'USENET_SPOOL_ABORTED' ? 'AbortError' : 'UsenetSpoolError';
    if (options.cause !== undefined) this.cause = options.cause;
    Error.captureStackTrace?.(this, UsenetSpoolError);
  }
}

function errnoCode(error: unknown): string | undefined {
  if (!(error instanceof Error) || !('code' in error)) return undefined;
  return typeof error.code === 'string' ? error.code : undefined;
}

/** True only for an atomic-create collision. */
export function isExistingSpoolError(error: unknown): boolean {
  return errnoCode(error) === 'EEXIST';
}

/** Convert an async filesystem failure into a stable spool error. */
export function classifySpoolFileError(
  error: unknown,
  operation: string
): UsenetSpoolError {
  if (error instanceof UsenetSpoolError) return error;
  const code = errnoCode(error);
  if (code === 'ENOSPC' || code === 'EDQUOT') {
    return new UsenetSpoolError(
      'USENET_SPOOL_DISK_FULL',
      `Insufficient disk space while ${operation}`,
      { cause: error }
    );
  }
  if (code === 'EACCES' || code === 'EPERM' || code === 'EROFS') {
    return new UsenetSpoolError(
      'USENET_SPOOL_UNAVAILABLE',
      `Spool storage is unavailable while ${operation}`,
      { cause: error }
    );
  }
  if (code === 'ENOENT') {
    return new UsenetSpoolError(
      'USENET_SPOOL_NOT_FOUND',
      `Spool data disappeared while ${operation}`,
      { cause: error }
    );
  }
  return new UsenetSpoolError(
    'USENET_SPOOL_IO',
    `Spool I/O failed while ${operation}`,
    { cause: error }
  );
}

/** Create the shared typed cancellation error without exposing path data. */
export function spoolAbortError(reason?: unknown): UsenetSpoolError {
  if (
    reason instanceof UsenetSpoolError &&
    reason.code === 'USENET_SPOOL_ABORTED'
  ) {
    return reason;
  }
  return new UsenetSpoolError(
    'USENET_SPOOL_ABORTED',
    'Spool operation aborted',
    { cause: reason }
  );
}

/** True only for the benign "already removed" cleanup condition. */
export function isMissingSpoolError(error: unknown): boolean {
  return (
    (error instanceof UsenetSpoolError &&
      error.code === 'USENET_SPOOL_NOT_FOUND') ||
    errnoCode(error) === 'ENOENT'
  );
}
