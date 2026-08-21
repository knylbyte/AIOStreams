import type { Response } from 'express';

const MAX_SHUTDOWN_ERROR_DEPTH = 8;

function isShutdownMarker(error: object): boolean {
  if (!('code' in error)) return false;
  if (error.code === 'PROCESS_SHUTDOWN') return true;
  if (error.code === 'USENET_ENGINE_CLOSED') return true;
  return (
    error.code === 'STREAM_STOPPED' &&
    'reason' in error &&
    error.reason === 'shutdown'
  );
}

/** Errors raised after stream or engine admission has been synchronously sealed. */
export function isStreamShutdownError(error: unknown): boolean {
  let current = error;
  const seen = new Set<object>();
  for (let depth = 0; depth < MAX_SHUTDOWN_ERROR_DEPTH; depth++) {
    if (typeof current !== 'object' || current === null || seen.has(current)) {
      return false;
    }
    seen.add(current);
    if (isShutdownMarker(current)) return true;

    if (current instanceof AggregateError) {
      if (current.cause instanceof Error) {
        current = current.cause;
        continue;
      }
      let firstError: Error | undefined;
      const inspected = Math.min(
        current.errors.length,
        MAX_SHUTDOWN_ERROR_DEPTH - depth - 1
      );
      for (let index = 0; index < inspected; index++) {
        const candidate = current.errors[index];
        if (candidate instanceof Error) {
          firstError = candidate;
          break;
        }
      }
      if (!firstError) return false;
      current = firstError;
      continue;
    }

    if (current instanceof Error && current.cause instanceof Error) {
      current = current.cause;
      continue;
    }
    return false;
  }
  return false;
}

/** Stable pre-header response for a request overtaken by shutdown. */
export function sendStreamShutdownResponse(response: Response): void {
  // A proxy may have staged upstream headers through `res.set()` without
  // flushing them yet. Remove representation-specific values before replacing
  // that unsent response with the stable JSON shutdown contract.
  for (const name of [
    'Content-Type',
    'Content-Encoding',
    'Content-Length',
    'Content-Range',
    'Content-Disposition',
    'Transfer-Encoding',
  ]) {
    response.removeHeader(name);
  }
  response
    .status(503)
    .setHeader('Connection', 'close')
    .json({ error: 'Server is shutting down', success: false });
}
