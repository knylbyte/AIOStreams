import type { Response } from 'express';

/** Errors raised after stream or engine admission has been synchronously sealed. */
export function isStreamShutdownError(error: unknown): boolean {
  if (typeof error !== 'object' || error === null || !('code' in error)) {
    return false;
  }
  if (error.code === 'USENET_ENGINE_CLOSED') return true;
  return (
    error.code === 'STREAM_STOPPED' &&
    'reason' in error &&
    error.reason === 'shutdown'
  );
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
