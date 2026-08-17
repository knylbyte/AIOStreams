import type { Response } from 'express';

/** Errors raised after stream or engine admission has been synchronously sealed. */
export function isStreamShutdownError(error: unknown): boolean {
  if (typeof error !== 'object' || error === null || !('code' in error)) {
    return false;
  }
  return (
    error.code === 'STREAM_STOPPED' || error.code === 'USENET_ENGINE_CLOSED'
  );
}

/** Stable pre-header response for a request overtaken by shutdown. */
export function sendStreamShutdownResponse(response: Response): void {
  response
    .status(503)
    .setHeader('Connection', 'close')
    .json({ error: 'Server is shutting down', success: false });
}
