import type { Server } from 'node:http';
import express from 'express';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import {
  isStreamShutdownError,
  sendStreamShutdownResponse,
} from './stream-shutdown.js';

describe('stream shutdown response contract', () => {
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    const app = express();
    app.get('/stopped', (_request, response) => {
      sendStreamShutdownResponse(response);
    });
    server = await new Promise<Server>((resolve, reject) => {
      const listener = app.listen(0, '127.0.0.1', () => resolve(listener));
      listener.once('error', reject);
    });
    const address = server.address();
    if (!address || typeof address === 'string') {
      throw new Error('stream shutdown test server did not bind a TCP port');
    }
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  afterAll(async () => {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  });

  test('recognises stream and engine admission fences only', () => {
    expect(
      isStreamShutdownError({ code: 'STREAM_STOPPED', reason: 'shutdown' })
    ).toBe(true);
    expect(isStreamShutdownError({ code: 'USENET_ENGINE_CLOSED' })).toBe(true);
    expect(
      isStreamShutdownError({ code: 'STREAM_STOPPED', reason: 'limit' })
    ).toBe(false);
    expect(
      isStreamShutdownError({ code: 'STREAM_STOPPED', reason: 'stale' })
    ).toBe(false);
    expect(isStreamShutdownError({ code: 'STREAM_STOPPED' })).toBe(false);
    expect(isStreamShutdownError({ code: 'ECONNRESET' })).toBe(false);
    expect(isStreamShutdownError(new Error('stopped'))).toBe(false);
  });

  test('writes the stable pre-header 503 response', async () => {
    const response = await fetch(`${baseUrl}/stopped`);
    expect(response.status).toBe(503);
    expect(response.headers.get('connection')).toBe('close');
    await expect(response.json()).resolves.toEqual({
      error: 'Server is shutting down',
      success: false,
    });
  });
});
