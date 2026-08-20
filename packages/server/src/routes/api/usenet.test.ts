import type { Server } from 'node:http';
import { once } from 'node:events';
import { PassThrough } from 'node:stream';
import express, {
  type NextFunction,
  type Request,
  type Response,
} from 'express';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from 'vitest';

const mocks = vi.hoisted(() => ({
  openNativeUsenetStream: vi.fn(),
  debug: vi.fn(),
  info: vi.fn(),
  warn: vi.fn(),
}));

vi.mock('@aiostreams/core', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@aiostreams/core')>();
  return {
    ...actual,
    createLogger: () => ({
      debug: mocks.debug,
      info: mocks.info,
      warn: mocks.warn,
    }),
    openNativeUsenetStream: mocks.openNativeUsenetStream,
  };
});

vi.mock('../../app.js', () => ({
  mapDebridErrorToStaticFile: (code: string) => `${code}.html`,
}));

vi.mock('../../middlewares/cors.js', () => ({
  corsMiddleware: (_request: unknown, _response: unknown, next: () => void) =>
    next(),
}));

import {
  DebridError,
  PrioritySemaphoreError,
  toDebridError,
} from '@aiostreams/core';
import usenetRouter from './usenet.js';

function opened(stream: PassThrough, size = 6) {
  return {
    stream,
    size,
    start: 0,
    end: size,
    filename: 'video.mkv',
    etag: '"test-etag"',
    lastModified: new Date('2024-01-01T00:00:00Z'),
  };
}

async function waitForCalls(
  mock: { readonly mock: { readonly calls: readonly unknown[][] } },
  count: number
): Promise<void> {
  for (let turn = 0; turn < 20; turn++) {
    if (mock.mock.calls.length >= count) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  expect(mock.mock.calls).toHaveLength(count);
}

async function waitForStreamErrorOwnership(stream: PassThrough): Promise<void> {
  for (let turn = 0; turn < 20; turn++) {
    if (stream.listenerCount('error') >= 2) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  expect(stream.listenerCount('error')).toBeGreaterThanOrEqual(2);
}

const CAPACITY_CODES = [
  'SEMAPHORE_GLOBAL_CAPACITY',
  'SEMAPHORE_OWNER_CAPACITY',
  'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
] as const;

const CAPACITY_DETAIL =
  'The Usenet download scheduler is at capacity. Please retry shortly.';

describe('native usenet route failure ownership', () => {
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    const app = express();
    app.use('/usenet', usenetRouter);
    app.use(
      (
        error: unknown,
        _request: Request,
        response: Response,
        _next: NextFunction
      ) => {
        response.status(500).json({
          error: error instanceof Error ? error.name : 'UnknownError',
        });
      }
    );
    server = app.listen(0, '127.0.0.1');
    await once(server, 'listening');
    const address = server.address();
    if (!address || typeof address === 'string') {
      throw new Error('usenet route test server did not bind a TCP port');
    }
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  beforeEach(() => {
    mocks.openNativeUsenetStream.mockReset();
    mocks.debug.mockReset();
    mocks.info.mockReset();
    mocks.warn.mockReset();
  });

  afterAll(async () => {
    server.closeAllConnections?.();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  test('an internal post-header failure is logged once with its root cause', async () => {
    const stream = new PassThrough();
    mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
    const responsePromise = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`
    );
    stream.write(Buffer.from('a'));
    const response = await responsePromise;
    expect(response.status).toBe(200);

    const root = Object.assign(new Error('secret-message-id=<do-not-log>'), {
      code: 'USENET_STREAMING_LOCAL_BACKPRESSURE',
    });
    const outer = Object.assign(new Error('secret outer path'), {
      code: 'USENET_SPOOL_IO',
      cause: root,
    });
    stream.destroy(outer);

    await expect(response.arrayBuffer()).rejects.toBeDefined();
    await waitForCalls(mocks.warn, 1);
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    const [fields, message] = mocks.warn.mock.calls[0]!;
    expect(message).toBe('usenet stream failed after headers sent');
    expect(fields).toMatchObject({
      outerErrorName: 'Error',
      outerCode: 'USENET_SPOOL_IO',
      rootErrorName: 'Error',
      rootCode: 'USENET_STREAMING_LOCAL_BACKPRESSURE',
      streamTermination: 'internal_error',
      headersSent: true,
      clientAborted: false,
      responseClosedByInternalFailure: true,
    });
    expect(JSON.stringify(fields)).not.toContain('secret');
    expect(
      mocks.debug.mock.calls.some(
        ([, text]) => text === 'client disconnected from usenet stream'
      )
    ).toBe(false);
  });

  test('a genuine client abort remains a quiet client-owned cancellation', async () => {
    const stream = new PassThrough();
    mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
    const controller = new AbortController();
    const responsePromise = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      { signal: controller.signal }
    );
    stream.write(Buffer.from('a'));
    const response = await responsePromise;
    expect(response.status).toBe(200);

    controller.abort();
    await expect(response.arrayBuffer()).rejects.toBeDefined();
    await waitForCalls(mocks.debug, 2);
    expect(mocks.warn).not.toHaveBeenCalled();
    expect(
      mocks.debug.mock.calls.some(
        ([, text]) => text === 'client disconnected from usenet stream'
      )
    ).toBe(true);
  });

  test('a pre-header DebridError retains the existing public mapping', async () => {
    mocks.openNativeUsenetStream.mockRejectedValue(
      new DebridError('safe public failure', {
        statusCode: 503,
        statusText: 'Service Unavailable',
        code: 'SERVICE_UNAVAILABLE',
        headers: {},
        body: null,
        type: 'api_error',
      })
    );
    const response = await fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv?download=1`
    );
    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual({
      success: false,
      detail: 'safe public failure',
    });
    expect(mocks.warn).toHaveBeenCalledTimes(1);
  });

  test('opening capacity is a stable public 503 without a playback redirect', async () => {
    for (const [index, code] of CAPACITY_CODES.entries()) {
      mocks.openNativeUsenetStream.mockReset();
      mocks.warn.mockReset();
      const capacity = new PrioritySemaphoreError(
        code,
        'internal owner and scheduler detail'
      );
      mocks.openNativeUsenetStream.mockRejectedValue(
        index % 2 === 0 ? capacity : toDebridError(capacity)
      );

      const response = await fetch(
        `${baseUrl}/usenet/stream/test-token/video.mkv`,
        { redirect: 'manual' }
      );
      expect(response.status).toBe(503);
      expect(response.headers.get('location')).toBeNull();
      await expect(response.json()).resolves.toEqual({
        success: false,
        detail: CAPACITY_DETAIL,
        usenetCode: code,
      });
      expect(mocks.warn).toHaveBeenCalledTimes(1);
      expect(JSON.stringify(mocks.warn.mock.calls)).not.toContain(
        'internal owner and scheduler detail'
      );
    }
  });

  test('lazy capacity before first byte uses the same 503 taxonomy', async () => {
    for (const code of CAPACITY_CODES) {
      mocks.openNativeUsenetStream.mockReset();
      mocks.debug.mockReset();
      mocks.warn.mockReset();
      const stream = new PassThrough();
      mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
      const responsePromise = fetch(
        `${baseUrl}/usenet/stream/test-token/video.mkv`,
        { redirect: 'manual' }
      );
      await waitForStreamErrorOwnership(stream);
      stream.destroy(
        new PrioritySemaphoreError(code, 'internal lazy scheduler detail')
      );

      const response = await responsePromise;
      expect(response.status).toBe(503);
      expect(response.headers.get('location')).toBeNull();
      await expect(response.json()).resolves.toEqual({
        success: false,
        detail: CAPACITY_DETAIL,
        usenetCode: code,
      });
      expect(mocks.warn).toHaveBeenCalledTimes(1);
      expect(
        mocks.debug.mock.calls.some(
          ([, text]) => text === 'client disconnected from usenet stream'
        )
      ).toBe(false);
      expect(JSON.stringify(mocks.warn.mock.calls)).not.toContain(
        'internal lazy scheduler detail'
      );
    }
  });

  test('post-header capacity destroys once and retains its typed local root cause', async () => {
    const stream = new PassThrough();
    mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
    const responsePromise = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`
    );
    stream.write(Buffer.from('a'));
    const response = await responsePromise;
    expect(response.status).toBe(200);

    stream.destroy(
      new PrioritySemaphoreError(
        'SEMAPHORE_OWNER_CAPACITY',
        'internal owner key'
      )
    );

    await expect(response.arrayBuffer()).rejects.toBeDefined();
    await waitForCalls(mocks.warn, 1);
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    const [fields, message] = mocks.warn.mock.calls[0]!;
    expect(message).toBe('usenet stream failed after headers sent');
    expect(fields).toMatchObject({
      rootErrorName: 'PrioritySemaphoreError',
      rootCode: 'SEMAPHORE_OWNER_CAPACITY',
      faultDomain: 'local',
      streamTermination: 'internal_error',
      headersSent: true,
      clientAborted: false,
      responseClosedByInternalFailure: true,
    });
    expect(JSON.stringify(fields)).not.toContain('internal owner key');
    expect(
      mocks.debug.mock.calls.some(
        ([, text]) => text === 'client disconnected from usenet stream'
      )
    ).toBe(false);
  });

  test('download admission capacity remains a stable 503 with its public code', async () => {
    const capacity = new PrioritySemaphoreError(
      'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
      'internal scheduler detail'
    );
    mocks.openNativeUsenetStream.mockRejectedValue(toDebridError(capacity));

    const response = await fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv?download=1`
    );
    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual({
      success: false,
      detail: CAPACITY_DETAIL,
      usenetCode: 'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
    });
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(mocks.warn.mock.calls)).not.toContain(
      'internal scheduler detail'
    );
  });

  test('non-capacity playback errors retain static and download behavior', async () => {
    const error = new DebridError('safe public failure', {
      statusCode: 503,
      statusText: 'Service Unavailable',
      code: 'SERVICE_UNAVAILABLE',
      headers: {},
      body: null,
      type: 'api_error',
    });
    mocks.openNativeUsenetStream.mockRejectedValue(error);

    const playback = await fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      { redirect: 'manual' }
    );
    expect(playback.status).toBe(302);
    expect(playback.headers.get('location')).toBe(
      '/static/SERVICE_UNAVAILABLE.html'
    );

    mocks.openNativeUsenetStream.mockRejectedValue(error);
    const download = await fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv?download=1`
    );
    expect(download.status).toBe(503);
    await expect(download.json()).resolves.toEqual({
      success: false,
      detail: 'safe public failure',
    });
  });
});
