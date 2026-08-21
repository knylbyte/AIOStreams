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
  responseJson: vi.fn(),
  responseRedirect: vi.fn(),
  responseClose: vi.fn(),
  responseAfterClose: vi.fn(),
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
  ArticleNotFoundError,
  DebridError,
  NntpError,
  PrioritySemaphoreError,
  toDebridError,
  UsenetEngineClosedError,
  UsenetSpoolError,
  YencDecodeError,
  YencMetadataError,
} from '@aiostreams/core';
import usenetRouter, {
  destroySourceAndWait,
  pipeToResponse,
} from './usenet.js';

class DeferredCloseStream extends PassThrough {
  readonly closeStarted = Promise.withResolvers<void>();
  private readonly closeGate = Promise.withResolvers<void>();
  private closeError: Error | undefined;

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    this.closeStarted.resolve();
    void this.closeGate.promise.then(() => callback(this.closeError ?? error));
  }

  fail(error: Error): void {
    this.emit('error', error);
  }

  releaseClose(error?: Error): void {
    this.closeError = error;
    this.closeGate.resolve();
  }
}

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

async function expectPending(promise: Promise<unknown>): Promise<void> {
  let settled = false;
  void promise.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    }
  );
  await Promise.resolve();
  await new Promise<void>((resolve) => setImmediate(resolve));
  expect(settled).toBe(false);
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

async function beginLazyFailure(
  url: string,
  error: Error,
  init?: RequestInit
): Promise<{
  readonly stream: DeferredCloseStream;
  readonly response: Promise<globalThis.Response>;
}> {
  const stream = new DeferredCloseStream();
  mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
  const response = fetch(url, init);
  void response.catch(() => undefined);
  await waitForStreamErrorOwnership(stream);
  stream.fail(error);
  await stream.closeStarted.promise;
  return { stream, response };
}

async function expectTypedFailure(
  responsePromise: Promise<globalThis.Response>,
  status: number,
  usenetCode?: string
): Promise<void> {
  const response = await responsePromise;
  expect(response.status).toBe(status);
  expect(response.headers.get('location')).toBeNull();
  const body = await response.json();
  expect(body).toMatchObject({
    success: false,
    ...(usenetCode ? { usenetCode } : {}),
  });
  expect(JSON.stringify(body)).not.toContain('private');
}

function expectNoPublicResponseAttempt(testId: string): void {
  expect(
    mocks.responseJson.mock.calls.filter(([id]) => id === testId)
  ).toHaveLength(0);
  expect(
    mocks.responseRedirect.mock.calls.filter(([id]) => id === testId)
  ).toHaveLength(0);
  expect(
    mocks.responseAfterClose.mock.calls.filter(([id]) => id === testId)
  ).toHaveLength(0);
}

const CAPACITY_CODES = [
  'SEMAPHORE_GLOBAL_CAPACITY',
  'SEMAPHORE_OWNER_CAPACITY',
  'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
] as const;

const CAPACITY_DETAIL =
  'The Usenet download scheduler is at capacity. Please retry shortly.';

describe('usenet response/source settlement', () => {
  test('response finish does not resolve before the readable actually closes', async () => {
    const source = new DeferredCloseStream();
    const destination = new PassThrough();
    destination.resume();
    const piping = pipeToResponse(source, destination);
    source.end(Buffer.from('hello'));

    await source.closeStarted.promise;
    expect(destination.writableFinished).toBe(true);
    await expectPending(piping);
    source.releaseClose();

    await expect(piping).resolves.toBeUndefined();
    expect(source.closed).toBe(true);
    expect(source.listenerCount('error')).toBe(0);
    expect(source.listenerCount('close')).toBe(0);
  });

  test('a cleanup EIO after response finish rejects only after source close', async () => {
    const source = new DeferredCloseStream();
    const destination = new PassThrough();
    destination.resume();
    const piping = pipeToResponse(source, destination);
    source.end(Buffer.from('hello'));
    await source.closeStarted.promise;
    await expectPending(piping);

    const cleanup = Object.assign(new Error('async source close failed'), {
      code: 'EIO',
    });
    source.releaseClose(cleanup);
    await expect(piping).rejects.toBe(cleanup);
    expect(source.closed).toBe(true);
  });

  test('explicit source destruction observes asynchronous close settlement', async () => {
    const source = new DeferredCloseStream();
    const closing = destroySourceAndWait(source);
    await source.closeStarted.promise;
    await expectPending(closing);
    source.releaseClose();
    await expect(closing).resolves.toBeUndefined();
  });
});

describe('native usenet route failure ownership', () => {
  let server: Server;
  let baseUrl: string;

  beforeAll(async () => {
    const app = express();
    app.use((request, response, next) => {
      const testId = request.get('x-route-test-id');
      let responseClosed = false;
      response.once('close', () => {
        responseClosed = true;
        mocks.responseClose(testId);
      });
      const originalStatus = response.status.bind(response);
      response.status = (statusCode: number) => {
        if (responseClosed) {
          mocks.responseAfterClose(testId, 'status', statusCode);
        }
        return originalStatus(statusCode);
      };
      const originalJson = response.json.bind(response);
      response.json = (body: unknown) => {
        if (responseClosed) mocks.responseAfterClose(testId, 'json');
        mocks.responseJson(testId, body);
        return originalJson(body);
      };
      const originalRedirect = response.redirect.bind(response);
      function trackedRedirect(url: string): void;
      function trackedRedirect(status: number, url: string): void;
      function trackedRedirect(statusOrUrl: number | string, url?: string) {
        if (responseClosed) mocks.responseAfterClose(testId, 'redirect');
        mocks.responseRedirect(testId, statusOrUrl, url);
        if (typeof statusOrUrl === 'number') {
          originalRedirect(statusOrUrl, url ?? '/');
        } else {
          originalRedirect(statusOrUrl);
        }
      }
      response.redirect = trackedRedirect;
      next();
    });
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
    mocks.responseJson.mockReset();
    mocks.responseRedirect.mockReset();
    mocks.responseClose.mockReset();
    mocks.responseAfterClose.mockReset();
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
    const testId = 'pure-client-abort';
    const stream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
    const controller = new AbortController();
    const responsePromise = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      {
        signal: controller.signal,
        headers: { 'x-route-test-id': testId },
      }
    );
    stream.write(Buffer.from('a'));
    const response = await responsePromise;
    expect(response.status).toBe(200);

    controller.abort();
    await expect(response.arrayBuffer()).rejects.toBeDefined();
    await stream.closeStarted.promise;
    expect(
      mocks.debug.mock.calls.some(
        ([, text]) => text === 'client disconnected from usenet stream'
      )
    ).toBe(false);
    expect(mocks.warn).not.toHaveBeenCalled();

    stream.releaseClose();
    await waitForCalls(mocks.debug, 2);
    expect(mocks.warn).not.toHaveBeenCalled();
    expect(
      mocks.debug.mock.calls.some(
        ([, text]) => text === 'client disconnected from usenet stream'
      )
    ).toBe(true);
    expectNoPublicResponseAttempt(testId);
  });

  test('a genuine client abort still reports an unexpected async cleanup EIO', async () => {
    const testId = 'client-abort-cleanup-error';
    const stream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
    const controller = new AbortController();
    const responsePromise = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      {
        signal: controller.signal,
        headers: { 'x-route-test-id': testId },
      }
    );
    stream.write(Buffer.from('a'));
    const response = await responsePromise;
    controller.abort();
    await expect(response.arrayBuffer()).rejects.toBeDefined();
    await stream.closeStarted.promise;
    stream.releaseClose(
      Object.assign(new Error('close failed'), { code: 'EIO' })
    );

    await waitForCalls(mocks.warn, 1);
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
      cleanupCode: 'EIO',
      clientAborted: true,
    });
    expectNoPublicResponseAttempt(testId);
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

  test('lazy capacity waits async source close before returning its public 503', async () => {
    const stream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
    const responsePromise = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      { redirect: 'manual' }
    );
    await waitForStreamErrorOwnership(stream);
    stream.fail(
      new PrioritySemaphoreError(
        'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
        'private owner detail'
      )
    );
    await stream.closeStarted.promise;
    await expectPending(responsePromise);

    stream.releaseClose();
    const response = await responsePromise;
    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual({
      success: false,
      detail: CAPACITY_DETAIL,
      usenetCode: 'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
    });
    expect(stream.closed).toBe(true);
  });

  test('lazy capacity remains the public primary when async close also fails', async () => {
    const stream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
    const responsePromise = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      { redirect: 'manual' }
    );
    await waitForStreamErrorOwnership(stream);
    stream.fail(
      new PrioritySemaphoreError(
        'SEMAPHORE_OWNER_CAPACITY',
        'private owner detail'
      )
    );
    await stream.closeStarted.promise;
    const cleanup = Object.assign(new Error('private file path'), {
      code: 'EIO',
    });
    stream.releaseClose(cleanup);

    const response = await responsePromise;
    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toEqual({
      success: false,
      detail: CAPACITY_DETAIL,
      usenetCode: 'SEMAPHORE_OWNER_CAPACITY',
    });
    await waitForCalls(mocks.warn, 1);
    const [fields] = mocks.warn.mock.calls[0]!;
    expect(fields).toMatchObject({
      rootCode: 'SEMAPHORE_OWNER_CAPACITY',
      cleanupCode: 'EIO',
      headersSent: false,
    });
    expect(JSON.stringify(fields)).not.toContain('private');
  });

  test('lazy disk exhaustion maps to 507 after successful source close', async () => {
    const pending = await beginLazyFailure(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      new UsenetSpoolError(
        'USENET_SPOOL_DISK_FULL',
        'private spool path and operation'
      ),
      { redirect: 'manual' }
    );
    pending.stream.releaseClose();
    await expectTypedFailure(pending.response, 507, 'USENET_SPOOL_DISK_FULL');
    expect(mocks.warn).toHaveBeenCalledTimes(1);
  });

  test('lazy disk exhaustion remains 507 when async close also fails', async () => {
    const pending = await beginLazyFailure(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      new UsenetSpoolError('USENET_SPOOL_DISK_FULL', 'private spool path'),
      { redirect: 'manual' }
    );
    pending.stream.releaseClose(
      Object.assign(new Error('private cleanup path'), { code: 'EIO' })
    );
    await expectTypedFailure(pending.response, 507, 'USENET_SPOOL_DISK_FULL');
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
      rootCode: 'USENET_SPOOL_DISK_FULL',
      cleanupCode: 'EIO',
      cleanupErrorCount: 1,
    });
    expect(JSON.stringify(mocks.warn.mock.calls)).not.toContain('private');
  });

  test('lazy spool and memory capacity retain their typed 503 after close EIO', async () => {
    for (const code of [
      'USENET_SPOOL_CAPACITY',
      'USENET_MEMORY_BUDGET',
      'USENET_SPOOL_OPEN_FILE_LIMIT',
      'USENET_SPOOL_CLOSED',
    ] as const) {
      mocks.warn.mockReset();
      const pending = await beginLazyFailure(
        `${baseUrl}/usenet/stream/test-token/video.mkv`,
        new UsenetSpoolError(code, 'private resource detail'),
        { redirect: 'manual' }
      );
      pending.stream.releaseClose(
        Object.assign(new Error('private cleanup path'), { code: 'EIO' })
      );
      await expectTypedFailure(pending.response, 503, code);
      expect(mocks.warn).toHaveBeenCalledTimes(1);
      expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
        rootCode: code,
        cleanupCode: 'EIO',
      });
    }
  });

  test('lazy spool I/O and metadata failures retain their typed 502 after close EIO', async () => {
    for (const code of [
      'USENET_SPOOL_IO',
      'USENET_SPOOL_METADATA_MISMATCH',
    ] as const) {
      mocks.warn.mockReset();
      const pending = await beginLazyFailure(
        `${baseUrl}/usenet/stream/test-token/video.mkv`,
        new UsenetSpoolError(code, 'private spool detail'),
        { redirect: 'manual' }
      );
      pending.stream.releaseClose(
        Object.assign(new Error('private cleanup path'), { code: 'EIO' })
      );
      await expectTypedFailure(pending.response, 502, code);
      expect(mocks.warn).toHaveBeenCalledTimes(1);
      expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
        rootCode: code,
        cleanupCode: 'EIO',
      });
    }
  });

  test('lazy shutdown remains the stable shutdown 503 after close EIO', async () => {
    const shutdowns = [
      new UsenetEngineClosedError(),
      Object.assign(new Error('private process detail'), {
        code: 'PROCESS_SHUTDOWN',
      }),
    ];
    for (const shutdown of shutdowns) {
      mocks.info.mockReset();
      const pending = await beginLazyFailure(
        `${baseUrl}/usenet/stream/test-token/video.mkv`,
        shutdown
      );
      pending.stream.releaseClose(
        Object.assign(new Error('private cleanup path'), { code: 'EIO' })
      );
      const response = await pending.response;
      expect(response.status).toBe(503);
      await expect(response.json()).resolves.toEqual({
        error: 'Server is shutting down',
        success: false,
      });
      expect(mocks.info).toHaveBeenCalledTimes(1);
      expect(mocks.info.mock.calls[0]?.[0]).toMatchObject({
        rootCode: shutdown.code,
        cleanupCode: 'EIO',
        streamTermination: 'shutdown',
      });
    }
  });

  test('lazy Debrid failures preserve playback and download presentation after close EIO', async () => {
    for (const download of [false, true]) {
      mocks.warn.mockReset();
      const error = new DebridError('safe public failure', {
        statusCode: 409,
        statusText: 'Conflict',
        code: 'CONFLICT',
        headers: {},
        body: null,
        type: 'api_error',
      });
      const pending = await beginLazyFailure(
        `${baseUrl}/usenet/stream/test-token/video.mkv${download ? '?download=1' : ''}`,
        error,
        { redirect: 'manual' }
      );
      pending.stream.releaseClose(
        Object.assign(new Error('private cleanup path'), { code: 'EIO' })
      );
      const response = await pending.response;
      if (download) {
        expect(response.status).toBe(409);
        await expect(response.json()).resolves.toEqual({
          success: false,
          detail: 'safe public failure',
        });
      } else {
        expect(response.status).toBe(302);
        expect(response.headers.get('location')).toBe('/static/CONFLICT.html');
      }
      expect(mocks.warn).toHaveBeenCalledTimes(1);
      expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
        cleanupCode: 'EIO',
      });
    }
  });

  test('lazy yEnc and local NNTP failures retain their typed 502 after close EIO', async () => {
    const failures: ReadonlyArray<{
      readonly error: Error;
      readonly code: string;
    }> = [
      {
        error: new YencDecodeError('invalid_header', 'private article data'),
        code: 'USENET_STREAMING_DECODE',
      },
      {
        error: new YencMetadataError('invalid_header', 'private metadata'),
        code: 'USENET_STREAMING_METADATA',
      },
      {
        error: new NntpError('local_backpressure', 'private transport detail'),
        code: 'USENET_STREAMING_LOCAL_BACKPRESSURE',
      },
    ];
    for (const failure of failures) {
      mocks.warn.mockReset();
      const pending = await beginLazyFailure(
        `${baseUrl}/usenet/stream/test-token/video.mkv`,
        failure.error,
        { redirect: 'manual' }
      );
      pending.stream.releaseClose(
        Object.assign(new Error('private cleanup path'), { code: 'EIO' })
      );
      await expectTypedFailure(pending.response, 502, failure.code);
      expect(mocks.warn).toHaveBeenCalledTimes(1);
      expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
        cleanupCode: 'EIO',
      });
    }
  });

  test('lazy article-not-found remains 404 after close EIO', async () => {
    const pending = await beginLazyFailure(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      new ArticleNotFoundError('private message id', { allProviders: true }),
      { redirect: 'manual' }
    );
    pending.stream.releaseClose(
      Object.assign(new Error('private cleanup path'), { code: 'EIO' })
    );
    await expectTypedFailure(pending.response, 404);
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({ cleanupCode: 'EIO' });
  });

  test('unknown lazy failures remain generic 500 with or without close EIO', async () => {
    for (const cleanupError of [
      undefined,
      Object.assign(new Error('private cleanup path'), { code: 'EIO' }),
    ]) {
      mocks.warn.mockReset();
      const pending = await beginLazyFailure(
        `${baseUrl}/usenet/stream/test-token/video.mkv`,
        new Error('private internal invariant'),
        { redirect: 'manual' }
      );
      pending.stream.releaseClose(cleanupError);
      const response = await pending.response;
      expect(response.status).toBe(500);
      const body = await response.json();
      expect(body).toEqual({ error: 'APIError' });
      expect(JSON.stringify(body)).not.toContain('private');
      expect(mocks.warn).toHaveBeenCalledTimes(1);
      expect(JSON.stringify(mocks.warn.mock.calls)).not.toContain('private');
    }
  });

  test('client disconnect during lazy capacity close suppresses every public response attempt', async () => {
    const testId = 'lazy-capacity-disconnect';
    const controller = new AbortController();
    const pending = await beginLazyFailure(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      new PrioritySemaphoreError(
        'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
        'private owner key'
      ),
      {
        redirect: 'manual',
        signal: controller.signal,
        headers: { 'x-route-test-id': testId },
      }
    );
    controller.abort();
    await expect(pending.response).rejects.toBeDefined();
    await waitForCalls(mocks.responseClose, 1);
    pending.stream.releaseClose();
    await waitForCalls(mocks.warn, 1);

    expectNoPublicResponseAttempt(testId);
    expect(pending.stream.closed).toBe(true);
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
      rootCode: 'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
      clientAborted: true,
    });
  });

  test('an internal lazy spool failure followed by disconnect warns once without responding', async () => {
    const testId = 'lazy-spool-disconnect';
    const controller = new AbortController();
    const pending = await beginLazyFailure(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      new UsenetSpoolError('USENET_SPOOL_IO', 'private spool path'),
      {
        redirect: 'manual',
        signal: controller.signal,
        headers: { 'x-route-test-id': testId },
      }
    );
    controller.abort();
    await expect(pending.response).rejects.toBeDefined();
    await waitForCalls(mocks.responseClose, 1);
    pending.stream.releaseClose();
    await waitForCalls(mocks.warn, 1);

    expectNoPublicResponseAttempt(testId);
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
      rootCode: 'USENET_SPOOL_IO',
      clientAborted: true,
      cleanupErrorCount: 0,
    });
  });

  test('post-header source failure is not logged until async source close settles', async () => {
    const stream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
    const responsePromise = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`
    );
    stream.write(Buffer.from('a'));
    const response = await responsePromise;
    expect(response.status).toBe(200);
    const failure = new UsenetSpoolError(
      'USENET_SPOOL_IO',
      'internal stream failure'
    );
    stream.fail(failure);
    await stream.closeStarted.promise;
    expect(mocks.warn).not.toHaveBeenCalled();

    stream.releaseClose(
      Object.assign(new Error('private cleanup path'), { code: 'EIO' })
    );
    await expect(response.arrayBuffer()).rejects.toBeDefined();
    await waitForCalls(mocks.warn, 1);
    expect(mocks.warn).toHaveBeenCalledTimes(1);
    expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
      rootCode: 'USENET_SPOOL_IO',
      cleanupCode: 'EIO',
      outerErrorName: 'AggregateError',
      headersSent: true,
    });
  });

  test('natural response finish plus late source-close EIO is not normal EOF', async () => {
    const stream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValue(opened(stream));
    const responsePromise = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`
    );
    stream.end(Buffer.from('123456'));
    const response = await responsePromise;
    expect(Buffer.from(await response.arrayBuffer())).toEqual(
      Buffer.from('123456')
    );
    await stream.closeStarted.promise;
    expect(mocks.warn).not.toHaveBeenCalled();

    stream.releaseClose(
      Object.assign(new Error('late close failure'), { code: 'EIO' })
    );
    await waitForCalls(mocks.warn, 1);
    expect(mocks.warn.mock.calls[0]?.[0]).toMatchObject({
      rootCode: 'EIO',
      streamTermination: 'internal_error',
      headersSent: true,
    });
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

  test('range delivery remains immediate while HEAD, 304, and 416 await reader close', async () => {
    const rangeStream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValueOnce({
      ...opened(rangeStream),
      start: 1,
      end: 4,
    });
    const rangeResponse = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      { headers: { Range: 'bytes=1-3' } }
    );
    rangeStream.end(Buffer.from('bcd'));
    const partial = await rangeResponse;
    expect(partial.status).toBe(206);
    await expect(partial.arrayBuffer()).resolves.toEqual(
      Uint8Array.from(Buffer.from('bcd')).buffer
    );
    await rangeStream.closeStarted.promise;
    expect(rangeStream.closed).toBe(false);
    rangeStream.releaseClose();
    await new Promise<void>((resolve) => rangeStream.once('close', resolve));

    const headStream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValueOnce(opened(headStream));
    const headResponse = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      { method: 'HEAD' }
    );
    await headStream.closeStarted.promise;
    await expectPending(headResponse);
    headStream.releaseClose();
    expect((await headResponse).status).toBe(200);

    const notModifiedStream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValueOnce(
      opened(notModifiedStream)
    );
    const notModifiedResponse = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      { headers: { 'If-None-Match': '"test-etag"' } }
    );
    await notModifiedStream.closeStarted.promise;
    await expectPending(notModifiedResponse);
    notModifiedStream.releaseClose();
    expect((await notModifiedResponse).status).toBe(304);

    const unsatisfiableStream = new DeferredCloseStream();
    mocks.openNativeUsenetStream.mockResolvedValueOnce(
      opened(unsatisfiableStream)
    );
    const unsatisfiableResponse = fetch(
      `${baseUrl}/usenet/stream/test-token/video.mkv`,
      { headers: { Range: 'bytes=6-9' } }
    );
    await unsatisfiableStream.closeStarted.promise;
    await expectPending(unsatisfiableResponse);
    unsatisfiableStream.releaseClose();
    expect((await unsatisfiableResponse).status).toBe(416);
  });
});
