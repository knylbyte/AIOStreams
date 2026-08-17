import type { Server } from 'node:http';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import express, {
  type NextFunction,
  type Request,
  type Response,
} from 'express';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import {
  closeDb,
  initDb,
  settingsStore,
  streamRegistry,
} from '@aiostreams/core';
import proxyRouter from './proxy.js';

interface UpstreamHarness {
  readonly url: string;
  readonly entered: Promise<void>;
  readonly connectionClosed: Promise<void>;
  release(): void;
  close(): Promise<void>;
}

async function listen(app: express.Express): Promise<{
  server: Server;
  baseUrl: string;
}> {
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('proxy shutdown test server did not expose a TCP address');
  }
  return { server, baseUrl: `http://127.0.0.1:${address.port}` };
}

async function pausedUpstream(options?: {
  readonly publishHeaders?: boolean;
  readonly redirect?: boolean;
}): Promise<UpstreamHarness> {
  const entered = Promise.withResolvers<void>();
  const connectionClosed = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const server = createServer(async (request, response) => {
    if (options?.redirect && request.url === '/redirect') {
      response.writeHead(302, { location: '/blocked' });
      response.end();
      return;
    }
    request.socket.once('close', () => connectionClosed.resolve());
    if (options?.publishHeaders) {
      response.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-length': '10',
      });
      response.write('hello');
    }
    entered.resolve();
    await release.promise;
    if (!response.destroyed) {
      if (options?.publishHeaders) response.end('world');
      else response.end('upstream');
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('paused upstream did not expose a TCP address');
  }
  const path = options?.redirect ? '/redirect' : '/blocked';
  return {
    url: `http://127.0.0.1:${address.port}${path}`,
    entered: entered.promise,
    connectionClosed: connectionClosed.promise,
    release: () => release.resolve(),
    close: async () => {
      release.resolve();
      server.closeAllConnections?.();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

function proxyToken(url: string): string {
  const auth = Buffer.from(
    JSON.stringify({
      username: 'proxy-shutdown-user',
      password: 'proxy-shutdown-password',
    })
  ).toString('base64url');
  const data = Buffer.from(
    JSON.stringify({ url, filename: 'proxy.bin', type: 'stream' })
  ).toString('base64url');
  return `u.${auth}.${data}`;
}

describe('proxy request shutdown ownership', () => {
  let server: Server;
  let proxyBaseUrl: string;
  let databaseDirectory: string;

  beforeAll(async () => {
    databaseDirectory = await mkdtemp(join(tmpdir(), 'proxy-shutdown-'));
    await initDb(`sqlite://${join(databaseDirectory, 'settings.sqlite')}`);
    await settingsStore.initialise();
    const app = express();
    app.use('/proxy', proxyRouter);
    app.use(
      (
        error: unknown,
        _request: Request,
        response: Response,
        _next: NextFunction
      ) => {
        response.status(500).json({
          error: error instanceof Error ? error.message : String(error),
          success: false,
        });
      }
    );
    const listening = await listen(app);
    server = listening.server;
    proxyBaseUrl = listening.baseUrl;
  });

  afterAll(async () => {
    if (server) {
      server.closeAllConnections?.();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
    await closeDb();
    await rm(databaseDirectory, { recursive: true, force: true });
  });

  function stopOnlySession(reason: 'shutdown' | 'limit' | 'stopped'): void {
    const sessions = streamRegistry.snapshot();
    expect(sessions).toHaveLength(1);
    expect(streamRegistry.kill(sessions[0]!.id, reason)).toBe(true);
  }

  test('shutdown aborts a pre-header upstream and returns the stable 503', async () => {
    const upstream = await pausedUpstream();
    try {
      const responsePromise = fetch(
        `${proxyBaseUrl}/proxy/${proxyToken(upstream.url)}/proxy.bin`
      );
      await upstream.entered;
      stopOnlySession('shutdown');

      const [response] = await Promise.all([
        responsePromise,
        upstream.connectionClosed,
      ]);
      expect(response.status).toBe(503);
      expect(response.headers.get('connection')).toBe('close');
      await expect(response.json()).resolves.toEqual({
        error: 'Server is shutting down',
        success: false,
      });
      expect(streamRegistry.snapshot()).toEqual([]);
    } finally {
      await upstream.close();
    }
  });

  test('shutdown after headers terminates the body without a second response', async () => {
    const upstream = await pausedUpstream({ publishHeaders: true });
    try {
      const response = await fetch(
        `${proxyBaseUrl}/proxy/${proxyToken(upstream.url)}/proxy.bin`
      );
      await upstream.entered;
      expect(response.status).toBe(200);
      stopOnlySession('shutdown');

      await Promise.all([
        expect(response.arrayBuffer()).rejects.toBeDefined(),
        upstream.connectionClosed,
      ]);
      expect(streamRegistry.snapshot()).toEqual([]);
    } finally {
      await upstream.close();
    }
  });

  test('a non-shutdown stream stop retains the disconnect contract', async () => {
    const upstream = await pausedUpstream();
    try {
      const response = fetch(
        `${proxyBaseUrl}/proxy/${proxyToken(upstream.url)}/proxy.bin`
      );
      await upstream.entered;
      stopOnlySession('limit');

      await Promise.all([
        expect(response).rejects.toBeDefined(),
        upstream.connectionClosed,
      ]);
      expect(streamRegistry.snapshot()).toEqual([]);
    } finally {
      await upstream.close();
    }
  });

  test('shutdown aborts the current redirect hop before its headers', async () => {
    const upstream = await pausedUpstream({ redirect: true });
    try {
      const responsePromise = fetch(
        `${proxyBaseUrl}/proxy/${proxyToken(upstream.url)}/proxy.bin`
      );
      await upstream.entered;
      stopOnlySession('shutdown');

      const [response] = await Promise.all([
        responsePromise,
        upstream.connectionClosed,
      ]);
      expect(response.status).toBe(503);
      await expect(response.json()).resolves.toMatchObject({
        error: 'Server is shutting down',
      });
      expect(streamRegistry.snapshot()).toEqual([]);
    } finally {
      await upstream.close();
    }
  });
});
