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
  DownloadManager,
  GrabCache,
  initDb,
  settingsStore,
} from '@aiostreams/core';
import proxyRouter, { overrideProxyLifecycleForTest } from './proxy.js';
import { ShutdownAdmissionGate } from '../../shutdown.js';

interface BlockingUpstream {
  readonly baseUrl: string;
  entered(path: string): Promise<void>;
  closed(path: string): Promise<void>;
  close(): Promise<void>;
}

async function blockingUpstream(
  paths: readonly string[]
): Promise<BlockingUpstream> {
  const entered = new Map(
    paths.map((path) => [path, Promise.withResolvers<void>()] as const)
  );
  const closed = new Map(
    paths.map((path) => [path, Promise.withResolvers<void>()] as const)
  );
  const release = Promise.withResolvers<void>();
  const server = createServer(async (request, response) => {
    const path = request.url ?? '';
    request.socket.once('close', () => closed.get(path)?.resolve());
    entered.get(path)?.resolve();
    await release.promise;
    if (!response.destroyed) response.end('late');
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('blocking upstream has no TCP address');
  }
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    entered: (path) => entered.get(path)?.promise ?? Promise.reject(),
    closed: (path) => closed.get(path)?.promise ?? Promise.reject(),
    close: async () => {
      release.resolve();
      server.closeAllConnections?.();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

async function listen(app: express.Express): Promise<{
  readonly server: Server;
  readonly baseUrl: string;
}> {
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('proxy test server has no TCP address');
  }
  return { server, baseUrl: `http://127.0.0.1:${address.port}` };
}

function proxyToken(url: string, type: 'nzb' | 'stream'): string {
  const auth = Buffer.from(
    JSON.stringify({
      username: 'proxy-shutdown-user',
      password: 'proxy-shutdown-password',
    })
  ).toString('base64url');
  const data = Buffer.from(
    JSON.stringify({ url, filename: 'shutdown.bin', type })
  ).toString('base64url');
  return `u.${auth}.${data}`;
}

function testApp(): express.Express {
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
  return app;
}

describe('process shutdown ownership for every proxy branch', () => {
  let databaseDirectory: string;

  beforeAll(async () => {
    databaseDirectory = await mkdtemp(
      join(tmpdir(), 'proxy-process-shutdown-')
    );
    await initDb(`sqlite://${join(databaseDirectory, 'settings.sqlite')}`);
    await settingsStore.initialise();
  });

  afterAll(async () => {
    await closeDb();
    await rm(databaseDirectory, { recursive: true, force: true });
  });

  test('shutdown aborts NZB GET/HEAD, stream HEAD, and a body request', async () => {
    const paths = ['/nzb-get', '/nzb-head', '/stream-head', '/body'] as const;
    const upstream = await blockingUpstream(paths);
    const cacheDirectory = await mkdtemp(join(tmpdir(), 'proxy-nzb-grabs-'));
    const cache = new GrabCache<Buffer>({
      name: 'proxy-process-nzb',
      dir: cacheDirectory,
      maxMemBytes: 1024,
      maxDiskBytes: 4096,
      serialize: (value) => value,
      deserialize: (value) => value,
      sizeOf: (value) => value.length,
    });
    const manager = new DownloadManager(cache);
    const gate = new ShutdownAdmissionGate();
    const restoreProxyLifecycle = overrideProxyLifecycleForTest({
      processShutdownSignal: gate.signal,
      nzbDownloads: manager,
    });
    const { server, baseUrl } = await listen(testApp());
    try {
      const nzbGet = fetch(
        `${baseUrl}/proxy/${proxyToken(`${upstream.baseUrl}/nzb-get`, 'nzb')}`
      );
      const nzbHead = fetch(
        `${baseUrl}/proxy/${proxyToken(`${upstream.baseUrl}/nzb-head`, 'nzb')}`,
        { method: 'HEAD' }
      );
      const streamHead = fetch(
        `${baseUrl}/proxy/${proxyToken(`${upstream.baseUrl}/stream-head`, 'stream')}`,
        { method: 'HEAD' }
      );
      const body = fetch(
        `${baseUrl}/proxy/${proxyToken(`${upstream.baseUrl}/body`, 'stream')}`,
        { method: 'POST', body: 'payload' }
      );
      await Promise.all(paths.map((path) => upstream.entered(path)));

      gate.beginDraining();
      const grabClose = manager.close();
      const listenerClose = new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });

      const [getResponse, headResponse, streamHeadResponse, bodyResponse] =
        await Promise.all([nzbGet, nzbHead, streamHead, body]);
      expect(getResponse.status).toBe(503);
      expect(getResponse.headers.get('connection')).toBe('close');
      await expect(getResponse.json()).resolves.toEqual({
        error: 'Server is shutting down',
        success: false,
      });
      expect(headResponse.status).toBe(503);
      expect(headResponse.headers.get('connection')).toBe('close');
      expect(streamHeadResponse.status).toBe(503);
      expect(bodyResponse.status).toBe(503);
      await expect(bodyResponse.json()).resolves.toMatchObject({
        error: 'Server is shutting down',
      });

      await Promise.all([
        grabClose,
        listenerClose,
        ...paths.map((path) => upstream.closed(path)),
      ]);
      expect(cache.activeFlights).toBe(0);
      expect(cache.waitingRequests).toBe(0);

      const reopened = new GrabCache<Buffer>({
        name: 'proxy-process-nzb',
        dir: cacheDirectory,
        maxMemBytes: 1024,
        maxDiskBytes: 4096,
        serialize: (value) => value,
        deserialize: (value) => value,
        sizeOf: (value) => value.length,
      });
      expect(
        await reopened.cached(`${upstream.baseUrl}/nzb-get`)
      ).toBeUndefined();
      expect(
        await reopened.cached(`${upstream.baseUrl}/nzb-head`)
      ).toBeUndefined();
      await reopened.close();
    } finally {
      server.closeAllConnections?.();
      if (server.listening) {
        await new Promise<void>((resolve) => server.close(() => resolve()));
      }
      await manager.close();
      await upstream.close();
      await rm(cacheDirectory, { recursive: true, force: true });
      restoreProxyLifecycle();
    }
  });

  test('a client abort remains distinct from process shutdown', async () => {
    const upstream = await blockingUpstream(['/head']);
    const cacheDirectory = await mkdtemp(join(tmpdir(), 'proxy-client-abort-'));
    const cache = new GrabCache<Buffer>({
      name: 'proxy-client-abort',
      dir: cacheDirectory,
      maxMemBytes: 0,
      maxDiskBytes: 0,
      serialize: (value) => value,
      deserialize: (value) => value,
      sizeOf: (value) => value.length,
    });
    const manager = new DownloadManager(cache);
    const gate = new ShutdownAdmissionGate();
    const restoreProxyLifecycle = overrideProxyLifecycleForTest({
      processShutdownSignal: gate.signal,
      nzbDownloads: manager,
    });
    const { server, baseUrl } = await listen(testApp());
    const client = new AbortController();
    try {
      const response = fetch(
        `${baseUrl}/proxy/${proxyToken(`${upstream.baseUrl}/head`, 'stream')}`,
        { method: 'HEAD', signal: client.signal }
      );
      await upstream.entered('/head');
      client.abort(new DOMException('client left', 'AbortError'));

      await expect(response).rejects.toBeDefined();
      await upstream.closed('/head');
      expect(gate.isDraining).toBe(false);
    } finally {
      server.closeAllConnections?.();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await manager.close();
      await upstream.close();
      await rm(cacheDirectory, { recursive: true, force: true });
      restoreProxyLifecycle();
    }
  });
});
