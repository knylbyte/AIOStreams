import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { settingsStore } from '../../config/index.js';
import { closeDb, initDb } from '../../db/index.js';
import { streamRegistry } from '../../stream-sessions/index.js';
import { downloadManager } from '../../utils/download-manager.js';
import {
  openNativeUsenetStream,
  shutdownCensusShadows,
  shutdownNativeUsenetSessionOpens,
  shutdownNativeUsenetSessionPersistence,
  shutdownUsenetEngines,
  usenetEngineRegistry,
} from './index.js';
import { encodeUsenetStreamToken } from './tokens.js';

const NZB = `<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head />
  <file poster="test" date="0" subject="&quot;shutdown.bin&quot; yEnc">
    <groups><group>alt.binaries.test</group></groups>
    <segments><segment bytes="128" number="1">shutdown-message</segment></segments>
  </file>
</nzb>`;

test('native request stopped during session open creates no late reader or engine', async () => {
  const testRoot = await mkdtemp(join(tmpdir(), 'native-shutdown-'));
  process.env.NODE_ENV = 'test';
  process.env.SECRET_KEY = '0'.repeat(64);
  process.env.BASE_URL = 'http://localhost:3000';
  process.env.LOG_LEVEL = 'error';
  delete process.env.USENET_PROVIDERS;

  const entered = Promise.withResolvers<void>();
  const proceed = Promise.withResolvers<void>();
  const upstreamClosed = Promise.withResolvers<void>();
  let server: Server | undefined;
  try {
    server = await new Promise<Server>((resolve, reject) => {
      const listener = createServer(async (_request, response) => {
        _request.socket.once('close', () => upstreamClosed.resolve());
        entered.resolve();
        await proceed.promise;
        response.writeHead(200, {
          'content-type': 'application/x-nzb',
          'content-length': String(Buffer.byteLength(NZB)),
        });
        response.end(NZB);
      });
      listener.listen(0, '127.0.0.1', () => resolve(listener));
      listener.once('error', reject);
    });
    const address = server.address();
    if (!address || typeof address === 'string') {
      throw new Error('native shutdown test server did not bind a TCP port');
    }

    await initDb(`sqlite://${join(testRoot, 'test.sqlite')}`);
    await settingsStore.initialise();
    const configured = await settingsStore.applyBatch(
      {
        sets: [
          {
            key: 'usenet.providers',
            value: [
              {
                id: 'must-not-dispatch',
                host: '127.0.0.1',
                port: 1,
                tls: false,
                maxConnections: 1,
                priority: 0,
              },
            ],
          },
        ],
      },
      {
        expectedVersion: settingsStore.currentVersion,
        updatedBy: 'native-shutdown-test',
      }
    );
    assert.equal(configured, true);
    const opening = openNativeUsenetStream({
      token: encodeUsenetStreamToken({
        nzb: `http://127.0.0.1:${address.port}/shutdown.nzb`,
        hash: 'shutdown-token',
        fileIndex: 0,
        filename: 'shutdown.bin',
        owner: 'shutdown-user',
      }),
    });

    await entered.promise;
    const rejected = assert.rejects(
      opening,
      (error: unknown) =>
        (error as NodeJS.ErrnoException).code === 'STREAM_STOPPED' &&
        (error as { reason?: unknown }).reason === 'shutdown'
    );
    streamRegistry.sealAndCloseAll('shutdown');
    const openingShutdown = shutdownNativeUsenetSessionOpens();
    const grabShutdown = downloadManager.close();
    const censusShutdown = shutdownCensusShadows();
    await upstreamClosed.promise;
    await Promise.all([openingShutdown, grabShutdown]);
    await shutdownUsenetEngines();
    await censusShutdown;
    await shutdownNativeUsenetSessionPersistence();

    await rejected;
    assert.deepEqual(streamRegistry.snapshot(), []);
    assert.equal(usenetEngineRegistry.size, 0);
  } finally {
    proceed.resolve();
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server?.close((error) => (error ? reject(error) : resolve()));
      });
    }
    await closeDb();
    await rm(testRoot, { recursive: true, force: true });
  }
});
