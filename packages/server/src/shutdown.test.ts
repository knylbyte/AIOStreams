import type { Server } from 'node:http';
import { once } from 'node:events';
import { Readable } from 'node:stream';
import express from 'express';
import { describe, expect, test } from 'vitest';
import { StreamRegistry } from '@aiostreams/core';
import {
  ProcessShutdownError,
  ShutdownAdmissionGate,
  ShutdownCoordinator,
} from './shutdown.js';
import { closeUsenetOwners } from './usenet-shutdown.js';

class CoordinatedReader extends Readable {
  readonly destroyEntered = Promise.withResolvers<void>();

  constructor(private readonly destroyGate: Promise<void>) {
    super({ read() {} });
  }

  override _destroy(
    error: Error | null,
    callback: (error?: Error | null) => void
  ): void {
    this.destroyEntered.resolve();
    void this.destroyGate.then(() => callback(error));
  }
}

async function listen(app: express.Express): Promise<{
  server: Server;
  baseUrl: string;
}> {
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('test server did not expose a TCP address');
  }
  return { server, baseUrl: `http://127.0.0.1:${address.port}` };
}

describe('shutdown admission and ordering', () => {
  test('the HTTP gate returns a stable 503 while draining', async () => {
    const gate = new ShutdownAdmissionGate();
    expect(gate.signal.aborted).toBe(false);
    const app = express();
    app.use(gate.middleware);
    app.get('/work', (_request, response) => response.json({ ok: true }));
    const { server, baseUrl } = await listen(app);
    try {
      expect((await fetch(`${baseUrl}/work`)).status).toBe(200);
      gate.beginDraining();
      expect(gate.signal.aborted).toBe(true);
      expect(gate.signal.reason).toBeInstanceOf(ProcessShutdownError);
      const reason = gate.signal.reason;
      gate.beginDraining();
      expect(gate.signal.reason).toBe(reason);
      const response = await fetch(`${baseUrl}/work`);
      expect(response.status).toBe(503);
      expect(response.headers.get('connection')).toBe('close');
      await expect(response.json()).resolves.toEqual({
        error: 'Server is shutting down',
        success: false,
      });
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  test('a request admitted before shutdown is refused at delayed session open', async () => {
    const gate = new ShutdownAdmissionGate();
    const entered = Promise.withResolvers<void>();
    const proceed = Promise.withResolvers<void>();
    const app = express();
    app.use(gate.middleware);
    app.get('/stream', async (_request, response) => {
      entered.resolve();
      await proceed.promise;
      if (gate.isDraining) {
        response.status(503).end();
        return;
      }
      response.status(200).end();
    });
    const { server, baseUrl } = await listen(app);
    try {
      const request = fetch(`${baseUrl}/stream`);
      await entered.promise;
      gate.beginDraining();
      proceed.resolve();
      expect((await request).status).toBe(503);
    } finally {
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  });

  test('shutdown is idempotent and drains engines before persistent services', async () => {
    const events: string[] = [];
    const engineStarted = Promise.withResolvers<void>();
    const engineMayFinish = Promise.withResolvers<void>();
    const coordinator = new ShutdownCoordinator({
      admission: new ShutdownAdmissionGate(),
      server: () => undefined,
      stopTasks: () => events.push('tasks'),
      sealStreams: () => events.push('streams-sealed'),
      beforeListenerClose: [
        {
          label: 'engines',
          run: async () => {
            events.push('engines-start');
            engineStarted.resolve();
            await engineMayFinish.promise;
            events.push('engines-end');
          },
        },
      ],
      afterListenerClose: [
        {
          label: 'database',
          run: async () => {
            events.push('database');
          },
        },
      ],
    });

    const first = coordinator.close();
    const second = coordinator.close();
    expect(second).toBe(first);
    await engineStarted.promise;
    expect(events).toEqual(['tasks', 'streams-sealed', 'engines-start']);
    engineMayFinish.resolve();
    await first;
    expect(events).toEqual([
      'tasks',
      'streams-sealed',
      'engines-start',
      'engines-end',
      'database',
    ]);
    await coordinator.close();
    expect(events.filter((event) => event === 'database')).toHaveLength(1);
  });

  test('persistent cleanup failures are aggregated after every cleanup is attempted', async () => {
    const failure = new Error('disk cache index failed');
    const events: string[] = [];
    const reported: Array<{ label: string; error: unknown }> = [];
    const coordinator = new ShutdownCoordinator({
      admission: new ShutdownAdmissionGate(),
      server: () => undefined,
      stopTasks: () => undefined,
      sealStreams: () => undefined,
      beforeListenerClose: [],
      afterListenerClose: [
        {
          label: 'disk caches',
          run: async () => {
            events.push('disk caches');
            throw failure;
          },
        },
        {
          label: 'database',
          run: async () => {
            events.push('database');
          },
        },
      ],
      onCleanupError: (label, error) => reported.push({ label, error }),
    });

    await expect(coordinator.close()).rejects.toMatchObject({
      errors: expect.arrayContaining([failure]),
    });
    expect(events).toEqual(['disk caches', 'database']);
    expect(reported).toEqual([{ label: 'disk caches', error: failure }]);
  });

  test('real listener admission closes before engine and final-index barriers finish', async () => {
    const gate = new ShutdownAdmissionGate();
    const app = express();
    let dispatched = 0;
    app.use(gate.middleware);
    app.get('/work', (_request, response) => {
      dispatched++;
      response.json({ ok: true });
    });
    const { server, baseUrl } = await listen(app);
    expect((await fetch(`${baseUrl}/work`)).status).toBe(200);

    const events: string[] = [];
    const engineStarted = Promise.withResolvers<void>();
    const engineMayFinish = Promise.withResolvers<void>();
    const coordinator = new ShutdownCoordinator({
      admission: gate,
      server: () => server,
      stopTasks: () => events.push('tasks'),
      sealStreams: () => events.push('streams-sealed'),
      beforeListenerClose: [
        {
          label: 'engines',
          run: async () => {
            events.push('engine-close-start');
            engineStarted.resolve();
            await engineMayFinish.promise;
            events.push('engine-final-index');
          },
        },
      ],
      afterListenerClose: [
        {
          label: 'database',
          run: async () => {
            events.push('database');
          },
        },
      ],
    });

    const closing = coordinator.close();
    await engineStarted.promise;
    expect(gate.isDraining).toBe(true);
    const admittedAfterShutdown = await fetch(`${baseUrl}/work`).then(
      (response) => response.status,
      () => undefined
    );
    expect(
      admittedAfterShutdown === undefined || admittedAfterShutdown === 503
    ).toBe(true);
    expect(dispatched).toBe(1);
    expect(events).toEqual(['tasks', 'streams-sealed', 'engine-close-start']);

    engineMayFinish.resolve();
    await closing;
    expect(events).toEqual([
      'tasks',
      'streams-sealed',
      'engine-close-start',
      'engine-final-index',
      'database',
    ]);
    expect(server.listening).toBe(false);
  });

  test('coordinated stream seal remains the reader-owner close barrier', async () => {
    const registry = new StreamRegistry(() => ({ ok: true }));
    const opened = registry.open({
      transport: 'usenet',
      username: 'shutdown-user',
      targetKey: 'coordinated-engine-reader',
    });
    expect(opened.ok).toBe(true);
    if (!opened.ok) return;

    const destroyGate = Promise.withResolvers<void>();
    const reader = new CoordinatedReader(destroyGate.promise);
    reader.on('error', () => undefined);
    opened.handle.attach(reader);

    const coordinator = new ShutdownCoordinator({
      admission: new ShutdownAdmissionGate(),
      server: () => undefined,
      stopTasks: () => undefined,
      sealStreams: () => registry.sealAndCloseAll('shutdown'),
      beforeListenerClose: [
        {
          label: 'reader owner',
          run: async () => {
            if (!reader.closed) {
              await new Promise<void>((resolve) =>
                reader.once('close', resolve)
              );
            }
          },
        },
      ],
      afterListenerClose: [],
    });
    let settled = false;
    const closing = coordinator.close().then(() => {
      settled = true;
    });
    await reader.destroyEntered.promise;
    await Promise.resolve();
    expect(reader.destroyed).toBe(true);
    expect(reader.closed).toBe(false);
    expect(settled).toBe(false);

    destroyGate.resolve();
    await closing;
    expect(reader.closed).toBe(true);
    expect(registry.snapshot()).toEqual([]);
  });

  test('reader cleanup can persist before the repository and database fences', async () => {
    const engineEntered = Promise.withResolvers<void>();
    const engineGate = Promise.withResolvers<void>();
    const events: string[] = [];
    let persistenceOpen = true;
    let databaseOpen = true;
    let writes = 0;

    const coordinator = new ShutdownCoordinator({
      admission: new ShutdownAdmissionGate(),
      server: () => undefined,
      stopTasks: () => undefined,
      sealStreams: () => undefined,
      beforeListenerClose: [
        {
          label: 'usenet owners',
          run: () =>
            closeUsenetOwners({
              closeOpenings: async () => {
                events.push('openings');
              },
              closeGrabs: async () => {
                events.push('grabs');
              },
              closeEngines: async () => {
                events.push('engine-start');
                engineEntered.resolve();
                await engineGate.promise;
                expect(persistenceOpen).toBe(true);
                expect(databaseOpen).toBe(true);
                writes++;
                events.push('reader-hook-write');
              },
              closePersistence: async () => {
                expect(writes).toBe(1);
                events.push('persistence-close');
                persistenceOpen = false;
              },
            }),
        },
      ],
      afterListenerClose: [
        {
          label: 'database',
          run: async () => {
            expect(persistenceOpen).toBe(false);
            events.push('database-close');
            databaseOpen = false;
          },
        },
      ],
    });

    const closing = coordinator.close();
    expect(events).toEqual(['openings', 'grabs']);
    await engineEntered.promise;
    expect(events).toEqual(['openings', 'grabs', 'engine-start']);
    expect(persistenceOpen).toBe(true);
    engineGate.resolve();
    await closing;
    expect(events.slice(3)).toEqual([
      'reader-hook-write',
      'persistence-close',
      'database-close',
    ]);
    expect(writes).toBe(1);
    expect(databaseOpen).toBe(false);
  });

  test('engine and repository close failures are both retained', async () => {
    const engineFailure = new Error('engine cleanup failed');
    const persistenceFailure = new Error('repository close failed');
    const events: string[] = [];

    await expect(
      closeUsenetOwners({
        closeOpenings: async () => {
          events.push('openings');
        },
        closeGrabs: async () => {
          events.push('grabs');
        },
        closeEngines: async () => {
          events.push('engines');
          throw engineFailure;
        },
        closePersistence: async () => {
          events.push('persistence');
          throw persistenceFailure;
        },
      })
    ).rejects.toMatchObject({
      errors: [engineFailure, persistenceFailure],
    });
    expect(events).toEqual(['openings', 'grabs', 'engines', 'persistence']);
  });
});
