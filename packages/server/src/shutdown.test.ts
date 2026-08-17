import type { Server } from 'node:http';
import { once } from 'node:events';
import express from 'express';
import { describe, expect, test } from 'vitest';
import { ShutdownAdmissionGate, ShutdownCoordinator } from './shutdown.js';

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
    const app = express();
    app.use(gate.middleware);
    app.get('/work', (_request, response) => response.json({ ok: true }));
    const { server, baseUrl } = await listen(app);
    try {
      expect((await fetch(`${baseUrl}/work`)).status).toBe(200);
      gate.beginDraining();
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
});
