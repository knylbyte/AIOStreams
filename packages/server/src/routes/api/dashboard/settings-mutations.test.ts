import type { Server } from 'node:http';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import express, { type ErrorRequestHandler } from 'express';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
  vi,
} from 'vitest';

const MEBIBYTE_BYTES = 1024 * 1024;
const RESOURCE_ENV_NAMES = [
  'USENET_STREAMING_MODE',
  'USENET_MAX_CONCURRENT_DOWNLOADS',
  'USENET_MAX_DOWNLOAD_CONNECTIONS',
  'USENET_SEGMENT_MEMORY_CACHE_BYTES',
  'USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES',
  'USENET_SEGMENT_SPOOLING_STREAM_BUFFER_BYTES',
  'USENET_SEGMENT_SPOOLING_SPOOL_BYTES',
  'USENET_SEGMENT_SPOOLING_MIN_FREE_DISK_BYTES',
] as const;

const originalEnvironment = new Map(
  RESOURCE_ENV_NAMES.map((name) => [name, process.env[name]])
);
const originalBootstrapEnvironment = new Map(
  ['NODE_ENV', 'SECRET_KEY', 'BASE_URL', 'LOG_LEVEL', 'LOG_FORMAT'].map(
    (name) => [name, process.env[name]]
  )
);

let core: typeof import('@aiostreams/core');
let databaseDirectory: string;
let server: Server;
let baseUrl: string;

function clearResourceEnvironment(): void {
  for (const name of RESOURCE_ENV_NAMES) delete process.env[name];
}

function setValidSpoolingEnvironment(): void {
  process.env.USENET_STREAMING_MODE = 'segment_spooling';
  process.env.USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES = '128MB';
  process.env.USENET_SEGMENT_SPOOLING_STREAM_BUFFER_BYTES = String(
    2 * MEBIBYTE_BYTES
  );
  process.env.USENET_SEGMENT_SPOOLING_SPOOL_BYTES = String(64 * MEBIBYTE_BYTES);
}

async function resetStoredSettings(): Promise<void> {
  for (const row of await core.SettingsRepository.getAll()) {
    await core.SettingsRepository.delete(row.key);
  }
  await core.settingsStore.reload({ emit: false });
}

async function storedUsenetValues(): Promise<Map<string, string>> {
  return new Map(
    (await core.SettingsRepository.getAll())
      .filter((row) => row.key.startsWith('usenet.'))
      .map((row) => [row.key, row.value])
  );
}

function currentResourceValues() {
  const current = core.settingsStore.current.usenet;
  return {
    streamingMode: current.streamingMode,
    segmentSpoolingMemoryBudgetBytes: current.segmentSpoolingMemoryBudgetBytes,
    segmentSpoolingStreamBufferBytes: current.segmentSpoolingStreamBufferBytes,
    segmentSpoolingSpoolBytes: current.segmentSpoolingSpoolBytes,
  };
}

async function post(path: string, body?: unknown): Promise<Response> {
  return fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers:
      body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}

describe.sequential('dashboard Usenet settings mutation routes', () => {
  beforeAll(async () => {
    process.env.NODE_ENV = 'test';
    process.env.SECRET_KEY = '0'.repeat(64);
    process.env.BASE_URL = 'http://localhost:3000';
    process.env.LOG_LEVEL = 'error';
    clearResourceEnvironment();

    core = await import('@aiostreams/core');
    databaseDirectory = await mkdtemp(
      join(tmpdir(), 'aiostreams-server-settings-')
    );
    await core.initDb(`sqlite://${join(databaseDirectory, 'settings.sqlite')}`);
    await core.settingsStore.initialise();
    delete process.env.LOG_LEVEL;
    delete process.env.LOG_FORMAT;

    const dashboardRouter = (await import('./index.js')).default;
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      req.user = {
        username: 'route-test',
        isAdmin: true,
        permissions: ['admin'],
        source: 'password',
      };
      next();
    });
    app.use('/dashboard', dashboardRouter);
    const errorHandler: ErrorRequestHandler = (error, _req, res, _next) => {
      res.status(500).json({ error: String(error) });
    };
    app.use(errorHandler);

    server = await new Promise<Server>((resolve, reject) => {
      const listener = app.listen(0, '127.0.0.1', () => resolve(listener));
      listener.once('error', reject);
    });
    const address = server.address();
    if (!address || typeof address === 'string') {
      throw new Error('Dashboard test server did not bind a TCP port');
    }
    baseUrl = `http://127.0.0.1:${address.port}`;
  });

  beforeEach(async () => {
    clearResourceEnvironment();
    await resetStoredSettings();
  });

  afterAll(async () => {
    for (const [name, value] of originalEnvironment) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    for (const [name, value] of originalBootstrapEnvironment) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    if (server) {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    }
    if (core) await core.closeDb();
    if (databaseDirectory) {
      await rm(databaseDirectory, { recursive: true, force: true });
    }
  });

  test('scoped JSON import rejects an invalid candidate without Usenet writes', async () => {
    const versionBefore = await core.SettingsRepository.getVersion();
    const response = await post('/dashboard/settings/import/json', {
      settings: {
        'usenet.streamingMode': 'segment_spooling',
        'usenet.prefetchSegments': 17,
        'usenet.segmentSpoolingMemoryBudgetBytes': 16 * MEBIBYTE_BYTES,
        'usenet.segmentSpoolingStreamBufferBytes': 8 * MEBIBYTE_BYTES + 1,
        'usenet.segmentSpoolingSpoolBytes': 64 * MEBIBYTE_BYTES,
      },
    });

    expect(response.status).toBe(200);
    expect(await response.text()).toContain(
      'usenet.segmentSpoolingStreamBufferBytes'
    );
    expect(await storedUsenetValues()).toEqual(new Map());
    expect(await core.SettingsRepository.getVersion()).toBe(versionBefore);
  });

  test('mixed JSON import preserves non-Usenet semantics when its Usenet subset is invalid', async () => {
    const response = await post('/dashboard/settings/import/json', {
      settings: {
        'usenet.streamingMode': 'segment_spooling',
        'usenet.segmentSpoolingMemoryBudgetBytes': 16 * MEBIBYTE_BYTES,
        'usenet.segmentSpoolingStreamBufferBytes': 8 * MEBIBYTE_BYTES + 1,
        'logging.logLevel': 'debug',
      },
    });

    expect(response.status).toBe(200);
    expect(await response.text()).toContain(
      'usenet.segmentSpoolingStreamBufferBytes'
    );
    expect(await storedUsenetValues()).toEqual(new Map());
    expect(
      (await core.SettingsRepository.getAll()).find(
        (row) => row.key === 'logging.logLevel'
      )?.value
    ).toBe('"debug"');
  });

  test('scoped JSON import commits one valid Usenet batch and exposes one valid snapshot', async () => {
    const versionBefore = await core.SettingsRepository.getVersion();
    const observedPlans: ReturnType<typeof currentResourceValues>[] = [];
    const unsubscribe = core.settingsStore.subscribe(() => {
      observedPlans.push(currentResourceValues());
    });

    const response = await post('/dashboard/settings/import/json', {
      settings: {
        'usenet.streamingMode': 'segment_spooling',
        'usenet.segmentSpoolingMemoryBudgetBytes': 16 * MEBIBYTE_BYTES,
        'usenet.segmentSpoolingStreamBufferBytes': 2 * MEBIBYTE_BYTES,
        'usenet.segmentSpoolingSpoolBytes': 64 * MEBIBYTE_BYTES,
      },
    });
    unsubscribe();

    expect(response.status).toBe(200);
    expect(await core.SettingsRepository.getVersion()).toBe(versionBefore + 1);
    expect(observedPlans).toHaveLength(1);
    expect(observedPlans[0]).toEqual({
      streamingMode: 'segment_spooling',
      segmentSpoolingMemoryBudgetBytes: 16 * MEBIBYTE_BYTES,
      segmentSpoolingStreamBufferBytes: 2 * MEBIBYTE_BYTES,
      segmentSpoolingSpoolBytes: 64 * MEBIBYTE_BYTES,
    });
  });

  test('reset rejects a resource-invalid Usenet delete as one batch', async () => {
    await core.saveUsenetSettings(
      {
        'usenet.streamingMode': 'segment_spooling',
        'usenet.segmentSpoolingMemoryBudgetBytes': 256 * MEBIBYTE_BYTES,
        'usenet.segmentSpoolingStreamBufferBytes': 100 * MEBIBYTE_BYTES,
        'usenet.segmentSpoolingSpoolBytes': 64 * MEBIBYTE_BYTES,
      },
      'route-test'
    );
    const rowsBefore = await storedUsenetValues();
    const versionBefore = await core.SettingsRepository.getVersion();

    const response = await post('/dashboard/settings/reset', {
      keys: ['usenet.segmentSpoolingMemoryBudgetBytes'],
    });

    expect(response.status).toBe(200);
    expect(await response.text()).toContain('must not exceed half');
    expect(await storedUsenetValues()).toEqual(rowsBefore);
    expect(await core.SettingsRepository.getVersion()).toBe(versionBefore);
  });

  test('ENV import removes a stale default-shadowed override through the route', async () => {
    await core.saveUsenetSettings(
      { 'usenet.segmentSpoolingMemoryBudgetBytes': 1 },
      'route-test'
    );
    setValidSpoolingEnvironment();
    await core.settingsStore.reload({ emit: false });
    const versionBefore = await core.SettingsRepository.getVersion();
    const reload = vi.spyOn(core.settingsStore, 'reload');

    const response = await post('/dashboard/settings/import/env');
    const payload: unknown = await response.json();

    expect(response.status).toBe(200);
    expect(payload).toMatchObject({
      success: true,
      data: {
        imported: [
          'usenet.streamingMode',
          'usenet.segmentSpoolingMemoryBudgetBytes',
          'usenet.segmentSpoolingStreamBufferBytes',
          'usenet.segmentSpoolingSpoolBytes',
        ],
        skippedAsDefault: [],
        failed: [],
      },
    });
    expect(reload).toHaveBeenCalledTimes(1);
    reload.mockRestore();
    expect(await core.SettingsRepository.getVersion()).toBe(versionBefore + 1);
    expect(
      (await storedUsenetValues()).has(
        'usenet.segmentSpoolingMemoryBudgetBytes'
      )
    ).toBe(false);

    clearResourceEnvironment();
    await core.settingsStore.reload({ emit: false });
    expect(() =>
      core.buildUsenetEngineOptions(
        [],
        core.settingsStore.current.usenet,
        () => databaseDirectory
      )
    ).not.toThrow();
    expect(
      core.settingsStore.current.usenet.segmentSpoolingMemoryBudgetBytes
    ).toBe(128_000_000);
  });

  test('ENV import rejects invalid Usenet values and commits a valid set once', async () => {
    process.env.USENET_STREAMING_MODE = 'segment_spooling';
    process.env.USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES = String(
      16 * MEBIBYTE_BYTES
    );
    process.env.USENET_SEGMENT_SPOOLING_STREAM_BUFFER_BYTES = String(
      8 * MEBIBYTE_BYTES + 1
    );
    process.env.USENET_SEGMENT_SPOOLING_SPOOL_BYTES = String(
      64 * MEBIBYTE_BYTES
    );
    process.env.USENET_SEGMENT_SPOOLING_MIN_FREE_DISK_BYTES = '0';
    await core.settingsStore.reload({ emit: false });

    const invalidVersion = await core.SettingsRepository.getVersion();
    const invalid = await post('/dashboard/settings/import/env');
    expect(invalid.status).toBe(200);
    expect(await invalid.text()).toContain(
      'usenet.segmentSpoolingStreamBufferBytes'
    );
    expect(await storedUsenetValues()).toEqual(new Map());
    expect(await core.SettingsRepository.getVersion()).toBe(invalidVersion);

    process.env.USENET_SEGMENT_SPOOLING_STREAM_BUFFER_BYTES = String(
      2 * MEBIBYTE_BYTES
    );
    await core.settingsStore.reload({ emit: false });
    const versionBefore = await core.SettingsRepository.getVersion();
    const reload = vi.spyOn(core.settingsStore, 'reload');

    const valid = await post('/dashboard/settings/import/env');

    expect(valid.status).toBe(200);
    expect(reload).toHaveBeenCalledTimes(1);
    reload.mockRestore();
    expect(await core.SettingsRepository.getVersion()).toBe(versionBefore + 1);
    expect((await storedUsenetValues()).size).toBe(5);
  });
});
