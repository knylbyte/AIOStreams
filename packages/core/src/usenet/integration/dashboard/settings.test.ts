import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test, { after, before, beforeEach } from 'node:test';
import { settingsStore } from '../../../config/index.js';
import { closeDb, getDb, initDb } from '../../../db/index.js';
import { SettingsRepository } from '../../../db/repositories/settings.js';
import {
  resolveEngineResourcePlan,
  type EngineResourcePlanOptions,
} from '../../resource-plan.js';
import { saveUsenetSettings } from './settings.js';

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

let databaseDirectory: string;

function clearResourceEnvironment(): void {
  for (const name of RESOURCE_ENV_NAMES) delete process.env[name];
}

async function resetStoredSettings(): Promise<void> {
  for (const row of await SettingsRepository.getAll()) {
    await SettingsRepository.delete(row.key);
  }
  await settingsStore.reload({ emit: false });
}

async function storedSettingValues(): Promise<Map<string, string>> {
  return new Map(
    (await SettingsRepository.getAll()).map((row) => [row.key, row.value])
  );
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function currentResourceOptions(): EngineResourcePlanOptions {
  const current = settingsStore.current.usenet;
  return {
    streamingMode: current.streamingMode,
    maxConcurrentDownloads: current.maxConcurrentDownloads,
    segmentMemoryCacheBytes: current.segmentMemoryCacheBytes,
    segmentSpoolingMemoryBudgetBytes: current.segmentSpoolingMemoryBudgetBytes,
    segmentSpoolingStreamBufferBytes: current.segmentSpoolingStreamBufferBytes,
    segmentSpoolingSpoolBytes: current.segmentSpoolingSpoolBytes,
    segmentSpoolingMinFreeDiskBytes: current.segmentSpoolingMinFreeDiskBytes,
  };
}

before(async () => {
  clearResourceEnvironment();
  databaseDirectory = await mkdtemp(join(tmpdir(), 'aiostreams-settings-'));
  await initDb(`sqlite://${join(databaseDirectory, 'settings.sqlite')}`);
  await settingsStore.initialise();
});

beforeEach(async () => {
  clearResourceEnvironment();
  await resetStoredSettings();
});

after(async () => {
  for (const [name, value] of originalEnvironment) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
  await closeDb();
  await rm(databaseDirectory, { recursive: true, force: true });
});

test('invalid segment-spooling candidates are rejected before every write', async () => {
  const versionBefore = await SettingsRepository.getVersion();
  const snapshotBefore = settingsStore.current;
  let reloads = 0;
  let events = 0;
  const originalReload = settingsStore.reload;
  settingsStore.reload = async (options = {}) => {
    reloads++;
    return originalReload.call(settingsStore, options);
  };
  const unsubscribe = settingsStore.subscribe(() => {
    events++;
  });

  let result: Awaited<ReturnType<typeof saveUsenetSettings>>;
  try {
    result = await saveUsenetSettings(
      {
        'usenet.streamingMode': 'segment_spooling',
        'usenet.prefetchSegments': 17,
        'usenet.segmentMemoryCacheBytes': 24_000_000,
        'usenet.segmentSpoolingMemoryBudgetBytes': 16 * MEBIBYTE_BYTES,
        'usenet.segmentSpoolingStreamBufferBytes': 8 * MEBIBYTE_BYTES + 1,
      },
      'review-test'
    );
  } finally {
    unsubscribe();
    settingsStore.reload = originalReload;
  }

  assert.deepEqual(result.updated, []);
  assert.equal(result.requiresRestart, false);
  assert.match(
    result.errors['usenet.segmentSpoolingStreamBufferBytes'],
    /must not exceed half of usenet\.segmentSpoolingMemoryBudgetBytes/
  );
  assert.deepEqual(await SettingsRepository.getAll(), []);
  assert.equal(await SettingsRepository.getVersion(), versionBefore);
  assert.equal(settingsStore.current, snapshotBefore);
  assert.equal(reloads, 0);
  assert.equal(events, 0);
});

test('dormant invalid spooling budgets remain writable in buffering mode', async () => {
  const patch = {
    'usenet.segmentSpoolingMemoryBudgetBytes': 1,
    'usenet.segmentSpoolingStreamBufferBytes': 1,
    'usenet.segmentSpoolingSpoolBytes': 1,
    'usenet.segmentSpoolingMinFreeDiskBytes': 0,
  };

  const result = await saveUsenetSettings(patch, 'review-test');

  assert.deepEqual(result, {
    updated: Object.keys(patch),
    requiresRestart: true,
    errors: {},
  });
  assert.deepEqual(
    await storedSettingValues(),
    new Map([
      ['usenet.segmentSpoolingMemoryBudgetBytes', '1'],
      ['usenet.segmentSpoolingStreamBufferBytes', '1'],
      ['usenet.segmentSpoolingSpoolBytes', '1'],
      ['usenet.segmentSpoolingMinFreeDiskBytes', '0'],
    ])
  );
});

test('mode and budget changes are validated as one complete candidate', async () => {
  await saveUsenetSettings(
    {
      'usenet.segmentSpoolingMemoryBudgetBytes': 1,
      'usenet.segmentSpoolingStreamBufferBytes': 1,
      'usenet.segmentSpoolingSpoolBytes': 1,
    },
    'review-test'
  );
  const patch = {
    'usenet.streamingMode': 'segment_spooling',
    'usenet.segmentSpoolingMemoryBudgetBytes': 16 * MEBIBYTE_BYTES,
    'usenet.segmentSpoolingStreamBufferBytes': 2 * MEBIBYTE_BYTES,
    'usenet.segmentSpoolingSpoolBytes': 64 * MEBIBYTE_BYTES,
    'usenet.segmentSpoolingMinFreeDiskBytes': 0,
  };
  const versionBefore = await SettingsRepository.getVersion();
  let reloads = 0;
  const observed: {
    streamingMode: string;
    memoryBudgetBytes: number;
    streamBufferBytes: number;
    spoolBytes: number;
  }[] = [];
  const originalReload = settingsStore.reload;
  settingsStore.reload = async (options = {}) => {
    reloads++;
    return originalReload.call(settingsStore, options);
  };
  const unsubscribe = settingsStore.subscribe(({ current }) => {
    observed.push({
      streamingMode: current.usenet.streamingMode,
      memoryBudgetBytes: current.usenet.segmentSpoolingMemoryBudgetBytes,
      streamBufferBytes: current.usenet.segmentSpoolingStreamBufferBytes,
      spoolBytes: current.usenet.segmentSpoolingSpoolBytes,
    });
  });

  let result: Awaited<ReturnType<typeof saveUsenetSettings>>;
  try {
    result = await saveUsenetSettings(patch, 'review-test');
  } finally {
    unsubscribe();
    settingsStore.reload = originalReload;
  }

  assert.deepEqual(result, {
    updated: Object.keys(patch),
    requiresRestart: true,
    errors: {},
  });
  assert.deepEqual(
    await storedSettingValues(),
    new Map([
      ['usenet.segmentSpoolingMemoryBudgetBytes', String(16 * MEBIBYTE_BYTES)],
      ['usenet.segmentSpoolingStreamBufferBytes', String(2 * MEBIBYTE_BYTES)],
      ['usenet.segmentSpoolingSpoolBytes', String(64 * MEBIBYTE_BYTES)],
      ['usenet.streamingMode', '"segment_spooling"'],
      ['usenet.segmentSpoolingMinFreeDiskBytes', '0'],
    ])
  );
  assert.equal(await SettingsRepository.getVersion(), versionBefore + 1);
  assert.equal(reloads, 1);
  assert.deepEqual(observed, [
    {
      streamingMode: 'segment_spooling',
      memoryBudgetBytes: 16 * MEBIBYTE_BYTES,
      streamBufferBytes: 2 * MEBIBYTE_BYTES,
      spoolBytes: 64 * MEBIBYTE_BYTES,
    },
  ]);
});

test('valid fields retain partial semantics but commit as one accepted batch', async () => {
  const versionBefore = await SettingsRepository.getVersion();
  const result = await saveUsenetSettings(
    {
      'usenet.prefetchSegments': 0,
      'usenet.segmentMemoryCacheBytes': 24_000_000,
      'usenet.segmentSpoolingMemoryBudgetBytes': 96_000_000,
    },
    'review-test'
  );

  assert.deepEqual(result.updated, [
    'usenet.segmentMemoryCacheBytes',
    'usenet.segmentSpoolingMemoryBudgetBytes',
  ]);
  assert.match(result.errors['usenet.prefetchSegments'], /positive integer/i);
  assert.equal(result.requiresRestart, true);
  assert.equal(await SettingsRepository.getVersion(), versionBefore + 1);
  assert.deepEqual(
    await storedSettingValues(),
    new Map([
      ['usenet.segmentMemoryCacheBytes', '24000000'],
      ['usenet.segmentSpoolingMemoryBudgetBytes', '96000000'],
    ])
  );
});

test('a failure inside the batch rolls back every write, version and snapshot', async () => {
  await getDb().exec(`CREATE TRIGGER fail_usenet_settings_batch
    BEFORE INSERT ON settings
    WHEN NEW.key = 'usenet.segmentSpoolingSpoolBytes'
    BEGIN
      SELECT RAISE(ABORT, 'forced settings batch failure');
    END`);
  const versionBefore = await SettingsRepository.getVersion();
  const rowsBefore = await storedSettingValues();
  const snapshotBefore = settingsStore.current;
  let events = 0;
  const unsubscribe = settingsStore.subscribe(() => {
    events++;
  });

  try {
    await assert.rejects(
      saveUsenetSettings(
        {
          'usenet.streamingMode': 'segment_spooling',
          'usenet.segmentSpoolingMemoryBudgetBytes': 16 * MEBIBYTE_BYTES,
          'usenet.segmentSpoolingStreamBufferBytes': 2 * MEBIBYTE_BYTES,
          'usenet.segmentSpoolingSpoolBytes': 64 * MEBIBYTE_BYTES,
        },
        'review-test'
      ),
      /forced settings batch failure/
    );
  } finally {
    unsubscribe();
    await getDb().exec('DROP TRIGGER fail_usenet_settings_batch');
  }

  assert.deepEqual(await storedSettingValues(), rowsBefore);
  assert.equal(await SettingsRepository.getVersion(), versionBefore);
  assert.equal(settingsStore.current, snapshotBefore);
  assert.equal(events, 0);
});

test('concurrent valid patches cannot commit an invalid hybrid candidate', async () => {
  await saveUsenetSettings(
    {
      'usenet.streamingMode': 'segment_spooling',
      'usenet.segmentSpoolingMemoryBudgetBytes': 32 * MEBIBYTE_BYTES,
      'usenet.segmentSpoolingStreamBufferBytes': 8 * MEBIBYTE_BYTES,
      'usenet.segmentSpoolingSpoolBytes': 64 * MEBIBYTE_BYTES,
    },
    'review-test'
  );
  const initial = currentResourceOptions();
  assert.doesNotThrow(() =>
    resolveEngineResourcePlan({
      ...initial,
      segmentSpoolingMemoryBudgetBytes: 16 * MEBIBYTE_BYTES,
    })
  );
  assert.doesNotThrow(() =>
    resolveEngineResourcePlan({
      ...initial,
      segmentSpoolingStreamBufferBytes: 12 * MEBIBYTE_BYTES,
    })
  );

  const firstBatchEntered = deferred<void>();
  const releaseFirstBatch = deferred<void>();
  const originalApplyBatch = SettingsRepository.applyBatch;
  let calls = 0;
  SettingsRepository.applyBatch = async (batch) => {
    calls++;
    if (calls === 1) {
      firstBatchEntered.resolve();
      await releaseFirstBatch.promise;
    }
    return originalApplyBatch.call(SettingsRepository, batch);
  };
  const observed: EngineResourcePlanOptions[] = [];
  const unsubscribe = settingsStore.subscribe(() => {
    observed.push(currentResourceOptions());
  });

  try {
    const lowerMemory = saveUsenetSettings(
      { 'usenet.segmentSpoolingMemoryBudgetBytes': 16 * MEBIBYTE_BYTES },
      'review-test'
    );
    await firstBatchEntered.promise;
    const raiseStreamBuffer = saveUsenetSettings(
      { 'usenet.segmentSpoolingStreamBufferBytes': 12 * MEBIBYTE_BYTES },
      'review-test'
    );
    releaseFirstBatch.resolve();

    const [first, second] = await Promise.all([lowerMemory, raiseStreamBuffer]);
    assert.deepEqual(first.updated, [
      'usenet.segmentSpoolingMemoryBudgetBytes',
    ]);
    assert.deepEqual(second.updated, []);
    assert.match(
      second.errors['usenet.segmentSpoolingStreamBufferBytes'],
      /must not exceed half/
    );
  } finally {
    releaseFirstBatch.resolve();
    unsubscribe();
    SettingsRepository.applyBatch = originalApplyBatch;
  }

  assert.equal(calls, 1);
  assert.equal(
    settingsStore.current.usenet.segmentSpoolingMemoryBudgetBytes,
    16 * MEBIBYTE_BYTES
  );
  assert.equal(
    settingsStore.current.usenet.segmentSpoolingStreamBufferBytes,
    8 * MEBIBYTE_BYTES
  );
  assert.equal(observed.length, 1);
  for (const candidate of observed) {
    assert.doesNotThrow(() => resolveEngineResourcePlan(candidate));
  }
});

test('environment-locked fields keep the existing dotted-key error format', async () => {
  process.env.USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES = '96MB';
  await settingsStore.reload({ emit: false });

  const result = await saveUsenetSettings(
    { 'usenet.segmentSpoolingMemoryBudgetBytes': 64_000_000 },
    'review-test'
  );

  assert.deepEqual(result, {
    updated: [],
    requiresRestart: false,
    errors: {
      'usenet.segmentSpoolingMemoryBudgetBytes':
        'Overridden by USENET_SEGMENT_SPOOLING_MEMORY_BUDGET_BYTES',
    },
  });
  assert.equal(
    settingsStore.getEffectiveValue('usenet.segmentSpoolingMemoryBudgetBytes'),
    96_000_000
  );
  assert.deepEqual(await SettingsRepository.getAll(), []);
});

test('unknown and unmanaged keys retain the existing refusal formats', async () => {
  const result = await saveUsenetSettings(
    {
      'usenet.providers': [],
      'usenet.unknownResourceSetting': 1,
      'logging.logLevel': 'debug',
    },
    'review-test'
  );

  assert.deepEqual(result, {
    updated: [],
    requiresRestart: false,
    errors: {
      'usenet.providers': 'Not a usenet engine setting',
      'usenet.unknownResourceSetting': 'Unknown setting',
      'logging.logLevel': 'Not a usenet engine setting',
    },
  });
  assert.deepEqual(await SettingsRepository.getAll(), []);
});
