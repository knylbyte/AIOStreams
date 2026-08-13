import { settingsStore, describeSettings } from '../../../config/index.js';
import type { ManagedSettingsPatchResult } from '../../../config/managed.js';
import { usenetSchema } from '../../../config/schema/usenet.js';
import { createLogger } from '../../../logging/logger.js';
import { formatZodError } from '../../../utils/format-zod-error.js';
import {
  resolveEngineResourcePlan,
  UsenetResourcePlanConfigError,
  type EngineResourcePlanOptions,
} from '../../resource-plan.js';

// Re-exported so the dashboard route + frontend can show the concrete values
// each performance profile applies (single source of truth lives in the schema).
export { PERFORMANCE_PROFILES } from '../../../config/schema/usenet.js';

const logger = createLogger('usenet/dashboard');
const USENET_SETTINGS_MASK = '';
const MAX_QUEUED_USENET_MUTATIONS = 64;
const MAX_VERSION_RETRIES = 4;

/**
 * One usenet engine setting + metadata + current value, in the same shape the
 * generic settings page consumes, but served by the usenet dashboard so the
 * (intentionally `ui.hidden`) engine knobs live in one bespoke editor on the
 * usenet page instead of the generic settings page. Excludes `usenet.providers`
 * (its own editor) and never echoes secret values.
 */
export interface UsenetSettingDescriptor {
  key: string;
  label: string;
  description: string;
  env: string | null;
  requiresRestart: boolean;
  secret: boolean;
  valueType: string;
  default: unknown;
  source: string;
  value: unknown;
  secretSet: boolean;
  ui: unknown;
}

export interface UsenetSettingsMutation {
  readonly sets?: Readonly<Record<string, unknown>>;
  readonly deletes?: readonly string[];
}

type UsenetSettingsMutationInput =
  | UsenetSettingsMutation
  | (() => UsenetSettingsMutation);

export interface UsenetSettingsMutationResult {
  readonly updated: string[];
  readonly reset: string[];
  readonly requiresRestart: boolean;
  readonly errors: Record<string, string>;
}

export interface UsenetSettingsImportResult {
  readonly imported: string[];
  readonly skipped: { key: string; reason: string }[];
  readonly failed: { key: string; reason: string }[];
  readonly requiresRestart: boolean;
}

export interface UsenetSettingsResetResult {
  readonly reset: string[];
  readonly skipped: { key: string; reason: string }[];
  readonly requiresRestart: boolean;
}

export interface UsenetEnvironmentImportResult {
  readonly imported: string[];
  readonly skippedAsDefault: string[];
  readonly failed: { key: string; reason: string }[];
}

type MutationIssueKind =
  | 'unmanaged'
  | 'unknown'
  | 'environment'
  | 'already-default'
  | 'invalid'
  | 'resource-plan'
  | 'concurrent';

interface MutationIssue {
  readonly kind: MutationIssueKind;
  readonly message: string;
}

interface InternalMutationResult {
  readonly updated: string[];
  readonly reset: string[];
  readonly requiresRestart: boolean;
  readonly issues: Record<string, MutationIssue>;
}

interface ClassifiedMutation extends InternalMutationResult {
  readonly sets: { key: string; value: unknown }[];
  readonly deletes: string[];
  readonly candidateValues: ReadonlyMap<string, unknown>;
}

class UsenetSettingsMutationQueueFullError extends Error {
  readonly code = 'USENET_SETTINGS_MUTATION_QUEUE_FULL' as const;

  constructor() {
    super(
      `Too many queued Usenet settings mutations (maximum ${MAX_QUEUED_USENET_MUTATIONS})`
    );
    this.name = 'UsenetSettingsMutationQueueFullError';
  }
}

class BoundedSerialExecutor {
  private tail: Promise<void> = Promise.resolve();
  private pending = 0;

  run<T>(task: () => Promise<T>): Promise<T> {
    if (this.pending >= MAX_QUEUED_USENET_MUTATIONS) {
      return Promise.reject(new UsenetSettingsMutationQueueFullError());
    }
    this.pending++;
    const result = this.tail.then(task);
    this.tail = result.then(
      () => undefined,
      () => undefined
    );
    return result.finally(() => {
      this.pending--;
    });
  }
}

const mutationExecutor = new BoundedSerialExecutor();
const usenetFields = new Map(
  Object.entries(usenetSchema).map(([name, field]) => [`usenet.${name}`, field])
);

/** True for keys owned by the bespoke Usenet engine editor. */
export function isUsenetEngineSettingKey(key: string): boolean {
  return key.startsWith('usenet.') && key !== 'usenet.providers';
}

function candidateValue(
  key: string,
  candidateValues: ReadonlyMap<string, unknown>
): unknown {
  return candidateValues.has(key)
    ? candidateValues.get(key)
    : settingsStore.getEffectiveValue(key);
}

function buildResourcePlanCandidate(
  candidateValues: ReadonlyMap<string, unknown>
): EngineResourcePlanOptions {
  return {
    streamingMode: usenetSchema.streamingMode.schema.parse(
      candidateValue('usenet.streamingMode', candidateValues)
    ),
    maxConcurrentDownloads: usenetSchema.maxConcurrentDownloads.schema.parse(
      candidateValue('usenet.maxConcurrentDownloads', candidateValues)
    ),
    segmentMemoryCacheBytes: usenetSchema.segmentMemoryCacheBytes.schema.parse(
      candidateValue('usenet.segmentMemoryCacheBytes', candidateValues)
    ),
    segmentSpoolingMemoryBudgetBytes:
      usenetSchema.segmentSpoolingMemoryBudgetBytes.schema.parse(
        candidateValue(
          'usenet.segmentSpoolingMemoryBudgetBytes',
          candidateValues
        )
      ),
    segmentSpoolingStreamBufferBytes:
      usenetSchema.segmentSpoolingStreamBufferBytes.schema.parse(
        candidateValue(
          'usenet.segmentSpoolingStreamBufferBytes',
          candidateValues
        )
      ),
    segmentSpoolingSpoolBytes:
      usenetSchema.segmentSpoolingSpoolBytes.schema.parse(
        candidateValue('usenet.segmentSpoolingSpoolBytes', candidateValues)
      ),
    segmentSpoolingMinFreeDiskBytes:
      usenetSchema.segmentSpoolingMinFreeDiskBytes.schema.parse(
        candidateValue(
          'usenet.segmentSpoolingMinFreeDiskBytes',
          candidateValues
        )
      ),
  };
}

function classifyMutation(
  mutation: UsenetSettingsMutation,
  options: {
    readonly unmanagedMessage: string;
    readonly allowEnvironmentOverrides: boolean;
  }
): ClassifiedMutation {
  const sets: { key: string; value: unknown }[] = [];
  const deletes: string[] = [];
  const updated: string[] = [];
  const reset: string[] = [];
  const issues: Record<string, MutationIssue> = {};
  const candidateValues = new Map<string, unknown>();
  const setKeys = new Set(Object.keys(mutation.sets ?? {}));
  const deleteKeys = new Set(mutation.deletes ?? []);
  const conflictingKeys = new Set(
    [...setKeys].filter((key) => deleteKeys.has(key))
  );
  const metadata = new Map(
    settingsStore.metadata.map((item) => [item.key, item])
  );
  let requiresRestart = false;

  for (const key of conflictingKeys) {
    issues[key] = {
      kind: 'invalid',
      message: 'A setting cannot be updated and reset in the same mutation',
    };
  }

  const classifyKey = (
    key: string
  ):
    | {
        metadata: (typeof settingsStore.metadata)[number];
        field: (typeof usenetSchema)[keyof typeof usenetSchema];
      }
    | undefined => {
    if (!isUsenetEngineSettingKey(key)) {
      issues[key] = { kind: 'unmanaged', message: options.unmanagedMessage };
      return undefined;
    }
    const item = metadata.get(key);
    if (!item) {
      issues[key] = { kind: 'unknown', message: 'Unknown setting' };
      return undefined;
    }
    if (item.source === 'environment' && !options.allowEnvironmentOverrides) {
      issues[key] = {
        kind: 'environment',
        message: `Overridden by ${item.env}`,
      };
      return undefined;
    }
    const field = usenetFields.get(key);
    if (!field) {
      issues[key] = { kind: 'unknown', message: 'Unknown setting' };
      return undefined;
    }
    return { metadata: item, field };
  };

  for (const [key, value] of Object.entries(mutation.sets ?? {})) {
    if (conflictingKeys.has(key)) continue;
    const classified = classifyKey(key);
    if (!classified) continue;
    const parsed = classified.field.schema.safeParse(value);
    if (!parsed.success) {
      issues[key] = {
        kind: 'invalid',
        message: formatZodError(parsed.error, { singleLine: true }),
      };
      continue;
    }
    sets.push({ key, value: parsed.data });
    updated.push(key);
    candidateValues.set(key, parsed.data);
    if (classified.metadata.requiresRestart) requiresRestart = true;
  }

  for (const key of deleteKeys) {
    if (conflictingKeys.has(key)) continue;
    const classified = classifyKey(key);
    if (!classified) continue;
    if (classified.metadata.source === 'default') {
      issues[key] = {
        kind: 'already-default',
        message: 'Already using the default value',
      };
      continue;
    }
    const defaultValue = classified.field.schema.parse(
      classified.field.default
    );
    deletes.push(key);
    reset.push(key);
    candidateValues.set(key, defaultValue);
    if (classified.metadata.requiresRestart) requiresRestart = true;
  }

  return {
    sets,
    deletes,
    updated,
    reset,
    requiresRestart,
    issues,
    candidateValues,
  };
}

function mergeResourcePlanIssues(
  issues: Record<string, MutationIssue>,
  error: UsenetResourcePlanConfigError
): void {
  const messagesByField = new Map<string, string[]>();
  for (const issue of error.issues) {
    const messages = messagesByField.get(issue.field) ?? [];
    messages.push(issue.message);
    messagesByField.set(issue.field, messages);
  }
  for (const [field, messages] of messagesByField) {
    issues[field] ??= {
      kind: 'resource-plan',
      message: messages.join(' '),
    };
  }
}

function issueMessages(
  issues: Readonly<Record<string, MutationIssue>>
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(issues).map(([key, issue]) => [key, issue.message])
  );
}

async function executeMutation(
  mutationInput: UsenetSettingsMutationInput,
  username: string,
  options: {
    readonly unmanagedMessage: string;
    readonly allowEnvironmentOverrides?: boolean;
  }
): Promise<InternalMutationResult> {
  return mutationExecutor.run(async () => {
    await settingsStore.refreshIfChanged();

    for (let attempt = 0; attempt < MAX_VERSION_RETRIES; attempt++) {
      const expectedVersion = settingsStore.currentVersion;
      const mutation =
        typeof mutationInput === 'function' ? mutationInput() : mutationInput;
      const classified = classifyMutation(mutation, {
        unmanagedMessage: options.unmanagedMessage,
        allowEnvironmentOverrides: options.allowEnvironmentOverrides === true,
      });

      if (classified.sets.length === 0 && classified.deletes.length === 0) {
        return classified;
      }

      try {
        resolveEngineResourcePlan(
          buildResourcePlanCandidate(classified.candidateValues)
        );
      } catch (error) {
        if (!(error instanceof UsenetResourcePlanConfigError)) throw error;
        mergeResourcePlanIssues(classified.issues, error);
        return {
          updated: [],
          reset: [],
          requiresRestart: false,
          issues: classified.issues,
        };
      }

      const applied = await settingsStore.applyBatch(
        { sets: classified.sets, deletes: classified.deletes },
        {
          expectedVersion,
          updatedBy: username,
          allowEnvironmentOverrides: options.allowEnvironmentOverrides === true,
        }
      );
      if (applied) return classified;

      await settingsStore.reload();
    }

    const mutation =
      typeof mutationInput === 'function' ? mutationInput() : mutationInput;
    const classified = classifyMutation(mutation, {
      unmanagedMessage: options.unmanagedMessage,
      allowEnvironmentOverrides: options.allowEnvironmentOverrides === true,
    });
    for (const key of [...classified.updated, ...classified.reset]) {
      classified.issues[key] ??= {
        kind: 'concurrent',
        message: 'Settings changed concurrently; retry the request',
      };
    }
    return {
      updated: [],
      reset: [],
      requiresRestart: false,
      issues: classified.issues,
    };
  });
}

/** Every editable usenet engine setting (incl. the hidden ones) with its value. */
export function getUsenetSettings(): UsenetSettingDescriptor[] {
  const hints = describeSettings();
  return settingsStore.metadata
    .filter((m) => isUsenetEngineSettingKey(m.key))
    .map((m) => {
      let value: unknown;
      try {
        value = settingsStore.getEffectiveValue(m.key);
      } catch {
        value = m.default;
      }
      const secretSet =
        m.secret && m.source !== 'default' && value !== '' && value != null;
      return {
        ...m,
        ui: hints[m.key] ?? { kind: 'json' },
        value: m.secret ? USENET_SETTINGS_MASK : value,
        secretSet,
      };
    });
}

/**
 * Atomically mutate Usenet engine settings after validating the complete
 * effective candidate. Environment-owned values cannot be bypassed here.
 */
export async function mutateUsenetSettings(
  mutation: UsenetSettingsMutation,
  username: string
): Promise<UsenetSettingsMutationResult> {
  const result = await executeMutation(mutation, username, {
    unmanagedMessage: 'Not a usenet engine setting',
  });
  return {
    updated: result.updated,
    reset: result.reset,
    requiresRestart: result.requiresRestart,
    errors: issueMessages(result.issues),
  };
}

/** Persist a patch of usenet engine settings (dotted-key → value). */
export async function saveUsenetSettings(
  patch: Record<string, unknown>,
  username: string
): Promise<ManagedSettingsPatchResult> {
  const result = await executeMutation({ sets: patch }, username, {
    unmanagedMessage: 'Not a usenet engine setting',
  });
  if (result.updated.length) {
    logger.info(
      { updated: result.updated, username },
      'usenet settings updated'
    );
  }
  return {
    updated: result.updated,
    requiresRestart: result.requiresRestart,
    errors: issueMessages(result.issues),
  };
}

/** Import the Usenet subset of a generic JSON settings import atomically. */
export async function importUsenetSettings(
  patch: Record<string, unknown>,
  username: string
): Promise<UsenetSettingsImportResult> {
  const result = await executeMutation({ sets: patch }, username, {
    unmanagedMessage: 'Not a usenet engine setting',
  });
  const skipped: { key: string; reason: string }[] = [];
  const failed: { key: string; reason: string }[] = [];
  for (const [key, issue] of Object.entries(result.issues)) {
    if (issue.kind === 'unknown' || issue.kind === 'unmanaged') {
      skipped.push({ key, reason: 'unknown' });
    } else if (issue.kind === 'environment') {
      skipped.push({ key, reason: 'env-locked' });
    } else {
      failed.push({ key, reason: issue.message });
    }
  }
  return {
    imported: result.updated,
    skipped,
    failed,
    requiresRestart: result.requiresRestart,
  };
}

/** Reset the Usenet subset of a generic reset request as one delete batch. */
export async function resetUsenetSettings(
  keys: readonly string[],
  username: string
): Promise<UsenetSettingsResetResult> {
  const result = await executeMutation(
    { deletes: [...new Set(keys)] },
    username,
    { unmanagedMessage: 'Not a usenet engine setting' }
  );
  const skipped = Object.entries(result.issues).map(([key, issue]) => ({
    key,
    reason:
      issue.kind === 'unknown' || issue.kind === 'unmanaged'
        ? 'unknown'
        : issue.kind === 'environment'
          ? 'env-locked'
          : issue.kind === 'already-default'
            ? 'already-default'
            : issue.message,
  }));
  return {
    reset: result.reset,
    skipped,
    requiresRestart: result.requiresRestart,
  };
}

/**
 * Copy current environment-owned Usenet values into the DB through the same
 * candidate validator and atomic batch path. This is the sole env-lock bypass.
 */
export async function importUsenetEnvironmentSettings(
  username: string
): Promise<UsenetEnvironmentImportResult> {
  let skippedAsDefault: string[] = [];
  let readFailures: { key: string; reason: string }[] = [];
  let mutationKeys: string[] = [];

  const buildEnvironmentMutation = (): UsenetSettingsMutation => {
    const sets: Record<string, unknown> = {};
    const deletes: string[] = [];
    skippedAsDefault = [];
    readFailures = [];
    mutationKeys = [];

    for (const item of settingsStore.metadata) {
      if (
        !isUsenetEngineSettingKey(item.key) ||
        item.source !== 'environment'
      ) {
        continue;
      }
      try {
        const value = settingsStore.getEffectiveValue(item.key);
        if (JSON.stringify(value) === JSON.stringify(item.default)) {
          if (settingsStore.hasStoredValue(item.key)) {
            deletes.push(item.key);
            mutationKeys.push(item.key);
          } else {
            skippedAsDefault.push(item.key);
          }
        } else {
          sets[item.key] = value;
          mutationKeys.push(item.key);
        }
      } catch (error) {
        readFailures.push({
          key: item.key,
          reason: error instanceof Error ? error.message : 'unreadable',
        });
      }
    }

    return { sets, deletes };
  };

  let result: InternalMutationResult;
  try {
    result = await executeMutation(buildEnvironmentMutation, username, {
      unmanagedMessage: 'Not a usenet engine setting',
      allowEnvironmentOverrides: true,
    });
  } catch (error) {
    if (mutationKeys.length === 0) throw error;
    const reason = error instanceof Error ? error.message : 'write failed';
    return {
      imported: [],
      skippedAsDefault,
      failed: [
        ...readFailures,
        ...mutationKeys.map((key) => ({ key, reason })),
      ],
    };
  }

  const importedKeys = new Set([...result.updated, ...result.reset]);
  const failed = [
    ...readFailures,
    ...Object.entries(result.issues).map(([key, issue]) => ({
      key,
      reason: issue.message,
    })),
  ];
  return {
    imported: mutationKeys.filter((key) => importedKeys.has(key)),
    skippedAsDefault,
    failed,
  };
}
