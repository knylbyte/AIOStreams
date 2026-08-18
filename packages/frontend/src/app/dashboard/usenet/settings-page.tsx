import React from 'react';
import { useNavigate, useSearch } from '@tanstack/react-router';
import { z } from 'zod';
import { toast } from 'sonner';
import { BiCog } from 'react-icons/bi';
import { useFormContext, useWatch, type UseFormReturn } from 'react-hook-form';
import { Form } from '@/components/ui/form';
import { Card } from '@/components/ui/card';
import { Alert } from '@/components/ui/alert';
import { DashboardLoading } from '@/components/shared/dashboard-query-boundary';
import {
  SettingsCard,
  SettingsPageHeader,
} from '../settings/_components/settings-card';
import { SettingsField, toName } from '../settings/_components/settings-field';
import {
  SettingsSubmitButton,
  SettingsIsDirty,
} from '../settings/_components/settings-submit-button';
import type { SettingsKey } from '../settings/queries';
import { SettingsActionsMenu } from '../settings/_components/settings-actions-menu';
import MarkdownLite from '@/components/shared/markdown-lite';
import { useScrollToField } from '@/components/shared/command-palette/use-scroll-to-field';
import {
  useUsenetSettings,
  useSaveUsenetSettings,
  USENET_SETTINGS_QUERY_KEY,
  type UsenetProfiles,
} from './queries';
import {
  BUNDLED_LEAVES,
  PROFILE_LEAF,
  STREAMING_MODE_LEAF,
  bundledValuesMatchPreset,
  groupUsenetSettings,
  leafOf,
  usenetKey,
  type BundledLeaf,
} from './settings-page-logic';

/** Scope for the settings actions menu: every usenet engine key except the
 *  provider accounts (managed in their own editor). Mirrors the backend's
 *  `isManagedUsenetKey` so reset/import/export can never touch providers. */
const USENET_SCOPE = {
  includes: (key: string) =>
    key.startsWith('usenet.') && key !== 'usenet.providers',
  fileStem: 'aiostreams-usenet-settings',
  noun: 'usenet',
} as const;

const PROFILE_NAME = toName(usenetKey(PROFILE_LEAF));
const STREAMING_MODE_NAME = toName(usenetKey(STREAMING_MODE_LEAF));
const BUNDLED_NAMES: Readonly<Record<BundledLeaf, string>> = {
  prefetchSegments: toName(usenetKey('prefetchSegments')),
  maxConcurrentDownloads: toName(usenetKey('maxConcurrentDownloads')),
  segmentDiskCacheBytes: toName(usenetKey('segmentDiskCacheBytes')),
};

/**
 * Two-way link between the performance profile and its bundled fields:
 *  - selecting a profile fills the three fields with that profile's values
 *    (silently on first mount, so the form shows what's actually in effect);
 *  - editing any bundled field switches the profile to "custom".
 * Renders nothing — it just drives form state via the surrounding <Form>.
 */
function ProfileLinker({ profiles }: { profiles: UsenetProfiles }) {
  const { setValue, getValues } = useFormContext();
  const watchedProfile = useWatch({ name: PROFILE_NAME });
  const profile =
    typeof watchedProfile === 'string' ? watchedProfile : undefined;

  // These are deliberately three explicit, unconditional hooks. The profile
  // bundle has three leaves, so no dynamic hook count or undefined field name
  // can be introduced by iterating the tuple.
  const prefetchSegments = useWatch({
    name: BUNDLED_NAMES.prefetchSegments,
  });
  const maxConcurrentDownloads = useWatch({
    name: BUNDLED_NAMES.maxConcurrentDownloads,
  });
  const segmentDiskCacheBytes = useWatch({
    name: BUNDLED_NAMES.segmentDiskCacheBytes,
  });

  const applyingRef = React.useRef(false);

  // When the profile changes to a preset, fill the bundled fields with its
  // values. Skips when already in sync (e.g. on mount, since defaults are
  // seeded from the active profile) so it never dirties the form spuriously.
  React.useEffect(() => {
    const preset =
      profile && profile !== 'custom' ? profiles[profile] : undefined;
    if (!preset) return;
    const synced = bundledValuesMatchPreset(
      {
        prefetchSegments: getValues(BUNDLED_NAMES.prefetchSegments),
        maxConcurrentDownloads: getValues(BUNDLED_NAMES.maxConcurrentDownloads),
        segmentDiskCacheBytes: getValues(BUNDLED_NAMES.segmentDiskCacheBytes),
      },
      preset
    );
    if (synced) return;
    applyingRef.current = true;
    BUNDLED_LEAVES.forEach((leaf) =>
      setValue(BUNDLED_NAMES[leaf], preset[leaf], { shouldDirty: true })
    );
    const t = setTimeout(() => {
      applyingRef.current = false;
    }, 0);
    return () => clearTimeout(t);
  }, [getValues, profile, profiles, setValue]);

  // Editing a bundled field while a profile is active flips it to "custom".
  React.useEffect(() => {
    if (applyingRef.current) return;
    const preset =
      profile && profile !== 'custom' ? profiles[profile] : undefined;
    if (!preset) return;
    const matches = bundledValuesMatchPreset(
      {
        prefetchSegments,
        maxConcurrentDownloads,
        segmentDiskCacheBytes,
      },
      preset
    );
    if (!matches) setValue(PROFILE_NAME, 'custom', { shouldDirty: true });
  }, [
    maxConcurrentDownloads,
    prefetchSegments,
    profile,
    profiles,
    segmentDiskCacheBytes,
    setValue,
  ]);

  return null;
}

function SegmentHandlingInfo() {
  return (
    <Alert intent="info-basic" title="Segment handling">
      <div className="space-y-2 text-sm">
        <p>
          <strong>Segment Buffering</strong> downloads and decodes complete
          segments into memory, where they remain available for ordered playback
          and prefetching.
        </p>
        <p>
          <strong>Segment Spooling</strong> decodes segment data incrementally
          into a transient disk spool. Playback can read growing segments while
          fixed memory and disk budgets bound read-ahead.
        </p>
        <p>
          The performance profile remains independent and continues to control
          concurrency and prefetch aggressiveness.
        </p>
      </div>
    </Alert>
  );
}

function UsenetSettingsSections({ keys }: { keys: SettingsKey[] }) {
  const streamingMode = useWatch({ name: STREAMING_MODE_NAME });
  const groups = groupUsenetSettings(keys, streamingMode);

  return groups.map((group) => (
    <SettingsCard key={group.id} title={group.title}>
      {group.note && (
        <p className="text-xs text-[--muted] -mt-1 mb-1">
          <MarkdownLite>{group.note}</MarkdownLite>
        </p>
      )}
      {group.keys.map((key) => (
        <div key={key.key} id={`setting-${key.key}`}>
          <SettingsField k={key} />
        </div>
      ))}
      {group.id === 'streaming-mode' && <SegmentHandlingInfo />}
    </SettingsCard>
  ));
}

export function UsenetSettingsPage() {
  const query = useUsenetSettings();
  const { mutateAsync, isPending } = useSaveUsenetSettings();
  const methodsRef = React.useRef<UseFormReturn<any> | null>(null);
  const search = useSearch({ from: '/dashboard/usenet/settings' });
  const navigate = useNavigate({ from: '/dashboard/usenet/settings' });

  const clearField = React.useCallback(() => {
    navigate({
      to: '.',
      search: (prev) => ({ ...prev, field: undefined }),
      replace: true,
      resetScroll: false,
    });
  }, [navigate]);

  // Must run before the early-return guards below, to satisfy the rules of
  // hooks. The fields only exist once the settings payload has arrived.
  useScrollToField(search.field, Boolean(query.data), clearField);

  const keys = query.data?.keys ?? [];
  const profiles = query.data?.profiles ?? {};

  const { schema, defaults, byName } = React.useMemo(() => {
    const shape: Record<string, z.ZodTypeAny> = {};
    const defaults: Record<string, unknown> = {};
    const byName = new Map<string, SettingsKey>();
    for (const k of keys) {
      const n = toName(k.key);
      shape[n] = z.any();
      defaults[n] = k.value === null && k.ui.kind === 'enum' ? '' : k.value;
      byName.set(n, k);
    }
    // When a non-custom profile is active, the engine derives the bundled values
    // from it — so seed the form with the PROFILE's values (not the stored
    // shadows) so the page shows what's actually in effect, without dirtying.
    const activeProfile = keys.find((k) => leafOf(k.key) === PROFILE_LEAF)
      ?.value as string | undefined;
    const preset =
      activeProfile && activeProfile !== 'custom'
        ? profiles[activeProfile]
        : undefined;
    if (preset) {
      for (const leaf of BUNDLED_LEAVES) {
        const n = toName(usenetKey(leaf));
        if (n in defaults) defaults[n] = preset[leaf];
      }
    }
    return { schema: z.object(shape), defaults, byName };
  }, [keys, profiles]);

  if (query.isLoading) return <DashboardLoading />;
  if (query.isError) {
    return (
      <Card className="p-6 text-sm text-red-500">
        Failed to load usenet settings.
      </Card>
    );
  }

  const bundledNames = new Set(BUNDLED_LEAVES.map((l) => toName(usenetKey(l))));
  const profileNameKey = toName(usenetKey(PROFILE_LEAF));

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-2">
        <SettingsPageHeader
          title="Settings"
          description="Configuration for the built-in usenet engine"
          icon={BiCog}
        />
        <div className="pt-1">
          <SettingsActionsMenu
            sectionKeys={keys}
            sectionLabel="Usenet"
            invalidate={[USENET_SETTINGS_QUERY_KEY]}
            scope={USENET_SCOPE}
          />
        </div>
      </div>
      <Form
        // Re-key on the loaded values so a refetch after save re-seeds defaults.
        key={keys.map((k) => `${k.key}:${String(k.value)}`).join('|')}
        schema={schema}
        defaultValues={defaults}
        stackClass="space-y-4 relative"
        onSubmit={async (data: Record<string, unknown>) => {
          const profileActive = data[profileNameKey] !== 'custom';
          const patch: Record<string, unknown> = {};
          for (const [n, val] of Object.entries(data)) {
            const k = byName.get(n);
            if (!k || k.source === 'environment') continue;
            if (profileActive && bundledNames.has(n)) continue;
            const isNullable = k.value === null || k.default === null;
            const normalised = isNullable && val === '' ? null : val;
            if (JSON.stringify(normalised) !== JSON.stringify(k.value)) {
              patch[k.key] = normalised;
            }
          }
          if (Object.keys(patch).length === 0) {
            toast.info('No changes to save.');
            methodsRef.current?.reset(data, { keepValues: true });
            return;
          }
          try {
            const res = await mutateAsync(patch);
            toast.success(
              `Saved ${res.updated.length} setting${res.updated.length === 1 ? '' : 's'}.`
            );
            methodsRef.current?.reset(data, { keepValues: true });
            if (res.requiresRestart)
              toast.warning('Some changes require a restart to take effect.', {
                duration: 8000,
              });
          } catch (e: any) {
            const issues = e?.issues as Record<string, string> | undefined;
            if (issues)
              for (const [key, msg] of Object.entries(issues))
                toast.error(`${key}: ${msg}`);
            else toast.error(e?.message ?? 'Failed to save settings');
          }
        }}
      >
        {(methods) => {
          methodsRef.current = methods;
          return (
            <>
              <ProfileLinker profiles={profiles} />
              <UsenetSettingsSections keys={keys} />
              <div className="flex justify-end">
                <SettingsSubmitButton isPending={isPending} />
              </div>
              <SettingsIsDirty isPending={isPending} />
            </>
          );
        }}
      </Form>
    </div>
  );
}
