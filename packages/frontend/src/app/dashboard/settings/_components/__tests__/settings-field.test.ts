import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { FormProvider, useForm } from 'react-hook-form';
import { describe, expect, it } from 'vitest';
import type { SettingsKey } from '../../queries';
import { enumOptionsFor, SettingsField, toName } from '../settings-field';

const ENV_NAME = 'USENET_SEGMENT_SPOOLING_SPOOL_BYTES';

function sizeSetting(source: SettingsKey['source']): SettingsKey {
  return {
    key: 'usenet.segmentSpoolingSpoolBytes',
    label: 'Segment spooling spool size',
    description: 'Transient spool budget.',
    env: ENV_NAME,
    requiresRestart: true,
    secret: false,
    valueType: 'number',
    default: 2_000_000_000,
    source,
    value: 2_000_000_000,
    secretSet: false,
    ui: { kind: 'size' },
  };
}

function renderSetting(setting: SettingsKey): string {
  function Harness() {
    const methods = useForm({
      defaultValues: { [toName(setting.key)]: setting.value },
    });
    return React.createElement(FormProvider, {
      ...methods,
      children: React.createElement(SettingsField, { k: setting }),
    });
  }

  return renderToStaticMarkup(React.createElement(Harness));
}

describe('SettingsField size ENV lock', () => {
  it('shows the concrete ENV lock and disables an environment-sourced size field', () => {
    const markup = renderSetting(sizeSetting('environment'));

    expect(markup).toContain(
      `aria-label="Set by environment variable: ${ENV_NAME}"`
    );
    expect(markup).toContain(ENV_NAME);
    expect(markup).toMatch(/<input[^>]* disabled=""/);
  });

  it('keeps a database-sourced size field editable without a lock hint', () => {
    const markup = renderSetting(sizeSetting('database'));

    expect(markup).not.toContain('Set by environment variable:');
    expect(markup).not.toContain(' disabled=""');
  });
});

describe('SettingsField enum labels', () => {
  it('keeps stable values while displaying schema-provided labels', () => {
    expect(
      enumOptionsFor({
        options: ['segment_buffering', 'segment_spooling'],
        optionLabels: {
          segment_buffering: 'Segment Buffering',
          segment_spooling: 'Segment Spooling',
        },
      })
    ).toEqual([
      { label: 'Segment Buffering', value: 'segment_buffering' },
      { label: 'Segment Spooling', value: 'segment_spooling' },
    ]);
  });

  it('falls back to the stable value when no display label is defined', () => {
    expect(enumOptionsFor({ options: ['custom'] })).toEqual([
      { label: 'custom', value: 'custom' },
    ]);
  });
});
