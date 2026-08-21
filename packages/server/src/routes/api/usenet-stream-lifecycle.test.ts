import { describe, expect, it } from 'vitest';
import {
  UsenetStreamLifecycle,
  usenetStreamFailureLogFields,
} from './usenet-stream-lifecycle.js';

describe('UsenetStreamLifecycle', () => {
  it('classifies a genuine client close without an internal failure', () => {
    const lifecycle = new UsenetStreamLifecycle();
    lifecycle.advance('streaming');
    expect(lifecycle.recordResponseClose(false)).toBe(true);
    expect(lifecycle.termination).toBe('client_aborted');
    expect(lifecycle.clientAborted).toBe(true);
  });

  it('does not promote a cleanup error emitted after client-close ownership', () => {
    const lifecycle = new UsenetStreamLifecycle();
    lifecycle.advance('streaming');
    expect(lifecycle.recordResponseClose(false)).toBe(true);
    const cleanup = Object.assign(new Error('private close path'), {
      code: 'EIO',
    });
    expect(lifecycle.recordStreamError(cleanup, false)).toBe(false);
    expect(lifecycle.firstError).toBeUndefined();
    expect(lifecycle.termination).toBe('client_aborted');
  });

  it('keeps the first internal error when response close follows', () => {
    const lifecycle = new UsenetStreamLifecycle();
    lifecycle.advance('streaming');
    const internal = Object.assign(new Error('do not log this message'), {
      code: 'USENET_SPOOL_IO',
    });
    expect(lifecycle.recordStreamError(internal, false)).toBe(true);
    expect(lifecycle.recordResponseClose(false)).toBe(false);
    expect(lifecycle.firstError).toBe(internal);
    expect(lifecycle.termination).toBe('internal_error');
    expect(lifecycle.clientAborted).toBe(true);
  });

  it('does not mark normal EOF until source close settlement is recorded', () => {
    const lifecycle = new UsenetStreamLifecycle();
    lifecycle.advance('streaming');
    expect(lifecycle.recordResponseClose(true)).toBe(false);
    expect(lifecycle.termination).toBe('active');
    expect(lifecycle.stage).toBe('streaming');
    lifecycle.recordNormalEof();
    expect(lifecycle.termination).toBe('normal_eof');
    expect(lifecycle.stage).toBe('complete');
  });

  it('preserves the shutdown terminal state', () => {
    const lifecycle = new UsenetStreamLifecycle();
    const shutdown = Object.assign(new Error('shutdown'), {
      code: 'USENET_ENGINE_CLOSED',
    });
    lifecycle.recordStreamError(shutdown, true);
    expect(lifecycle.recordResponseClose(false)).toBe(false);
    expect(lifecycle.termination).toBe('shutdown');
    expect(lifecycle.clientAborted).toBe(true);
  });

  it('emits credential-free structured fields without error messages', () => {
    const lifecycle = new UsenetStreamLifecycle();
    lifecycle.advance('streaming');
    const failure = Object.assign(new Error('secret-message'), {
      code: 'USENET_SPOOL_IO',
    });
    lifecycle.recordStreamError(failure, false);
    const fields = usenetStreamFailureLogFields(
      new Error('outer-secret'),
      failure,
      lifecycle,
      true
    );
    expect(fields).toMatchObject({
      outerErrorName: 'Error',
      rootErrorName: 'Error',
      rootCode: 'USENET_SPOOL_IO',
      streamStage: 'streaming',
      streamTermination: 'internal_error',
      headersSent: true,
      clientAborted: false,
      responseClosedByInternalFailure: true,
    });
    expect(JSON.stringify(fields)).not.toContain('secret');
  });

  it('reports one bounded secondary cleanup code without exposing messages', () => {
    const lifecycle = new UsenetStreamLifecycle();
    lifecycle.advance('streaming');
    const primary = Object.assign(new Error('primary-secret'), {
      code: 'USENET_SPOOL_CAPACITY',
    });
    const cleanup = Object.assign(new Error('cleanup-secret-path'), {
      code: 'EIO',
    });
    lifecycle.recordStreamError(primary, false);
    const aggregate = new AggregateError([primary, cleanup], 'aggregate', {
      cause: primary,
    });
    const fields = usenetStreamFailureLogFields(
      aggregate,
      primary,
      lifecycle,
      true,
      [cleanup]
    );
    expect(fields).toMatchObject({
      outerErrorName: 'AggregateError',
      rootCode: 'USENET_SPOOL_CAPACITY',
      cleanupErrorName: 'Error',
      cleanupCode: 'EIO',
      cleanupErrorCount: 1,
    });
    expect(JSON.stringify(fields)).not.toContain('secret');
  });
});
