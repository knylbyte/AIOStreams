import assert from 'node:assert/strict';
import test from 'node:test';
import { DebridError } from '../../debrid/base.js';
import { UsenetSpoolError } from '../spool/errors.js';
import { ArticleNotFoundError, NntpError } from '../nntp/errors.js';
import { YencDecodeError, YencMetadataError } from '../pool/yenc.js';
import { PrioritySemaphoreError } from '../pool/priority-semaphore.js';
import { UsenetEngineClosedError } from '../pool/tracked-stream.js';
import {
  describeUsenetError,
  downloadAdmissionCapacityCode,
  friendlyUsenetError,
  isDownloadAdmissionCapacityError,
  isExpectedUsenetClientAbort,
  toDebridError,
  toPublicUsenetStreamError,
} from './errors.js';

test('maps spool disk exhaustion to a clear stable user error', () => {
  const error = new UsenetSpoolError(
    'USENET_SPOOL_DISK_FULL',
    'low-level operation failed'
  );
  assert.deepEqual(friendlyUsenetError(error), {
    reason: 'The transient Usenet spool disk has insufficient free space.',
    code: 'USENET_SPOOL_DISK_FULL',
  });
  const mapped = toDebridError(error);
  assert.equal(mapped.statusCode, 507);
  assert.equal(mapped.code, 'STORE_LIMIT_EXCEEDED');
  assert.deepEqual(mapped.body, {
    usenetCode: 'USENET_SPOOL_DISK_FULL',
  });
});

test('maps transient resource admission errors to service unavailable', () => {
  for (const code of [
    'USENET_SPOOL_UNAVAILABLE',
    'USENET_SPOOL_CAPACITY',
    'USENET_SPOOL_OPEN_FILE_LIMIT',
    'USENET_MEMORY_BUDGET',
  ] as const) {
    const mapped = toDebridError(new UsenetSpoolError(code, 'internal'));
    assert.equal(mapped.statusCode, 503);
    assert.equal(mapped.code, 'SERVICE_UNAVAILABLE');
    assert.deepEqual(mapped.body, { usenetCode: code });
  }
});

test('maps typed download-admission capacity to a stable 503 without erasing its code', () => {
  for (const code of [
    'SEMAPHORE_GLOBAL_CAPACITY',
    'SEMAPHORE_OWNER_CAPACITY',
    'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
  ] as const) {
    const error = new PrioritySemaphoreError(code, 'internal capacity');
    assert.equal(isDownloadAdmissionCapacityError(error), true);
    assert.deepEqual(friendlyUsenetError(error), {
      reason:
        'The Usenet download scheduler is at capacity. Retry after active streams release their slots.',
      code,
    });
    const mapped = toDebridError(error);
    assert.equal(mapped.statusCode, 503);
    assert.equal(mapped.code, 'SERVICE_UNAVAILABLE');
    assert.deepEqual(mapped.body, { usenetCode: code });
    assert.equal(mapped.cause, error);
    assert.equal(downloadAdmissionCapacityCode(mapped), code);

    const lazilyWrapped = new DebridError('internal wrapper', {
      statusCode: 502,
      statusText: 'Bad Gateway',
      code: 'DOWNLOAD_FAILED',
      headers: {},
      body: null,
      type: 'upstream_error',
      cause: error,
    });
    const normalized = toDebridError(lazilyWrapped);
    assert.equal(normalized.statusCode, 503);
    assert.equal(normalized.code, 'SERVICE_UNAVAILABLE');
    assert.deepEqual(normalized.body, { usenetCode: code });
    assert.equal(normalized.cause, lazilyWrapped);
    assert.deepEqual(describeUsenetError(lazilyWrapped), {
      rootErrorName: 'PrioritySemaphoreError',
      rootCode: code,
      nntpKind: undefined,
      timeoutSource: undefined,
      faultDomain: 'local',
      providerLabel: undefined,
      connId: undefined,
      localBackpressureMs: undefined,
      carryBytes: undefined,
      carryChunks: undefined,
      carryLimitBytes: undefined,
    });
  }

  for (const code of [
    'SEMAPHORE_INVALID_OWNER',
    'SEMAPHORE_INVALID_PRIORITY',
    'SEMAPHORE_CLOSED',
    'SEMAPHORE_ABORTED',
  ] as const) {
    assert.equal(
      isDownloadAdmissionCapacityError(
        new PrioritySemaphoreError(code, 'not capacity')
      ),
      false
    );
    assert.equal(
      downloadAdmissionCapacityCode(
        new PrioritySemaphoreError(code, 'not capacity')
      ),
      undefined
    );
  }
});

test('maps the engine admission fence to service unavailable', async () => {
  const { UsenetEngineClosedError } = await import('../pool/tracked-stream.js');
  const error = new UsenetEngineClosedError();
  assert.equal(friendlyUsenetError(error).code, 'USENET_ENGINE_CLOSED');
  const mapped = toDebridError(error);
  assert.equal(mapped.statusCode, 503);
  assert.equal(mapped.code, 'SERVICE_UNAVAILABLE');
  assert.deepEqual(mapped.body, { usenetCode: 'USENET_ENGINE_CLOSED' });
});

test('maps segment metadata corruption to a download failure', () => {
  const error = new UsenetSpoolError(
    'USENET_SPOOL_METADATA_MISMATCH',
    'internal'
  );
  const mapped = toDebridError(error);
  assert.equal(mapped.statusCode, 502);
  assert.equal(mapped.code, 'DOWNLOAD_FAILED');
  assert.match(friendlyUsenetError(error).reason, /file range/);
});

test('maps decode, seek-metadata and local-backpressure failures distinctly', () => {
  const errors = [
    {
      error: new YencDecodeError('invalid_header', 'internal'),
      code: 'USENET_STREAMING_DECODE',
    },
    {
      error: new YencMetadataError('inconsistent_layout', 'internal'),
      code: 'USENET_STREAMING_METADATA',
    },
    {
      error: new NntpError('timeout', 'internal', {
        timeoutSource: 'local_backpressure',
      }),
      code: 'USENET_STREAMING_BACKPRESSURE_TIMEOUT',
    },
    {
      error: new NntpError('local_backpressure', 'internal'),
      code: 'USENET_STREAMING_LOCAL_BACKPRESSURE',
    },
  ];
  for (const item of errors) {
    assert.equal(friendlyUsenetError(item.error).code, item.code);
    const mapped = toDebridError(item.error);
    assert.equal(mapped.statusCode, 502);
    assert.deepEqual(mapped.body, { usenetCode: item.code });
  }
});

test('extracts safe root-cause fields through the public error wrapper', () => {
  const root = new NntpError('local_backpressure', 'sensitive internal text', {
    provider: 'provider-label',
    connId: 17,
    carryBytes: 786_432,
    carryChunks: 3,
    carryLimitBytes: 1_048_576,
    localBackpressureMs: 42,
  });
  const wrapped = toDebridError(root);
  assert.deepEqual(describeUsenetError(wrapped), {
    rootErrorName: 'NntpError',
    rootCode: undefined,
    nntpKind: 'local_backpressure',
    timeoutSource: undefined,
    faultDomain: 'local',
    providerLabel: 'provider-label',
    connId: 17,
    localBackpressureMs: 42,
    carryBytes: 786_432,
    carryChunks: 3,
    carryLimitBytes: 1_048_576,
  });
});

test('maps only established public stream errors and preserves unknown 500 semantics', () => {
  const known: ReadonlyArray<{
    readonly error: Error;
    readonly status: number;
  }> = [
    {
      error: new ArticleNotFoundError('private article identifier'),
      status: 404,
    },
    {
      error: new UsenetSpoolError('USENET_SPOOL_DISK_FULL', 'private path'),
      status: 507,
    },
    {
      error: new UsenetSpoolError('USENET_SPOOL_CAPACITY', 'private detail'),
      status: 503,
    },
    {
      error: new UsenetSpoolError('USENET_SPOOL_IO', 'private path'),
      status: 502,
    },
    { error: new UsenetEngineClosedError(), status: 503 },
    {
      error: new YencDecodeError('invalid_header', 'private article data'),
      status: 502,
    },
    {
      error: new YencMetadataError('invalid_header', 'private metadata'),
      status: 502,
    },
    {
      error: new NntpError('local_backpressure', 'private provider detail'),
      status: 502,
    },
    {
      error: new PrioritySemaphoreError(
        'SEMAPHORE_ACTIVE_OWNER_CAPACITY',
        'private owner key'
      ),
      status: 503,
    },
  ];

  for (const { error, status } of known) {
    const mapped = toPublicUsenetStreamError(error);
    assert.ok(mapped);
    assert.equal(mapped.statusCode, status);
    assert.equal(mapped.cause, error);
  }

  const existing = new DebridError('safe public failure', {
    statusCode: 418,
    statusText: "I'm a Teapot",
    code: 'UNKNOWN',
    headers: {},
    body: null,
    type: 'api_error',
  });
  assert.equal(toPublicUsenetStreamError(existing), existing);
  assert.equal(
    toPublicUsenetStreamError(new Error('internal invariant and private path')),
    undefined
  );
  assert.equal(
    toPublicUsenetStreamError(new NntpError('protocol', 'private command')),
    undefined
  );
});

test('recognises typed client aborts without inspecting their messages', () => {
  const typed = new NntpError('connection', 'message text is irrelevant', {
    faultDomain: 'client',
  });
  assert.equal(isExpectedUsenetClientAbort(typed), true);
  assert.equal(
    isExpectedUsenetClientAbort(
      new NntpError('connection', 'aborted', { faultDomain: 'provider' })
    ),
    false
  );
  assert.equal(
    isExpectedUsenetClientAbort(
      Object.assign(new Error('private detail'), { code: 'ABORT_ERR' })
    ),
    true
  );
});

test('classifies aggregate client-abort graphs boundedly and fail-closed', () => {
  const premature = Object.assign(new Error('private response state'), {
    code: 'USENET_STREAM_PREMATURE_CLOSE',
  });
  const nntpAbort = new NntpError('connection', 'aborted');
  const spoolAbort = new UsenetSpoolError(
    'USENET_SPOOL_ABORTED',
    'private spool state'
  );
  assert.equal(
    isExpectedUsenetClientAbort(
      new AggregateError([premature, nntpAbort, spoolAbort], 'private', {
        cause: premature,
      })
    ),
    true
  );

  const cleanup = Object.assign(new Error('private cleanup path'), {
    code: 'EIO',
  });
  assert.equal(
    isExpectedUsenetClientAbort(
      new AggregateError([premature, nntpAbort, cleanup], 'private', {
        cause: premature,
      })
    ),
    false
  );

  const cyclic = new Error('private cycle');
  cyclic.cause = cyclic;
  assert.equal(isExpectedUsenetClientAbort(cyclic), false);

  const oversized = new AggregateError(
    Array.from({ length: 9 }, () => new NntpError('connection', 'aborted')),
    'private oversized aggregate'
  );
  assert.equal(isExpectedUsenetClientAbort(oversized), false);
});
