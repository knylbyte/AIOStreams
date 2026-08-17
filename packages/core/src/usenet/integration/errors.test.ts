import assert from 'node:assert/strict';
import test from 'node:test';
import { UsenetSpoolError } from '../spool/errors.js';
import { NntpError } from '../nntp/errors.js';
import { YencDecodeError, YencMetadataError } from '../pool/yenc.js';
import { friendlyUsenetError, toDebridError } from './errors.js';

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
  ];
  for (const item of errors) {
    assert.equal(friendlyUsenetError(item.error).code, item.code);
    const mapped = toDebridError(item.error);
    assert.equal(mapped.statusCode, 502);
    assert.deepEqual(mapped.body, { usenetCode: item.code });
  }
});
