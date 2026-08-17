import { DebridError } from '../../debrid/base.js';
import {
  ArticleNotFoundError,
  NntpError,
  NotStreamableError,
  type ArchiveErrorCode,
  type NzbContent,
} from '../index.js';
import {
  UsenetSpoolError,
  type UsenetSpoolErrorCode,
} from '../spool/errors.js';
import { YencDecodeError, YencMetadataError } from '../pool/yenc.js';

const ARCHIVE_REASONS: Record<ArchiveErrorCode, string> = {
  archive_compressed: 'Archive is compressed: not streamable',
  archive_encrypted: 'Archive is encrypted: not streamable',
  archive_bad_password: 'Archive password is incorrect',
  archive_solid: 'Archive is solid: not streamable',
  archive_nested: 'Nested archive (disabled)',
  archive_unsupported: 'Archive type not supported',
  archive_no_video: 'No streamable video found in archive',
  archive_disabled: 'Archived results are disabled',
  archive_incomplete: 'Archive volumes missing or unreadable: not streamable',
};

const SPOOL_REASONS: Record<UsenetSpoolErrorCode, string> = {
  USENET_SPOOL_UNAVAILABLE:
    'The transient Usenet spool is unavailable. Check its disk path and permissions.',
  USENET_SPOOL_CAPACITY:
    'The transient Usenet spool is at capacity. Reduce concurrent streams or increase its budget.',
  USENET_SPOOL_DISK_FULL:
    'The transient Usenet spool disk has insufficient free space.',
  USENET_SPOOL_IO: 'The transient Usenet spool encountered a disk I/O error.',
  USENET_SPOOL_NOT_FOUND:
    'Transient Usenet spool data disappeared before playback completed.',
  USENET_SPOOL_OPEN_FILE_LIMIT:
    'The transient Usenet spool reached its open-file limit.',
  USENET_SPOOL_CLOSED:
    'The Usenet streaming engine closed while the request was active.',
  USENET_SPOOL_ABORTED: 'The Usenet streaming request was cancelled.',
  USENET_SPOOL_INVALID_ARGUMENT:
    'The Usenet streaming resource configuration is invalid.',
  USENET_SPOOL_METADATA_MISMATCH:
    'The Usenet segment metadata does not match the requested file range.',
  USENET_MEMORY_BUDGET:
    'The transient Usenet memory budget cannot admit this stream.',
};

/**
 * Classify why an inspected NZB yielded no streamable files. Missing articles
 * (incomplete or removed on every provider) get a dedicated code + message so
 * the dashboard can distinguish "gone from usenet" from "present but not
 * streamable" (encrypted/compressed/solid archives, or simply no video).
 */
export function classifyNoStreamable(content: NzbContent): {
  reason: string;
  code: string;
} {
  const total = content.files.length;
  const missing = content.files.filter(
    (f) => f.error === 'article_not_found'
  ).length;
  if (missing > 0) {
    return {
      reason:
        missing >= total
          ? 'Missing on all providers: incomplete or removed'
          : `Missing on providers: ${missing}/${total} files unavailable (incomplete or removed)`,
      code: 'missing_on_providers',
    };
  }
  // Articles present but not decodable (broken yEnc part headers, uuencode-era
  // posts): name the real problem instead of "no streamable files".
  const decodeFailed = content.files.filter(
    (f) => f.error === 'decode_failed'
  ).length;
  if (decodeFailed > 0 && decodeFailed * 2 >= total) {
    return {
      reason:
        'Articles are malformed or not yEnc encoded: encoding not supported',
      code: 'unsupported_encoding',
    };
  }
  // The archive parsed but its only candidates have fragment-map gaps
  // (truncated post / unreadable volumes): say that, not "no files".
  const incomplete = content.files.some((f) =>
    f.archiveInner?.some((i) => i.reason === 'archive_incomplete')
  );
  if (incomplete) {
    return {
      reason: 'Archive incomplete: volumes missing from the post',
      code: 'incomplete_archive',
    };
  }
  // Encrypted RAR5/7z: a supplied password that didn't match ranks above a
  // missing password (more actionable for the user).
  const hasInnerReason = (reason: ArchiveErrorCode): boolean =>
    content.files.some((f) => f.archiveInner?.some((i) => i.reason === reason));
  if (hasInnerReason('archive_bad_password')) {
    return {
      reason: 'Archive password is incorrect',
      code: 'bad_password',
    };
  }
  if (hasInnerReason('archive_encrypted')) {
    return {
      reason: 'Archive is encrypted: password required',
      code: 'archive_encrypted',
    };
  }
  // Archive parsed cleanly but its contents can't be byte-range streamed
  // (compressed/solid volumes, an unsupported archive type, or no video
  // inside).
  const structural: ArchiveErrorCode[] = [
    'archive_compressed',
    'archive_solid',
    'archive_unsupported',
    'archive_no_video',
  ];
  for (const reason of structural) {
    if (hasInnerReason(reason))
      return { reason: ARCHIVE_REASONS[reason], code: reason };
  }
  // Archives exist but none was opened, so there is no inner listing to
  // classify: their volumes never grouped into a set.
  const archives = content.files.filter((f) => f.category === 'archive');
  if (archives.length > 0 && !content.files.some((f) => f.archiveInner)) {
    return {
      reason: 'Archive volumes could not be ordered into a set',
      code: 'archive_ungrouped',
    };
  }
  return { reason: 'No streamable files in NZB', code: 'no_streamable_files' };
}

/**
 * If import-time availability sampling found a sampled segment missing on every
 * provider, the chosen video would die mid-playback; surface it as a
 * definitive `missing_on_providers` failure rather than letting playback start
 * and stall.
 */
export function classifyAvailability(
  content: NzbContent
): { reason: string; code: string } | undefined {
  const a = content.availability;
  if (!a || a.missing <= 0) return undefined;
  return {
    reason: `Missing on providers: ${a.missing}/${a.sampled} sampled segments unavailable (incomplete or removed)`,
    code: 'missing_on_providers',
  };
}

/** Map an engine error onto a user-friendly reason + machine code. */
export function friendlyUsenetError(err: unknown): {
  reason: string;
  code: string;
} {
  if (err instanceof NotStreamableError) {
    return { reason: ARCHIVE_REASONS[err.code] ?? err.message, code: err.code };
  }
  if (err instanceof ArticleNotFoundError) {
    return {
      reason: 'Missing on all providers (incomplete or removed)',
      code: 'article_not_found',
    };
  }
  if (err instanceof UsenetSpoolError) {
    return { reason: SPOOL_REASONS[err.code], code: err.code };
  }
  if (err instanceof YencDecodeError) {
    return {
      reason: 'The Usenet article is malformed or not valid yEnc data.',
      code: 'USENET_STREAMING_DECODE',
    };
  }
  if (err instanceof YencMetadataError) {
    return {
      reason: 'The Usenet article does not contain trustworthy seek metadata.',
      code: 'USENET_STREAMING_METADATA',
    };
  }
  if (
    err instanceof NntpError &&
    err.kind === 'timeout' &&
    err.timeoutSource === 'local_backpressure'
  ) {
    return {
      reason:
        'The Usenet segment exceeded its total time limit while local disk or player backpressure was active.',
      code: 'USENET_STREAMING_BACKPRESSURE_TIMEOUT',
    };
  }
  return {
    reason: err instanceof Error ? err.message : 'Inspection failed',
    code: 'inspect_failed',
  };
}

/** Map an engine/transport error onto a {@link DebridError}. */
export function toDebridError(err: unknown): DebridError {
  if (err instanceof DebridError) return err;
  if (err instanceof ArticleNotFoundError) {
    return new DebridError('article not found on any provider', {
      statusCode: 404,
      statusText: 'Not Found',
      code: 'DOWNLOAD_FAILED',
      headers: {},
      body: null,
      type: 'upstream_error',
      cause: err,
    });
  }
  if (err instanceof UsenetSpoolError) {
    const diskFull = err.code === 'USENET_SPOOL_DISK_FULL';
    const resourceUnavailable =
      err.code === 'USENET_SPOOL_UNAVAILABLE' ||
      err.code === 'USENET_SPOOL_CAPACITY' ||
      err.code === 'USENET_SPOOL_OPEN_FILE_LIMIT' ||
      err.code === 'USENET_MEMORY_BUDGET' ||
      err.code === 'USENET_SPOOL_CLOSED';
    return new DebridError(SPOOL_REASONS[err.code], {
      statusCode: diskFull ? 507 : resourceUnavailable ? 503 : 502,
      statusText: diskFull
        ? 'Insufficient Storage'
        : resourceUnavailable
          ? 'Service Unavailable'
          : 'Bad Gateway',
      code: diskFull
        ? 'STORE_LIMIT_EXCEEDED'
        : resourceUnavailable
          ? 'SERVICE_UNAVAILABLE'
          : 'DOWNLOAD_FAILED',
      headers: {},
      body: { usenetCode: err.code },
      type: 'upstream_error',
      cause: err,
    });
  }
  if (
    err instanceof YencDecodeError ||
    err instanceof YencMetadataError ||
    (err instanceof NntpError &&
      err.kind === 'timeout' &&
      err.timeoutSource === 'local_backpressure')
  ) {
    const friendly = friendlyUsenetError(err);
    return new DebridError(friendly.reason, {
      statusCode: 502,
      statusText: 'Bad Gateway',
      code: 'DOWNLOAD_FAILED',
      headers: {},
      body: { usenetCode: friendly.code },
      type: 'upstream_error',
      cause: err,
    });
  }
  return new DebridError(
    err instanceof Error ? err.message : 'usenet inspection failed',
    {
      statusCode: 502,
      statusText: 'Bad Gateway',
      code: 'BAD_GATEWAY',
      headers: {},
      body: null,
      type: 'upstream_error',
      cause: err,
    }
  );
}
