import { NextFunction, Request, Response, Router } from 'express';
import type { Readable, Writable } from 'node:stream';
import {
  APIError,
  constants,
  createLogger,
  downloadAdmissionCapacityCode,
  openNativeUsenetStream,
  DebridError,
  toPublicUsenetStreamError,
} from '@aiostreams/core';
import { mapDebridErrorToStaticFile } from '../../app.js';
import { corsMiddleware } from '../../middlewares/cors.js';
import {
  isStreamShutdownError,
  sendStreamShutdownResponse,
} from './stream-shutdown.js';
import {
  safeErrorCode,
  UsenetStreamLifecycle,
  usenetStreamFailureLogFields,
} from './usenet-stream-lifecycle.js';

const logger = createLogger('server:usenet');
const router: Router = Router();

router.use(corsMiddleware);

const MIME_BY_EXT: Record<string, string> = {
  mkv: 'video/x-matroska',
  mp4: 'video/mp4',
  m4v: 'video/x-m4v',
  avi: 'video/x-msvideo',
  mov: 'video/quicktime',
  webm: 'video/webm',
  ts: 'video/mp2t',
  wmv: 'video/x-ms-wmv',
  flv: 'video/x-flv',
  mpg: 'video/mpeg',
  mpeg: 'video/mpeg',
};

function mimeForFilename(name: string): string {
  const ext = name.split('.').pop()?.toLowerCase() ?? '';
  return MIME_BY_EXT[ext] ?? 'application/octet-stream';
}

const STREAM_REPRESENTATION_HEADERS = [
  'Accept-Ranges',
  'Content-Disposition',
  'Content-Length',
  'Content-Range',
  'Content-Type',
  'ETag',
  'Last-Modified',
  'Location',
] as const;

function publicUsenetCode(error: DebridError): string | undefined {
  const body = error.body;
  if (typeof body !== 'object' || body === null || !('usenetCode' in body)) {
    return undefined;
  }
  return typeof body.usenetCode === 'string' ? body.usenetCode : undefined;
}

function sendTypedUsenetFailureResponse(
  res: Response,
  error: DebridError
): void {
  for (const header of STREAM_REPRESENTATION_HEADERS) {
    res.removeHeader(header);
  }
  const usenetCode = publicUsenetCode(error);
  res.status(error.statusCode || 502).json({
    success: false,
    detail: error.message,
    ...(usenetCode ? { usenetCode } : {}),
  });
}

function prematureStreamClose(): Error & { readonly code: string } {
  return Object.assign(new Error('Usenet stream closed before completion'), {
    code: 'USENET_STREAM_PREMATURE_CLOSE',
  });
}

const MAX_ROUTE_SETTLEMENT_ERRORS = 8;

interface ResponsePipeSettlementState {
  sourceEnded: boolean;
  sourceClosed: boolean;
  responseFinished: boolean;
  responseClosed: boolean;
  primaryError?: Error;
  readonly secondaryErrors: Error[];
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error('Usenet stream failed');
}

function recordSettlementError(
  state: ResponsePipeSettlementState,
  error: unknown
): void {
  const failure = asError(error);
  if (!state.primaryError) {
    state.primaryError = failure;
  } else if (
    failure !== state.primaryError &&
    state.secondaryErrors.length < MAX_ROUTE_SETTLEMENT_ERRORS - 1 &&
    !state.secondaryErrors.includes(failure)
  ) {
    state.secondaryErrors.push(failure);
  }
}

function settlementError(state: ResponsePipeSettlementState): Error {
  const primary = state.primaryError;
  if (!primary) return prematureStreamClose();
  if (state.secondaryErrors.length === 0) return primary;
  return new AggregateError(
    [primary, ...state.secondaryErrors],
    'Usenet stream failed and source cleanup also failed',
    { cause: primary }
  );
}

function combineSettlementErrors(primary: unknown, secondary: unknown): Error {
  const first = asError(primary);
  const second = asError(secondary);
  if (first === second) return first;
  return new AggregateError(
    [first, second],
    'Usenet stream failed and source cleanup also failed',
    { cause: first }
  );
}

interface UsenetRouteSettlementFailure {
  readonly outer: Error;
  readonly primary: Error;
  readonly cleanupErrors: readonly Error[];
}

function aggregatePrimary(error: Error): Error {
  let current = error;
  const seen = new Set<Error>();
  for (let depth = 0; depth < MAX_ROUTE_SETTLEMENT_ERRORS; depth++) {
    if (seen.has(current)) return error;
    seen.add(current);
    if (!(current instanceof AggregateError)) return current;

    if (current.cause instanceof Error && !seen.has(current.cause)) {
      current = current.cause;
      continue;
    }

    let firstError: Error | undefined;
    const inspected = Math.min(
      current.errors.length,
      MAX_ROUTE_SETTLEMENT_ERRORS - depth - 1
    );
    for (let index = 0; index < inspected; index++) {
      const candidate = current.errors[index];
      if (candidate instanceof Error && !seen.has(candidate)) {
        firstError = candidate;
        break;
      }
    }
    if (!firstError) return error;
    current = firstError;
  }
  return error;
}

function leadsToPrimary(error: Error, primary: Error): boolean {
  let current: Error | undefined = error;
  const seen = new Set<Error>();
  for (let depth = 0; current && depth < MAX_ROUTE_SETTLEMENT_ERRORS; depth++) {
    if (current === primary) return true;
    if (seen.has(current)) return false;
    seen.add(current);
    current = current.cause instanceof Error ? current.cause : undefined;
  }
  return false;
}

function cleanupErrorsFor(outer: Error, primary: Error): readonly Error[] {
  if (!(outer instanceof AggregateError)) return [];
  const cleanupErrors: Error[] = [];
  const seen = new Set<Error>();
  let inspected = 0;

  const visit = (error: Error, depth: number): void => {
    if (
      depth >= MAX_ROUTE_SETTLEMENT_ERRORS ||
      inspected >= MAX_ROUTE_SETTLEMENT_ERRORS ||
      seen.has(error)
    ) {
      return;
    }
    inspected++;
    seen.add(error);
    if (error instanceof AggregateError) {
      if (error.cause instanceof Error) visit(error.cause, depth + 1);
      const remaining = MAX_ROUTE_SETTLEMENT_ERRORS - inspected;
      const count = Math.min(error.errors.length, remaining);
      for (let index = 0; index < count; index++) {
        const candidate = error.errors[index];
        if (candidate instanceof Error) visit(candidate, depth + 1);
      }
      return;
    }
    if (
      !leadsToPrimary(error, primary) &&
      safeErrorCode(error) !== 'USENET_STREAM_PREMATURE_CLOSE'
    ) {
      cleanupErrors.push(error);
    }
  };

  visit(outer, 0);
  return cleanupErrors;
}

function routeSettlementFailure(
  caught: unknown,
  lifecyclePrimary: Error | undefined
): UsenetRouteSettlementFailure {
  const outer = asError(caught);
  const primary = lifecyclePrimary ?? aggregatePrimary(outer);
  return {
    outer,
    primary,
    cleanupErrors: cleanupErrorsFor(outer, primary),
  };
}

const EXPECTED_CLIENT_CLOSE_CODES = new Set([
  'USENET_STREAM_PREMATURE_CLOSE',
  'ERR_STREAM_PREMATURE_CLOSE',
  'ECONNRESET',
  'EPIPE',
  'ERR_STREAM_DESTROYED',
  'ABORT_ERR',
  'USENET_SPOOL_ABORTED',
]);

interface ExpectedCloseTraversal {
  remaining: number;
  readonly seen: Set<Error>;
}

function isExpectedClientCloseError(
  error: unknown,
  traversal: ExpectedCloseTraversal = {
    remaining: MAX_ROUTE_SETTLEMENT_ERRORS,
    seen: new Set<Error>(),
  }
): boolean {
  if (
    traversal.remaining <= 0 ||
    !(error instanceof Error) ||
    traversal.seen.has(error)
  ) {
    return false;
  }
  traversal.remaining--;
  traversal.seen.add(error);
  if (error instanceof AggregateError) {
    if (
      error.errors.length === 0 ||
      error.errors.length > traversal.remaining
    ) {
      return false;
    }
    for (const entry of error.errors) {
      if (!isExpectedClientCloseError(entry, traversal)) return false;
    }
    return true;
  }
  const code = safeErrorCode(error);
  if (
    (typeof code === 'string' && EXPECTED_CLIENT_CLOSE_CODES.has(code)) ||
    error.name === 'AbortError'
  ) {
    return true;
  }
  return error.cause instanceof Error
    ? isExpectedClientCloseError(error.cause, traversal)
    : false;
}

/**
 * Preserve ordinary Node pipe backpressure without letting a pre-byte source
 * failure destroy the HTTP response before it can be mapped to a public 503.
 * Post-header failures are destroyed deliberately by the route catch path.
 */
export function pipeToResponse(stream: Readable, res: Writable): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const state: ResponsePipeSettlementState = {
      sourceEnded: stream.readableEnded,
      sourceClosed: stream.closed,
      responseFinished: res.writableFinished,
      responseClosed: res.closed,
      secondaryErrors: [],
    };
    let settled = false;
    const cleanup = (): void => {
      stream.removeListener('end', onStreamEnd);
      stream.removeListener('error', onStreamError);
      stream.removeListener('close', onStreamClose);
      res.removeListener('error', onResponseError);
      res.removeListener('finish', onResponseFinish);
      res.removeListener('close', onResponseClose);
    };
    const settle = (error?: Error): void => {
      if (settled) return;
      settled = true;
      stream.unpipe(res);
      cleanup();
      if (error) reject(error);
      else resolve();
    };
    const finishIfSettled = (): void => {
      if (!state.sourceClosed) return;
      if (!state.sourceEnded && !state.primaryError) {
        recordSettlementError(state, prematureStreamClose());
      }
      if (state.primaryError) {
        settle(settlementError(state));
        return;
      }
      if (state.responseFinished) {
        settle();
        return;
      }
      if (state.responseClosed) {
        recordSettlementError(state, prematureStreamClose());
        settle(settlementError(state));
      }
    };
    const detachAndDestroySource = (): void => {
      stream.unpipe(res);
      if (stream.destroyed) return;
      try {
        stream.destroy();
      } catch (error) {
        recordSettlementError(state, error);
      }
    };
    const onStreamEnd = (): void => {
      state.sourceEnded = true;
      finishIfSettled();
    };
    const onStreamError = (error: Error): void => {
      recordSettlementError(state, error);
      detachAndDestroySource();
      finishIfSettled();
    };
    const onStreamClose = (): void => {
      state.sourceClosed = true;
      state.sourceEnded ||= stream.readableEnded;
      finishIfSettled();
    };
    const onResponseError = (error: Error): void => {
      recordSettlementError(state, error);
      detachAndDestroySource();
      finishIfSettled();
    };
    const onResponseFinish = (): void => {
      state.responseFinished = true;
      finishIfSettled();
    };
    const onResponseClose = (): void => {
      state.responseClosed = true;
      state.responseFinished ||= res.writableFinished;
      if (!state.responseFinished) {
        recordSettlementError(state, prematureStreamClose());
        detachAndDestroySource();
      }
      finishIfSettled();
    };

    stream.once('end', onStreamEnd);
    // Keep error ownership through the actual close: an async `_destroy()` may
    // emit a cleanup error after the primary producer error.
    stream.on('error', onStreamError);
    stream.once('close', onStreamClose);
    res.once('error', onResponseError);
    res.once('finish', onResponseFinish);
    res.once('close', onResponseClose);
    stream.pipe(res);
    finishIfSettled();
  });
}

/** Destroy one request-owned reader and observe its real async close outcome. */
export function destroySourceAndWait(stream: Readable): Promise<void> {
  if (stream.closed) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    const state: ResponsePipeSettlementState = {
      sourceEnded: stream.readableEnded,
      sourceClosed: false,
      responseFinished: false,
      responseClosed: false,
      secondaryErrors: [],
    };
    const onError = (error: Error): void => {
      recordSettlementError(state, error);
    };
    const onClose = (): void => {
      stream.removeListener('error', onError);
      state.sourceClosed = true;
      if (state.primaryError) reject(settlementError(state));
      else resolve();
    };
    stream.on('error', onError);
    stream.once('close', onClose);
    if (!stream.destroyed) {
      try {
        stream.destroy();
      } catch (error) {
        recordSettlementError(state, error);
      }
    }
  });
}

/**
 * Parse a single-range `Range` header. Returns `undefined` for no range or an
 * unsupported suffix range (`bytes=-N`), in which case the full file is served.
 * `endExclusive` is `undefined` for open-ended ranges (`bytes=START-`).
 */
function parseRange(
  header: string | undefined
): { start: number; endExclusive?: number } | undefined {
  if (!header) return undefined;
  const match = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!match) return undefined;
  const [, rawStart, rawEnd] = match;
  if (rawStart === '') return undefined; // suffix range: serve full
  const start = Number(rawStart);
  const endExclusive = rawEnd === '' ? undefined : Number(rawEnd) + 1;
  return { start, endExclusive };
}

interface UsenetStreamParams {
  token: string;
  filename?: string;
}

/**
 * Byte-serving endpoint for native usenet streams. The token is an encrypted
 * capability minted by `NativeUsenetService.resolve` (which already validated
 * the user's `aiostreamsAuth`), so no additional auth is required here. Serves
 * HTTP Range requests directly from the NNTP engine — never via the builtin
 * proxy.
 */
router.get(
  '/stream/:token{/:filename}',
  async (
    req: Request<UsenetStreamParams>,
    res: Response,
    next: NextFunction
  ) => {
    const { token } = req.params;
    const requested = parseRange(req.headers.range);
    const controller = new AbortController();
    const lifecycle = new UsenetStreamLifecycle();
    const onClose = () => {
      if (
        lifecycle.recordResponseClose(
          res.writableEnded || res.writableFinished
        ) &&
        !controller.signal.aborted
      ) {
        controller.abort(new Error('Usenet client disconnected'));
      }
    };
    res.on('close', onClose);
    const socket = req.socket;
    socket.setKeepAlive(true, 60_000);

    let opened: Awaited<ReturnType<typeof openNativeUsenetStream>> | undefined;
    try {
      opened = await openNativeUsenetStream({
        token,
        start: requested?.start,
        end: requested?.endExclusive,
        signal: controller.signal,
        clientIp: req.requestIp || req.ip || req.socket.remoteAddress,
      });

      const { size, start, end, stream, filename, etag, lastModified } = opened;
      lifecycle.advance('headers');

      // set appropriate cache headers
      res.setHeader('ETag', etag);
      res.setHeader('Last-Modified', lastModified.toUTCString());
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Accept-Ranges', 'bytes');

      // Conditional GET: a re-request of the unchanged file with a matching
      // If-None-Match is a cheap 304
      const ifNoneMatch = req.headers['if-none-match'];
      if (
        ifNoneMatch &&
        (ifNoneMatch === '*' ||
          ifNoneMatch.split(',').some((t) => t.trim() === etag))
      ) {
        res.removeListener('close', onClose);
        await destroySourceAndWait(stream);
        lifecycle.recordNormalEof();
        res.status(304).end();
        return;
      }

      // Unsatisfiable range.
      if (requested && requested.start >= size) {
        res.removeListener('close', onClose);
        await destroySourceAndWait(stream);
        lifecycle.recordNormalEof();
        res.status(416).set('Content-Range', `bytes */${size}`).end();
        return;
      }

      const disposition =
        req.query.download !== undefined ? 'attachment' : 'inline';
      res.setHeader('Content-Type', mimeForFilename(filename));
      res.setHeader(
        'Content-Disposition',
        `${disposition}; filename="${encodeURIComponent(filename)}"`
      );
      res.setHeader('Content-Length', String(end - start));

      if (requested) {
        res.status(206);
        res.setHeader('Content-Range', `bytes ${start}-${end - 1}/${size}`);
      } else {
        res.status(200);
      }

      logger.debug(
        { filename, size, start, end, range: req.headers.range ?? null },
        'serving native usenet stream'
      );

      if (req.method === 'HEAD') {
        await destroySourceAndWait(stream);
        lifecycle.recordNormalEof();
        res.end();
        return;
      }

      lifecycle.advance('streaming');
      // Capture the first producer failure before pipeline tears down the
      // response and emits `close`; that later close cannot reclassify it as a
      // client abort. A clean shutdown FIN is invisible to a buffered player.
      const onOwnedStreamError = (err: NodeJS.ErrnoException): void => {
        lifecycle.recordStreamError(err, isStreamShutdownError(err));
        if (!controller.signal.aborted) controller.abort(err);
        if (
          (err?.code === 'USENET_STREAM_REAPED' ||
            err?.code === 'STREAM_STOPPED' ||
            err?.code === 'USENET_ENGINE_CLOSED') &&
          res.headersSent &&
          !socket.destroyed
        ) {
          socket.resetAndDestroy();
        }
      };
      stream.once('error', onOwnedStreamError);

      try {
        await pipeToResponse(stream, res);
      } finally {
        stream.removeListener('error', onOwnedStreamError);
      }
      lifecycle.recordNormalEof();
    } catch (err) {
      let caughtError = err;
      if (opened && !opened.stream.closed) {
        try {
          await destroySourceAndWait(opened.stream);
        } catch (cleanupError) {
          caughtError = combineSettlementErrors(err, cleanupError);
        }
      }

      const failure = routeSettlementFailure(caughtError, lifecycle.firstError);
      const { outer, primary, cleanupErrors } = failure;
      const code = safeErrorCode(primary);
      const responseUnavailable =
        lifecycle.clientAborted ||
        res.destroyed ||
        res.closed ||
        socket.destroyed;
      if (responseUnavailable && !res.writableFinished) {
        lifecycle.recordResponseClose(false);
      }
      const logFields = usenetStreamFailureLogFields(
        outer,
        primary,
        lifecycle,
        res.headersSent,
        cleanupErrors
      );
      const shutdown = isStreamShutdownError(primary);

      if (shutdown && !responseUnavailable && !res.headersSent) {
        logger.info(logFields, 'usenet stream stopped before response startup');
        sendStreamShutdownResponse(res);
        return;
      }
      if (shutdown) {
        logger.debug(
          logFields,
          'usenet stream stopped during response shutdown'
        );
        return;
      }

      const expectedClientClose =
        isExpectedClientCloseError(primary) &&
        cleanupErrors.every((error) => isExpectedClientCloseError(error));
      if (expectedClientClose && responseUnavailable) {
        logger.debug({ code }, 'client disconnected from usenet stream');
        return;
      }

      if (responseUnavailable) {
        logger.warn(
          logFields,
          'usenet stream failed after response connection closed'
        );
        return;
      }

      if (res.headersSent) {
        logger.warn(logFields, 'usenet stream failed after headers sent');
        res.destroy();
        return;
      }

      const admissionCapacityCode = downloadAdmissionCapacityCode(primary);
      const publicError = toPublicUsenetStreamError(primary);
      if (publicError) {
        logger.warn(
          {
            ...logFields,
            code: publicError.code,
            status: publicError.statusCode,
          },
          'usenet stream failed before any bytes were sent'
        );
        if (!(primary instanceof DebridError) || admissionCapacityCode) {
          sendTypedUsenetFailureResponse(res, publicError);
        } else if (req.query.download !== undefined) {
          res.status(publicError.statusCode || 502).json({
            success: false,
            detail: publicError.message,
          });
        } else {
          res.redirect(
            302,
            `/static/${mapDebridErrorToStaticFile(publicError.code)}`
          );
        }
        return;
      }

      logger.warn(logFields, 'usenet stream failed before any bytes were sent');
      next(new APIError(constants.ErrorCode.INTERNAL_SERVER_ERROR));
    } finally {
      res.removeListener('close', onClose);
    }
  }
);

export default router;
