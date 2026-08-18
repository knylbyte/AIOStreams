import { NextFunction, Request, Response, Router } from 'express';
import { pipeline } from 'stream/promises';
import {
  createLogger,
  openNativeUsenetStream,
  DebridError,
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
        stream.destroy();
        lifecycle.recordNormalEof();
        res.status(304).end();
        return;
      }

      // Unsatisfiable range.
      if (requested && requested.start >= size) {
        res.removeListener('close', onClose);
        stream.destroy();
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
        stream.destroy();
        lifecycle.recordNormalEof();
        res.end();
        return;
      }

      lifecycle.advance('streaming');
      // Capture the first producer failure before pipeline tears down the
      // response and emits `close`; that later close cannot reclassify it as a
      // client abort. A clean shutdown FIN is invisible to a buffered player.
      stream.once('error', (err: NodeJS.ErrnoException) => {
        lifecycle.recordStreamError(err, isStreamShutdownError(err));
        if (!controller.signal.aborted) controller.abort(err);
        if (
          (err?.code === 'USENET_STREAM_REAPED' ||
            err?.code === 'STREAM_STOPPED' ||
            err?.code === 'USENET_ENGINE_CLOSED') &&
          !socket.destroyed
        ) {
          socket.resetAndDestroy();
        }
      });

      await pipeline(stream, res);
      lifecycle.recordNormalEof();
    } catch (err) {
      if (opened && !opened.stream.destroyed) opened.stream.destroy();

      const effectiveError = lifecycle.firstError ?? err;
      const code = safeErrorCode(effectiveError);
      if (
        isStreamShutdownError(effectiveError) &&
        !lifecycle.clientAborted &&
        !res.headersSent &&
        !res.destroyed
      ) {
        logger.info({ code }, 'usenet stream stopped before response startup');
        sendStreamShutdownResponse(res);
        return;
      }
      if (isStreamShutdownError(effectiveError)) {
        logger.debug(
          usenetStreamFailureLogFields(
            err,
            effectiveError,
            lifecycle,
            res.headersSent
          ),
          'usenet stream stopped during response shutdown'
        );
        return;
      }
      const isClientDisconnect =
        lifecycle.clientAborted ||
        (!lifecycle.firstError &&
          (code === 'ERR_STREAM_PREMATURE_CLOSE' ||
            code === 'ECONNRESET' ||
            code === 'EPIPE' ||
            code === 'ERR_STREAM_DESTROYED' ||
            code === 'ABORT_ERR'));

      if (isClientDisconnect) {
        logger.debug({ code }, 'client disconnected from usenet stream');
        return;
      }

      if (res.headersSent) {
        logger.warn(
          usenetStreamFailureLogFields(
            err,
            effectiveError,
            lifecycle,
            res.headersSent
          ),
          'usenet stream failed after headers sent'
        );
        res.destroy();
        return;
      }

      if (effectiveError instanceof DebridError) {
        logger.warn(
          {
            ...usenetStreamFailureLogFields(
              err,
              effectiveError,
              lifecycle,
              res.headersSent
            ),
            code: effectiveError.code,
            status: effectiveError.statusCode,
          },
          'usenet stream failed before any bytes were sent'
        );
        if (req.query.download !== undefined) {
          res.status(effectiveError.statusCode || 502).json({
            success: false,
            detail: effectiveError.message,
          });
        } else {
          res.redirect(
            302,
            `/static/${mapDebridErrorToStaticFile(effectiveError.code)}`
          );
        }
        return;
      }
      next(effectiveError);
    } finally {
      res.removeListener('close', onClose);
    }
  }
);

export default router;
