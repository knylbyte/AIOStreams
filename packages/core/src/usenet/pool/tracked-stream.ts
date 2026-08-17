import { Readable } from 'node:stream';
import { StatsAccumulator } from '../stats/accumulator.js';
import { SeekableStream } from './file-stream.js';
import { createLogger } from '../../logging/logger.js';

const logger = createLogger('usenet/tracked-stream');

/**
 * The engine force-closed a read stream
 */
export class UsenetStreamReapedError extends Error {
  readonly code = 'USENET_STREAM_REAPED';
}

/** Stable admission error published synchronously when an engine starts close. */
export class UsenetEngineClosedError extends Error {
  readonly code = 'USENET_ENGINE_CLOSED';

  constructor() {
    super('usenet engine closed');
  }
}

/**
 * Whether a reader error is an expected lifecycle terminal cause while the
 * engine is closing. This list is deliberately narrow: arbitrary errors from
 * an asynchronous `_destroy()` (for example EIO) remain cleanup failures.
 */
export function isExpectedReaderTermination(
  error: unknown,
  closeError: UsenetEngineClosedError
): boolean {
  if (error === closeError) return true;
  if (typeof error !== 'object' || error === null) return false;

  const code = 'code' in error ? error.code : undefined;
  const name = 'name' in error ? error.name : undefined;
  const message = 'message' in error ? error.message : undefined;
  return (
    code === closeError.code ||
    code === 'STREAM_STOPPED' ||
    code === 'USENET_STREAM_REAPED' ||
    code === 'ABORT_ERR' ||
    code === 'USENET_CLIENT_CLOSED' ||
    name === 'AbortError' ||
    message === 'client closed'
  );
}

/**
 * Destroy the current reader snapshot and settle only after every reader has
 * emitted `close`. Observation is installed before `destroy()`, so synchronous
 * and asynchronous `_destroy()` implementations share the same barrier.
 */
export async function destroyTrackedReaders(
  liveReaders: ReadonlyMap<number, Readable>,
  closeError: UsenetEngineClosedError
): Promise<unknown[]> {
  const readers = [...liveReaders.values()];
  const outcomes = await Promise.all(
    readers.map(
      (reader) =>
        new Promise<unknown[]>((resolve) => {
          const wasDestroyed = reader.destroyed;
          const wasClosed = reader.closed;
          if (wasClosed) {
            resolve([]);
            return;
          }
          const errors: unknown[] = [];
          const onError = (error: unknown): void => {
            if (!isExpectedReaderTermination(error, closeError)) {
              errors.push(error);
            }
          };
          const onClose = (): void => {
            reader.removeListener('error', onError);
            resolve(errors);
          };
          reader.on('error', onError);
          reader.once('close', onClose);
          if (wasDestroyed) return;
          try {
            reader.destroy(closeError);
          } catch (error) {
            reader.removeListener('error', onError);
            reader.removeListener('close', onClose);
            resolve([error]);
          }
        })
    )
  );
  return outcomes.flat();
}

/**
 * Destroy every tracked reader that has pushed no bytes for `thresholdMs`.
 * Cleanup then flows through the reader's own 'close' handler (the one
 * registered at open), so there is no second bookkeeping path. Returns the
 * number of readers reaped.
 */
export function reapIdleStreams(
  stats: StatsAccumulator,
  liveReaders: ReadonlyMap<number, Readable>,
  thresholdMs: number,
  now = Date.now()
): number {
  let reaped = 0;
  for (const idle of stats.idleStreams(thresholdMs, now)) {
    const reader = liveReaders.get(idle.id);
    if (!reader || reader.destroyed) continue;
    logger.warn(
      {
        id: idle.id,
        filename: idle.filename,
        nzbHash: idle.nzbHash,
        idleMs: idle.idleMs,
        bytesServed: idle.bytesServed,
      },
      'reaping idle usenet stream'
    );
    reader.destroy(
      new UsenetStreamReapedError(
        `stream idle for ${Math.round(idle.idleMs / 1000)}s`
      )
    );
    reaped++;
  }
  return reaped;
}

/**
 * Wrap a {@link SeekableStream} handed out by the engine so every read stream
 * opened on it is registered with the engine's {@link StatsAccumulator}: the
 * live "Streams" dashboard view and the active-stream gauge. Registration
 * lives at this single choke point so plain files and archive inner streams
 * are counted uniformly; the engine's internal per-volume streams (opened
 * directly, not through the public open methods) stay untracked.
 */
export function trackSeekableStream(
  stream: SeekableStream,
  stats: StatsAccumulator,
  nzbHash: string,
  liveReaders: Map<number, Readable> | undefined,
  assertOpen: () => void
): SeekableStream {
  return new TrackedSeekableStream(
    stream,
    stats,
    nzbHash,
    liveReaders,
    assertOpen
  );
}

class TrackedSeekableStream implements SeekableStream {
  constructor(
    private readonly inner: SeekableStream,
    private readonly stats: StatsAccumulator,
    private readonly nzbHash: string,
    private readonly liveReaders: Map<number, Readable> | undefined,
    private readonly assertOpen: () => void
  ) {}

  get filename(): string | undefined {
    return this.inner.filename;
  }

  size(): number {
    return this.inner.size();
  }

  private async awaitInner<T>(operation: Promise<T>): Promise<T> {
    try {
      const result = await operation;
      this.assertOpen();
      return result;
    } catch (error) {
      // Closing wins over transport/cache fallout caused by that same close,
      // giving every issued wrapper one stable terminal contract.
      this.assertOpen();
      throw error;
    }
  }

  async open(signal?: AbortSignal): Promise<void> {
    this.assertOpen();
    await this.awaitInner(this.inner.open(signal));
  }

  async readAt(offset: number, length: number): Promise<Buffer> {
    this.assertOpen();
    return this.awaitInner(this.inner.readAt(offset, length));
  }

  async readAtInto(
    dst: Buffer,
    dstOffset: number,
    offset: number,
    length: number
  ): Promise<number> {
    this.assertOpen();
    if (this.inner.readAtInto) {
      return this.awaitInner(
        this.inner.readAtInto(dst, dstOffset, offset, length)
      );
    }
    const buffer = await this.awaitInner(this.inner.readAt(offset, length));
    buffer.copy(dst, dstOffset);
    return buffer.length;
  }

  createReadStream(range?: { start?: number; end?: number }): Readable {
    this.assertOpen();
    const out = this.inner.createReadStream(range);
    try {
      this.assertOpen();
    } catch (error) {
      out.destroy();
      throw error;
    }
    const id = this.stats.streamOpened({
      nzbHash: this.nzbHash,
      filename: this.inner.filename,
      size: this.inner.size(),
      start: Math.max(0, range?.start ?? 0),
    });
    // Count served bytes by intercepting push() rather than listening to
    // 'data', which would flip the stream into flowing mode before the real
    // consumer attaches and lose chunks.
    const push = out.push.bind(out);
    out.push = (chunk: unknown, encoding?: BufferEncoding): boolean => {
      const length = (chunk as { length?: number } | null)?.length;
      if (typeof length === 'number' && length > 0) {
        this.stats.streamBytes(id, length);
      }
      return push(chunk, encoding);
    };
    this.liveReaders?.set(id, out);
    // 'close' always follows end/destroy (autoDestroy default), so neither
    // the gauge nor the live-reader registry can leak an open entry as long
    // as the reader is eventually destroyed; the engine's idle reaper is the
    // backstop for readers whose response socket never closes.
    out.once('close', () => {
      this.liveReaders?.delete(id);
      this.stats.streamClosed(id);
    });
    return out;
  }
}
