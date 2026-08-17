import type { Server } from 'node:http';
import type { NextFunction, Request, Response } from 'express';

export interface NamedShutdownCleanup {
  readonly label: string;
  readonly run: () => Promise<unknown>;
}

export interface ShutdownCoordinatorOptions {
  readonly admission: ShutdownAdmissionGate;
  readonly server: () => Server | undefined;
  readonly stopTasks: () => void;
  readonly sealStreams: () => void;
  readonly beforeListenerClose: readonly NamedShutdownCleanup[];
  readonly afterListenerClose: readonly NamedShutdownCleanup[];
  readonly onCleanupError?: (label: string, error: unknown) => void;
}

/**
 * Synchronous process-shutdown admission fence. It is installed before route
 * middleware so every request arriving after the linearization point receives
 * a stable 503 without starting new work.
 */
export class ShutdownAdmissionGate {
  private draining = false;

  readonly middleware = (
    _request: Request,
    response: Response,
    next: NextFunction
  ): void => {
    if (!this.draining) {
      next();
      return;
    }
    response
      .status(503)
      .setHeader('Connection', 'close')
      .json({ error: 'Server is shutting down', success: false });
  };

  beginDraining(): void {
    this.draining = true;
  }

  get isDraining(): boolean {
    return this.draining;
  }
}

async function closeHttpListener(server: Server | undefined): Promise<void> {
  if (!server?.listening) return;
  const closed = new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) reject(error);
      else resolve();
    });
  });
  server.closeIdleConnections?.();
  await closed;
}

/**
 * Idempotent ordered shutdown barrier. Admission and session sealing happen
 * synchronously before the first await; listener drain precedes persistent
 * cache and database teardown.
 */
export class ShutdownCoordinator {
  private closePromise: Promise<void> | undefined;

  constructor(private readonly options: ShutdownCoordinatorOptions) {}

  close(): Promise<void> {
    this.closePromise ??= this.closeOnce();
    return this.closePromise;
  }

  private async closeOnce(): Promise<void> {
    const errors: unknown[] = [];
    const run = async (cleanup: NamedShutdownCleanup): Promise<void> => {
      try {
        await cleanup.run();
      } catch (error) {
        errors.push(error);
        this.options.onCleanupError?.(cleanup.label, error);
      }
    };

    this.options.admission.beginDraining();
    const listenerClosed = closeHttpListener(this.options.server()).catch(
      (error: unknown) => {
        errors.push(error);
        this.options.onCleanupError?.('http listener', error);
      }
    );

    try {
      this.options.stopTasks();
    } catch (error) {
      errors.push(error);
      this.options.onCleanupError?.('task manager', error);
    }
    try {
      this.options.sealStreams();
    } catch (error) {
      errors.push(error);
      this.options.onCleanupError?.('stream admission', error);
    }

    for (const cleanup of this.options.beforeListenerClose) await run(cleanup);
    await listenerClosed;
    for (const cleanup of this.options.afterListenerClose) await run(cleanup);

    if (errors.length > 0) {
      throw new AggregateError(errors, 'One or more shutdown cleanups failed');
    }
  }
}

export const shutdownAdmission = new ShutdownAdmissionGate();
