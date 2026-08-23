import { describeUsenetError } from '@aiostreams/core';

export type UsenetStreamStage =
  | 'opening'
  | 'headers'
  | 'streaming'
  | 'complete';

export type UsenetStreamTermination =
  | 'active'
  | 'client_aborted'
  | 'internal_error'
  | 'shutdown'
  | 'normal_eof';

export function safeErrorCode(error: unknown): string | number | undefined {
  if (!(error instanceof Error) || !('code' in error)) return undefined;
  const code = error.code;
  return typeof code === 'string' || typeof code === 'number'
    ? code
    : undefined;
}

/** Monotonic request-local state; later response close cannot mask an error. */
export class UsenetStreamLifecycle {
  private clientAbortedValue = false;
  private firstErrorValue: Error | undefined;
  private stageValue: UsenetStreamStage = 'opening';
  private terminationValue: UsenetStreamTermination = 'active';

  get firstError(): Error | undefined {
    return this.firstErrorValue;
  }

  get stage(): UsenetStreamStage {
    return this.stageValue;
  }

  get termination(): UsenetStreamTermination {
    return this.terminationValue;
  }

  get clientAborted(): boolean {
    return this.clientAbortedValue;
  }

  advance(stage: Exclude<UsenetStreamStage, 'opening'>): void {
    if (this.stageValue === 'complete') return;
    this.stageValue = stage;
  }

  recordStreamError(error: Error, shutdown: boolean): boolean {
    if (
      this.firstErrorValue ||
      this.terminationValue === 'client_aborted' ||
      this.terminationValue === 'normal_eof'
    ) {
      return false;
    }
    this.firstErrorValue = error;
    if (this.terminationValue === 'active') {
      this.terminationValue = shutdown ? 'shutdown' : 'internal_error';
    }
    return true;
  }

  /** Returns true only when the close is a genuine client-owned cancellation. */
  recordResponseClose(responseComplete: boolean): boolean {
    if (responseComplete) {
      // HTTP bytes being flushed is not the source-owner linearization point.
      // The route records normal EOF only after the request-owned reader emits
      // its actual close (which may follow asynchronous file cleanup).
      return false;
    }
    this.clientAbortedValue = true;
    if (this.terminationValue !== 'active') return false;
    this.terminationValue = 'client_aborted';
    return true;
  }

  recordNormalEof(): void {
    if (this.terminationValue !== 'active') return;
    this.terminationValue = 'normal_eof';
    this.stageValue = 'complete';
  }
}

/** Credential-free fields for the route's single internal-failure warning. */
export function usenetStreamFailureLogFields(
  outerError: unknown,
  primaryError: unknown,
  lifecycle: UsenetStreamLifecycle,
  headersSent: boolean,
  unexpectedCleanupErrors: readonly Error[] = []
): Record<string, unknown> {
  const cleanupError = unexpectedCleanupErrors[0];
  return {
    outerErrorName:
      outerError instanceof Error ? outerError.name : 'UnknownError',
    outerCode: safeErrorCode(outerError),
    cleanupErrorName:
      cleanupError instanceof Error ? cleanupError.name : undefined,
    cleanupCode: safeErrorCode(cleanupError),
    cleanupErrorCount: Math.min(unexpectedCleanupErrors.length, 8),
    ...describeUsenetError(primaryError),
    streamStage: lifecycle.stage,
    streamTermination: lifecycle.termination,
    headersSent,
    clientAborted: lifecycle.clientAborted,
    responseClosedByInternalFailure:
      lifecycle.termination === 'internal_error' && headersSent,
  };
}
