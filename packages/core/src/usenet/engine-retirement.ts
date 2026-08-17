const RETIREMENT_FAILURE_LIMIT = 64;

/**
 * Serial, fail-safe cleanup tail. The internal tail always settles so one
 * failed retirement cannot poison every later cleanup. Individual enqueue
 * calls still observe their own failure, while a bounded ledger keeps the
 * registry fail-closed until process shutdown reports all retained causes.
 */
export class EngineRetirementBarrier {
  private tail: Promise<void> = Promise.resolve();
  private readonly failures: unknown[] = [];
  private droppedFailures = 0;

  snapshot(): Promise<void> {
    return this.tail;
  }

  enqueue(operation: () => Promise<void>): Promise<void> {
    const result = this.tail.then(operation);
    this.tail = result.then(
      () => undefined,
      (error: unknown) => {
        if (this.failures.length < RETIREMENT_FAILURE_LIMIT) {
          this.failures.push(error);
        } else {
          this.droppedFailures++;
        }
      }
    );
    return result;
  }

  error(): AggregateError | undefined {
    if (this.failures.length === 0 && this.droppedFailures === 0) {
      return undefined;
    }
    const errors = [...this.failures];
    if (this.droppedFailures > 0) {
      errors.push(
        new Error(
          `${this.droppedFailures} additional usenet engine retirement failures`
        )
      );
    }
    return new AggregateError(errors, 'Usenet engine retirement failed');
  }
}
