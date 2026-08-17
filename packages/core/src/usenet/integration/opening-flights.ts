interface OpeningWaiter<T> {
  readonly resolve: (value: T) => void;
  readonly reject: (error: unknown) => void;
  readonly listeners: Array<{
    readonly signal: AbortSignal;
    readonly listener: () => void;
  }>;
  settled: boolean;
}

type OpeningOutcome<T> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly error: unknown };

interface OpeningFlight<T> {
  readonly controller: AbortController;
  readonly waiters: Set<OpeningWaiter<T>>;
  task: Promise<void>;
  outcome?: OpeningOutcome<T>;
}

type OpeningFlightErrorCode =
  | 'USENET_SESSION_OPEN_CAPACITY'
  | 'USENET_SESSION_OPEN_INVALID_LIMIT';

/** Stable capacity/configuration error for the bounded session-open registry. */
export class OpeningFlightError extends Error {
  constructor(readonly code: OpeningFlightErrorCode) {
    super(
      code === 'USENET_SESSION_OPEN_CAPACITY'
        ? 'too many native usenet session opens are pending'
        : 'native usenet session-open limits must be safe positive integers'
    );
    this.name = 'OpeningFlightError';
  }
}

function abortReason(signal: AbortSignal): unknown {
  return (
    signal.reason ?? new DOMException('The operation was aborted', 'AbortError')
  );
}

/**
 * Bounded, keyed single-flight owner for asynchronous request setup.
 *
 * Each key owns one process-abortable task. Request abort only removes that
 * request's waiter; it never cancels work shared by another request. Process
 * close rejects every waiter, aborts every task once, and settles only after
 * all task `finally` paths have removed their map entries.
 */
export class BoundedOpeningFlights<T> {
  private readonly flights = new Map<string, OpeningFlight<T>>();
  private closedError: Error | undefined;
  private closePromise: Promise<void> | undefined;

  constructor(
    private readonly maxFlights: number,
    private readonly maxWaitersPerFlight: number
  ) {
    if (
      !Number.isSafeInteger(maxFlights) ||
      maxFlights <= 0 ||
      !Number.isSafeInteger(maxWaitersPerFlight) ||
      maxWaitersPerFlight <= 0
    ) {
      throw new OpeningFlightError('USENET_SESSION_OPEN_INVALID_LIMIT');
    }
  }

  get activeFlights(): number {
    return this.flights.size;
  }

  get waitingRequests(): number {
    let waiting = 0;
    for (const flight of this.flights.values()) {
      waiting += flight.waiters.size;
    }
    return waiting;
  }

  run(
    key: string,
    start: (signal: AbortSignal) => Promise<T>,
    requestSignals: readonly (AbortSignal | undefined)[] = []
  ): Promise<T> {
    for (const signal of requestSignals) {
      if (signal?.aborted) return Promise.reject(abortReason(signal));
    }
    if (this.closedError) return Promise.reject(this.closedError);

    let flight = this.flights.get(key);
    if (!flight) {
      if (this.flights.size >= this.maxFlights) {
        return Promise.reject(
          new OpeningFlightError('USENET_SESSION_OPEN_CAPACITY')
        );
      }
      flight = this.createFlight(key, start);
    }

    if (flight.outcome) {
      return flight.outcome.ok
        ? Promise.resolve(flight.outcome.value)
        : Promise.reject(flight.outcome.error);
    }
    if (flight.waiters.size >= this.maxWaitersPerFlight) {
      return Promise.reject(
        new OpeningFlightError('USENET_SESSION_OPEN_CAPACITY')
      );
    }
    return this.addWaiter(flight, requestSignals);
  }

  close(error: Error): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closedError = error;
    const flights = [...this.flights.values()];
    for (const flight of flights) {
      this.settleFlight(flight, { ok: false, error });
      if (!flight.controller.signal.aborted) {
        flight.controller.abort(error);
      }
    }
    this.closePromise = Promise.all(flights.map((flight) => flight.task)).then(
      () => undefined
    );
    return this.closePromise;
  }

  private createFlight(
    key: string,
    start: (signal: AbortSignal) => Promise<T>
  ): OpeningFlight<T> {
    const flight: OpeningFlight<T> = {
      controller: new AbortController(),
      waiters: new Set(),
      task: Promise.resolve(),
    };
    this.flights.set(key, flight);
    const operation = Promise.resolve().then(() => {
      flight.controller.signal.throwIfAborted();
      return start(flight.controller.signal);
    });
    flight.task = operation
      .then(
        (value) => this.settleFlight(flight, { ok: true, value }),
        (error: unknown) => this.settleFlight(flight, { ok: false, error })
      )
      .finally(() => {
        if (this.flights.get(key) === flight) this.flights.delete(key);
      });
    // The flight is an owner task, not a caller-owned promise. Its explicit
    // observer prevents an abandoned request waiter from creating an unhandled
    // rejection while process close still waits the task's finalizer.
    void flight.task.catch(() => undefined);
    return flight;
  }

  private addWaiter(
    flight: OpeningFlight<T>,
    requestSignals: readonly (AbortSignal | undefined)[]
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const waiter: OpeningWaiter<T> = {
        resolve,
        reject,
        listeners: [],
        settled: false,
      };
      flight.waiters.add(waiter);
      for (const signal of requestSignals) {
        if (!signal) continue;
        const listener = (): void => {
          this.settleWaiter(flight, waiter, {
            ok: false,
            error: abortReason(signal),
          });
        };
        waiter.listeners.push({ signal, listener });
        signal.addEventListener('abort', listener, { once: true });
        if (signal.aborted) {
          listener();
          return;
        }
      }
      if (flight.outcome) this.settleWaiter(flight, waiter, flight.outcome);
    });
  }

  private settleFlight(
    flight: OpeningFlight<T>,
    outcome: OpeningOutcome<T>
  ): void {
    if (flight.outcome) return;
    flight.outcome = outcome;
    for (const waiter of [...flight.waiters]) {
      this.settleWaiter(flight, waiter, outcome);
    }
  }

  private settleWaiter(
    flight: OpeningFlight<T>,
    waiter: OpeningWaiter<T>,
    outcome: OpeningOutcome<T>
  ): void {
    if (waiter.settled) return;
    waiter.settled = true;
    flight.waiters.delete(waiter);
    for (const { signal, listener } of waiter.listeners) {
      signal.removeEventListener('abort', listener);
    }
    waiter.listeners.length = 0;
    if (outcome.ok) waiter.resolve(outcome.value);
    else waiter.reject(outcome.error);
  }
}
