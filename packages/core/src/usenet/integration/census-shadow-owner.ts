export type CensusShadowOwnerErrorCode =
  | 'USENET_CENSUS_SHADOW_CAPACITY'
  | 'USENET_CENSUS_SHADOW_CLOSED';

export class CensusShadowOwnerError extends Error {
  constructor(readonly code: CensusShadowOwnerErrorCode) {
    super(
      code === 'USENET_CENSUS_SHADOW_CAPACITY'
        ? 'too many usenet census shadows are active'
        : 'usenet census shadow persistence is closed'
    );
    this.name = 'CensusShadowOwnerError';
  }
}

interface OwnedCensus<TSnapshot> {
  readonly done: Promise<TSnapshot>;
  cancel(): void;
}

export type CensusShadowStep<T> =
  | { readonly current: false }
  | { readonly current: true; readonly value: T };

export interface CensusShadowPublication {
  isCurrent(): boolean;
  step<T>(operation: () => Promise<T>): Promise<CensusShadowStep<T>>;
}

export interface CensusShadowHandle {
  readonly generation: number;
  readonly done: Promise<void>;
  cancel(): void;
}

interface CensusShadowSpawn<TSnapshot> {
  readonly nzbHash: string;
  readonly census: OwnedCensus<TSnapshot>;
  readonly apply: (
    snapshot: TSnapshot,
    publication: CensusShadowPublication
  ) => Promise<void>;
  readonly onError: (error: unknown) => void;
  readonly onRejected?: (error: CensusShadowOwnerError) => void;
}

interface CensusShadowState<TSnapshot> {
  readonly nzbHash: string;
  readonly generation: number;
  readonly census: OwnedCensus<TSnapshot>;
  readonly task: Promise<void>;
  cancelled: boolean;
}

const DEFAULT_MAX_CENSUS_SHADOW_TASKS = 64;
const MAX_CENSUS_SHADOW_CLOSE_FAILURES = 64;

/**
 * Bounded process owner for census continuations and their repository writes.
 * A hash has one current generation; replacements synchronously invalidate the
 * predecessor and wait its full task before publishing. Close fences spawn
 * before cancelling and settling every registered task.
 */
export class CensusShadowOwner<TSnapshot> {
  private readonly current = new Map<string, CensusShadowState<TSnapshot>>();
  private readonly active = new Set<CensusShadowState<TSnapshot>>();
  private readonly closeFailures: unknown[] = [];
  private droppedCloseFailures = 0;
  private generation = 0;
  private closed = false;
  private closePromise: Promise<void> | undefined;

  constructor(private readonly maxTasks = DEFAULT_MAX_CENSUS_SHADOW_TASKS) {
    if (!Number.isSafeInteger(maxTasks) || maxTasks <= 0) {
      throw new CensusShadowOwnerError('USENET_CENSUS_SHADOW_CAPACITY');
    }
  }

  get activeTasks(): number {
    return this.active.size;
  }

  get currentGenerations(): number {
    return this.current.size;
  }

  spawn(args: CensusShadowSpawn<TSnapshot>): CensusShadowHandle | undefined {
    if (this.closed) {
      args.census.cancel();
      this.rejectSpawn(
        args,
        new CensusShadowOwnerError('USENET_CENSUS_SHADOW_CLOSED')
      );
      return undefined;
    }

    // Reject before touching the current generation. Capacity pressure must
    // never evict an already-owned same-hash task without adopting a successor.
    if (this.active.size >= this.maxTasks) {
      args.census.cancel();
      this.rejectSpawn(
        args,
        new CensusShadowOwnerError('USENET_CENSUS_SHADOW_CAPACITY')
      );
      return undefined;
    }
    if (this.generation >= Number.MAX_SAFE_INTEGER) {
      args.census.cancel();
      this.rejectSpawn(
        args,
        new CensusShadowOwnerError('USENET_CENSUS_SHADOW_CAPACITY')
      );
      return undefined;
    }

    const predecessor = this.current.get(args.nzbHash);
    if (predecessor) this.cancelState(predecessor);
    const generation = ++this.generation;
    const completion = Promise.withResolvers<void>();
    const state: CensusShadowState<TSnapshot> = {
      nzbHash: args.nzbHash,
      generation,
      census: args.census,
      task: completion.promise,
      cancelled: false,
    };
    this.active.add(state);
    this.current.set(args.nzbHash, state);
    // runState catches operation failures and always settles. Keep a rejection
    // observer here as a final ownership guard against a future implementation
    // error creating an unhandled continuation.
    void this.runState(state, predecessor, args).then(
      completion.resolve,
      (error) => {
        if (this.closed) this.recordCloseFailure(error);
        this.reportError(args, error);
        completion.resolve();
      }
    );

    return {
      generation,
      done: state.task,
      cancel: () => this.cancelState(state),
    };
  }

  async invalidate(nzbHash: string): Promise<void> {
    const state = this.current.get(nzbHash);
    if (!state) return;
    this.cancelState(state);
    await state.task;
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    // Linearization point: later spawn calls fail synchronously before the
    // first await in closeOnce.
    this.closed = true;
    for (const state of this.active) this.cancelState(state);
    this.closePromise = this.closeOnce([...this.active]);
    return this.closePromise;
  }

  private async runState(
    state: CensusShadowState<TSnapshot>,
    predecessor: CensusShadowState<TSnapshot> | undefined,
    args: CensusShadowSpawn<TSnapshot>
  ): Promise<void> {
    try {
      if (predecessor) await predecessor.task;
      if (!this.isCurrent(state)) return;
      const snapshot = await state.census.done;
      if (!this.isCurrent(state)) return;
      await args.apply(snapshot, this.publicationFor(state));
    } catch (error) {
      if (this.closed) this.recordCloseFailure(error);
      this.reportError(args, error);
    } finally {
      if (this.current.get(state.nzbHash) === state) {
        this.current.delete(state.nzbHash);
      }
      this.active.delete(state);
    }
  }

  private publicationFor(
    state: CensusShadowState<TSnapshot>
  ): CensusShadowPublication {
    return {
      isCurrent: () => this.isCurrent(state),
      step: async <T>(operation: () => Promise<T>) => {
        if (!this.isCurrent(state)) return { current: false };
        const value = await operation();
        if (!this.isCurrent(state)) return { current: false };
        return { current: true, value };
      },
    };
  }

  private isCurrent(state: CensusShadowState<TSnapshot>): boolean {
    return !this.closed && this.current.get(state.nzbHash) === state;
  }

  private cancelState(state: CensusShadowState<TSnapshot>): void {
    if (this.current.get(state.nzbHash) === state) {
      this.current.delete(state.nzbHash);
    }
    if (state.cancelled) return;
    state.cancelled = true;
    state.census.cancel();
  }

  private rejectSpawn(
    args: CensusShadowSpawn<TSnapshot>,
    error: CensusShadowOwnerError
  ): void {
    try {
      args.onRejected?.(error);
    } catch {
      // Admission reporting is best-effort and must stay synchronous.
    }
  }

  private reportError(
    args: CensusShadowSpawn<TSnapshot>,
    error: unknown
  ): void {
    try {
      args.onError(error);
    } catch {
      // Error reporting must not create an unowned rejection.
    }
  }

  private recordCloseFailure(error: unknown): void {
    if (this.closeFailures.length < MAX_CENSUS_SHADOW_CLOSE_FAILURES - 1) {
      this.closeFailures.push(error);
    } else {
      this.droppedCloseFailures++;
    }
  }

  private async closeOnce(
    states: CensusShadowState<TSnapshot>[]
  ): Promise<void> {
    await Promise.all(states.map((state) => state.task));
    this.current.clear();
    const errors = this.closeFailures.splice(0);
    if (this.droppedCloseFailures > 0) {
      errors.push(
        new Error(
          `${this.droppedCloseFailures} additional census shadow failures`
        )
      );
      this.droppedCloseFailures = 0;
    }
    if (errors.length > 0) {
      throw new AggregateError(errors, 'Usenet census shadow close failed');
    }
  }
}
