export type RepositoryPersistenceErrorCode =
  | 'USENET_SESSION_PERSISTENCE_CAPACITY'
  | 'USENET_SESSION_PERSISTENCE_CLOSED';

/** Stable admission error for the bounded deferred-write owner. */
export class RepositoryPersistenceError extends Error {
  constructor(readonly code: RepositoryPersistenceErrorCode) {
    super(
      code === 'USENET_SESSION_PERSISTENCE_CAPACITY'
        ? 'too many usenet session repository writes are pending'
        : 'usenet session repository persistence is closed'
    );
    this.name = 'RepositoryPersistenceError';
  }
}

interface PendingRepositoryWrite {
  timer?: NodeJS.Timeout;
  ready: boolean;
  readonly operation: () => Promise<void>;
  readonly onError: (error: unknown) => void;
}

interface KeyedRepositoryState {
  active?: Promise<void>;
  successor?: PendingRepositoryWrite;
}

const MAX_REPOSITORY_PERSISTENCE_FAILURES = 64;

/**
 * Bounded keyed-serial owner for session repository writes.
 *
 * Each key owns at most one active mutation and one latest-wins successor;
 * mutations for different keys may use the global active-write window in
 * parallel. Close synchronously fences admission, cancels debounce timers,
 * awaits active predecessors, runs only each key's newest successor, and
 * aggregates every crossing failure.
 */
export class RepositoryPersistenceOwner {
  private readonly states = new Map<string, KeyedRepositoryState>();
  private readonly active = new Set<Promise<void>>();
  private pendingCount = 0;
  private readonly failures: unknown[] = [];
  private droppedFailures = 0;
  private closed = false;
  private closePromise: Promise<void> | undefined;

  constructor(
    private readonly maxPendingKeys = 512,
    private readonly maxActiveWrites = 128
  ) {
    if (
      !Number.isSafeInteger(maxPendingKeys) ||
      maxPendingKeys <= 0 ||
      !Number.isSafeInteger(maxActiveWrites) ||
      maxActiveWrites <= 0
    ) {
      throw new RepositoryPersistenceError(
        'USENET_SESSION_PERSISTENCE_CAPACITY'
      );
    }
  }

  get pendingWrites(): number {
    return this.pendingCount;
  }

  get activeWrites(): number {
    return this.active.size;
  }

  get trackedKeys(): number {
    return this.states.size;
  }

  schedule(
    key: string,
    delayMs: number,
    operation: () => Promise<void>,
    onError: (error: unknown) => void
  ): boolean {
    if (this.closed) return false;
    const state = this.stateForSuccessor(key);
    if (!state) return false;

    const timer = setTimeout(() => {
      const current = this.states.get(key);
      if (!current || current.successor?.timer !== timer) return;
      current.successor.timer = undefined;
      current.successor.ready = true;
      this.drainReady();
    }, delayMs);
    timer.unref?.();
    this.replaceSuccessor(state, {
      timer,
      ready: false,
      operation,
      onError,
    });
    return true;
  }

  /**
   * Remove a pending successor before close begins. The close linearization
   * point freezes its captured successor set, so later cancellation is a
   * non-mutating rejection.
   */
  cancel(key: string): boolean {
    if (this.closed) return false;
    const state = this.states.get(key);
    if (!state?.successor) return false;
    this.dropSuccessor(state);
    if (!state.active) this.states.delete(key);
    return true;
  }

  run(
    key: string,
    operation: () => Promise<void>,
    onError: (error: unknown) => void
  ): boolean {
    if (this.closed) return false;
    const existing = this.states.get(key);
    if (!existing && this.active.size < this.maxActiveWrites) {
      const state: KeyedRepositoryState = {};
      this.states.set(key, state);
      this.start(key, state, { ready: true, operation, onError });
      return true;
    }

    const state = existing ?? this.stateForSuccessor(key);
    if (!state) return false;
    if (!state.successor && this.pendingCount >= this.maxPendingKeys) {
      if (!state.active) this.states.delete(key);
      return false;
    }
    this.replaceSuccessor(state, { ready: true, operation, onError });
    this.drainReady();
    return true;
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closed = true;
    for (const state of this.states.values()) {
      if (!state.successor) continue;
      if (state.successor.timer) clearTimeout(state.successor.timer);
      state.successor.timer = undefined;
      state.successor.ready = true;
    }
    this.closePromise = this.closeOnce();
    return this.closePromise;
  }

  private async closeOnce(): Promise<void> {
    await this.settle([...this.active]);

    for (;;) {
      const batch: Promise<void>[] = [];
      for (const [key, state] of this.states) {
        if (batch.length >= this.maxActiveWrites) break;
        if (state.active || !state.successor) continue;
        const successor = this.takeSuccessor(state);
        batch.push(this.start(key, state, successor));
      }
      if (batch.length === 0) break;
      await this.settle(batch);
    }

    for (const [key, state] of this.states) {
      if (!state.active && !state.successor) this.states.delete(key);
    }
    const failures = this.takeFailures();
    if (failures.length > 0) {
      throw new AggregateError(
        failures,
        'Usenet session repository persistence failed'
      );
    }
  }

  private stateForSuccessor(key: string): KeyedRepositoryState | undefined {
    const existing = this.states.get(key);
    if (existing) {
      if (existing.successor || this.pendingCount < this.maxPendingKeys) {
        return existing;
      }
      return undefined;
    }
    if (this.pendingCount >= this.maxPendingKeys) return undefined;
    const state: KeyedRepositoryState = {};
    this.states.set(key, state);
    return state;
  }

  private replaceSuccessor(
    state: KeyedRepositoryState,
    successor: PendingRepositoryWrite
  ): void {
    if (state.successor?.timer) clearTimeout(state.successor.timer);
    if (!state.successor) this.pendingCount++;
    state.successor = successor;
  }

  private dropSuccessor(state: KeyedRepositoryState): void {
    if (!state.successor) return;
    if (state.successor.timer) clearTimeout(state.successor.timer);
    state.successor = undefined;
    this.pendingCount--;
  }

  private takeSuccessor(state: KeyedRepositoryState): PendingRepositoryWrite {
    const successor = state.successor;
    if (!successor) {
      throw new Error('Repository persistence successor is missing');
    }
    this.dropSuccessor(state);
    return successor;
  }

  private start(
    key: string,
    state: KeyedRepositoryState,
    write: PendingRepositoryWrite
  ): Promise<void> {
    if (state.active) {
      throw new Error('Repository persistence key is already active');
    }
    let tracked: Promise<void>;
    const raw = Promise.resolve().then(write.operation);
    tracked = raw.finally(() => {
      this.active.delete(tracked);
      if (state.active === tracked) state.active = undefined;
      if (!state.successor) this.states.delete(key);
      this.drainReady();
    });
    state.active = tracked;
    this.active.add(tracked);
    void tracked.catch((error: unknown) => {
      this.recordFailure(error);
      try {
        write.onError(error);
      } catch {
        // Error reporting must not create a second unhandled rejection.
      }
    });
    return tracked;
  }

  private drainReady(): void {
    if (this.closed) return;
    while (this.active.size < this.maxActiveWrites) {
      let selected:
        | {
            readonly key: string;
            readonly state: KeyedRepositoryState;
          }
        | undefined;
      for (const [key, state] of this.states) {
        if (!state.active && state.successor?.ready) {
          selected = { key, state };
          break;
        }
      }
      if (!selected) return;
      const successor = this.takeSuccessor(selected.state);
      this.start(selected.key, selected.state, successor);
    }
  }

  private async settle(operations: readonly Promise<void>[]): Promise<void> {
    await Promise.allSettled(operations);
  }

  private recordFailure(error: unknown): void {
    if (this.failures.length < MAX_REPOSITORY_PERSISTENCE_FAILURES - 1) {
      this.failures.push(error);
    } else {
      this.droppedFailures++;
    }
  }

  private takeFailures(): unknown[] {
    const failures = this.failures.splice(0);
    if (this.droppedFailures > 0) {
      failures.push(
        new Error(
          `${this.droppedFailures} additional repository persistence failures`
        )
      );
      this.droppedFailures = 0;
    }
    return failures;
  }
}
