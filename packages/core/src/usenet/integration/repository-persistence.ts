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

/**
 * Bounded owner for debounced and immediate session repository writes.
 *
 * Runtime callbacks remain best-effort and report through `onError`. Close is
 * a durability barrier: it synchronously fences new work, cancels every timer,
 * runs the latest pending operation for each key immediately, and aggregates
 * failures from those writes plus operations already active at the fence.
 */
export class RepositoryPersistenceOwner {
  private readonly pending = new Map<string, PendingRepositoryWrite>();
  private readonly active = new Set<Promise<void>>();
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
    return this.pending.size;
  }

  get activeWrites(): number {
    return this.active.size;
  }

  schedule(
    key: string,
    delayMs: number,
    operation: () => Promise<void>,
    onError: (error: unknown) => void
  ): boolean {
    if (this.closed) return false;
    const existing = this.pending.get(key);
    if (!existing && this.pending.size >= this.maxPendingKeys) return false;
    if (existing?.timer) clearTimeout(existing.timer);

    const timer = setTimeout(() => {
      const current = this.pending.get(key);
      if (!current || current.timer !== timer) return;
      current.timer = undefined;
      current.ready = true;
      this.drainReady();
    }, delayMs);
    timer.unref?.();
    this.pending.set(key, { timer, ready: false, operation, onError });
    return true;
  }

  cancel(key: string): void {
    const pending = this.pending.get(key);
    if (!pending) return;
    if (pending.timer) clearTimeout(pending.timer);
    this.pending.delete(key);
  }

  run(
    operation: () => Promise<void>,
    onError: (error: unknown) => void
  ): boolean {
    if (this.closed || this.active.size >= this.maxActiveWrites) return false;
    this.start(operation, onError);
    return true;
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closed = true;
    const deferred = [...this.pending.values()];
    this.pending.clear();
    for (const write of deferred) {
      if (write.timer) clearTimeout(write.timer);
    }
    const alreadyActive = [...this.active];
    this.closePromise = (async () => {
      const errors: unknown[] = [];
      for (const result of await Promise.allSettled(alreadyActive)) {
        if (result.status === 'rejected') errors.push(result.reason);
      }
      // Flush latest debounced values serially. This keeps shutdown DB
      // concurrency within the same hard active-write bound without creating
      // a second queue or a hold-and-wait lifecycle.
      for (const write of deferred) {
        const operation = this.start(write.operation, write.onError, true);
        if (!operation) continue;
        const [result] = await Promise.allSettled([operation]);
        if (result?.status === 'rejected') errors.push(result.reason);
      }
      if (errors.length > 0) {
        throw new AggregateError(
          errors,
          'Usenet session repository persistence failed'
        );
      }
    })();
    return this.closePromise;
  }

  private start(
    operation: () => Promise<void>,
    onError: (error: unknown) => void,
    duringClose = false
  ): Promise<void> | undefined {
    if (!duringClose && this.active.size >= this.maxActiveWrites) {
      onError(
        new RepositoryPersistenceError('USENET_SESSION_PERSISTENCE_CAPACITY')
      );
      return undefined;
    }
    let tracked: Promise<void>;
    const raw = Promise.resolve().then(operation);
    tracked = raw.finally(() => {
      this.active.delete(tracked);
      this.drainReady();
    });
    this.active.add(tracked);
    void tracked.catch(onError);
    return tracked;
  }

  private drainReady(): void {
    if (this.closed) return;
    while (this.active.size < this.maxActiveWrites) {
      let selected:
        | { readonly key: string; readonly write: PendingRepositoryWrite }
        | undefined;
      for (const [key, write] of this.pending) {
        if (write.ready) {
          selected = { key, write };
          break;
        }
      }
      if (!selected) return;
      this.pending.delete(selected.key);
      this.start(selected.write.operation, selected.write.onError);
    }
  }
}
