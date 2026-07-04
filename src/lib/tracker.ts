/** Tracks in-flight operations and allows awaiting a "drain to zero" condition. */
export class InFlightTracker {
  #inflight: number = 0;
  /** Set once `drain()` runs; late `exit()` calls become no-ops instead of underflow errors. */
  #drained: boolean = false;
  readonly #resolvers: Array<() => void> = [];

  enter(): void {
    this.#inflight++;
  }

  exit(): void {
    // Teardown already reset counters and forfeited pairing guarantees;
    // an `enter()` whose matching `exit()` resolves after `drain()` is
    // expected, not a bug.
    if (this.#drained) return;
    if (this.#inflight <= 0) throw new Error("Tracker underflow (exit without matching enter)");

    this.#inflight--;
    if (this.#inflight === 0) {
      const resolvers = this.#resolvers.splice(0);
      for (const resolve of resolvers) resolve();
    }
  }

  /**
   * Number of currently active operations.
   */
  get active(): number {
    return this.#inflight;
  }

  /**
   * Returns a promise that resolves once the active count reaches zero.
   * If already idle, resolves immediately.
   */
  idle(): Promise<void> {
    if (this.#inflight === 0) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.#resolvers.push(resolve);
    });
  }

  /**
   * Runs an async function within the barrier, ensuring proper pairing.
   */
  async run<T>(fn: () => Promise<T>): Promise<T> {
    this.enter();
    try {
      return await fn();
    } finally {
      this.exit();
    }
  }

  /**
   * Resets the in-flight count to zero without pairing `exit` calls — use for teardown only;
   * resolves all pending {@link InFlightTracker.idle} waiters. After this, further `exit()`
   * calls are no-ops (see `#drained`).
   */
  drain(): void {
    this.#drained = true;
    this.#inflight = 0;
    const resolvers = this.#resolvers.splice(0);
    for (const resolve of resolvers) resolve();
  }
}

/**
 * Tracks in-flight operations partitioned by a key, with a per-key
 * "drain to zero" awaiter. Same semantics as {@link InFlightTracker} but
 * scoped to a label so independent ref-groups don't block on each other.
 */
export class RefScopedTracker<K> {
  readonly #counts = new Map<K, number>();
  readonly #waiters = new Map<K, Array<() => void>>();
  /** Set once `drain()` runs; late `exit()` calls become no-ops instead of underflow errors. */
  #drained: boolean = false;

  enter(key: K): void {
    this.#counts.set(key, (this.#counts.get(key) ?? 0) + 1);
  }

  exit(key: K): void {
    if (this.#drained) return;

    const n = this.#counts.get(key);
    if (n === undefined || n <= 0)
      throw new Error("RefScopedTracker underflow (exit without matching enter)");

    if (n > 1) {
      this.#counts.set(key, n - 1);
      return;
    }

    this.#counts.delete(key);
    const waiters = this.#waiters.get(key);
    if (waiters) {
      this.#waiters.delete(key);
      for (const resolve of waiters) resolve();
    }
  }

  /** Number of currently active operations under `key`. */
  active(key: K): number {
    return this.#counts.get(key) ?? 0;
  }

  /**
   * Returns a promise that resolves once the active count for `key` reaches
   * zero. If already idle, resolves immediately.
   */
  idle(key: K): Promise<void> {
    if (!this.#counts.has(key)) return Promise.resolve();
    return new Promise<void>((resolve) => {
      let list = this.#waiters.get(key);
      if (!list) {
        list = [];
        this.#waiters.set(key, list);
      }
      list.push(resolve);
    });
  }

  /**
   * Resets the count for a single key and resolves its pending waiters. Use
   * when abandoning a key (e.g. timeout fall-through). Pairing of subsequent
   * `exit` calls is the caller's responsibility — typically this is only
   * called once the caller can guarantee no further `exit`s will arrive.
   */
  clear(key: K): void {
    this.#counts.delete(key);
    const waiters = this.#waiters.get(key);
    if (waiters) {
      this.#waiters.delete(key);
      for (const resolve of waiters) resolve();
    }
  }

  /**
   * Resets every key's count to zero and resolves all pending waiters — use
   * for teardown only; after this, further `exit()` calls are no-ops (see
   * `#drained`).
   */
  drain(): void {
    this.#drained = true;
    this.#counts.clear();
    const allWaiters = Array.from(this.#waiters.values());
    this.#waiters.clear();
    for (const waiters of allWaiters) {
      for (const resolve of waiters) resolve();
    }
  }
}
