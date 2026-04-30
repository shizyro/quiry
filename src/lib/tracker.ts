/** Tracks in-flight operations and allows awaiting a "drain to zero" condition. */
export class InFlightTracker {
  #inflight: number = 0;
  readonly #resolvers: Array<() => void> = [];

  enter(): void {
    this.#inflight++;
  }

  exit(): void {
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
   * Forces the tracker into an idle state by clearing all in-flight accounting,
   * and resolving all pending idle() waiters.
   */
  drain(): void {
    this.#inflight = 0;
    const resolvers = this.#resolvers.splice(0);
    for (const resolve of resolvers) resolve();
  }
}
