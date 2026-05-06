type Predicate<T> = (value: T) => boolean;

interface WaitOptions {
  signal?: AbortSignal;
  timeout?: number;
}

interface Waiter<T> {
  predicate: Predicate<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
}

interface Listener<T> {
  predicate: Predicate<T>;
  handler: (value: T) => void;
}

interface Interceptor<T> {
  predicate: Predicate<T>;
  handler: (value: T) => boolean; // consumed if true
}

/**
 * A simple router for handling asynchronous streams of values.
 * Fans an async-iterable source out to typed, priority-ordered consumers.
 */
export class Router<T> {
  #running: boolean = false;

  readonly #waiters = new Set<Waiter<T>>();
  readonly #listeners = new Set<Listener<T>>();
  readonly #interceptors = new Set<Interceptor<T>>();

  constructor(private readonly source: AsyncIterable<T>) {}

  /**
   * Starts consuming `source`. Each value is dispatched to waiting predicates,
   * then interceptors (may consume), then listeners, then the default `handler`.
   * Rejects all pending `wait` promises when the source closes or errors.
   */
  async start(handler: (value: T) => void): Promise<void> {
    if (this.#running) throw new Error("Router is already running");
    this.#running = true;

    try {
      for await (const value of this.source) {
        // one-shot waiters (highest priority)
        for (const waiter of Array.from(this.#waiters)) {
          if (waiter.predicate(value)) {
            this.#waiters.delete(waiter);
            waiter.resolve(value);
          }
        }

        // interceptors (can consume the value)
        let consumed = false;
        for (const interceptor of this.#interceptors) {
          if (interceptor.predicate(value)) {
            if (interceptor.handler(value)) {
              consumed = true;
              break;
            }
          }
        }

        // passive listeners (lowest priority)
        for (const listener of this.#listeners) {
          if (listener.predicate(value)) {
            listener.handler(value);
          }
        }

        // default handler
        if (!consumed) handler(value);
      }

      // flush waiters
      for (const waiter of this.#waiters) waiter.reject(new Error("Stream closed"));
    } catch (error: unknown) {
      for (const waiter of this.#waiters)
        waiter.reject(error instanceof Error ? error : new Error(String(error)));
    } finally {
      this.#waiters.clear();
      this.#running = false;
    }
  }

  /**
   * Resolves the next time a value satisfying `predicate` passes through.
   *
   * Higher routing priority than interceptors and listeners — the waiter is matched and removed
   * before the value reaches any other consumer.
   */
  async wait<U extends T>(predicate: Predicate<U>, options?: WaitOptions): Promise<U> {
    if (!this.#running) throw new Error("Router is not running");

    return new Promise((resolve, reject) => {
      const waiter = { predicate, resolve, reject } as Waiter<T>;
      this.#waiters.add(waiter);

      let timeout: NodeJS.Timeout | null = null;
      const cleanup = () => {
        this.#waiters.delete(waiter);
        if (timeout) clearTimeout(timeout);
      };

      if (options?.signal) {
        const handler = () => {
          cleanup();
          reject(new Error("Operation was aborted"));
        };

        if (options.signal.aborted) return handler();
        options.signal.addEventListener("abort", handler, { once: true });
      }

      if (options?.timeout) {
        timeout = setTimeout(() => {
          cleanup();
          reject(new Error("Timeout waiting for value"));
        }, options.timeout);
      }
    });
  }

  /** Passive, persistent subscription. Receives every value matching `predicate` but cannot consume it. */
  listen<U extends T>(predicate: Predicate<U>, handler: (value: U) => void): Unsubscribe {
    const l = { predicate, handler } as Listener<T>;
    this.#listeners.add(l);
    return () => this.#listeners.delete(l);
  }

  /**
   * Active interceptor with consumption semantics. When `handler` returns `true` the value
   * is not forwarded to listeners or the default handler.
   * @returns Unsubscribe callback.
   */
  intercept<U extends T>(predicate: Predicate<U>, handler: (value: U) => boolean): Unsubscribe {
    const i = { predicate, handler } as Interceptor<T>;
    this.#interceptors.add(i);
    return () => this.#interceptors.delete(i);
  }

  stop(): void {
    this.#running = false;
    this.#waiters.clear();
    this.#listeners.clear();
    this.#interceptors.clear();
  }
}
