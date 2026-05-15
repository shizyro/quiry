import { channel as diagnostics_channel } from "node:diagnostics_channel";

/**
 * A typed, zero-allocation-when-quiet event bus used for internal observability.
 *
 * The bus optionally bridges every emit through `node:diagnostics_channel`
 * under the configured prefix. External tools (OpenTelemetry, async hooks,
 * custom collectors) subscribe via the standard `dc.subscribe(name, ...)`
 * API without needing a reference to the {@link DiagnosticBus} instance.
 *
 * Local listener errors are caught and surfaced via {@link process.emitWarning} so a
 * misbehaving subscriber cannot tear down the session. Bridged dc subscriber
 * errors are NOT caught here — `node:diagnostics_channel` already isolates
 * them by re-throwing on the next tick, and intercepting that would violate
 * dc's contract with external consumers.
 */
export class DiagnosticBus<TEvents extends object> {
  readonly #observers = new Map<keyof TEvents, Set<(payload: any) => void>>();
  readonly #channels = new Map<keyof TEvents, ReturnType<typeof diagnostics_channel>>();
  readonly #emitters = new Map<keyof TEvents, (payload: any) => void>();

  /**
   * @param prefix Optional prefix for `node:diagnostics_channel` bridging.
   */
  constructor(private readonly prefix?: string) {}

  /**
   * Returns `true` if at least one local listener OR one `diagnostics_channel`
   * subscriber is currently attached for `name`. Cheap; safe to call before
   * every emit to gate payload construction.
   */
  has<K extends keyof TEvents>(name: K): boolean {
    const set = this.#observers.get(name);
    if (set !== undefined && set.size > 0) return true;
    if (this.prefix !== undefined) {
      // Lazy channel creation: we only allocate a Channel object the first
      // time anyone asks about this event name. Subsequent calls reuse it.
      return this.#getChannel(name).hasSubscribers;
    }
    return false;
  }

  /**
   * Number of local listeners attached for `name`. Does not include dc subscribers.
   * Mostly useful for tests; production code should prefer {@link has}.
   */
  listenerCount<K extends keyof TEvents>(name: K): number {
    return this.#observers.get(name)?.size ?? 0;
  }

  /**
   * Returns a cached emitter function bound to `name` if at least one observer
   * is attached, or `undefined` otherwise. Designed for the optional-chain
   * pattern `diag.maybe("X")?.({ ... })` — when the chain short-circuits,
   * the payload literal is never evaluated, so no allocation occurs.
   *
   * The returned function is cached per event name and is stable across calls.
   * In tight loops, hoist it once and reuse:
   *
   *     const emit = this.diag.maybe("stream:chunk");
   *     if (emit) for (let i = 0; i < n; i++) emit({ ref, seq: i, ... });
   */
  maybe<K extends keyof TEvents>(name: K): ((payload: TEvents[K]) => void) | undefined {
    if (!this.has(name)) return undefined;
    let emitter = this.#emitters.get(name);
    if (emitter === undefined) {
      emitter = (payload: any): void => this.emit(name, payload);
      this.#emitters.set(name, emitter);
    }
    return emitter;
  }

  /**
   * Dispatch a payload directly. Allocates the payload unconditionally;
   * prefer {@link maybe} for any path that fires more than occasionally.
   */
  emit<K extends keyof TEvents>(name: K, payload: TEvents[K]): void {
    const set = this.#observers.get(name);
    if (set !== undefined && set.size > 0) {
      // Set iteration is safe under self-deletion (one-shot listeners), but
      // not under concurrent insertion. We accept that newly-added listeners
      // during a fan-out are not invoked for the in-flight event.
      for (const listener of set) {
        try {
          listener(payload);
        } catch (error: unknown) {
          const message =
            error instanceof Error
              ? error.message
              : typeof error === "string"
                ? error
                : "non-Error value thrown";
          process.emitWarning(
            `Diagnostic listener for "${String(name)}" threw: ${message}`,
            "QuiryDiagnosticError",
          );
        }
      }
    }

    if (this.prefix !== undefined) {
      const channel = this.#getChannel(name);
      // dc.Channel.publish catches subscriber throws internally and re-throws
      // on `process.nextTick`. We don't wrap it: catching would never fire
      // synchronously, and intercepting the next-tick rethrow would silently
      // hide subscriber bugs from external consumers who reasonably expect
      // dc's documented error semantics.
      if (channel.hasSubscribers) channel.publish(payload);
    }
  }

  on<K extends keyof TEvents>(name: K, fn: (payload: TEvents[K]) => void): Unsubscribe {
    let set = this.#observers.get(name);
    if (set === undefined) {
      set = new Set();
      this.#observers.set(name, set);
    }
    const handle = fn as (payload: any) => void;
    set.add(handle);
    return () => {
      this.#observers.get(name)?.delete(handle);
    };
  }

  once<K extends keyof TEvents>(name: K, fn: (payload: TEvents[K]) => void): Unsubscribe {
    const off = this.on(name, (payload: TEvents[K]) => {
      // Detach before invoking so a throwing `fn` doesn't leave the
      // listener registered. The outer try/catch in `emit` swallows the
      // throw and emits a warning.
      off();
      fn(payload);
    });
    return off;
  }

  off<K extends keyof TEvents>(name: K, fn: (payload: TEvents[K]) => void): void {
    this.#observers.get(name)?.delete(fn as (payload: any) => void);
  }

  /**
   * Remove every local listener. Bridged dc subscriptions are unaffected
   * (they are owned by the subscriber, not the bus).
   */
  clear(): void {
    this.#observers.clear();
  }

  #getChannel<K extends keyof TEvents>(name: K): ReturnType<typeof diagnostics_channel> {
    let channel = this.#channels.get(name);
    if (channel === undefined) {
      channel = diagnostics_channel(`${this.prefix}:${String(name)}`);
      this.#channels.set(name, channel);
    }
    return channel;
  }
}
