/**
 * In this example, I'm favouring composition over inheritance;
 *
 *    node:events' listener-mutating methods (on, off, once, etc.) return `this` by design,
 *    for chaining. Across our boundary, "this" means the entire instance, which is neither
 *    plain data nor a registered serializable type, so returning it as-is crosses the wire
 *    as an unhandled class instance and throws.
 *
 *    There is also the fact that going for inheritance makes the entire method surface
 *    remotably callable by default; the actual shape of an object is always exposed, not a
 *    curated subset of it. Composition keeps the remote surface clean and predictable.
 *
 * Of course, going for the typical inheritance-based approach is possible, but means
 * manually overriding every chainable method to discard the reference.
 */

import * as Quiry from "~";
import EventEmitter from "node:events";

interface CounterEvents {
  increment: [prev: number, next: number];
  reset: [];
}

class Counter {
  private count = 0;
  readonly #emitter = new EventEmitter();

  on<TEventName extends string | symbol>(
    event: TEventName,
    listener: (...args: TEventName extends keyof CounterEvents ? CounterEvents[TEventName] : never) => void,
  ) {
    this.#emitter.on(event, listener);
    // Make sure to void the return value; emitter.off returns an unserializable
    // reference to the emitter (default), which we don't want to leak.
    return () => void this.#emitter.off(event, listener);
  }

  emit<TEventName extends keyof CounterEvents>(
    event: TEventName,
    ...args: CounterEvents[TEventName]
  ): boolean {
    return this.#emitter.emit(event, ...args);
  }

  increment(): void {
    const prev = this.count;
    this.count++;
    this.emit("increment", prev, this.count);
  }

  reset(): void {
    this.count = 0;
    this.emit("reset");
  }

  get value(): number {
    return this.count;
  }
}

Quiry.attach(new Quiry.ChildProcessTransport());
Quiry.expose("counter", new Counter());

Quiry.on("peer-disconnected", () => process.exit(0));

export type { Counter };
