import { isPlainObject } from "@/lib/helpers";
import type { CallbackId, CorrelationId } from "@/interface/base";

import { nanoid } from "nanoid";

/**
 * A symbol used to mark callback that aren't real local functions,
 * but rather stubs that point to remote functions on the session.
 *
 * Intentionally a string literal to allow for exchange with the remote
 * side. Though, I don't know if there is a better way to do this.
 */
export const stub = "quiry.callback.stub" as const;

export interface Callback {
  readonly [stub]: true;
  readonly id: CallbackId;
  readonly scope: CallbackScope;
}

export enum CallbackScope {
  /** Registered at invocation, and automatically released after the call completes. */
  LOCAL,
  /** Pre-registered long-lived callback, available till manual release. */
  STACK,
}

export function isCallbackStub(value: unknown): value is Callback {
  return typeof value === "object" && value !== null && stub in value;
}

type CallbackEntry = {
  readonly id: CallbackId;
  readonly fn: Function;
} & (
  | { readonly scope: CallbackScope.LOCAL; readonly ref: CorrelationId }
  | { readonly scope: CallbackScope.STACK }
);

/** In-process callback table keyed by id; LOCAL entries are grouped by request `ref` for bulk release. */
export class CallbackRegistry {
  readonly #by_id = new Map<CallbackId, CallbackEntry>();
  readonly #by_ref = new Map<CorrelationId, Set<CallbackId>>();
  readonly #session_scoped = new Set<CallbackId>();

  /**
   * Registers a callback with the given scope and optional correlation ID.
   */
  register(fn: Function, scope: CallbackScope.STACK): CallbackId;
  register(fn: Function, scope: CallbackScope.LOCAL, ref: CorrelationId): CallbackId;
  register(fn: Function, scope: CallbackScope, ref?: CorrelationId): CallbackId {
    if (scope === CallbackScope.LOCAL && !ref) {
      throw new Error("A bound callback must be registered with a correlation ID.");
    }

    const id = nanoid() as CallbackId;
    const entry = { id, fn, scope, ref: ref! } satisfies CallbackEntry;

    this.#by_id.set(id, entry);
    if (scope === CallbackScope.LOCAL) {
      let set = this.#by_ref.get(ref!);
      if (!set) {
        set = new Set();
        this.#by_ref.set(ref!, set);
      }
      set.add(id);
    } else {
      this.#session_scoped.add(id);
    }

    return id;
  }

  get(id: CallbackId): Function | undefined {
    return this.#by_id.get(id)?.fn;
  }

  release(id: CallbackId): boolean {
    const entry = this.#by_id.get(id);
    if (!entry) return false;

    this.#by_id.delete(id);
    if (entry.scope === CallbackScope.LOCAL) {
      this.#by_ref.get(entry.ref)?.delete(id);
    } else this.#session_scoped.delete(id);

    return true;
  }

  /**
   * Removes all `LOCAL`-scoped callbacks registered under `ref` and returns their ids.
   * Used after a request completes to bulk-release all function arguments that were
   * substituted as stubs for that request.
   */
  releaseScoped(ref: CorrelationId): ReadonlyArray<CallbackId> {
    const set = this.#by_ref.get(ref);
    if (!set || set.size === 0) return [];

    for (const id of set) this.#by_id.delete(id);
    this.#by_ref.delete(ref);
    return Array.from(set);
  }

  /**
   * Removes all `STACK`-scoped (session-lifetime) callbacks and returns their ids.
   * Called during session drain so the remote side can be notified via `CBK:RELEASE`.
   */
  releaseStackScoped(): ReadonlyArray<CallbackId> {
    if (this.#session_scoped.size === 0) return [];
    const ids = Array.from(this.#session_scoped);
    for (const id of ids) this.#by_id.delete(id);
    this.#session_scoped.clear();
    return ids;
  }

  clear(): void {
    this.#by_id.clear();
    this.#by_ref.clear();
    this.#session_scoped.clear();
  }

  /**
   * Substitute all functions reachable through the argument graph with callback stubs,
   * register them under the given correlation ID, and return a transformed copy.
   *
   * Walks arrays and plain objects recursively; class instances and other non-plain
   * objects are returned as-is (they wouldn't survive structured cloning anyway).
   * Already-substituted {@link Callback} stubs pass through untouched. Cycles are
   * detected and short-circuited.
   */
  substitute(args: ReadonlyArray<unknown>, ref: CorrelationId): ReadonlyArray<unknown> {
    const seen = new WeakMap<object, unknown>();

    const walk = (value: unknown): unknown => {
      if (typeof value === "function") {
        const id = this.register(value, CallbackScope.LOCAL, ref);
        return { [stub]: true, id, scope: CallbackScope.LOCAL } satisfies Callback;
      }

      if (value === null || typeof value !== "object") return value;
      if (isCallbackStub(value)) return value;

      const cached = seen.get(value as object);
      if (cached !== undefined) return cached;

      if (Array.isArray(value)) {
        const result: unknown[] = new Array(value.length);
        seen.set(value as object, result);
        for (let i = 0; i < value.length; i++) result[i] = walk(value[i]);
        return result;
      }

      if (isPlainObject(value)) {
        const result: Record<string, unknown> = {};
        seen.set(value as object, result);
        for (const [key, val] of Object.entries(value as object)) {
          result[key] = walk(val);
        }
        return result;
      }

      return value;
    };

    return args.map(walk);
  }

  get size(): number {
    return this.#by_id.size;
  }

  toArray(): ReadonlyArray<CallbackId> {
    return Array.from(this.#by_id.keys());
  }
}
