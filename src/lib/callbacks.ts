import { v4 as uuid } from "uuid";
import { isPlainObject } from "@/lib/helpers";
import type { CallbackId, CorrelationId } from "@/interface/base";

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

export class CallbackRegistry {
  readonly #by_id = new Map<CallbackId, CallbackEntry>();
  readonly #by_ref = new Map<CorrelationId, Set<CallbackId>>();
  readonly #session_scoped = new Set<CallbackId>();

  register(fn: Function, scope: CallbackScope.STACK): CallbackId;
  register(fn: Function, scope: CallbackScope.LOCAL, ref: CorrelationId): CallbackId;
  register(fn: Function, scope: CallbackScope, ref?: CorrelationId): CallbackId {
    if (scope === CallbackScope.LOCAL && !ref) {
      throw new Error("A bound callback must be registered with a correlation ID.");
    }

    const id = uuid() as CallbackId;
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

  releaseScoped(ref: CorrelationId): ReadonlyArray<CallbackId> {
    const set = this.#by_ref.get(ref);
    if (!set || set.size === 0) return [];

    for (const id of set) this.#by_id.delete(id);
    this.#by_ref.delete(ref);
    return Array.from(set);
  }

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
   * Substitute all functions in the given array with callback stubs, register them
   * with the given correlation ID, and return the new array.
   */
  substitute(args: ReadonlyArray<unknown>, ref: CorrelationId): ReadonlyArray<unknown> {
    return args.map((value: unknown) => {
      if (typeof value === "function") {
        const id = this.register(value, CallbackScope.LOCAL, ref);
        return { [stub]: true, id, scope: CallbackScope.LOCAL } satisfies Callback;
      }

      if (isPlainObject(value)) {
        const result: Record<string, unknown> = {};
        for (const [key, val] of Object.entries(value as object)) {
          // One level deep only — functions nested inside nested objects are not substituted.
          // TODO: decide on handling nested functions
          result[key] =
            typeof val === "function"
              ? (() => {
                  const id = this.register(val, CallbackScope.LOCAL, ref);
                  return { [stub]: true, id, scope: CallbackScope.LOCAL } satisfies Callback;
                })()
              : val;
        }
        return result;
      }

      return value;
    });
  }

  get size(): number {
    return this.#by_id.size;
  }

  toArray(): ReadonlyArray<CallbackId> {
    return Array.from(this.#by_id.keys());
  }
}
