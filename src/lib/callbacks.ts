import { isPlainObject } from "./helpers";
import type { CallbackId, CorrelationId } from "../protocol/types";

import { randomUUID } from "node:crypto";

/**
 * A symbol used to mark callback that aren't real local functions,
 * but rather stubs that point to remote functions on the session.
 *
 * Intentionally a string literal to allow for exchange with the remote
 * side. Though, I don't know if there is a better way to do this.
 */
const stub = "quiry.callback.stub" as const;

export interface CallbackStub {
  readonly [stub]: true;
  readonly id: CallbackId;
  readonly scope: CallbackScope;
}

export enum CallbackScope {
  /** Registered at invocation, and automatically released after the call completes. */
  CALL,
  /** Pre-registered long-lived callback, available till manual release. */
  SESSION,
}

export function isCallbackStub(value: unknown): value is CallbackStub {
  return typeof value === "object" && value !== null && stub in value;
}

type CallbackEntry = {
  readonly id: CallbackId;
  readonly fn: Function;
} & (
  | { readonly scope: CallbackScope.CALL; readonly ref: CorrelationId }
  | { readonly scope: CallbackScope.SESSION }
);

/** In-process callback table keyed by id; CALL entries are grouped by request `ref` for bulk release. */
export class CallbackRegistry {
  static genid(): CallbackId {
    return randomUUID() as CallbackId;
  }

  readonly #by_id = new Map<CallbackId, CallbackEntry>();
  readonly #by_ref = new Map<CorrelationId, Set<CallbackId>>();
  readonly #session_scoped = new Set<CallbackId>();

  /**
   * Registers a callback with the given scope and optional correlation ID.
   */
  register(fn: Function, scope: CallbackScope.SESSION): CallbackId;
  register(fn: Function, scope: CallbackScope.CALL, ref: CorrelationId): CallbackId;
  register(fn: Function, scope: CallbackScope, ref?: CorrelationId): CallbackId {
    if (scope === CallbackScope.CALL && !ref) {
      throw new Error("A bound callback must be registered with a correlation ID.");
    }

    const id = CallbackRegistry.genid();
    const entry = { id, fn, scope, ref: ref! } satisfies CallbackEntry;

    this.#by_id.set(id, entry);
    if (scope === CallbackScope.CALL) {
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
    if (entry.scope === CallbackScope.CALL) {
      this.#by_ref.get(entry.ref)?.delete(id);
    } else this.#session_scoped.delete(id);

    return true;
  }

  /**
   * Removes all `CALL`-scoped callbacks registered under `ref` and returns their ids.
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
   * Removes all `SESSION`-scoped (session-lifetime) callbacks and returns their ids.
   * Called during session drain so the remote side can be notified via `CBK:RELEASE`.
   */
  releaseSessionScoped(): ReadonlyArray<CallbackId> {
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
   * register them under the given correlation ID (if any), and return a transformed copy.
   *
   * Walks arrays and plain objects recursively; class instances and other non-plain
   * objects are returned as-is (they wouldn't survive structured cloning anyway).
   * Already-substituted {@link CallbackStub} stubs pass through untouched. Cycles are
   * detected and short-circuited.
   */
  substitute<T>(value: T, ref?: CorrelationId): T {
    const seen = new WeakMap<object, unknown>();
    const scope = ref ? CallbackScope.CALL : CallbackScope.SESSION;

    const walk = (block: unknown): unknown => {
      if (typeof block === "function") {
        // @ts-expect-error - `scope` is always `CallbackScope.CALL` or `CallbackScope.SESSION`
        const id = this.register(block, scope, ref);
        return { [stub]: true, id, scope } satisfies CallbackStub;
      }

      if (block === null || typeof block !== "object") return block;
      if (isCallbackStub(block)) return block;

      const cached = seen.get(block as object);
      if (cached !== undefined) return cached;

      if (Array.isArray(block)) {
        const result: unknown[] = new Array(block.length);
        seen.set(block as object, result);
        for (let i = 0; i < block.length; i++) result[i] = walk(block[i]);
        return result;
      }

      if (isPlainObject(block)) {
        const result: Record<string, unknown> = {};
        seen.set(block as object, result);
        for (const [key, val] of Object.entries(block as object)) {
          result[key] = walk(val);
        }
        return result;
      }

      return block;
    };

    return walk(value) as T;
  }

  /** Registers a function as a `SESSION`-scoped callback and returns a callback handle. */
  bind(fn: Function): CallbackStub {
    const id = this.register(fn, CallbackScope.SESSION);
    return { [stub]: true, id, scope: CallbackScope.SESSION } satisfies CallbackStub;
  }

  get size(): number {
    return this.#by_id.size;
  }

  toArray(): ReadonlyArray<CallbackId> {
    return Array.from(this.#by_id.keys());
  }
}
