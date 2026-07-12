import type { CallbackId, CorrelationId } from "../protocol/types";
import { randomUUID } from "node:crypto";

/**
 * A symbol used to mark callback that aren't real local functions,
 * but rather stubs that point to remote functions on the session.
 *
 * Intentionally a string literal to allow for exchange with the remote
 * side. Though, I don't know if there is a better way to do this.
 */
const CALLBACK_MARKER = "__quiry.callback" as const;

interface CallbackEnvelope {
  readonly [CALLBACK_MARKER]: true;
  readonly id: CallbackId;
  readonly scope: CallbackScope;
}

export enum CallbackScope {
  /** Registered at invocation, and automatically released after the call completes. */
  CALL,
  /** Pre-registered long-lived callback, available till manual release. */
  SESSION,
}

export function isCallbackEnvelope(value: unknown): value is CallbackEnvelope {
  return typeof value === "object" && value !== null && CALLBACK_MARKER in value;
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

  static envelope(id: CallbackId, scope: CallbackScope): CallbackEnvelope {
    return { [CALLBACK_MARKER]: true, id, scope } satisfies CallbackEnvelope;
  }

  readonly #by_id = new Map<CallbackId, CallbackEntry>();
  readonly #by_ref = new Map<CorrelationId, Set<CallbackId>>();
  readonly #session_scoped = new Set<CallbackId>();

  /**
   * Registers a callback with the given scope and optional correlation ID.
   */
  register(fn: Function, scope: CallbackScope.SESSION): CallbackId;
  register(fn: Function, scope: CallbackScope.CALL, cid: CorrelationId): CallbackId;
  register(fn: Function, scope: CallbackScope, cid?: CorrelationId): CallbackId {
    if (scope === CallbackScope.CALL && !cid) {
      throw new Error("A bound callback must be registered with a correlation ID.");
    }

    const id = CallbackRegistry.genid();
    const entry = { id, fn, scope, ref: cid! } satisfies CallbackEntry;

    this.#by_id.set(id, entry);
    if (scope === CallbackScope.CALL) {
      let set = this.#by_ref.get(cid!);
      if (!set) {
        set = new Set();
        this.#by_ref.set(cid!, set);
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
   * Removes all `CALL`-scoped callbacks registered under correlated packet and returns their ids.
   * Used after a request completes to bulk-release all function arguments that were
   * substituted as stubs for that request.
   */
  releaseScoped(cid: CorrelationId): ReadonlyArray<CallbackId> {
    const set = this.#by_ref.get(cid);
    if (!set || set.size === 0) return [];

    for (const id of set) this.#by_id.delete(id);
    this.#by_ref.delete(cid);
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

  /** Registers a function as a `SESSION`-scoped callback and returns a callback handle. */
  bind(fn: Function): CallbackEnvelope {
    const id = this.register(fn, CallbackScope.SESSION);
    return CallbackRegistry.envelope(id, CallbackScope.SESSION);
  }

  get size(): number {
    return this.#by_id.size;
  }

  toArray(): ReadonlyArray<CallbackId> {
    return Array.from(this.#by_id.keys());
  }
}
