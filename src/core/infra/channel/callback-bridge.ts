import * as Packets from "../../../interface/packets";
import * as QuirySymbol from "../symbol";

import { WireKind, WireStatus } from "../../../interface/protocol";
import type { CallbackId, CorrelationId, InvocationId } from "../../../interface/types";

import { type Callback, CallbackRegistry, CallbackScope, isCallbackStub } from "../../../lib/callbacks";
import { InFlightTracker, RefScopedTracker } from "../../../lib/tracker";

import { SessionState } from "../../infra/state";
import type { SessionContext } from "../context";

import { fromWireError, QuiryError, toWireError } from "../../../shared/errors";
import { isPlainObject } from "../../../lib/helpers";

/**
 * A wrapper around a callback function that provides a release method
 * and a dispose symbol for explicit resource management.
 */
export type CallbackHandle<T extends Function> = T & {
  id: CallbackId;
  [QuirySymbol.release](): boolean;
  [Symbol.dispose](): void;
};

interface PendingCallbackInvocation<T = unknown> {
  /** Original request correlation id under which this invocation was issued. */
  readonly ref: CorrelationId | null; // null for session-scoped returned stubs
  resolve: (value: T) => void;
  reject: (error: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
  timestamp: number;
}

type BridgeContext = Omit<SessionContext, "callbacks">;

/**
 * Composes a {@link CallbackRegistry} for the in-process table and adds the
 * IPC plumbing on top: per-ref tracking of which stubs were handed to peer,
 * pending RETURN map for outbound invocations, FinalizationRegistry for
 * GC-driven release, and the per-ref activity counter that lets
 * `releaseRemoteSubs` block until in-flight callback work settles.
 *
 * @diagnostics `callback:invoke`, `callback:return`, `callback:release`.
 */
export class CallbackBridge {
  private readonly registry = new CallbackRegistry();
  private readonly outbound = new InFlightTracker();

  /**
   * Per-ref counter for in-flight callback work, spanning both inbound
   * INVOKEs we're servicing locally and outbound INVOKEs we've issued and
   * are awaiting RETURN for. {@link drainInflightInvocations} awaits zero
   * here before letting RELEASE go out for the same ref.
   */
  private readonly invocations = new RefScopedTracker<CorrelationId>();

  private readonly localFinalization: FinalizationRegistry<CallbackId>;
  private readonly remoteFinalization: FinalizationRegistry<CallbackId>;

  readonly #remote_stubs = new Map<CorrelationId, Set<CallbackId>>();
  readonly #pending_invocations = new Map<InvocationId, PendingCallbackInvocation>();

  constructor(
    private readonly ctx: BridgeContext,
    private readonly config: { defaultTimeout: number },
  ) {
    this.localFinalization = new FinalizationRegistry<CallbackId>((id: CallbackId) => {
      if (this.ctx.state() === SessionState.CLOSED) return;
      const released = this.registry.release(id);
      if (released) this.ctx.diagnostic.maybe("callback:release")?.({ id, reason: "gc" });
    });

    this.remoteFinalization = new FinalizationRegistry<CallbackId>((id: CallbackId) => {
      if (this.ctx.state() === SessionState.CLOSED) return;
      void this.ctx
        .send<Packets.CallbackReleasePacket>({
          kind: WireKind.CALLBACK,
          type: Packets.CallbackMessageType.RELEASE,
          payload: { ref: null, callbacks: [id], gc: true },
        })
        .catch(() => {});
    });
  }

  // --------- PUBLIC: SUBSTITUTION & RESTORATION --------- //

  /** Walk a value graph, replacing functions with stubs registered under `ref`. */
  substitute<T>(value: T, ref?: CorrelationId): T {
    return this.registry.substitute(value, ref);
  }

  /**
   * Rebuilds the argument list on the receiver side: replaces each {@link Callback} stub
   * found anywhere in the graph with a live async function that sends `CBK:INVOKE`
   * and awaits `CBK:RETURN`.
   *
   * `CALL`-scoped stub ids are tracked in `#remote_stubs[ref]` for bulk `CBK:RELEASE`
   * once the owning request completes; `SESSION`-scoped stubs survive the request.
   *
   * Walks arrays and plain objects symmetrically with {@link CallbackRegistry.substitute}.
   * Cycles are short-circuited via the `seen` map.
   */
  restoreStubs<T>(value: T, ref: CorrelationId | null = null): T {
    const track = (stub: Callback): CallbackId => {
      if (ref === null || stub.scope === CallbackScope.SESSION) return stub.id;
      let set = this.#remote_stubs.get(ref);
      if (!set) {
        set = new Set();
        this.#remote_stubs.set(ref, set);
      }
      set.add(stub.id);
      return stub.id;
    };

    const seen = new WeakMap<object, unknown>();
    const walk = (val: unknown): unknown => {
      if (isCallbackStub(val)) {
        return this.#makeRemoteCallback(track(val), ref, val.scope);
      }
      if (val === null || typeof val !== "object") return val;

      const cached = seen.get(val as object);
      if (cached !== undefined) return cached;

      if (Array.isArray(val)) {
        const result: unknown[] = new Array(val.length);
        seen.set(val as object, result);
        for (let i = 0; i < val.length; i++) result[i] = walk(val[i]);
        return result;
      }
      if (isPlainObject(val)) {
        const result: Record<string, unknown> = {};
        seen.set(val as object, result);
        for (const [key, v] of Object.entries(val as object)) result[key] = walk(v);
        return result;
      }
      return val;
    };

    return walk(value) as T;
  }

  // --------- PUBLIC: PROXY HANDLES --------- //

  /**
   * Creates a SESSION-scoped callback handle. The wrapper still satisfies
   * `typeof === "function"`, so passing it as a callback argument flows
   * through serialization to the existing stub. Released explicitly,
   * via `[Symbol.dispose]`, or on local GC.
   */
  proxy<T extends Function>(fn: T): CallbackHandle<T> {
    const callback = this.registry.bind(fn);
    const release = (): boolean => {
      const ok = this.registry.release(callback.id);
      if (ok) this.ctx.diagnostic.maybe("callback:release")?.({ id: callback.id, reason: "explicit" });
      this.localFinalization.unregister(callback);
      return ok;
    };

    const handle = (...args: unknown[]): unknown => (fn as unknown as (...a: unknown[]) => unknown)(...args);
    Object.defineProperties(handle, {
      [QuirySymbol.identifier]: { value: callback.id, enumerable: false, writable: false },
      [QuirySymbol.release]: { value: release, enumerable: false },
      [Symbol.dispose]: { value: release, enumerable: false },
      // A serialize field to survive the structured clone hop.
      [QuirySymbol.serialize]: { value: callback, enumerable: false },
    });

    this.localFinalization.register(handle, callback.id);
    return handle as unknown as CallbackHandle<T>;
  }

  // --------- PACKET HANDLING --------- //

  /** Routes an inbound CALLBACK packet by sub-type. */
  handleCallbackPacket(packet: Packets.AnyCallbackPacket) {
    switch (packet.type) {
      case Packets.CallbackMessageType.INVOKE:
        return this.#handleInvoke(packet);
      case Packets.CallbackMessageType.RETURN:
        return this.#handleReturn(packet);
      case Packets.CallbackMessageType.RELEASE:
        return this.#handleRelease(packet);
    }
  }

  async #handleInvoke(packet: Packets.CallbackInvokePacket): Promise<void> {
    const { ref, eid, callback, args } = packet.payload;
    if (ref) this.invocations.enter(ref);

    const fn = this.registry.get(callback);
    const context = { correlationId: ref ?? undefined };

    this.ctx.diagnostic.maybe("callback:invoke")?.({ eid, id: callback, ref });

    const startedAt = Date.now();
    try {
      if (!fn) {
        // Callback not found
        await this.ctx.send<Packets.CallbackReturnPacket>({
          kind: WireKind.CALLBACK,
          type: Packets.CallbackMessageType.RETURN,
          payload: {
            ref,
            eid,
            callback,
            status: WireStatus.NOT_FOUND,
            error: toWireError(
              new QuiryError(WireStatus.NOT_FOUND, "Callback not found. Did you release it?", context),
            ),
          },
        });

        this.ctx.diagnostic.maybe("callback:return")?.({
          ref,
          eid,
          status: WireStatus.NOT_FOUND,
          durationMs: Date.now() - startedAt,
        });
        return;
      }

      const result = await fn(...args);
      await this.ctx.send<Packets.CallbackReturnPacket>({
        kind: WireKind.CALLBACK,
        type: Packets.CallbackMessageType.RETURN,
        payload: { ref, eid, callback, status: WireStatus.OK, result },
      });

      this.ctx.diagnostic.maybe("callback:return")?.({
        ref,
        eid,
        status: WireStatus.OK,
        durationMs: Date.now() - startedAt,
      });
    } catch (cause: unknown) {
      // Callback-invoke boundary — non fatal.
      const error = QuiryError.from(cause, context);
      await this.ctx
        .send<Packets.CallbackReturnPacket>({
          kind: WireKind.CALLBACK,
          type: Packets.CallbackMessageType.RETURN,
          payload: {
            ref,
            eid,
            callback,
            status: error.code as Exclude<WireStatus, typeof WireStatus.OK>,
            error: toWireError(error),
          },
        })
        .catch(() => {});

      this.ctx.diagnostic.maybe("callback:return")?.({
        ref,
        eid,
        status: error.code,
        durationMs: Date.now() - startedAt,
      });
    } finally {
      if (ref) this.invocations.exit(ref);
    }
  }

  #handleReturn(packet: Packets.CallbackReturnPacket): void {
    const { eid, status } = packet.payload;
    const invocation = this.#pending_invocations.get(eid);
    if (!invocation) return; // stale return — local already timed out

    clearTimeout(invocation.timer);
    this.#pending_invocations.delete(eid);
    this.outbound.exit();
    if (invocation.ref) this.invocations.exit(invocation.ref);

    this.ctx.diagnostic.maybe("callback:return")?.({
      ref: invocation.ref,
      eid,
      status,
      durationMs: Date.now() - invocation.timestamp,
    });

    if (status === WireStatus.OK) invocation.resolve(packet.payload.result);
    else {
      // Remote callback failed — reject the local promise with the
      // reconstructed error so the caller can handle it.
      invocation.reject(fromWireError(packet.payload.error));
    }
  }

  #handleRelease(packet: Packets.CallbackReleasePacket): void {
    const { gc, callbacks } = packet.payload;
    const reason = gc ? "remote-gc" : "remote";
    for (const id of callbacks) {
      // Missing callback on release is idempotent and expected —
      // e.g. we released it locally first. Observable-only.
      const released = this.registry.release(id);
      if (released) {
        this.ctx.diagnostic.maybe("callback:release")?.({ id, reason });
      }
    }
  }

  // --------- PUBLIC: RELEASE COORDINATION --------- //

  /**
   * Awaits in-flight callback work tied to `ref`, then sends a bulk RELEASE
   * for every CALL-scoped stub the local side received under that request.
   * Called by the inbound request handler after the request settles.
   */
  async releaseRemoteStubs(ref: CorrelationId): Promise<void> {
    await this.#drainInflightInvocations(ref);

    const stubs = this.#remote_stubs.get(ref);
    this.#remote_stubs.delete(ref);
    if (!stubs || stubs.size === 0) return;

    await this.ctx.send<Packets.CallbackReleasePacket>({
      kind: WireKind.CALLBACK,
      type: Packets.CallbackMessageType.RELEASE,
      payload: { ref, callbacks: Array.from(stubs) },
    });
  }

  /**
   * Bulk-releases every SESSION-scoped local callback.
   * Called once during drain after the peer has signaled it's done.
   */
  releaseSessionCallbacks(): void {
    const released = this.registry.releaseSessionScoped();
    if (released.length === 0) return;

    for (const id of released) {
      this.ctx.diagnostic.maybe("callback:release")?.({ id, reason: "scope" });
    }

    // Callbacks are local; no need to notify peer.
  }

  // --------- PUBLIC: LIFECYCLE --------- //

  /** Resolves once outbound invocations awaiting RETURN have settled. */
  idle(): Promise<void> {
    return this.outbound.idle();
  }

  /**
   * Force-resets every counter and rejects every pending invocation.
   *
   * Called from `Session.terminate` during teardown — paired `exit` calls
   * will arrive after teardown for any in-flight invocations and must not
   * underflow, so we use the trackers' force-drain behavior.
   */
  drain(error: Error): void {
    for (const invocation of this.#pending_invocations.values()) {
      if (invocation.timer) clearTimeout(invocation.timer);
      invocation.reject(error);
    }
    this.#pending_invocations.clear();

    this.registry.clear();
    this.outbound.drain();
    this.invocations.drain();
    this.#remote_stubs.clear();
  }

  // --------- PUBLIC: INTROSPECTION --------- //

  get callbackCount(): number {
    return this.registry.size;
  }

  get pendingInvocationCount(): number {
    return this.#pending_invocations.size;
  }

  get remoteStubCount(): number {
    let count = 0;
    for (const set of this.#remote_stubs.values()) count += set.size;
    return count;
  }

  // --------- INTERNALS --------- //

  /**
   * Creates an async proxy for a remote callback. Each call sends CBK:INVOKE
   * and awaits the matching CBK:RETURN. CALL-scoped invocations are bounded
   * by `defaultTimeout`; SESSION-scoped invocations are intentionally
   * unbounded (long-lived event handlers are the whole point).
   */
  #makeRemoteCallback(
    id: CallbackId,
    ref: CorrelationId | null,
    scope: CallbackScope,
  ): (...args: unknown[]) => Promise<unknown> {
    return (...args: unknown[]): Promise<unknown> => {
      const eid = `${id}:${Date.now()}:${Math.random().toString(36).substring(2, 15)}` as InvocationId;

      const timeout = scope === CallbackScope.SESSION ? null : this.config.defaultTimeout;
      const promise = new Promise<unknown>((resolve, reject) => {
        const startedAt = Date.now();
        let timer: ReturnType<typeof setTimeout> | undefined;
        if (timeout !== null) {
          // Bound the pending yield with a deadline — a misbehaving peer
          // must not be able to leak promises indefinitely.
          timer = setTimeout(() => {
            this.#pending_invocations.delete(eid);
            this.outbound.exit();
            if (ref) this.invocations.exit(ref);
            reject(
              new QuiryError(
                WireStatus.DEADLINE_EXCEEDED,
                `Remote callback did not return within ${timeout}ms`,
                {
                  correlationId: ref ?? undefined,
                  detail: { callback: id, eid, timeout },
                },
              ),
            );
          }, timeout);
        }

        this.#pending_invocations.set(eid, {
          ref,
          timestamp: startedAt,
          resolve: (value: unknown) => {
            if (timer) clearTimeout(timer);
            resolve(value);
          },
          reject: (error: Error) => {
            if (timer) clearTimeout(timer);
            reject(error);
          },
          timer,
        } satisfies PendingCallbackInvocation);

        this.outbound.enter();
        if (ref) this.invocations.enter(ref);

        this.ctx.diagnostic.maybe("callback:invoke")?.({ eid, id, ref });

        void this.ctx
          .send<Packets.CallbackInvokePacket>({
            kind: WireKind.CALLBACK,
            type: Packets.CallbackMessageType.INVOKE,
            payload: { ref, eid, callback: id, args },
          })
          .catch((error: unknown) => {
            const pending = this.#pending_invocations.get(eid);
            if (!pending) return;

            if (pending.timer) clearTimeout(pending.timer);
            this.#pending_invocations.delete(eid);
            this.outbound.exit();
            if (ref) this.invocations.exit(ref);

            reject(
              new QuiryError(WireStatus.DATA_LOSS, "Failed to send callback invocation", {
                correlationId: ref ?? undefined,
                cause: error,
              }),
            );
          });
      });

      // Mark unobserved rejections as observed; remote callbacks are
      // frequently invoked in fire-and-forget contexts, and an unhandled
      // rejection from a remote error would crash the host process.
      promise.catch(() => {});

      return promise;
    };
  }

  /**
   * Awaits all in-flight CBK:INVOKE work under `ref` — both inbound INVOKEs
   * we're servicing and outbound INVOKEs awaiting RETURN.
   */
  async #drainInflightInvocations(ref: CorrelationId): Promise<void> {
    if (this.invocations.active(ref) === 0) return;

    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      this.invocations.idle(ref),
      new Promise<void>((resolve) => {
        timer = setTimeout(() => resolve(), this.config.defaultTimeout);
      }),
    ]);

    if (timer) clearTimeout(timer);
  }
}
