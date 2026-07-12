// biome-ignore-all lint/complexity/noThisInStatic: intended behavior

import type { CallbackBridge } from "./callback-bridge";
import type { CallbackId, CorrelationId } from "../../../protocol/types";

import * as Transfers from "../../../lib/transfer";

import { CallbackRegistry, CallbackScope, isCallbackEnvelope } from "../../../lib/callbacks";
import { rebuild, type StepTransformer } from "../../../lib/helpers";

/**
 * Prepares a local value for the wire: serializes registered class
 * instances into their envelope, and functions into callback stubs. Both
 * happen in one pass over the value graph, rather than two.
 */
export function toWire<T>(value: T, bridge: CallbackBridge, cid: CorrelationId | null = null): T {
  return rebuild(value, Transfers.transform, CallbackBridgeTransformer.transform.apply(bridge, [cid]));
}

/**
 * Inverse of {@link toWire}: rebuilds a value received off the wire in one
 * pass — callback stubs become live remote callables, and serialized
 * envelopes become their original class instances again.
 */
export function fromWire<T>(value: T, bridge: CallbackBridge, cid: CorrelationId | null = null): T {
  return rebuild(value, CallbackBridgeTransformer.restore.apply(bridge, [cid]), Transfers.restore);
}

class CallbackBridgeTransformer {
  /**
   * Returns a {@link BuildTransformer} that substitutes functions with callback stubs,
   * registering them under the given correlation ID (if any).
   *
   * When applied, it walks arrays and plain objects recursively; class instances and other
   * non-plain objects are returned as-is (they wouldn't survive structured cloning anyway).
   * Already-substituted {@link CallbackEnvelope} stubs pass through untouched. Cycles are
   * detected and short-circuited.
   */
  static transform(this: CallbackBridge, cid: CorrelationId | null = null): StepTransformer {
    const scope = cid ? CallbackScope.CALL : CallbackScope.SESSION;
    return (block) => {
      if (typeof block === "function") {
        // @ts-expect-error - `scope` is always `CallbackScope.CALL` or `CallbackScope.SESSION`
        const id = this.registry.register(block, scope, cid);
        return { value: CallbackRegistry.envelope(id, scope) };
      }
      if (isCallbackEnvelope(block)) return { value: block };
      return undefined;
    };
  }

  /**
   * Rebuilds the argument list on the receiver side: replaces each {@link CallbackStub} stub
   * found anywhere in the graph with a live async function that sends `CBK:INVOKE`
   * and awaits `CBK:RETURN`.
   *
   * `CALL`-scoped stub ids are tracked in `#remote_stubs[cid]` for bulk `CBK:RELEASE`
   * once the owning request completes; `SESSION`-scoped stubs survive the request.
   */
  static restore(this: CallbackBridge, cid: CorrelationId | null = null): StepTransformer {
    const track = (stub: ReturnType<typeof CallbackRegistry.prototype.bind>): CallbackId => {
      if (cid === null || stub.scope === CallbackScope.SESSION) return stub.id;
      let set = this.remoteCallbacks.get(cid);
      if (!set) {
        set = new Set();
        this.remoteCallbacks.set(cid, set);
      }
      set.add(stub.id);
      return stub.id;
    };

    return (block) => {
      if (!isCallbackEnvelope(block)) return undefined;
      return { value: this.makeRemoteCallback(track(block), cid) };
    };
  }
}
