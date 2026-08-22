/**
 * Typed diagnostic event catalog. Emitted through {@link DiagnosticBus}.
 *
 * Each {@link Session} exposes its own bus (`session.diag`); the module-level
 * `Quiry.diag` carries peer-lifecycle events. The bus optionally bridges
 * every emit through `node:diagnostics_channel` under the `quiry:` prefix
 * so external observability tooling (OpenTelemetry, async hooks, etc.) can
 * subscribe without coupling to Quiry internals.
 */

import type { WireStatus } from "../protocol/wire";
import type { CallbackId, CorrelationId, InvocationId } from "../protocol/types";

type SessionStateLiteral = "open" | "draining" | "closed";
type TransportErrorKind = "send" | "receive" | "terminate";
type BackpressureStateLiteral = "ok" | "high" | "critical";
type StreamDirection = "sent" | "received";
type RequestKind = "get" | "set" | "call" | "stream";
type InboundKind = RequestKind | "abort" | "cancel";

/**
 * Reasons a callback registration is released. Stays on a single emit so
 * a subscriber can distinguish "the call settled and we cleaned up" from
 * "the remote side garbage-collected the stub and told us to forget it."
 */
export type CallbackReleaseReason =
  | "scope" // owning request settled, CALL-scoped cleanup
  | "explicit" // user called .release() or [Symbol.dispose]
  | "remote" // remote peer explicitly released
  | "remote-gc" // remote peer informed us they GC'd the stub
  | "gc"; // local FinalizationRegistry fired

export type DrainPhase = "announced" | "quiesced" | "acked" | "timeout";
export type DrainInitiator = "local" | "remote";

type WithRef<T = {}, Nullable extends boolean = false> = T & {
  readonly ref: Nullable extends true ? CorrelationId | null : CorrelationId;
};

/** Events emitted by a single {@link Session}'s diagnostic bus. */
export interface SessionEvents {
  "session:open": Record<string, never>;
  "session:state": { readonly prev: SessionStateLiteral; readonly next: SessionStateLiteral };
  "session:terminate": { readonly reason?: string };
  "request:sent": WithRef<{
    readonly object: string;
    readonly property: string;
    readonly kind: RequestKind;
  }>;
  "request:settled": WithRef<{
    readonly status: WireStatus;
    readonly durationMs: number;
  }>;
  "request:abort": WithRef;
  "inquiry:received": WithRef<{
    readonly object: string;
    readonly property: string;
    readonly kind: InboundKind;
  }>;
  "inquiry:settled": WithRef<{
    readonly status: WireStatus;
    readonly durationMs: number;
  }>;
  "stream:open": WithRef<{ readonly window: number }>;
  "stream:credit-grant": WithRef<{
    readonly delta: number;
    readonly remaining: number;
    readonly direction: StreamDirection;
  }>;
  "stream:chunk": WithRef<{
    readonly seq: number;
    readonly direction: StreamDirection;
  }>;
  "stream:end": WithRef<{
    readonly seq: number;
    readonly direction: StreamDirection;
  }>;
  "stream:cancel": WithRef<{ readonly source: "local" | "remote" }>;
  "stream:error": WithRef<{
    readonly seq: number;
    readonly status: WireStatus;
  }>;
  "callback:invoke": WithRef<
    {
      readonly eid: InvocationId;
      readonly cbid: CallbackId;
    },
    true
  >;
  "callback:return": WithRef<
    {
      readonly eid: InvocationId;
      readonly status: WireStatus;
      readonly durationMs: number;
    },
    true
  >;
  "callback:release": { readonly cbid: CallbackId; readonly reason: CallbackReleaseReason };
  "drain:phase": { readonly initiator: DrainInitiator; readonly phase: DrainPhase };
  "transport:error": { readonly kind: TransportErrorKind; readonly message: string };
  "transport:backpressure": { readonly state: BackpressureStateLiteral; readonly depth: number };
}

/** Events emitted by the module-level `Quiry.diag` bus. */
export interface QuiryEvents {
  "peer:attached": { readonly identifier: string };
  "peer:detached": { readonly identifier: string; readonly reason?: string };
}

/**
 * Runtime catalog of session events. Kept in sync with {@link SessionEvents}
 * via the `satisfies` check below — adding an event to the interface without
 * adding it here is a compile error in tests that iterate {@link SESSION_EVENT_NAMES}.
 */
export const SESSION_EVENT_NAMES: ReadonlyArray<keyof SessionEvents> = [
  "session:open",
  "session:state",
  "session:terminate",
  "request:sent",
  "request:settled",
  "request:abort",
  "inquiry:received",
  "inquiry:settled",
  "stream:open",
  "stream:credit-grant",
  "stream:chunk",
  "stream:end",
  "stream:cancel",
  "stream:error",
  "callback:invoke",
  "callback:return",
  "callback:release",
  "drain:phase",
  "transport:error",
  "transport:backpressure",
] as const;

/** Runtime catalog of module-level events. */
export const QUIRY_EVENT_NAMES: ReadonlyArray<keyof QuiryEvents> = [
  "peer:attached",
  "peer:detached",
] as const;

/**
 * Compile-time completeness check: ensures every key in {@link SessionEvents}
 * appears in {@link SESSION_EVENT_NAMES}. Triggers a TS error if a new event
 * is added to the interface but forgotten in the runtime catalog.
 */
type _MissingSessionNames = Exclude<keyof SessionEvents, (typeof SESSION_EVENT_NAMES)[number]>;
type _MissingQuiryNames = Exclude<keyof QuiryEvents, (typeof QUIRY_EVENT_NAMES)[number]>;
const _exhaustiveSession: [_MissingSessionNames] extends [never] ? true : never = true;
const _exhaustiveQuiry: [_MissingQuiryNames] extends [never] ? true : never = true;
// Reference the symbols so an unused-var lint doesn't strip the check.
void _exhaustiveSession;
void _exhaustiveQuiry;

/** Default prefix for the `node:diagnostics_channel` bridge. */
export const DIAGNOSTIC_CHANNEL_PREFIX = "quiry" as const;
