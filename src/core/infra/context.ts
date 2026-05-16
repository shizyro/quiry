import type { SessionEvents } from "../../interface/diagnostics";
import type { DiagnosticBus } from "../../lib/diagnostics";
import type { CorrelationId } from "../../interface/types";
import type { AnyTypedPacket } from "../../interface/packets";

import type { SessionState } from "./state";
import type { CallbackBridge } from "./channel/callback-bridge";

/**
 * The shared injection surface every subsystem accepts. The orchestrator
 * constructs a single context and hands it to every subsystem.
 */
export interface SessionContext {
  /** Diagnostic event bus shared across all subsystems on this session. */
  readonly diagnostic: DiagnosticBus<SessionEvents>;
  /** Bridge for handling callback invocations and returns. */
  readonly callbacks: CallbackBridge;
  /** Live session state. Function form so subsystems always observe the latest value. */
  readonly state: () => SessionState;
  /**
   * Posts a packet to the transport, filling `timestamp` and (if absent)
   * a freshly generated `id`. Silently drops on a closed session.
   */
  readonly send: <P extends AnyTypedPacket>(
    packet: Omit<P, "id" | "timestamp"> & { id?: CorrelationId },
  ) => Promise<CorrelationId>;
  /** Generates a fresh correlation id. Mockable in tests for determinism. */
  readonly correlate: () => CorrelationId;
}
