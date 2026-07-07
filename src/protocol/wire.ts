import type { CorrelationId } from "./types";

/** Wire protocol enums */
export const WireKind = {
  /** A correlated request expecting a response or stream. */
  REQUEST: "REQ",
  /** Responses, chunks, errors, and cancellations — all part of a request exchange. */
  RESPONSE: "RES",
  /** A proxied function argument being invoked by the remote side. */
  CALLBACK: "CBK",
  /** System-level messages; drain, peer discovery, etc. */
  SYSTEM: "SYS",
} as const;
export type WireKind = (typeof WireKind)[keyof typeof WireKind];

export enum WireStatus {
  OK = 0,
  /** Cancelled by the caller. Not a failure. */
  CANCELLED,
  /** Argument invalid, malformed, or failed validation. */
  INVALID_ARGUMENT,
  /** The requested object or property does not exist. */
  NOT_FOUND,
  /** Data lost during transmission. */
  DATA_LOSS,
  /** Cancelled by the callee. Not a failure. */
  ABORTED,
  /** The caller has sent too many requests. */
  RESOURCE_EXHAUSTED,
  /** Operation rejected because the system is not in a valid state for it. */
  FAILED_PRECONDITION,
  /** Unhandled exception or error. */
  INTERNAL,
  /**
   * Handler returned a value that is not of the expected type.
   * @deprecated
   */
  MALFORMED_RESPONSE,
  /** Peer is not reachable or the transport failed. */
  UNAVAILABLE,
  /** Peer is draining and will not accept new work. */
  DRAINING,
  /** Concurrency limit exceeded on this thread or globally. */
  OVERLOADED,
}

/**
 * Base wire packet interface. All wire packets must extend this interface.
 * Payload shape is discriminated at the session layer, not here.
 */
export interface WirePacket<TKind extends WireKind = WireKind, TPayload = unknown> {
  readonly id: CorrelationId;
  readonly kind: TKind;
  readonly timestamp: number;
  readonly payload: TPayload;
  readonly metadata?: Record<string, unknown>;
}

/**
 * The wire representation of an error. Plain object so it can be structured-cloned
 * across a `postMessage` boundary without loss of essential information.
 *
 * Unlike HTTP-style error payloads, we DO propagate `stack` across the wire: the
 * origin node's stack is exactly the context a caller needs to diagnose a remote
 * failure, and dropping it on the floor is what made cross-thread errors useless.
 */
export interface WireError {
  readonly status: Exclude<WireStatus, typeof WireStatus.OK>;
  readonly message: string;
  readonly cid?: CorrelationId;
  readonly detail?: Record<string, unknown>;
  /** Remote origin's stack, if available. Reattached to the local rebuilt error. */
  readonly stack?: string;
  /** Depth-capped by `MAX_CAUSE_DEPTH`. */
  readonly cause?: WireError;
  readonly timestamp: number;
}
