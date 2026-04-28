declare const __brand: unique symbol;
type Brand<T, TBrand extends string> = T & { __brand: TBrand };

// Base identifiers with branded types for compile-time safety
export type NodeId = Brand<string, "NodeId">;
export type TraceId = Brand<string, "TraceId">;
export type CorrelationId = Brand<string, "CorrelationId">;

export type CallbackId = Brand<string, "CallbackId">;
export type InvocationId = Brand<string, "InvocationId">;

/** Wire protocol enums */
export const WireKind = {
  /** A correlated request expecting a response or stream. */
  REQUEST: "REQ",
  /** Responses, chunks, errors, and cancellations — all part of a request exchange. */
  RESPONSE: "RES",
  /** A proxied function argument being invoked by the remote side. */
  CALLBACK: "CBK",
  /** System-level messages; handshake, heartbeat, drain, peer discovery. */
  SYSTEM: "SYS",
} as const;
export type WireKind = (typeof WireKind)[keyof typeof WireKind];

export enum WireStatus {
  OK = 0,
  /** Cancelled by the caller. Not a failure. */
  CANCELLED,
  /** Argument invalid, malformed, or failed validation. */
  INVALID_ARGUMENT,
  /** Deadline expired before the call could be completed. */
  DEADLINE_EXCEEDED,
  /** The requested service or method does not exist. */
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
  /** Handler returned a value that is not of the expected type. */
  MALFORMED_RESPONSE,
  /** The operation is not implemented on the service. */
  UNIMPLEMENTED,
  /** Peer is not reachable or the transport failed. */
  UNAVAILABLE,
  /** Peer died. */
  PEER_GONE,
  /** Peer is draining and will not accept new work. */
  DRAINING,
  /** A broker relay loop was detected. */
  ROUTING_LOOP,
  /** Concurrency limit exceeded on this service or globally. */
  OVERLOADED,
}

/**
 * Base wire packet interface. All wire packets must extend this interface.
 * Payload shape is discriminated at the session layer, not here.
 */
export interface WirePacket<TKind extends WireKind = WireKind, TPayload = unknown> {
  readonly id: CorrelationId;
  readonly kind: TKind;
  readonly from: NodeId;
  readonly to?: NodeId;
  readonly timestamp: number;
  readonly payload: TPayload;
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
  readonly origin: NodeId;
  readonly reference?: CorrelationId;
  readonly traceId?: TraceId;
  readonly detail?: Record<string, unknown>;
  /** Remote origin's stack, if available. Reattached to the local rebuilt error. */
  readonly stack?: string;
  /** Depth-capped by `MAX_CAUSE_DEPTH`. */
  readonly cause?: WireError;
  readonly timestamp: number;
}

/** Request control options; used to define request behavior */
export interface RequestControl {
  readonly timeout?: number;
  readonly retry?: RetryPolicy;
  readonly abortable?: boolean;
  readonly traceId?: TraceId;
}

export interface RetryPolicy {
  readonly maxAttempts?: number;
  readonly backoff?: "fixed" | "exponential";
  readonly delay?: number;
}
