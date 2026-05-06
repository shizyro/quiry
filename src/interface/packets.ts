import type {
  NodeId,
  CorrelationId,
  InvocationId,
  CallbackId,
  WireKind,
  WirePacket,
  WireError,
  WireStatus,
  MetricsData,
  HeartbeatStatus,
  RequestControl,
} from "./base";

/** Typed wire packet with message type discrimination. */
export interface TypedWirePacket<TKind extends WireKind, TType extends string, TPayload = unknown>
  extends WirePacket<TKind, TPayload> {
  readonly type: TType;
}

type WithRef<T = {}, Nullable extends boolean = false> = T & {
  /** Reference to the original invoke request packet. */
  readonly ref: Nullable extends true ? CorrelationId | null : CorrelationId;
};

// --------- REQUEST PACKETS --------- //

export const RequestMessageType = { CALL: "call", ABORT: "abort", CANCEL: "cancel", GET: "get" } as const;
export type RequestMessageType = (typeof RequestMessageType)[keyof typeof RequestMessageType];

export type CallPayload = {
  readonly service: string;
  readonly method: string;
  readonly args: ReadonlyArray<unknown>;
  readonly control?: RequestControl;
};

export interface CallRequestPacket
  extends TypedWirePacket<typeof WireKind.REQUEST, typeof RequestMessageType.CALL, CallPayload> {}

export type AbortPayload = WithRef;

/**
 * A packet sent by a client to abort an in-flight request.
 *
 * I'm assuming {@link AbortSignal} can't cross thread boundaries, so this
 * is the only way to inform the peer that the request should be aborted.
 */
export interface AbortRequestPacket
  extends TypedWirePacket<typeof WireKind.REQUEST, typeof RequestMessageType.ABORT, AbortPayload> {}

export type CancelPayload = WithRef;

/** A request to cancel an ongoing stream operation. */
export interface CancelRequestPacket
  extends TypedWirePacket<typeof WireKind.REQUEST, typeof RequestMessageType.CANCEL, CancelPayload> {}

export type GetPayload = {
  readonly service: string;
  readonly property: string;
};

export interface GetRequestPacket
  extends TypedWirePacket<typeof WireKind.REQUEST, typeof RequestMessageType.GET, GetPayload> {}

// --------- RESPONSE PACKETS --------- //

export const ResponseMessageType = { VALUE: "value", STREAM: "stream" } as const;
export type ResponseMessageType = (typeof ResponseMessageType)[keyof typeof ResponseMessageType];

export type ValueResultPayload<TResult = unknown> = WithRef<{
  readonly status: typeof WireStatus.OK;
  readonly result: TResult;
}>;

export type ValueErrorPayload = WithRef<{
  readonly status: Exclude<WireStatus, typeof WireStatus.OK>;
  readonly error: WireError;
}>;

export type ValueResponsePayload<TResult = unknown> = ValueResultPayload<TResult> | ValueErrorPayload;

export interface ValueResponsePacket
  extends TypedWirePacket<typeof WireKind.RESPONSE, typeof ResponseMessageType.VALUE, ValueResponsePayload> {}

export type StreamChunkPayload<TChunk = unknown> = WithRef<{
  readonly seq: number;
  readonly chunk: TChunk;
}>;

export type StreamCreditPayload = WithRef<{
  /**
   * Additional chunks the producer is permitted to send on top of whatever
   * budget it already holds. Delta, not absolute: each grant extends the
   * producer's remaining window by this amount.
   */
  readonly credit: number;
}>;

export type StreamEndPayload = WithRef<{
  /** Expected next sequence number, for gap detection. */
  readonly seq: number;
}>;

export type StreamErrorPayload = WithRef<{
  readonly seq: number;
  readonly error: WireError;
}>;

/** Single packet type, discriminated by the internal `event` property. */
export type StreamResponsePayload<TChunk = unknown> =
  | ({ readonly event: "chunk" } & StreamChunkPayload<TChunk>)
  | ({ readonly event: "credit" } & StreamCreditPayload)
  | ({ readonly event: "end" } & StreamEndPayload)
  | ({ readonly event: "error" } & StreamErrorPayload);

export interface StreamResponsePacket
  extends TypedWirePacket<
    typeof WireKind.RESPONSE,
    typeof ResponseMessageType.STREAM,
    StreamResponsePayload
  > {}

// --------- CALLBACK PACKETS --------- //

export const CallbackMessageType = { INVOKE: "invoke", RETURN: "return", RELEASE: "release" } as const;
export type CallbackMessageType = (typeof CallbackMessageType)[keyof typeof CallbackMessageType];

export type CallbackInvokePayload = WithRef<{
  readonly eid: InvocationId;
  readonly callback: CallbackId;
  readonly args: ReadonlyArray<unknown>;
}>;

export type CallbackReturnPayload = WithRef<{
  readonly eid: InvocationId;
  readonly callback: CallbackId;
}> &
  (
    | { readonly status: typeof WireStatus.OK; readonly result: unknown }
    | { readonly status: Exclude<WireStatus, typeof WireStatus.OK>; readonly error: WireError }
  );

export type CallbackReleasePayload = WithRef<{ readonly callbacks: ReadonlyArray<CallbackId> }, true>;

export interface CallbackInvokePacket
  extends TypedWirePacket<
    typeof WireKind.CALLBACK,
    typeof CallbackMessageType.INVOKE,
    CallbackInvokePayload
  > {}
export interface CallbackReturnPacket
  extends TypedWirePacket<
    typeof WireKind.CALLBACK,
    typeof CallbackMessageType.RETURN,
    CallbackReturnPayload
  > {}
export interface CallbackReleasePacket
  extends TypedWirePacket<
    typeof WireKind.CALLBACK,
    typeof CallbackMessageType.RELEASE,
    CallbackReleasePayload
  > {}

// --------- SYSTEM PACKETS --------- //

export const SystemMessageType = {
  HANDSHAKE: "handshake",
  IDENTIFY: "identify",
  IDENTIFY_ACK: "identify_ack",
  HEARTBEAT: "heartbeat", // Heartbeat does not need an ack... right?
  DRAIN: "drain",
  DRAIN_ACK: "drain_ack",
} as const;
export type SystemMessageType = (typeof SystemMessageType)[keyof typeof SystemMessageType];

export type HandshakePayload = {
  readonly nodeId: NodeId;
  readonly threadId: number;
  readonly pid: number;
};

export type IdentifyPayload = {
  // Might need to add more fields here...
};

export type IdentifyAckPayload = WithRef<{
  readonly services: ReadonlyArray<string>;
  // ...
}>;

export type HeartbeatPayload = {
  readonly status: HeartbeatStatus;
  readonly metrics?: MetricsData;
};

export type DrainPayload = {
  readonly reason?: string;
  readonly timeout?: number;
};

export type DrainAckPayload = WithRef<{
  readonly uptime?: number;
}>;

/** @deprecated */
export interface SystemHandshakePacket
  extends Omit<
    TypedWirePacket<typeof WireKind.SYSTEM, typeof SystemMessageType.HANDSHAKE, HandshakePayload>,
    "from"
  > {}

/**
 * A packet sent to identify itself to the peer.
 * @deprecated
 */
export interface SystemIdentifyPacket
  extends TypedWirePacket<typeof WireKind.SYSTEM, typeof SystemMessageType.IDENTIFY, IdentifyPayload> {}

/** @deprecated */
export interface SystemIdentifyAckPacket
  extends TypedWirePacket<
    typeof WireKind.SYSTEM,
    typeof SystemMessageType.IDENTIFY_ACK,
    IdentifyAckPayload
  > {}

/** @deprecated */
export interface SystemHeartbeatPacket
  extends TypedWirePacket<typeof WireKind.SYSTEM, typeof SystemMessageType.HEARTBEAT, HeartbeatPayload> {}

export interface SystemDrainPacket
  extends TypedWirePacket<typeof WireKind.SYSTEM, typeof SystemMessageType.DRAIN, DrainPayload> {}

export interface SystemDrainAckPacket
  extends TypedWirePacket<typeof WireKind.SYSTEM, typeof SystemMessageType.DRAIN_ACK, DrainAckPayload> {}

// --------- UNION TYPES --------- //

export type AnySystemPacket =
  | SystemHandshakePacket
  | SystemIdentifyPacket
  | SystemIdentifyAckPacket
  | SystemHeartbeatPacket
  | SystemDrainPacket
  | SystemDrainAckPacket;
export type AnyRequestPacket =
  | CallRequestPacket
  | AbortRequestPacket
  | CancelRequestPacket
  | GetRequestPacket;
export type AnyResponsePacket = ValueResponsePacket | StreamResponsePacket;
export type AnyCallbackPacket = CallbackInvokePacket | CallbackReturnPacket | CallbackReleasePacket;

export type AnyPacket = AnySystemPacket | AnyRequestPacket | AnyResponsePacket | AnyCallbackPacket;
export type AnyTypedPacket = AnyPacket;

export type PacketByKind<K extends AnyPacket["kind"]> = Extract<AnyPacket, { kind: K }>;
