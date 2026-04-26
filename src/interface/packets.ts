import type {
  WireKind,
  WirePacket,
  WireStatus,
  NodeId,
  CorrelationId,
  RequestControl,
  WireError,
} from "./base";

/** Typed wire packet with message type discrimination. */
export interface TypedWirePacket<TKind extends WireKind, TType extends string, TPayload = unknown>
  extends WirePacket<TKind, TPayload> {
  readonly type: TType;
}

type WithRef<T = {}> = T & {
  /** Reference to the original invoke request packet. */
  readonly ref: CorrelationId;
};

/** --------- REQUEST PACKETS --------- */

export const RequestMessageType = { CALL: "call", ABORT: "abort" } as const;
export type RequestMessageType = (typeof RequestMessageType)[keyof typeof RequestMessageType];

export type CallPayload = {
  readonly service: string;
  readonly method: string;
  readonly args: ReadonlyArray<unknown>;
  readonly control?: RequestControl;
};

export interface CallRequestPacket
  extends TypedWirePacket<typeof WireKind.REQUEST, typeof RequestMessageType.CALL, CallPayload> {}

export type AbortPayload = WithRef<{}>;

/**
 * A packet sent by a client to abort an in-flight request.
 *
 * I'm assuming {@link AbortSignal} can't cross thread boundaries, so this
 * is the only way to inform the peer that the request should be aborted.
 */
export interface AbortRequestPacket
  extends TypedWirePacket<typeof WireKind.REQUEST, typeof RequestMessageType.ABORT, AbortPayload> {}

/** --------- RESPONSE PACKETS --------- */

export const ResponseMessageType = { VALUE: "value" } as const;
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

/** --------- STREAM PACKETS --------- */

export const SystemMessageType = {
  HANDSHAKE: "handshake",
  IDENTIFY: "identify",
  IDENTIFY_ACK: "identify_ack",
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
  // Still not sure what to put here...
  // I mean, it shouldn't matter anyway, right?
};

export type IdentifyAckPayload = WithRef<{
  readonly label?: string;
  readonly version?: string;
  readonly metadata?: Record<string, string | number | boolean>;
  // These are probably needed at some point in the future...
}>;

export type DrainPayload = {
  readonly reason?: string;
  readonly graceful?: boolean;
};

export type DrainAckPayload = WithRef<{
  readonly uptime?: number;
}>;

export interface SystemHandshakePacket
  extends Omit<
    TypedWirePacket<typeof WireKind.SYSTEM, typeof SystemMessageType.HANDSHAKE, HandshakePayload>,
    "from"
  > {}

/** A packet sent by a worker node to identify itself to the host. */
export interface SystemIdentifyPacket
  extends TypedWirePacket<typeof WireKind.SYSTEM, typeof SystemMessageType.IDENTIFY, IdentifyPayload> {}

export interface SystemIdentifyAckPacket
  extends TypedWirePacket<
    typeof WireKind.SYSTEM,
    typeof SystemMessageType.IDENTIFY_ACK,
    IdentifyAckPayload
  > {}

export interface SystemDrainPacket
  extends TypedWirePacket<typeof WireKind.SYSTEM, typeof SystemMessageType.DRAIN, DrainPayload> {}

export interface SystemDrainAckPacket
  extends TypedWirePacket<typeof WireKind.SYSTEM, typeof SystemMessageType.DRAIN_ACK, DrainAckPayload> {}

/** --------- UNION TYPES --------- */

export type AnySystemPacket =
  | SystemHandshakePacket
  | SystemIdentifyPacket
  | SystemIdentifyAckPacket
  | SystemDrainPacket
  | SystemDrainAckPacket;

export type AnyRequestPacket = CallRequestPacket | AbortRequestPacket;
export type AnyResponsePacket = ValueResponsePacket;

export type AnyPacket = AnySystemPacket | AnyRequestPacket | AnyResponsePacket;
export type AnyTypedPacket = AnyPacket;

export type PacketByKind<K extends AnyPacket["kind"]> = Extract<AnyPacket, { kind: K }>;
