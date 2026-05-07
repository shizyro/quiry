export { Session, type SessionConfig, type SessionState, type SessionStatus } from "./core/session";
export {
  TransportError,
  type Transport,
  type TransportOptions,
  type TransportEvents,
} from "./core/transport";

export { BaseTransport } from "./core/transport/base";
export { ChildProcessTransport } from "./core/transport/impl/child-process";
export { WorkerThreadsTransport } from "./core/transport/impl/worker-threads";

export * from "./interface/packets";
export {
  WireKind,
  WireStatus,
  type WirePacket,
  type WireError,
  type MetricsData,
  type RequestControl,
  type RetryPolicy,
} from "./interface/protocol";
export type { CallbackId, InvocationId, CorrelationId, TraceId } from "./interface/types";
