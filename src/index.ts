import type { Broker } from "@/core/broker";

export { Broker, type BrokerConfig, type BrokerEvents } from "@/core/broker";
export { Worker, type WorkerConfig, type WorkerEvents } from "@/core/client";

export { Session, type SessionConfig, type SessionState } from "@/core/session";

export { TransportError, type Transport, type TransportOptions } from "@/core/transport";
export { WorkerThreadsTransport } from "@/core/transport/worker-threads";
export { ChildProcessTransport } from "@/core/transport/child-process";

import type { MappedServiceRegistry } from "@/interface/transformers";
export type InferServiceRegistry<T> = MappedServiceRegistry<T extends Broker<infer R> ? R : never>;

export { localNodeId } from "@/shared";
