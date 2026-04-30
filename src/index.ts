export * from "@/internal";

export { Broker, type BrokerConfig, type BrokerEvents, type BrokerStatus } from "@/core/broker";
export { Worker, type WorkerConfig, type WorkerEvents, type WorkerStatus } from "@/core/client";

export { WireStatus, HeartbeatStatus, type MetricsData } from "@/interface/base";
export { QuiryError } from "@/lib/errors";

import type { Broker } from "@/core/broker";
import type { MappedServiceRegistry } from "@/interface/transformers";
export type InferServiceRegistry<T> = MappedServiceRegistry<T extends Broker<infer R> ? R : never>;

export { localNodeId } from "@/shared";
