export * from "@/internal";

export { Broker, type BrokerConfig, type BrokerEvents, type BrokerStatus } from "@/core/broker";
export { Worker, type WorkerConfig, type WorkerEvents, type WorkerStatus } from "@/core/client";

export { WireStatus, HeartbeatStatus, type MetricsData } from "@/interface/base";
export { QuiryError } from "@/shared/errors";

import type { Broker } from "@/core/broker";
/** Maps a `Broker<Registry>` (or `never` if `T` is not a broker) to the worker-facing remote registry type. */
export type InferServiceRegistry<T> = T extends Broker<infer R> ? R : never;

export { localNodeId } from "@/shared";
