import EventEmitter from "node:events";

import { Worker as NJSWorker, type WorkerOptions as NJSWorkerOptions } from "node:worker_threads";
import { fork, type ForkOptions } from "node:child_process";

import {
  Session,
  type InquiryRequest,
  type InquiryFunc,
  type OmitStandardFields,
  type SessionConfig,
} from "@/core/session";
import {
  SystemMessageType,
  type SystemIdentifyAckPacket,
  type SystemIdentifyPacket,
} from "@/interface/packets";

import type { BackpressureSnapshot, Transport } from "./transport";
import { WorkerThreadsTransport } from "./transport/worker-threads";
import { ChildProcessTransport } from "./transport/child-process";

import { HeartbeatStatus, WireKind, WireStatus, type MetricsData, type NodeId } from "@/interface/base";
import type { ServiceRegistry } from "@/interface/transformers";

import { QuiryError } from "@/shared/errors";
import { clip } from "@/lib/helpers";

export interface PeerHandle {
  readonly id: NodeId;
  readonly label?: string;
  readonly session: Session;
  readonly info: PeerInfo;
}

export interface PeerInfo {
  readonly health: PeerHealth;
  readonly heartbeat: { missed: number; last: number };
  readonly connectedAt: number;
  readonly lastActivity: number;
  readonly backpressure: BackpressureSnapshot;
}

export interface PeerHealth {
  readonly status: HeartbeatStatus;
  readonly updatedAt: number;
  readonly metrics?: MetricsData;
}

export interface BrokerConfig {
  readonly label?: string;
  readonly session?: SessionConfig;
  readonly identifyTimeout?: number;
  readonly heartbeat?: {
    readonly interval?: number;
    readonly timeout?: number;
    readonly maxMissed?: number;
    readonly checkInterval?: number;
  };
}

export interface BrokerEvents {
  "peer-connected": [handle: PeerHandle];
  "peer-disconnected": [handle: PeerHandle, reason?: string];
  "peer-health": [handle: PeerHandle, prev: HeartbeatStatus];
  shutdown: [reason?: string];
  error: [error: Error];
}

/**
 * Host-side registry: exposes services, attaches worker transports, runs identify + heartbeat.
 */
export class Broker<TServices extends ServiceRegistry> extends EventEmitter<BrokerEvents> {
  private readonly config: DeepRequired<Omit<BrokerConfig, "session">> & Pick<BrokerConfig, "session">;

  private readonly services = new Map<keyof TServices, object>();
  private readonly workers = new Map<NodeId, PeerHandle>();

  #heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  #isShuttingDown: boolean = false;

  constructor(
    config: BrokerConfig = {},
    private readonly logger: Logger | null = null,
  ) {
    super();

    this.config = {
      label: config.label ?? "broker",
      session: config.session ?? {},
      identifyTimeout: config.identifyTimeout ?? 10_000,
      heartbeat: {
        interval: config.heartbeat?.interval ?? 10_000,
        timeout: config.heartbeat?.timeout ?? 5000,
        maxMissed: config.heartbeat?.maxMissed ?? 3,
        checkInterval: config.heartbeat?.checkInterval ?? 15_000,
      },
    };
  }

  get status(): BrokerStatus {
    return {
      peers: this.workers.size,
      services: Array.from(this.services.keys()) as string[],
      inflight: { calls: 0, streams: 0 }, // TODO: count inflight calls and streams
    };
  }

  // --------- PUBLIC API: PEER MANAGEMENT --------- //

  /** Forks a child process at `filename` and attaches it as a new peer via {@link ChildProcessTransport}. */
  fork(filename: string | URL, options: ForkOptions = {}): Promise<PeerHandle> {
    const subprocess = fork(filename, options);
    return this.attach(new ChildProcessTransport({ child: subprocess }));
  }

  /** Spawns a worker thread at `filename` and attaches it as a new peer via {@link WorkerThreadsTransport}. */
  spawn(filename: string | URL, options: NJSWorkerOptions = {}): Promise<PeerHandle> {
    const worker = new NJSWorker(filename, options);
    return this.attach(new WorkerThreadsTransport({ worker }));
  }

  /**
   * Opens a session, runs identify, registers the peer. On duplicate node id, closes the session
   * and throws {@link QuiryError} `FAILED_PRECONDITION`. On identify failure, closes before rethrowing.
   */
  async attach(transport: Transport): Promise<PeerHandle> {
    const session = await new Session(
      transport,
      this.inquiry.bind(this),
      this.config.session,
      this.logger,
    ).open();

    const { from: peerId, payload } = await this.identify(session).catch(async (error: unknown) => {
      // Identify failed — tear the transport down before surfacing the
      // error to the caller so we don't leak the worker/port.
      await session.close().catch(() => {
        // best-effort; already failing
      });
      throw error;
    });

    if (this.workers.has(peerId)) {
      // Await the close so the transport is actually gone before the caller observes the error.
      await session.close().catch(() => null);
      throw new QuiryError(WireStatus.FAILED_PRECONDITION, `Worker with id ${peerId} already registered`, {
        detail: { peerId },
      });
    }

    const now = Date.now();
    const handle: PeerHandle = {
      id: peerId,
      label: payload.label,
      session,
      info: {
        connectedAt: now,
        lastActivity: now,
        backpressure: { state: "ok", depth: 0, updatedAt: now },
        heartbeat: { missed: 0, last: now },
        health: {
          status: HeartbeatStatus.HEALTHY,
          updatedAt: now,
        },
      },
    };

    this.workers.set(peerId, handle);

    session.on(
      "terminate",
      (reason?: string) => {
        if (this.workers.delete(peerId)) {
          this.emit("peer-disconnected", handle, reason);
          this.logger?.info(`Worker ${peerId}(${handle.label ?? "unknown"}) disconnected`);
        }
      },
      { once: true },
    );

    // Might want to check how this is cleaned up when the peer is detached...
    session.intercept(
      WireKind.SYSTEM,
      (packet) => packet.type === SystemMessageType.HEARTBEAT,
      ({ payload, timestamp }) => {
        const { status, metrics } = payload;
        const now = Date.now();

        // Update heartbeat stats
        handle.info.heartbeat.last = now;
        handle.info.heartbeat.missed = 0;

        const previousStatus = handle.info.health.status;
        Object.assign(handle.info.health, { status, updatedAt: now, metrics });
        if (previousStatus !== status) {
          this.logger?.warn(`Worker ${peerId} heartbeat status changed from ${previousStatus} to ${status}`);
          this.emit("peer-health", handle, previousStatus);
        }

        this.logger?.debug(`Heartbeat received from worker ${peerId} [${now - timestamp}ms]`, {
          detail: { status, metrics },
        });
        return true;
      },
    );

    this.logger?.info(`Worker ${peerId}(${handle.label ?? "unknown"}) attached`);
    this.emit("peer-connected", handle);

    if (!this.#heartbeatTimer) this.startHeartbeatMonitor();
    return handle;
  }

  private async identify(session: Session): Promise<SystemIdentifyAckPacket> {
    const ref = await session.send({
      kind: WireKind.SYSTEM,
      type: SystemMessageType.IDENTIFY,
      payload: {
        heartbeatInterval: this.config.heartbeat.interval,
      },
    } satisfies OmitStandardFields<SystemIdentifyPacket>);

    this.logger?.debug(`Waiting for identify ack packet from worker (${clip(ref)})`);
    return await session
      .wait(
        WireKind.SYSTEM,
        (packet): packet is SystemIdentifyAckPacket =>
          packet.type === SystemMessageType.IDENTIFY_ACK && packet.payload.ref === ref,
        { timeout: this.config.identifyTimeout },
      )
      .then((packet) => {
        this.logger?.debug("Acknowledged identify packet from worker");
        return packet;
      })
      .catch((error) => {
        this.logger?.error("Failed to identify worker", { error });
        throw error;
      });
  }

  /** @param kill When false (default), session closes cooperatively; when true, underlying worker may be force-stopped via transport. */
  async detach(id: string | number | NodeId, kill: boolean = false): Promise<void> {
    const peerId = String(id) as NodeId;
    const handle = this.workers.get(peerId);
    if (!handle) return;

    await handle.session.close("detached", !kill);
    this.workers.delete(peerId);

    this.logger?.info(`Worker ${peerId}(${handle.label ?? "unknown"}) detached`);
    this.emit("peer-disconnected", handle);

    if (this.workers.size === 0 && this.#heartbeatTimer) {
      clearInterval(this.#heartbeatTimer);
      this.#heartbeatTimer = null;
    }
  }

  peer(id: string | number | NodeId): PeerHandle | undefined {
    return this.workers.get(String(id) as NodeId);
  }

  peers(): IterableIterator<PeerHandle> {
    return this.workers.values();
  }

  // --------- PUBLIC API: SERVICES --------- //

  /**
   * Registers a service impl for remote dispatch. Duplicate names throw {@link QuiryError} `FAILED_PRECONDITION`.
   * @returns Same broker instance with narrowed registry type for chaining.
   */
  expose<TName extends string, TImpl extends object>(
    name: TName,
    impl: TImpl,
  ): Broker<TServices & { [K in TName]: TImpl }> {
    if (this.services.has(name)) {
      throw new QuiryError(WireStatus.FAILED_PRECONDITION, `Service ${name} already exposed`, {
        detail: { service: name },
      });
    }

    this.services.set(name, impl);
    return this;
  }

  delete<TName extends string>(name: TName): Broker<TServices & { [K in TName]: never }> {
    this.services.delete(name);
    return this;
  }

  /**
   * Closes all peer sessions (graceful by default), stops the heartbeat monitor,
   * and clears the service registry. Idempotent — subsequent calls return immediately.
   */
  async shutdown(reason?: string, graceful: boolean = true): Promise<void> {
    if (this.#isShuttingDown) return;
    this.#isShuttingDown = true;

    const start = Date.now();
    if (this.#heartbeatTimer) {
      clearInterval(this.#heartbeatTimer);
      this.#heartbeatTimer = null;

      this.logger?.debug("Heartbeat monitor stopped");
    }

    for (const peer of this.workers.values()) {
      await peer.session.close(reason, graceful);
      this.logger?.debug(`Worker ${peer.id}(${peer.label ?? "unknown"}) closed`);
    }

    this.workers.clear();
    this.services.clear();

    this.emit("shutdown", reason);
    this.logger?.info(`Broker shutdown complete in ${Date.now() - start}ms`);
  }

  // --------- INTERNALS: INQUIRY --------- //

  private inquiry(request: InquiryRequest): ReturnType<InquiryFunc> {
    this.logger?.trace(`Received inquiry request with id ${clip(request.id)}`);
    const impl = this.services.get(request.service);
    if (!impl) {
      throw new QuiryError(WireStatus.NOT_FOUND, `Service ${request.service} not found`, {
        correlationId: request.id,
        traceId: request.control?.traceId,
        detail: { query: { service: request.service, method: request.method } },
      });
    }

    if (!(request.method in impl) || typeof impl[request.method as keyof typeof impl] !== "function") {
      throw new QuiryError(
        WireStatus.NOT_FOUND,
        `Method ${request.method} not found in service ${request.service}`,
        {
          correlationId: request.id,
          traceId: request.control?.traceId,
          detail: { query: { service: request.service, method: request.method } },
        },
      );
    }

    this.logger?.trace(
      `Invoking method ${request.service}.${request.method} with ${request.args.length} arguments`,
    );

    const fn = impl[request.method as keyof typeof impl] as (...args: unknown[]) => Promise<unknown>;
    return fn.apply(impl, request.args as unknown[]);
  }

  // --------- INTERNALS: HEARTBEAT MONITOR --------- //

  private startHeartbeatMonitor(): void {
    this.#heartbeatTimer = setInterval(() => {
      const now = Date.now();
      for (const [peerId, handle] of this.workers.entries()) {
        const timeSinceLastHeartbeat = now - handle.info.heartbeat.last;
        if (timeSinceLastHeartbeat > this.config.heartbeat.timeout) {
          handle.info.heartbeat.missed++;

          if (handle.info.heartbeat.missed >= this.config.heartbeat.maxMissed) {
            // Worker is considered dead, detach it.
            this.logger?.error(
              `Worker ${peerId} heartbeat missed ${this.config.heartbeat.maxMissed} times, detaching`,
            );
            void handle.session.close("degraded", true);
            this.workers.delete(peerId);

            this.emit("peer-disconnected", handle, "heartbeat missed");
            continue;
          }

          // Update health status to DEGRADED
          const previousStatus = handle.info.health.status;
          Object.assign(handle.info.health, { status: HeartbeatStatus.DEGRADED, updatedAt: now });
          if (previousStatus !== HeartbeatStatus.DEGRADED) {
            this.logger?.warn(
              `Worker ${peerId} heartbeat degraded from ${previousStatus} to ${HeartbeatStatus.DEGRADED}`,
            );
            this.emit("peer-health", handle, previousStatus);
          }

          this.logger?.debug(
            `Worker ${peerId} heartbeat missed (${handle.info.heartbeat.missed} of ${this.config.heartbeat.maxMissed})`,
          );
        }
      }
    }, this.config.heartbeat.checkInterval);

    this.logger?.debug(`Heartbeat monitor started with interval ${this.config.heartbeat.checkInterval}ms`);
  }
}

export interface BrokerStatus {
  readonly peers: number;
  readonly services: ReadonlyArray<string>;
  readonly inflight: { calls: number; streams: number };
  // ...
}
