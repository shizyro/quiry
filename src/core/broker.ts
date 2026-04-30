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

import { WorkerThreadsTransport } from "@/core/transport/worker-threads";
import { QuiryError } from "@/lib/errors";

import { HeartbeatStatus, WireKind, WireStatus, type MetricsData, type NodeId } from "@/interface/base";
import type { ServiceRegistry } from "@/interface/transformers";

import { clip } from "@/lib/helpers";
import type { Transport } from "./transport";
import { ChildProcessTransport } from "./transport/child-process";

export interface PeerHandle {
  // readonly port: NJSWorker | ChildProcess;
  readonly descriptor: PeerDescriptor;
  readonly session: Session;
  readonly info: PeerInfo;
}

export interface PeerDescriptor {
  readonly id: NodeId;
  readonly label?: string;
  readonly version?: string;
  readonly metadata?: Record<string, string | number | boolean>;
}

export interface PeerInfo {
  readonly health: PeerHealth;
  readonly heartbeat: {
    missed: number;
    last: number;
  };
}

export interface PeerHealth {
  readonly status: HeartbeatStatus;
  readonly updatedAt: number;
  readonly metrics?: MetricsData;
  readonly errors?: readonly string[];
}

export interface BrokerConfig {
  readonly defaultSessionConfig?: SessionConfig;
  readonly identifyTimeout?: number;
  readonly heartbeat?: {
    readonly interval?: number;
    readonly timeout?: number;
    readonly maxMissed?: number;
    readonly monitorInterval?: number;
  };
  // ...
}

export interface BrokerEvents {
  "peer-connected": [handle: PeerHandle];
  "peer-disconnected": [handle: PeerHandle];
  "peer-health-changed": [handle: PeerHandle, health: PeerHealth];
  shutdown: [reason?: string];
}

export class Broker<TServices extends ServiceRegistry> extends EventEmitter<BrokerEvents> {
  private readonly config: DeepRequired<Omit<BrokerConfig, "defaultSessionConfig">> &
    Pick<BrokerConfig, "defaultSessionConfig">;

  private readonly services = new Map<keyof TServices, object>();
  private readonly peers = new Map<NodeId, PeerHandle>();

  #heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  #isShuttingDown: boolean = false;

  constructor(
    config: BrokerConfig = {},
    private readonly logger: Logger | null = null,
  ) {
    super();

    this.config = {
      defaultSessionConfig: config.defaultSessionConfig ?? {},
      identifyTimeout: config.identifyTimeout ?? 10_000,
      heartbeat: {
        interval: config.heartbeat?.interval ?? 10_000,
        timeout: config.heartbeat?.timeout ?? 5000,
        maxMissed: config.heartbeat?.maxMissed ?? 3,
        monitorInterval: config.heartbeat?.monitorInterval ?? 15_000,
      },
      // ...
    };
  }

  /** --------- PUBLIC API: PEER MANAGEMENT --------- */

  fork(filename: string | URL, options: ForkOptions = {}): Promise<PeerHandle> {
    const subprocess = fork(filename, options);
    return this.attach(new ChildProcessTransport({ child: subprocess }));
  }

  spawn(filename: string | URL, options: NJSWorkerOptions = {}): Promise<PeerHandle> {
    const worker = new NJSWorker(filename, options);
    return this.attach(new WorkerThreadsTransport({ worker }));
  }

  async attach(transport: Transport): Promise<PeerHandle> {
    const session = await new Session(
      transport,
      this.inquiry.bind(this),
      this.config.defaultSessionConfig,
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

    if (this.peers.has(peerId)) {
      // Await the close so the transport is actually gone before the caller observes the error.
      await session.close().catch(() => null);
      throw new QuiryError(WireStatus.FAILED_PRECONDITION, `Worker with id ${peerId} already registered`, {
        detail: { peerId },
      });
    }

    const now = Date.now();
    const handle = {
      descriptor: {
        id: peerId,
        label: payload.label,
        version: payload.version,
        metadata: payload.metadata,
      },
      session,
      info: {
        health: {
          status: HeartbeatStatus.HEALTHY,
          updatedAt: now,
        },
        heartbeat: {
          missed: 0,
          last: now,
        },
      },
    } satisfies PeerHandle;

    this.peers.set(peerId, handle);

    session.on(
      "close",
      () => {
        if (this.peers.delete(peerId)) {
          this.emit("peer-disconnected", handle);
          this.logger?.info(`Worker ${peerId}(${handle.descriptor.label ?? "unknown"}) disconnected`);
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
          this.emit("peer-health-changed", handle, handle.info.health);
        }

        this.logger?.debug(`Heartbeat received from worker ${peerId} [${now - timestamp}ms]`, {
          detail: { status, metrics },
        });
        return true;
      },
    );

    this.logger?.info(`Worker ${peerId}(${handle.descriptor.label ?? "unknown"}) attached`);
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

  async detach(id: string | number | NodeId, kill: boolean = false): Promise<void> {
    const peerId = String(id) as NodeId;
    const handle = this.peers.get(peerId);
    if (!handle) return;

    await handle.session.close(kill);
    this.peers.delete(peerId);

    this.logger?.info(`Worker ${peerId}(${handle.descriptor.label ?? "unknown"}) detached`);
    this.emit("peer-disconnected", handle);

    if (this.peers.size === 0 && this.#heartbeatTimer) {
      clearInterval(this.#heartbeatTimer);
      this.#heartbeatTimer = null;
    }
  }

  /** --------- PUBLIC API: SERVICES --------- */

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

  async shutdown(graceful: boolean = true): Promise<void> {
    if (this.#isShuttingDown) return;
    this.#isShuttingDown = true;

    this.logger?.info("Starting shutdown sequence");

    const start = Date.now();
    if (this.#heartbeatTimer) {
      clearInterval(this.#heartbeatTimer);
      this.#heartbeatTimer = null;

      this.logger?.debug("Heartbeat monitor stopped");
    }

    for (const peer of this.peers.values()) {
      await peer.session.close(!graceful);
      this.logger?.debug(`Worker ${peer.descriptor.id}(${peer.descriptor.label ?? "unknown"}) closed`);
    }

    this.peers.clear();
    this.services.clear();

    this.emit("shutdown");
    this.logger?.info(`Broker shutdown complete in ${Date.now() - start}ms`);
  }

  /** --------- INTERNALS: INQUIRY --------- */

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

  /** --------- INTERNALS: HEARTBEAT MONITOR --------- */

  private startHeartbeatMonitor(): void {
    this.#heartbeatTimer = setInterval(() => {
      const now = Date.now();
      for (const [peerId, handle] of this.peers.entries()) {
        const timeSinceLastHeartbeat = now - handle.info.heartbeat.last;
        if (timeSinceLastHeartbeat > this.config.heartbeat.timeout) {
          handle.info.heartbeat.missed++;

          if (handle.info.heartbeat.missed >= this.config.heartbeat.maxMissed) {
            // Worker is considered dead, detach it.
            this.logger?.error(
              `Worker ${peerId} heartbeat missed ${this.config.heartbeat.maxMissed} times, detaching`,
            );
            void handle.session.close(true);
            this.peers.delete(peerId);

            this.emit("peer-disconnected", handle);
            continue;
          }

          // Update health status to DEGRADED
          const previousStatus = handle.info.health.status;
          Object.assign(handle.info.health, { status: HeartbeatStatus.DEGRADED, updatedAt: now });
          if (previousStatus !== HeartbeatStatus.DEGRADED) {
            this.logger?.warn(
              `Worker ${peerId} heartbeat degraded from ${previousStatus} to ${HeartbeatStatus.DEGRADED}`,
            );
            this.emit("peer-health-changed", handle, handle.info.health);
          }

          this.logger?.debug(
            `Worker ${peerId} heartbeat missed (${handle.info.heartbeat.missed} of ${this.config.heartbeat.maxMissed})`,
          );
        }
      }
    }, this.config.heartbeat.monitorInterval);

    this.logger?.debug(`Heartbeat monitor started with interval ${this.config.heartbeat.monitorInterval}ms`);
  }
}
