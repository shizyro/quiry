import EventEmitter from "node:events";
import { Worker, type WorkerOptions } from "node:worker_threads";

import {
  Session,
  type InquiryRequest,
  type InquiryFunc,
  type OmitStandardFields,
  type SessionState,
} from "@/core/session";
import { WorkerThreadsTransport } from "@/core/transport/worker-threads";

import { QuiryError } from "@/lib/errors";

import {
  SystemMessageType,
  type SystemIdentifyAckPacket,
  type SystemIdentifyPacket,
} from "@/interface/packets";
import { WireKind, WireStatus, type NodeId } from "@/interface/base";
import type { ServiceRegistry } from "@/interface/transformers";

import { clip } from "@/lib/utils";

export interface QuiryBrokerConfig {
  readonly identifyTimeout?: number;
  // ...
}

export interface PeerDescriptor {
  readonly id: NodeId;
  readonly label?: string;
  readonly version?: string;
  readonly metadata?: Record<string, string | number | boolean>;
}

export interface PeerHandle {
  readonly worker: Worker;
  readonly descriptor: PeerDescriptor;
  readonly session: Session;
}

export interface PeerInfo {
  readonly id: NodeId;
  // ...
}

type ServiceImpl = object;
export type InferServiceRegistry<T> = T extends QuiryBroker<infer R> ? R : never;

export interface QuiryBrokerEvents {
  "peer-added": [handle: PeerHandle];
  "peer-removed": [handle: PeerHandle];
  shutdown: [reason?: string];
}

export class QuiryBroker<
  TServices extends ServiceRegistry = { [key: string]: ServiceImpl },
> extends EventEmitter<QuiryBrokerEvents> {
  private readonly config: Required<QuiryBrokerConfig>;

  private readonly services = new Map<keyof TServices, ServiceImpl>();
  private readonly peers = new Map<NodeId, PeerHandle>();

  constructor(
    config: QuiryBrokerConfig = {},
    private readonly logger: Logger | null = null,
  ) {
    super();

    this.config = {
      identifyTimeout: config.identifyTimeout ?? 10_000,
      // ...
    };
  }

  /** --------- PUBLIC API: PEER MANAGEMENT --------- */

  spawn(filename: string | URL, options: WorkerOptions = {}): Promise<PeerDescriptor> {
    const worker = new Worker(filename, options);
    return this.attach(worker);
  }

  async attach(worker: Worker): Promise<PeerDescriptor> {
    const transport = new WorkerThreadsTransport({ worker });
    const session = await new Session(
      transport,
      {
        inquiry: this.inquiry.bind(this),
      },
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

    const handle = {
      descriptor: {
        id: peerId,
        label: payload.label,
        version: payload.version,
        metadata: payload.metadata,
      },
      worker,
      session,
    } satisfies PeerHandle;

    this.peers.set(peerId, handle);

    session.once("close", () => {
      if (this.peers.delete(peerId)) {
        this.emit("peer-removed", handle);
        this.logger?.info(`Worker ${peerId}(${handle.descriptor.label ?? "unknown"}) disconnected`);
      }
    });

    this.logger?.info(`Worker ${peerId}(${handle.descriptor.label ?? "unknown"}) attached`);
    this.emit("peer-added", handle);
    return handle.descriptor;
  }

  private async identify(session: Session): Promise<SystemIdentifyAckPacket> {
    const ref = await session.send({
      kind: WireKind.SYSTEM,
      type: SystemMessageType.IDENTIFY,
      payload: {},
    } satisfies OmitStandardFields<SystemIdentifyPacket>);

    this.logger?.debug(`Waiting for identify ack packet from worker (${clip(ref)})`);
    return await session
      .wait(
        WireKind.SYSTEM,
        (packet): packet is SystemIdentifyAckPacket =>
          packet.type === SystemMessageType.IDENTIFY_ACK && packet.payload.ref === ref,
        this.config.identifyTimeout,
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
    this.emit("peer-removed", handle);
  }

  /** --------- PUBLIC API: SERVICES --------- */

  expose<TName extends string, TImpl extends object>(
    name: TName,
    impl: TImpl,
  ): QuiryBroker<TServices & { [K in TName]: TImpl }> {
    if (this.services.has(name)) {
      throw new QuiryError(WireStatus.FAILED_PRECONDITION, `Service ${name} already exposed`, {
        detail: { service: name },
      });
    }

    this.services.set(name, impl);
    return this;
  }

  delete<TName extends string>(name: TName): QuiryBroker<TServices & { [K in TName]: never }> {
    this.services.delete(name);
    return this;
  }

  async shutdown(graceful: boolean = true): Promise<void> {
    const start = Date.now();
    for (const peer of this.peers.values()) {
      await peer.session.close(!graceful);
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
}
