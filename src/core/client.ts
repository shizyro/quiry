import EventEmitter from "node:events";

import { Session, type OmitStandardFields } from "@/core/session";
import type { Transport } from "@/core/transport";

import { WireKind, type RequestControl } from "@/interface/base";
import { SystemMessageType, type SystemIdentifyAckPacket } from "@/interface/packets";
import type { RemoteServiceDefinition, ServiceRegistry } from "@/interface/transformers";

export interface QuiryClientConfig {
  readonly name?: string;
  readonly version?: string;
  readonly metadata?: Record<string, string | number | boolean>;
  // ...
}

type ServiceImpl = object;
export interface QuiryClientEvents {
  // ...
}

export class QuiryClient<
  TServices extends ServiceRegistry = { [key: string]: ServiceImpl },
> extends EventEmitter<QuiryClientEvents> {
  readonly session: Session;
  private readonly config: Required<QuiryClientConfig>;

  constructor(
    transport: Transport,
    config: QuiryClientConfig = {},
    private readonly logger: Logger | null = null,
  ) {
    super();

    this.session = new Session(transport, {}, this.logger);
    this.config = {
      name: config.name ?? "worker",
      version: config.version ?? "0.0.1",
      metadata: config.metadata ?? {},
      // ...
    };
  }

  async open(): Promise<this> {
    await this.session.open();
    this.logger?.info(`Client opened for ${this.session}`);

    await this.session
      .wait(WireKind.SYSTEM, (packet) => packet.type === SystemMessageType.IDENTIFY)
      .then((feedback) => {
        this.logger?.trace(`Received identify packet from master node ${feedback.from}`);

        return this.session.send({
          kind: WireKind.SYSTEM,
          type: SystemMessageType.IDENTIFY_ACK,
          payload: {
            ref: feedback.id,
            label: this.config.name,
            version: this.config.version,
            metadata: this.config.metadata,
          },
        } satisfies OmitStandardFields<SystemIdentifyAckPacket>);
      });

    return this;
  }

  async call(service: string, method: string, ...args: unknown[]): Promise<unknown>;
  async call(service: string, method: string, args: unknown[], options?: RequestControl): Promise<unknown>;
  async call(service: string, method: string, ...rest: unknown[]): Promise<unknown> {
    const [args, options] = splitArgsAndOptions(rest);
    return this.session.request(service, method, args, options);
  }

  service<TName extends keyof TServices>(identifier: TName): RemoteServiceDefinition<TServices[TName]> {
    return new Proxy({} as RemoteServiceDefinition<TServices[TName]>, {
      get: (_, prop) => {
        if (typeof prop !== "string") return undefined;
        const dispatch = (...args: unknown[]) => {
          // Route the call to the session's request method
          return this.call(identifier as string, prop, ...args);
        };
        return dispatch;
      },
    });
  }
}

/**
 * Accepts either `(arg1, arg2, ...)` or `([arg1, arg2, ...], options)` and
 * normalizes them into `(args, options?)`. The overload is detected by
 * sniffing the last argument for `RequestControl`-shaped keys, so callers
 * can pass positional arguments directly without wrapping them in an array.
 */
function splitArgsAndOptions(rest: unknown[]): [unknown[], RequestControl | undefined] {
  if (
    rest.length > 0 &&
    rest[rest.length - 1] &&
    typeof rest[rest.length - 1] === "object" &&
    ("timeout" in (rest[rest.length - 1] as object) ||
      "retries" in (rest[rest.length - 1] as object) ||
      "signal" in (rest[rest.length - 1] as object))
  ) {
    return [rest.slice(0, -1), rest[rest.length - 1] as RequestControl];
  }
  return [rest, undefined];
}
