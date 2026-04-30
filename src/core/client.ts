import EventEmitter from "node:events";

import {
  Session,
  normalized,
  type InquiryFunc,
  type InquiryRequest,
  type OmitStandardFields,
  type SessionConfig,
} from "@/core/session";
import type { Transport } from "@/core/transport";

import {
  HeartbeatStatus,
  WireKind,
  WireStatus,
  type MetricsData,
  type NodeId,
  type RequestControl,
} from "@/interface/base";
import { SystemMessageType, type SystemIdentifyAckPacket } from "@/interface/packets";
import type { RemoteServiceDefinition, ServiceRegistry } from "@/interface/transformers";

import { attachCallerStack, captureCallerStack, QuiryError } from "@/lib/errors";
import { getMemoryUsage } from "@/lib/helpers";

export type CallbackHandle<T extends Function> = T & {
  release(): boolean;
  [Symbol.dispose](): void;
};

export interface HostHandle {
  readonly id: NodeId;
  readonly label?: string;
  readonly session: Session;
  readonly connectedAt: number;
}

export interface WorkerConfig {
  readonly label?: string;
  readonly session?: SessionConfig;
  readonly heartbeat?: {
    readonly intervalOverride?: number;
    readonly metrics?: () => Partial<MetricsData> | Promise<Partial<MetricsData>>;
  };
}

export interface WorkerEvents {
  "host-connected": [host: HostHandle];
  "host-disconnected": [host: HostHandle, reason?: string];
  shutdown: [reason?: string];
  error: [error: Error];
}

export class Worker<TServices extends ServiceRegistry> extends EventEmitter<WorkerEvents> {
  private readonly session: Session;
  private readonly config: DeepRequired<Omit<WorkerConfig, "session">> & Pick<WorkerConfig, "session">;
  #heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  #isShuttingDown: boolean = false;

  constructor(
    transport: Transport,
    config: WorkerConfig = {},
    private readonly logger: Logger | null = null,
  ) {
    super();

    this.session = new Session(transport, this.inquiry.bind(this), config.session, this.logger);
    this.config = {
      label: config.label ?? "worker",
      session: config.session ?? {},
      heartbeat: {
        intervalOverride: config.heartbeat?.intervalOverride ?? 30_000,
        metrics:
          config.heartbeat?.metrics ??
          (() => ({
            memory: getMemoryUsage(),
            cpu: { usage: process.cpuUsage() },
            uptime: process.uptime(),
          })),
      },
    };
  }

  get host(): HostHandle | null {
    return this.session.peer
      ? {
          id: this.session.peer,
          label: this.config.label,
          session: this.session,
          connectedAt: this.session.connectedAt,
        }
      : null;
  }

  get status(): WorkerStatus {
    return {
      host: this.session.peer,
      // ...
    };
  }

  async open(): Promise<this> {
    await this.session.open();
    this.logger?.info(`Worker opened for ${this.session}`);

    this.session.on(
      "terminate",
      () => {
        if (this.#heartbeatTimer) {
          clearInterval(this.#heartbeatTimer);
          this.#heartbeatTimer = null;
        }

        this.emit("host-disconnected", this.host!, "session terminated");
      },
      { once: true },
    );

    await this.session
      .wait(WireKind.SYSTEM, (packet) => packet.type === SystemMessageType.IDENTIFY)
      .then((feedback) => {
        this.logger?.trace(`Received identify packet from master node ${feedback.from}`);

        return this.session
          .send({
            kind: WireKind.SYSTEM,
            type: SystemMessageType.IDENTIFY_ACK,
            payload: {
              ref: feedback.id,
              label: this.config.label,
            },
          } satisfies OmitStandardFields<SystemIdentifyAckPacket>)
          .finally(() => this.startPeriodicHeartbeat(feedback.payload.heartbeatInterval));
      });

    this.emit("host-connected", this.host!);
    return this;
  }

  async call(service: string, method: string, ...args: unknown[]): Promise<unknown>;
  async call(service: string, method: string, args: unknown[], options?: RequestControl): Promise<unknown>;
  async call(service: string, method: string, ...rest: unknown[]): Promise<unknown> {
    const [args, options] = splitArgsAndOptions(rest);
    return this.session.request(service, method, args, options);
  }

  /**
   * Open a server-streaming call. The returned iterator yields chunks as
   * they arrive from the remote service.
   */
  stream(service: string, method: string, ...args: unknown[]): AsyncIterableIterator<unknown>;
  stream(
    service: string,
    method: string,
    args: unknown[],
    options?: RequestControl,
  ): AsyncIterableIterator<unknown>;
  stream(service: string, method: string, ...rest: unknown[]): AsyncIterableIterator<unknown> {
    const [args, options] = splitArgsAndOptions(rest);
    return this.session.stream(service, method, args, options);
  }

  service<TName extends keyof TServices>(identifier: TName): RemoteServiceDefinition<TServices[TName]> {
    return new Proxy({} as RemoteServiceDefinition<TServices[TName]>, {
      get: (_, prop) => {
        if (typeof prop !== "string") return undefined;
        const dispatch = (...args: unknown[]) => {
          // Captured here (not inside `route`) so the top frame is
          // the actual user code that invoked the service method,
          // not any of the Proxy/dispatch plumbing.
          const callerStack = captureCallerStack(dispatch);
          const [positional, options] = splitArgsAndOptions(args);
          // The returned value is dual-nature, awaiting it routes to
          // a unary request; iterating it opens a server stream. The
          // RemoteMethod<T> type already narrows to one or the
          // other on a per-method basis, so the caller sees the
          // correct shape and the wrong usage is statically blocked.
          return this.route(identifier as string, prop, positional, options, callerStack);
        };
        return dispatch;
      },
    });
  }

  /**
   * Make a callback handle that can be manually released, or disposed out of scope.
   * This is useful for long-lived callbacks, like event handlers.
   */
  callback<T extends Function>(fn: T): CallbackHandle<T> {
    const stub = this.session.bind(fn);
    const release = (): boolean => this.session.release(stub.id);
    return Object.assign(fn, {
      release,
      [Symbol.dispose]: release,
      [normalized]: stub,
    }) as CallbackHandle<T>;
  }

  async shutdown(reason?: string, graceful: boolean = true): Promise<void> {
    if (this.#isShuttingDown) return;
    this.#isShuttingDown = true;
    const start = Date.now();

    if (this.#heartbeatTimer) {
      clearInterval(this.#heartbeatTimer);
      this.#heartbeatTimer = null;
    }

    await this.session.close(reason, graceful);

    this.emit("shutdown", reason);
    this.logger?.info(`Worker shutdown complete in ${Date.now() - start}ms`);
  }

  private route<T = unknown>(
    service: string,
    method: string,
    args: unknown[],
    options?: RequestControl,
    stack: string = "",
  ): CallOrStream<T> {
    let mode: QueryMode = QueryMode.PENDING;
    let call: Promise<unknown>;
    let iter: AsyncIterableIterator<unknown>;

    const tag = <E>(error: E): E => {
      if (stack) attachCallerStack(error, stack);
      return error;
    };

    const run = (): Promise<unknown> => {
      if (mode === QueryMode.STREAM)
        throw new Error(
          `Cannot await ${service}.${method}(...) — it has already been committed as a stream.`,
        );

      mode = QueryMode.CALL;
      return (call ??= this.session
        .request(service, method, args, options)
        .catch((error: unknown) => Promise.reject(tag(error))));
    };

    const flow = (): AsyncIterableIterator<unknown> => {
      if (mode === QueryMode.CALL)
        throw new Error(
          `Cannot iterate ${service}.${method}(...) — it has already been committed as a unary call.`,
        );

      mode = QueryMode.STREAM;
      if (iter) return iter;

      const source = this.session.stream(service, method, args, options);
      iter = {
        [Symbol.asyncIterator](): AsyncIterableIterator<unknown> {
          return this;
        },
        next: (...x): Promise<IteratorResult<unknown>> =>
          source.next(...x).catch((error: unknown) => Promise.reject(tag(error))),
        return: (value?: unknown): Promise<IteratorResult<unknown>> =>
          source.return ? source.return(value) : Promise.resolve({ value: undefined, done: true }),
        throw: (err?: unknown): Promise<IteratorResult<unknown>> =>
          source.throw ? source.throw(err) : Promise.reject(err),
      };
      return iter;
    };

    // Auto-trigger to call mode if no stream was engaged.
    //! I'm assuming this is risky in terms of semantic predictability...
    //! While its technically safe, it's behaviorally fragile; race conditions may occur in async-delayed use.
    // TODO: decide on a better solution, or perhaps work around it with proper documentation.
    queueMicrotask(() => mode === QueryMode.PENDING && run());

    // This object is deliberately awaitable so `proxy.method(...)` routes through the unary call path.
    return {
      // biome-ignore lint/suspicious/noThenProperty: intentional
      then: <TResult1 = unknown, TResult2 = never>(
        onfulfilled?: ((value: unknown) => TResult1 | PromiseLike<TResult1>) | null,
        onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
      ): Promise<TResult1 | TResult2> => run().then(onfulfilled, onrejected),
      catch: <TResult = never>(
        onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | undefined | null,
      ): Promise<unknown | TResult> => run().catch(onrejected),
      finally: (onfinally?: (() => void) | undefined | null): Promise<unknown> => run().finally(onfinally),

      [Symbol.asyncIterator]: (): AsyncIterableIterator<unknown> => flow(),
      next: (...x): Promise<IteratorResult<unknown>> => flow().next(...x),

      // If the stream was never engaged, there's nothing to clean up.
      return: (value?: unknown): Promise<IteratorResult<unknown>> =>
        mode === QueryMode.STREAM && iter?.return
          ? iter.return(value)
          : Promise.resolve({ value: undefined, done: true }),
      throw: (err?: unknown): Promise<IteratorResult<unknown>> =>
        mode === QueryMode.STREAM && iter?.throw ? iter.throw(err) : Promise.reject(err),
    } as CallOrStream<T>;
  }

  private inquiry(request: InquiryRequest): ReturnType<InquiryFunc> {
    // I suppose I could implement some kind of forwarded request that goes to here.
    throw new QuiryError(WireStatus.UNIMPLEMENTED, "Worker inquiry not implemented");
  }

  private startPeriodicHeartbeat(interval: number = this.config.heartbeat.intervalOverride): void {
    if (this.#heartbeatTimer) clearInterval(this.#heartbeatTimer);
    this.#heartbeatTimer = setInterval(async () => {
      if (!this.session.isConnected()) return;

      try {
        await this.session.send({
          kind: WireKind.SYSTEM,
          type: SystemMessageType.HEARTBEAT,
          payload: {
            status: HeartbeatStatus.HEALTHY,
            metrics: await this.config.heartbeat?.metrics?.(),
          },
        });
      } catch (error: unknown) {
        this.logger?.error(
          `Failed to send heartbeat: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }, interval);

    this.logger?.debug(`Heartbeat started with interval ${interval}ms`);
  }
}

export interface WorkerStatus {
  readonly host: NodeId | null;
  // ...
}

/**
 * A lazy handle returned by the service proxy that commits to either a
 * unary request or a server-stream on first use.
 *
 * The two paths are mutually exclusive: whichever protocol the caller engages
 * first wins, and subsequent attempts to use the other interface throw.
 */
interface CallOrStream<T = unknown>
  extends PromiseLike<T>,
    AsyncIterableIterator<T extends AsyncIterable<infer C> ? C : T> {}

enum QueryMode {
  PENDING = 0,
  CALL,
  STREAM,
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
