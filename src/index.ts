import EventEmitter from "node:events";
import { fork as NJSFork, type ForkOptions as NJSForkOptions } from "node:child_process";
import { Worker as NJSWorker, type WorkerOptions as NJSWorkerOptions } from "node:worker_threads";

import { Normalized, Session, type InquiryFunc, type InquiryRequest } from "@/core/session";
import { WireStatus, type NodeId, type RequestControl } from "@/interface/base";
import type {
  ServiceImpl,
  ServiceRegistry,
  RemotablePropertyKeys,
  RemoteServiceDefinition,
} from "@/interface/transformers";

import type { Transport } from "@/core/transport";
import { ChildProcessTransport } from "@/core/transport/child-process";
import { WorkerThreadsTransport } from "@/core/transport/worker-threads";

import { attachCallerStack, captureCallerStack, QuiryError } from "@/shared/errors";
import { isAnyIterableIterator, isSerializable } from "@/lib/helpers";

import { randomBytes } from "node:crypto";

export interface GlobalServiceRegistry extends Record<Quiry.PeerIdentifier, ServiceRegistry> {}

namespace Quiry {
  export type CallbackHandle<T extends Function> = T & {
    release(): boolean;
    [Symbol.dispose](): void;
    [Symbol.asyncDispose](): void;
  };

  export type PeerIdentifier = string | symbol | number | NodeId;

  export class PeerConnection<
    TIdentifier extends PeerIdentifier = PeerIdentifier,
    TServices extends ServiceRegistry = GlobalServiceRegistry[TIdentifier],
  > {
    private readonly cached = new Map<keyof TServices, ServiceImpl>();
    constructor(
      readonly identifier: TIdentifier,
      private readonly session: Session,
    ) {}

    service<TOverride extends ServiceImpl = never, TName extends string = string>(
      name: TName,
    ): RemoteServiceDefinition<
      [TOverride] extends [never]
        ? TName extends keyof TServices
          ? TServices[TName]
          : ServiceImpl
        : TOverride
    > {
      let proxy = this.cached.get(name);
      if (!proxy) {
        proxy = makeServiceProxy(name as string, this.session);
        this.cached.set(name, proxy);
      }
      // @ts-expect-error; ignore.
      return proxy;
    }

    controlled<TName extends keyof TServices>(
      identifier: TName,
      control: RequestControl,
    ): RemoteServiceDefinition<TServices[TName]> {
      return makeServiceProxy(identifier as string, this.session, control) as RemoteServiceDefinition<
        TServices[TName]
      >;
    }

    /**
     * Make a callback handle that can be manually released, or disposed out of scope.
     * This is useful for long-lived callbacks, like event handlers.
     */
    callback<T extends Function>(fn: T): CallbackHandle<T> {
      if (typeof fn !== "function")
        throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Callback must be a function");
      if (Normalized in fn)
        throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Function is already bound as a callback handle");

      const stub = this.session.bind(fn);
      const release = (): boolean => this.session.release(stub.id);

      // The wrapper still satisfies `typeof === "function"`, so passing it as
      // a callback argument flows through `normalize()` -> `[Normalized]` -> the
      // existing stub, just like a decorated function would.
      const handle = (...args: unknown[]): unknown =>
        (fn as unknown as (...a: unknown[]) => unknown)(...args);
      Object.defineProperties(handle, {
        release: { value: release, enumerable: false },
        [Symbol.dispose]: { value: release, enumerable: false },
        [Symbol.asyncDispose]: { value: release, enumerable: false },
        [Normalized]: { value: stub, enumerable: false },
      });

      return handle as unknown as CallbackHandle<T>;
    }

    /** Resolves to the value of a remote property. */
    async get<TName extends keyof TServices, TProperty extends RemotablePropertyKeys<TServices[TName]>>(
      name: TName,
      property: TProperty,
    ): Promise<TServices[TName][TProperty]> {
      return this.session.get(name as string, property as string) as Promise<TServices[TName][TProperty]>;
    }

    /**
     * Sends a unary RPC request to the remote service. Supporting both spread and explicit array arguments.
     */
    call(service: string, method: string, ...args: unknown[]): Promise<unknown>;
    call(service: string, method: string, args: unknown[], options?: RequestControl): Promise<unknown>;
    call(service: string, method: string, ...rest: unknown[]): Promise<unknown> {
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

    async close(reason?: string, graceful: boolean = true): Promise<void> {
      await this.session.close(reason, graceful).catch(() => {});
      this.cached.clear();
    }
  }

  /** Maps a `PeerConnection<Registry>` to the remote registry type. */
  export type InferServiceRegistry<T> = T extends PeerConnection<infer R> ? R : never;

  export interface QuiryEvents {
    "peer-connected": [handle: PeerConnection];
    "peer-disconnected": [handle: PeerConnection, reason?: string];
    shutdown: [reason?: string];
    error: [error: Error];
  }

  export interface AttachOptions<TIdentifier extends PeerIdentifier = PeerIdentifier> {
    readonly identifier?: TIdentifier;
  }
}

namespace Quiry {
  let _logger: Logger | null = null;

  const peers = new Map<PeerIdentifier, PeerConnection>();
  const emitter = new EventEmitter<QuiryEvents>();
  const services = new Map<string, ServiceImpl>();

  /**
   * Install a logger sink. Pass `null` to disable logging.
   * Logger is consulted by every active session and by the namespace itself.
   */
  export function setLogger(logger: Logger | null): void {
    _logger = logger;
  }

  // --------- PUBLIC API: PERSISTENCE --------- //

  /** Forks a child process at `filename` and attaches it as a new peer via {@link ChildProcessTransport}. */
  export function fork(filename: string | URL, options: NJSForkOptions = {}): PeerConnection {
    const subprocess = NJSFork(filename, options);
    return attach(new ChildProcessTransport({ child: subprocess }));
  }

  /** Spawns a worker thread at `filename` and attaches it as a new peer via {@link WorkerThreadsTransport}. */
  export function spawn(filename: string | URL, options: NJSWorkerOptions = {}): PeerConnection {
    const worker = new NJSWorker(filename, options);
    return attach(new WorkerThreadsTransport({ worker }));
  }

  export function attach<
    TIdentifier extends PeerIdentifier = PeerIdentifier,
    TServices extends ServiceRegistry = GlobalServiceRegistry[TIdentifier],
  >(transport: Transport, options?: AttachOptions<TIdentifier>): PeerConnection<TIdentifier, TServices>;

  export function attach<TServices extends ServiceRegistry>(
    transport: Transport,
    options?: AttachOptions,
  ): PeerConnection<PeerIdentifier, TServices>;

  export function attach(transport: Transport, options: AttachOptions = {}): PeerConnection {
    const identifier = options.identifier ?? randomBytes(4).toString("hex");
    if (peers.has(identifier)) {
      throw new QuiryError(
        WireStatus.FAILED_PRECONDITION,
        `Peer with identifier ${String(identifier)} already registered`,
      );
    }

    const session = new Session(transport, inquiry, {}, _logger).open();
    const connection = new PeerConnection(identifier, session);
    peers.set(identifier, connection);

    session.on(
      "terminate",
      (reason?: string) => {
        if (peers.delete(identifier)) {
          emitter.emit("peer-disconnected", connection, reason);
          _logger?.info(`Peer ${String(identifier)} disconnected`);
        }
      },
      { once: true },
    );

    _logger?.info(`Peer ${String(identifier)} attached`);
    emitter.emit("peer-connected", connection);
    return connection;
  }

  export async function detach(identifier: PeerIdentifier, kill: boolean = false): Promise<void> {
    const connection = peers.get(identifier);
    if (!connection) return;

    // `close()` triggers `session.terminate`, which fires the `terminate` listener
    // registered in `attach()`. That listener removes the entry from `peers` and
    // emits `peer-disconnected`, so we don't need to do either of those here.
    await connection.close("detached", !kill);
    _logger?.info(`Peer ${String(identifier)} detached`);
  }

  export function get(identifier: PeerIdentifier): PeerConnection | undefined {
    return peers.get(identifier);
  }

  // --------- PUBLIC API: SERVICES --------- //

  export function expose<TName extends string, TImpl extends ServiceImpl>(name: TName, impl: TImpl): void {
    if (services.has(name))
      throw new QuiryError(WireStatus.FAILED_PRECONDITION, `Service ${name} already exposed`);
    if (typeof impl !== "object" || impl === null || Array.isArray(impl))
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, `Service ${name} must be an object`);

    services.set(name, impl);
  }

  export function conceal<TName extends string>(name: TName): boolean {
    return services.delete(name);
  }

  function inquiry(request: InquiryRequest): ReturnType<InquiryFunc> {
    const context = {
      detail: { query: { service: request.service, property: request.property } },
    };

    const impl = services.get(request.service);
    if (!impl) throw new QuiryError(WireStatus.NOT_FOUND, `Service ${request.service} not found`, context);
    if (!(request.property in impl)) {
      throw new QuiryError(
        WireStatus.NOT_FOUND,
        `Property ${request.property} does not exist in service ${request.service}`,
        context,
      );
    }

    const prop = impl[request.property as keyof typeof impl] as unknown;
    // Property GET — key exists but is not a function
    if (typeof prop !== "function") {
      if (!isSerializable(prop)) {
        throw new QuiryError(
          WireStatus.MALFORMED_RESPONSE,
          "Cannot get a non-serializable property",
          context,
        );
      }

      return Promise.resolve(prop);
    }

    _logger?.trace(
      `Invoking method ${request.service}.${request.property} with ${request.args.length} arguments`,
    );

    let result: unknown;
    try {
      result = prop.apply(impl, request.args as unknown[]);
    } catch (error: unknown) {
      return Promise.reject(
        new QuiryError(WireStatus.INTERNAL, "Failed to invoke method", { ...context, cause: error }),
      );
    }

    if (typeof result === "object" && result !== null) {
      if (isAnyIterableIterator(result)) return result;
      if (typeof (result as PromiseLike<unknown>).then === "function") return result as Promise<unknown>;
    }

    return Promise.resolve(result);
  }

  export function on<K extends keyof QuiryEvents>(
    event: K,
    listener: (...args: QuiryEvents[K]) => void,
  ): Unsubscribe {
    emitter.on(event, listener as (...args: unknown[]) => void);
    return () => emitter.off(event, listener as (...args: unknown[]) => void);
  }
}

function makeServiceProxy(service: string, session: Session, control?: RequestControl): object {
  const callerStack = captureCallerStack(makeServiceProxy);

  return new Proxy(Object.create(null), {
    get(_, key: string) {
      // Only created when .then/.catch/.finally is accessed (lazy),
      // i.e. when the developer writes `await proxy.name` without calling it
      let getter: Promise<unknown> | null = null;
      const opt = (): Promise<unknown> => {
        return (getter ??= session.get(service, key).catch((error: unknown) => {
          attachCallerStack(error, callerStack);
          return Promise.reject(error);
        }));
      };

      return new Proxy(function () {} as unknown as object, {
        apply(_, __, args: unknown[]) {
          return makeCallOrStream(service, key, args, session, control, callerStack);
        },
        get(_, prop) {
          switch (prop) {
            case "then":
              return <TResult1 = unknown, TResult2 = never>(
                onfulfilled?: ((value: unknown) => TResult1 | PromiseLike<TResult1>) | null,
                onrejected?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
              ): Promise<TResult1 | TResult2> => opt().then(onfulfilled, onrejected);

            case "catch":
              return <TResult = never>(
                onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | undefined | null,
              ): Promise<unknown | TResult> => opt().catch(onrejected);

            case "finally":
              return (onfinally?: (() => void) | undefined | null): Promise<unknown> =>
                opt().finally(onfinally);

            default:
              throw new QuiryError(
                WireStatus.FAILED_PRECONDITION,
                "Remote properties must be used with `await` keyword",
              );
          }
        },
      });
    },
    set() {
      throw new QuiryError(WireStatus.FAILED_PRECONDITION, "Remote properties are read-only");
    },
  });
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

function makeCallOrStream<T = unknown>(
  service: string,
  method: string,
  args: unknown[],
  session: Session,
  control?: RequestControl,
  stack?: string,
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
      throw new Error(`Cannot await ${service}.${method}(...) — it has already been committed as a stream.`);

    mode = QueryMode.CALL;
    return (call ??= session
      .request(service, method, args, control)
      .catch((error: unknown) => Promise.reject(tag(error))));
  };

  const flow = (): AsyncIterableIterator<unknown> => {
    if (mode === QueryMode.CALL)
      throw new Error(
        `Cannot iterate ${service}.${method}(...) — it has already been committed as a unary call.`,
      );

    mode = QueryMode.STREAM;
    if (iter) return iter;

    const source = session.stream(service, method, args, control);
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

    [Symbol.iterator]: (): IterableIterator<unknown> => {
      throw new QuiryError(
        WireStatus.FAILED_PRECONDITION,
        "Remote iterators must be used with `await` keyword",
      );
    },
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

const REQUEST_CONTROL_KEYS = new Set(["timeout", "retry", "abortable", "traceId"]);

/**
 * Peels off a trailing {@link RequestControl} object from the variadic `rest` array.
 *
 * To minimize the chance of a regular argument being mistaken for control options,
 * we only treat the last element as control if it's a plain object whose keys are
 * a non-empty subset of {@link REQUEST_CONTROL_KEYS}. An empty object or any
 * unknown key disqualifies it.
 */
function splitArgsAndOptions(rest: unknown[]): [unknown[], RequestControl | undefined] {
  if (rest.length === 0) return [rest, undefined];

  const tail = rest[rest.length - 1];
  if (!tail || typeof tail !== "object" || Array.isArray(tail)) return [rest, undefined];

  const proto = Object.getPrototypeOf(tail);
  if (proto !== Object.prototype && proto !== null) return [rest, undefined];

  const keys = Object.keys(tail);
  if (keys.length === 0 || !keys.every((k) => REQUEST_CONTROL_KEYS.has(k))) {
    return [rest, undefined];
  }

  return [rest.slice(0, -1), tail as RequestControl];
}

export * from "@/internal";
export { QuiryError } from "@/shared/errors";
export default Quiry;
