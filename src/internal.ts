import type { Session, CallbackProxy } from "./core/session";
import { QuiryError, attachCallerStack, captureCallerStack } from "./protocol/errors";

import type { RemoteRegistry, RemoteImpl } from "./protocol/types";
import type { Remote, RemotablePropertyKeys } from "./interface/transformers";
import { WireStatus } from "./protocol/wire";

import * as QuirySymbol from "./core/symbols";

export type PeerIdentifier = string;

/**
 * A handle to one side of an attached IPC session.
 * Exposes an ergonomic proxy surface methods for direct interation with the remote peer.
 */
export class PeerConnection<TObjects extends RemoteRegistry = {}> {
  private readonly cached = new Map<keyof TObjects, RemoteImpl>();
  constructor(
    readonly identifier: PeerIdentifier,
    private readonly session: Session,
  ) {}

  /** Per-session diagnostic bus. See `interface/diagnostics.ts` for the event catalog. */
  get diagnostic(): typeof this.session.diagnostic {
    return this.session.diagnostic;
  }

  /**
   * Resolve a proxy for the remote object registered under `name`. The proxy has the shape
   * of `TObjects[name]` (or the explicit `TOverride`, if given), transformed so every method
   * and property reads as awaitable, and any generator-returning method reads as
   * async-iterable instead.
   *
   * Proxies are cached per `name` — repeated calls with the same name return the same object.
   */
  remote<TOverride extends RemoteImpl = never, TName extends string = string>(
    name: TName,
  ): Remote<
    [TOverride] extends [never] ? (TName extends keyof TObjects ? TObjects[TName] : RemoteImpl) : TOverride
  > {
    let proxy = this.cached.get(name) as Remote<unknown>;
    if (!proxy) {
      proxy = makeRemoteObjectProxy(name, this.session);
      this.cached.set(name, proxy);
    }
    // @ts-expect-error; ignore.
    return proxy;
  }

  /**
   * Make a callback handle that can be manually released, or disposed out of scope.
   * This is useful for long-lived callbacks, like event handlers.
   */
  callback<T extends Function>(fn: T): CallbackProxy<T> {
    if (typeof fn !== "function")
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Callback must be a function");
    if (QuirySymbol.override in fn)
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "This function cannot be reused as a callback proxy");

    return this.session.proxy(fn);
  }

  /** Resolves to the value of a remote property. Imperative equivalent of `await remote(name).property`. */
  async get<TName extends keyof TObjects, TProperty extends RemotablePropertyKeys<TObjects[TName]>>(
    name: TName,
    property: TProperty,
  ): Promise<TObjects[TName][TProperty]> {
    return this.session.get(name as string, property as string) as Promise<TObjects[TName][TProperty]>;
  }

  /**
   * Sends a unary RPC request to the remote object. Supports both spread and explicit array
   * arguments. Imperative equivalent of `await remote(name).method(...args)`.
   */
  call(name: string, method: string, ...args: unknown[]): Promise<unknown> {
    return this.session.request(name, method, args);
  }

  /**
   * Open a server-streaming call. The returned iterator yields chunks as they arrive from
   * the remote object. Imperative equivalent of `for await (const x of remote(name).method(...args))`.
   */
  stream(name: string, method: string, ...args: unknown[]): AsyncIterableIterator<unknown> {
    return this.session.stream(name, method, args);
  }

  /**
   * Close the session. Graceful by default — both sides drain in-flight work before the
   * transport tears down. Clears the cached {@link PeerConnection.remote} proxies either way.
   *
   * @param graceful - Set to `false` to close immediately, dropping any pending requests.
   */
  async close(reason?: string, graceful: boolean = true): Promise<void> {
    await this.session.close(reason, graceful).catch(() => {});
    this.cached.clear();
  }
}

function makeRemoteObjectProxy(object: string, session: Session): object {
  const callerStack = captureCallerStack(makeRemoteObjectProxy);

  return new Proxy(Object.create(null), {
    get(_, key: string) {
      // Only created when .then/.catch/.finally is accessed (lazy),
      // i.e. when the developer writes `await proxy.name` without calling it
      let getter: Promise<unknown> | null = null;
      const opt = (): Promise<unknown> => {
        return (getter ??= session.get(object, key).catch((error: unknown) => {
          attachCallerStack(error, callerStack);
          return Promise.reject(error);
        }));
      };

      return new Proxy(function () {} as unknown as object, {
        apply(_, __, args: unknown[]) {
          return makeCallOrStream(object, key, args, session, callerStack);
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

            case QuirySymbol.control:
              return (signal: AbortSignal) => {
                return (...args: unknown[]) =>
                  makeCallOrStream(object, key, args, session, callerStack, signal);
              };

            default:
              throw new QuiryError(
                WireStatus.FAILED_PRECONDITION,
                "Remote properties must be used with `await` keyword",
              );
          }
        },
      });
    },
    set(_, key: string, value: unknown): boolean {
      (async () => {
        // Trigger async side effects without awaiting
        await session.set(object, key, await value);
      })();
      return true;
    },
  });
}

/**
 * A lazy handle returned by the remote proxy that commits to either a
 * unary request or a server-stream on first use.
 *
 * The two paths are mutually exclusive: whichever protocol the caller engages
 * first wins, and subsequent attempts to use the other interface throw.
 */
interface CallOrStream<T = unknown>
  extends PromiseLike<T>, AsyncIterableIterator<T extends AsyncIterable<infer C> ? C : T> {}

enum QueryMode {
  PENDING = 0,
  CALL,
  STREAM,
}

function makeCallOrStream<T = unknown>(
  object: string,
  method: string,
  args: unknown[],
  session: Session,
  stack?: string,
  signal?: AbortSignal,
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
      throw new Error(`Cannot await ${object}.${method}(...) — it has already been committed as a stream.`);

    mode = QueryMode.CALL;
    return (call ??= session
      .request(object, method, args, signal)
      .catch((error: unknown) => Promise.reject(tag(error))));
  };

  const flow = (): AsyncIterableIterator<unknown> => {
    if (mode === QueryMode.CALL)
      throw new Error(
        `Cannot iterate ${object}.${method}(...) — it has already been committed as a unary call.`,
      );

    mode = QueryMode.STREAM;
    if (iter) return iter;

    const source = session.stream(object, method, args, signal);
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
