import type { Session, Callback } from "./core/session";
import { attachCallerStack, captureCallerStack, QuiryError } from "./protocol/errors";

import type { RemoteRegistry, RemoteImpl } from "./protocol/types";
import type { Remote, RemotablePropertyKeys } from "./interface/transformers";
import { WireStatus, type RequestControl } from "./protocol/wire";

import * as QuirySymbol from "./core/symbols";

export type PeerIdentifier = string;

export class PeerConnection<TObjects extends RemoteRegistry = {}> {
  private readonly cached = new Map<keyof TObjects, RemoteImpl>();
  constructor(
    readonly identifier: PeerIdentifier,
    private readonly session: Session,
  ) {}

  get diagnostic(): typeof this.session.diagnostic {
    return this.session.diagnostic;
  }

  remote<TOverride extends RemoteImpl = never, TName extends string = string>(
    name: TName,
    control?: RequestControl,
  ): Remote<
    [TOverride] extends [never] ? (TName extends keyof TObjects ? TObjects[TName] : RemoteImpl) : TOverride
  > {
    let proxy: Remote<unknown>;

    if (control) proxy = makeRemoteObjectProxy(name, this.session, control);
    else {
      proxy = this.cached.get(name) as Remote<unknown>;
      if (!proxy) {
        proxy = makeRemoteObjectProxy(name, this.session);
        this.cached.set(name, proxy);
      }
    }

    // @ts-expect-error; ignore.
    return proxy;
  }

  /**
   * Make a callback handle that can be manually released, or disposed out of scope.
   * This is useful for long-lived callbacks, like event handlers.
   */
  callback<T extends Function>(fn: T): Callback<T> {
    if (typeof fn !== "function")
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Callback must be a function");
    if (QuirySymbol.serialize in fn)
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Function is already bound as a callback handle");

    return this.session.proxy(fn);
  }

  /** Resolves to the value of a remote property. */
  async get<TName extends keyof TObjects, TProperty extends RemotablePropertyKeys<TObjects[TName]>>(
    name: TName,
    property: TProperty,
  ): Promise<TObjects[TName][TProperty]> {
    return this.session.get(name as string, property as string) as Promise<TObjects[TName][TProperty]>;
  }

  /**
   * Sends a unary RPC request to the remote object. Supporting both spread and explicit array arguments.
   */
  call(name: string, method: string, ...args: unknown[]): Promise<unknown>;
  call(name: string, method: string, args: unknown[], options?: RequestControl): Promise<unknown>;
  call(name: string, method: string, ...rest: unknown[]): Promise<unknown> {
    const [args, options] = splitArgsAndOptions(rest);
    return this.session.request(name, method, args, options);
  }

  /**
   * Open a server-streaming call. The returned iterator yields chunks as
   * they arrive from the remote object.
   */
  stream(name: string, method: string, ...args: unknown[]): AsyncIterableIterator<unknown>;
  stream(
    name: string,
    method: string,
    args: unknown[],
    options?: RequestControl,
  ): AsyncIterableIterator<unknown>;
  stream(name: string, method: string, ...rest: unknown[]): AsyncIterableIterator<unknown> {
    const [args, options] = splitArgsAndOptions(rest);
    return this.session.stream(name, method, args, options);
  }

  async close(reason?: string, graceful: boolean = true): Promise<void> {
    await this.session.close(reason, graceful).catch(() => {});
    this.cached.clear();
  }
}

function makeRemoteObjectProxy(object: string, session: Session, control?: RequestControl): object {
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
          return makeCallOrStream(object, key, args, session, control, callerStack);
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
  extends PromiseLike<T>,
    AsyncIterableIterator<T extends AsyncIterable<infer C> ? C : T> {}

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
      throw new Error(`Cannot await ${object}.${method}(...) — it has already been committed as a stream.`);

    mode = QueryMode.CALL;
    return (call ??= session
      .request(object, method, args, control)
      .catch((error: unknown) => Promise.reject(tag(error))));
  };

  const flow = (): AsyncIterableIterator<unknown> => {
    if (mode === QueryMode.CALL)
      throw new Error(
        `Cannot iterate ${object}.${method}(...) — it has already been committed as a unary call.`,
      );

    mode = QueryMode.STREAM;
    if (iter) return iter;

    const source = session.stream(object, method, args, control);
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
