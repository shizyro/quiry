/**
 * Service transformers take a real service class and produces an
 * equivalent remote proxy surface.
 */

// Helper types for service transformation

type AnyFn = (...args: any[]) => any;
type AnyConstructor = new (...args: any[]) => any;

type IsAsyncIterFn<T> = T extends (...args: any[]) => AsyncIterable<any> ? true : false;
/** Sync iterators (incl. sync generators) — proxied as async iterators on the remote side. */
type IsSyncIterFn<T> = T extends (...args: any[]) => Iterable<any>
  ? T extends (...args: any[]) => AsyncIterable<any>
    ? false
    : true
  : false;
type IsAsyncFn<T> = T extends (...args: any[]) => Promise<any> ? true : false;

/** Convert a function to an async-returning version. */
type AsyncifyFunction<F> = F extends (...args: infer A) => infer R
  ? (...args: A) => Promise<Awaited<R>>
  : never;

type DeepAsync<T> =
  // functions -> async functions
  T extends AnyFn
    ? AsyncifyFunction<T>
    : // arrays -> recursively transform elements
      T extends readonly (infer U)[]
      ? ReadonlyArray<DeepAsync<U>>
      : // objects -> recursively transform properties
        T extends object
        ? { [K in keyof T]: DeepAsync<T[K]> }
        : // primitives stay unchanged
          T;

// Method return type extraction

type UnwrapPromise<T> = T extends Promise<infer R> ? R : T;
type UnwrapAsyncIterable<T> =
  T extends AsyncIterable<infer R> ? R : T extends AsyncIterableIterator<infer R> ? R : never;
type UnwrapIterable<T> = T extends Iterable<infer R> ? R : never;

type AsyncReturnType<T extends AnyFn> = UnwrapPromise<ReturnType<T>>;
type StreamChunkType<T extends AnyFn> = UnwrapAsyncIterable<ReturnType<T>>;

// Per-method remote transformation

type RemoteMethod<T> = T extends AnyFn
  ? IsAsyncIterFn<T> extends true
    ? T // preserve; consumer drives pull on the existing async iterator
    : IsSyncIterFn<T> extends true
      ? (...args: Parameters<T>) => AsyncIterableIterator<UnwrapIterable<ReturnType<T>>> // sync gen -> async stream
      : IsAsyncFn<T> extends true
        ? T // already async, no transformation needed
        : (...args: Parameters<T>) => Promise<DeepAsync<ReturnType<T>>> // wrap sync in promise
  : never; // non-callable members are excluded entirely

// Filter for callable members

type RemotableMethodKey<T, K extends keyof T> = K extends string // string keys only, no symbols
  ? T[K] extends AnyFn
    ? T[K] extends AnyConstructor // exclude; can't be invoked remotely
      ? never
      : K
    : never
  : never;

export type RemotableMethodKeys<T> = { [K in keyof T]: RemotableMethodKey<T, K> }[keyof T];
export type RemotablePropertyKeys<T> = {
  [K in keyof T]: T[K] extends Serializable ? K : never;
}[keyof T];

// Full service proxy type

export type RemoteServiceDefinition<T> = {
  [K in RemotableMethodKeys<T>]: RemoteMethod<T[K]>;
} & {
  [K in RemotablePropertyKeys<T>]: Promise<T[K]>;
};

export type ServiceImpl = object;
/** A registry of named services. */
export type ServiceRegistry = Record<string, ServiceImpl>;
/** Wraps service registry in a readonly interface with remote service proxies. */
export type MappedServiceRegistry<S extends ServiceRegistry> = {
  readonly [K in keyof S]: RemoteServiceDefinition<S[K]>;
};

/**
 * Look up a specific service type from a registry by name.
 * Avoids direct S[K] indexing at call sites.
 */
export type ServiceName<S extends ServiceRegistry> = keyof S & string;
export type ServiceType<S extends ServiceRegistry, K extends keyof S> = S[K];

// Utility types

/** Infer the resolved value of a remote method call. */
export type InferRemoteReturn<T, M extends RemotableMethodKeys<T>> =
  RemoteMethod<T[M]> extends (...args: any[]) => infer R ? R : never;

/** Infer the chunk type of a remove streaming method. */
export type InferRemoteStreamChunk<T, M extends RemotableMethodKeys<T>> =
  RemoteMethod<T[M]> extends (...args: any[]) => AsyncIterable<infer R> ? R : never;

/** Infer the parameter tuple of a remote method. */
export type InferRemoteParams<T, M extends RemotableMethodKeys<T>> =
  RemoteMethod<T[M]> extends (...args: infer P) => any ? P : never;

type EventDefinition<TPayload extends readonly unknown[] = readonly unknown[]> = TPayload;
type EventRegistry = Record<string, EventDefinition<any>>;

export type InferEventPayload<
  TRegistry extends EventRegistry,
  TEventName extends keyof TRegistry,
> = TRegistry[TEventName];
