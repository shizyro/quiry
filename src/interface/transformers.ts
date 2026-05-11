/**
 * Service transformers take a real service class and produces an
 * equivalent remote proxy surface.
 */
// biome-ignore-all format: ignore.

// Helper types for service transformation

type AnyFn = (...args: any[]) => any;
type Promisify<T> = [T] extends [Promise<unknown>] ? T : Promise<T>;
type UnwrapIterable<T> = T extends Iterable<infer R> ? R : T extends AsyncIterable<infer R> ? R : never;

/** Recursively transforms all return values in a type to promises. */
type DeepAsync<T> = [T] extends [(...args: infer TArguments) => infer TReturn]
  ? (...args: TArguments) => Promisify<DeepAsync<Awaited<TReturn>>> // functions -> async functions
    : [T] extends [readonly (infer U)[]] ? ReadonlyArray<DeepAsync<U>> // arrays -> recursively transform elements
      : [T] extends [object] ? { [K in keyof T]: DeepAsync<T[K]> } // objects -> recursively transform properties
        : T; // primitives stay unchanged

// Filter for callable members

export type RemotableMethodKeys<T> = { 
  [K in keyof T]: T[K] extends AnyFn ? K : never
}[keyof T];
export type RemotablePropertyKeys<T> = {
  [K in keyof T]: T[K] extends Serializable ? K : never;
}[keyof T];

// Per-method remote transformation

type RemoteProperty<T, K extends keyof T> =
  ( T[K] extends Serializable ? Promisify<T[K]> : unknown) &
  ( T[K] extends AnyFn ? (...args: Parameters<T[K]>) =>
    ReturnType<T[K]> extends (Iterable<any> | AsyncIterable<any>)
      ? AsyncIterableIterator<UnwrapIterable<ReturnType<T[K]>>>
      : Promise<DeepAsync<Awaited<ReturnType<T[K]>>>>
    : unknown ) &
  ( T[K] extends { new (...args: infer TArguments): infer TInstance }
    ? { new (...args: TArguments): Promise<Remote<TInstance>> } : unknown)

/**
 * Given the raw type definition of an object that exists remotely (on the other side of the thread/process boundary),
 * produces the type as it appears to local code when accessed via a proxy.
 */
export type Remote<T> = { [K in keyof T]: RemoteProperty<T, K> };
