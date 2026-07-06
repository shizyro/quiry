/**
 * Remote object transformers take a real object class and produces an
 * equivalent remote proxy surface.
 */

import type * as QuirySymbol from "../core/symbols";

// Helper types

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

type RemoteReturn<T extends AnyFn> =
  ReturnType<T> extends string
    ? Promise<DeepAsync<Awaited<ReturnType<T>>>>
    : ReturnType<T> extends Iterable<any> | AsyncIterable<any>
      ? AsyncIterableIterator<UnwrapIterable<ReturnType<T>>>
      : Promise<DeepAsync<Awaited<ReturnType<T>>>>;

type RemoteFunction<T extends AnyFn> = ((...args: Parameters<T>) => RemoteReturn<T>) & {
  [QuirySymbol.control]: (signal: AbortSignal) => (...args: Parameters<T>) => RemoteReturn<T>
};

type RemoteProperty<T> =
  T extends AnyFn
    ? RemoteFunction<T>
    : T extends abstract new (...args: infer Args) => infer Instance
      ? { new (...args: Args): Promise<Remote<Instance>> }
      : T extends Serializable
        ? Promisify<T>
        : T extends object
          ? Remote<T>
          : never;

/**
 * Given the raw type definition of an object that exists remotely (on the other side of the thread/process boundary),
 * produces the type as it appears to local code when accessed via a proxy.
 */
export type Remote<T> = {
  [K in keyof T as RemoteProperty<T[K]> extends never ? never : K]:
    RemoteProperty<T[K]>
};
