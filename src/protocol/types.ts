declare const __brand: unique symbol;
type Brand<T, TBrand extends string> = T & { __brand: TBrand };

// Base identifiers with branded types for compile-time safety
export type CallbackId = Brand<string, "CallbackId">;
export type InvocationId = Brand<string, "InvocationId">;
export type CorrelationId = Brand<string, "CorrelationId">;

export type RemoteImpl = object;
/** A registry of named remote objects. */
export type RemoteRegistry = Record<string, RemoteImpl>;
