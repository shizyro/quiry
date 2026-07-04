declare const __brand: unique symbol;
type Brand<T, TBrand extends string> = T & { __brand: TBrand };

// Base identifiers with branded types for compile-time safety
export type TraceId = Brand<string, "TraceId">;
export type CorrelationId = Brand<string, "CorrelationId">;

export type CallbackId = Brand<string, "CallbackId">;
export type InvocationId = Brand<string, "InvocationId">;

export type ServiceImpl = object;
/** A registry of named services. */
export type ServiceRegistry = Record<string, ServiceImpl>;
