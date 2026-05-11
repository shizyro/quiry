/**
 * @license Copyright 2026 Shizuka Yashiro
 */

import { fork as NJSFork, type ForkOptions as NJSForkOptions } from "node:child_process";
import { Worker as NJSWorker, type WorkerOptions as NJSWorkerOptions } from "node:worker_threads";

import { EventEmitter } from "node:events";

import { QuiryError } from "./shared/errors";
import { Session, type InquiryFunc, type InquiryRequest } from "./core/session";
import { PeerConnection, type PeerIdentifier } from "./internal";

import { WireStatus } from "./interface/protocol";
import type { AnyPacket } from "./interface/packets";
import type { ServiceImpl, ServiceRegistry } from "./interface/types";

import type { Transport } from "./core/transport";
import { ChildProcessTransport } from "./core/transport/impl/child-process";
import { WorkerThreadsTransport } from "./core/transport/impl/worker-threads";
import { isAnyIterableIterator, isSerializable } from "./lib/helpers";

import { randomBytes } from "node:crypto";

// ...

const instances = new Map<Token, any>();
const descriptors = new Map<Token, ServiceDescriptor>();
const peers = new Map<PeerIdentifier, PeerConnection>();

export interface QuiryEvents {
  "peer-connected": [handle: PeerConnection];
  "peer-disconnected": [handle: PeerConnection, reason?: string];
  shutdown: [reason?: string];
  error: [error: Error];
}

const emitter = new EventEmitter<QuiryEvents>();
let _logger: Logger | null = null;

/**
 * Install a logger sink. Pass `null` to disable logging.
 * Logger is consulted by every active session and by the namespace itself.
 */
export function setLogger(logger: Logger | null): void {
  _logger = logger;
}

export function on<K extends keyof QuiryEvents>(
  event: K,
  listener: (...args: QuiryEvents[K]) => void,
): Unsubscribe {
  emitter.on(event, listener as (...args: unknown[]) => void);
  return () => emitter.off(event, listener as (...args: unknown[]) => void);
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

export function attach<TServices extends ServiceRegistry = {}>(
  transport: Transport<AnyPacket>,
): PeerConnection<TServices> {
  const session = new Session(transport, handleInquiry, {}, _logger).open();
  const identifier = randomBytes(4).toString("hex");
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
  return connection as unknown as PeerConnection<TServices>;
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

/** Resolve a peer connection by its identifier. */
export function peer(identifier: PeerIdentifier): PeerConnection | undefined {
  return peers.get(identifier);
}

// --------- PUBLIC API: SERVICE REGISTRATION --------- //

type AnyFn = (...args: any[]) => any;
type AnyCtor = new (...args: any[]) => any;
type NonFunctionValue<T> = T extends AnyFn ? never : T extends AnyCtor ? never : T;

type Constructor<T = unknown, Args extends unknown[] = unknown[]> = new (...args: Args) => T;
type Factory<T = unknown> = (...args: unknown[]) => T;

declare const TokenType: unique symbol;
export type Token<T = unknown> = string & {
  readonly [TokenType]: T;
};

/**
 * Service lifetime enumeration. Defines how long a service instance
 * should live and when it should be created.
 */
export enum ServiceLifetime {
  /**
   * Creates a new instance every time the service is resolved.
   * Use for stateless services or when you need fresh instances.
   */
  Transient = "transient",

  /**
   * Creates one instance for the entire application lifetime.
   * Use for expensive-to-create services or shared state.
   */
  Singleton = "singleton",
}

export interface ServiceHooks<T = unknown> {
  /** Hook called after the service instance is created. */
  activated?: (instance: T) => void | PromiseLike<void>;
  /** Hook called when the service instance is being disposed. */
  disposed?: (instance: T) => void | PromiseLike<void>;
}

export interface ServiceOptions<T = unknown> {
  /**
   * The lifetime of the service instance
   * @default ServiceLifetime.Transient
   */
  lifetime?: ServiceLifetime;
  /**
   * Allows the service registration to override an existing service
   * bound to the same token. Use with caution!
   */
  override?: true;
}

export type ServiceOf<T> = T extends ServiceDescriptor<infer TService> ? TService : never;

export type ServiceDefinition<T> =
  | { type: "value"; value: T }
  | { type: "factory"; builder: Factory<T> }
  | {
      type: "ctor";
      ctor: Constructor<T>;
      /** List of constructor parameters required by this service */
      dependencies?: unknown[];
    };

/**
 * Internal service descriptor containing all information about a registered service.
 * @template T - The type of service this descriptor represents
 */
export interface ServiceDescriptor<T = any> {
  /** Unique identifier for the service. */
  readonly token: Token<T>;
  /** Definition of the service */
  definition: ServiceDefinition<T>;
  /** Configuration options for the service */
  options: Required<ServiceOptions<T>> & ServiceHooks<T>;
}

/**
 * Base error class for all dependency injection related errors.
 */
export class DependencyInjectionError extends Error {
  constructor(
    message: string,
    /** The service identifier that caused the error (if applicable) */
    readonly identifier?: Token,
  ) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace?.(this, DependencyInjectionError);
  }
}

type ExposeOptions<T> = ServiceOptions<T> & ServiceHooks<T>;

export function expose<Ctor extends AnyCtor, K extends Token<InstanceType<Ctor>> | string>(
  token: K,
  ctor: Ctor,
  options?: ExposeOptions<InstanceType<Ctor>> & { dependencies?: ConstructorParameters<Ctor> },
): ServiceDescriptor<InstanceType<Ctor>>;

export function expose<F extends AnyFn, K extends Token<ReturnType<F>> | string>(
  token: K,
  builder: F,
  options?: ExposeOptions<ReturnType<F>>,
): ServiceDescriptor<ReturnType<F>>;

export function expose<T, K extends Token<T> | string>(
  token: K,
  value: NonFunctionValue<T>,
  options?: Omit<ExposeOptions<T>, "lifetime">,
): ServiceDescriptor<T>;

export function expose<T>(
  token: Token<T>,
  impl: T,
  options?: ExposeOptions<T> & { dependencies?: unknown[] },
): ServiceDescriptor {
  let definition: ServiceDefinition<T>;
  if (descriptors.has(token)) {
    if (options?.override) descriptors.delete(token);
    else throw new DependencyInjectionError("Service already registered", token);
  }

  const defaults: ServiceOptions<T> = {
    lifetime: ServiceLifetime.Transient,
  };

  // Determine if second parameter is a constructor or service options
  if (typeof impl === "function") {
    if (/^class\s/.test(Function.prototype.toString.call(impl))) {
      definition = {
        type: "ctor",
        ctor: impl as Constructor<T>,
        dependencies: options?.dependencies,
      };
    } else definition = { type: "factory", builder: impl as Factory<T> };
  } else if (typeof impl === "object" && impl !== null) {
    Object.assign(defaults, { lifetime: ServiceLifetime.Singleton }); // Value services are always singletons
    definition = { type: "value", value: impl };
  } else {
    throw new DependencyInjectionError("Service registry requires an implementation", token);
  }

  const descriptor: ServiceDescriptor<T> = {
    token: token,
    definition,
    options: { ...defaults, ...options } as typeof descriptor.options,
  };

  descriptors.set(token, descriptor);
  return descriptor;
}

/**
 * Resolve a service instance.
 * This is the main method for getting service instances from the container.
 *
 * @throws A {@link DependencyInjectionError} if the service is not registered.
 */
export function get<T>(token: Token<T>): T;
export function get<T = unknown>(identifier: string): T;
export function get<T>(key: Token<T> | string): T {
  return resolveService<T>(key as Token<T>);
}

/**
 * Resolve a service instance by its token or identifier.
 * @returns The service instance or null if the service is not registered.
 */
export function find<T>(token: Token<T>): T | null;
export function find<T = unknown>(identifier: string): T | null;
export function find<T>(key: Token<T> | string): T | null {
  try {
    return resolveService<T>(key as Token<T>);
  } catch (error: unknown) {
    if (error instanceof DependencyInjectionError) return null;
    throw error;
  }
}

/** Check if a service is registered. */
export function has(key: Token | string): boolean {
  return descriptors.has(key as Token);
}

export function dispose(key: Token | string): boolean {
  const token = key as Token;
  const instance = instances.get(token);
  if (!instance) return false;

  const descriptor = descriptors.get(token)!;
  if (descriptor.options.disposed) {
    void descriptor.options.disposed(instances.get(token));
  }

  return instances.delete(token);
}

export function clear(): void {
  const disposed = new Set<Token>();

  // Dispose singletons
  for (const [token, singleton] of instances) {
    if (!disposed.has(token)) {
      const descriptor = descriptors.get(token);
      if (descriptor?.options.disposed) {
        void descriptor.options.disposed(singleton);
      }
      disposed.add(token);
    }
  }

  // Clear descriptors
  descriptors.clear();
  instances.clear();
}

// --------- INTERNAL --------- //

/**
 * Internal method for resolving services. Handles the core resolution logic
 * including lifecycle management.
 */
function resolveService<T>(token: Token<T>): T {
  const descriptor = descriptors.get(token);
  if (!descriptor) {
    throw new DependencyInjectionError("Service not registered", token);
  }

  switch (descriptor.options.lifetime) {
    case ServiceLifetime.Singleton:
      if (instances.has(token)) return instances.get(token);
  }

  let instance: T;

  // Create instance based on definition
  switch (descriptor.definition.type) {
    case "value":
      instance = descriptor.definition.value;
      break;
    case "factory":
      instance = descriptor.definition.builder();
      break;
    case "ctor":
      instance = new descriptor.definition.ctor(...(descriptor.definition.dependencies ?? []));
      break;
    default:
      throw new DependencyInjectionError("Invalid service definition", token);
  }

  // Call activation hook
  if (descriptor.options.activated) {
    void descriptor.options.activated(instance);
  }

  // Store based on lifetime
  switch (descriptor.options.lifetime) {
    case ServiceLifetime.Singleton:
      instances.set(token, instance);
      break;
  }

  return instance;
}

function handleInquiry(request: InquiryRequest): ReturnType<InquiryFunc> {
  const context = {
    detail: { query: { service: request.service, property: request.property } },
  };

  const impl = find<ServiceImpl>(request.service);
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
        "Property value is not serializable and cannot be retrieved",
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

export { QuiryError, WorkerThreadsTransport, ChildProcessTransport, WireStatus };
export type { RetryPolicy, RequestControl } from "./interface/protocol";
