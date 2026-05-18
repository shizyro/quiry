/**
 * @license Copyright 2026 Shizuka Yashiro
 */

import { fork as NJSFork, type ForkOptions as NJSForkOptions } from "node:child_process";
import { Worker as NJSWorker, type WorkerOptions as NJSWorkerOptions } from "node:worker_threads";

import { EventEmitter } from "node:events";

import { QuiryError } from "./shared/errors";
import { type InquiryDescriptor, Session, type InquiryFunc, type InquiryRequest } from "./core/session";
import { PeerConnection, type PeerIdentifier } from "./internal";

import { WireStatus } from "./interface/protocol";
import type { AnyPacket } from "./interface/packets";
import type { ServiceImpl, ServiceRegistry } from "./interface/types";

import type { Transport } from "./core/transport";
import { ChildProcessTransport } from "./core/transport/impl/child-process";
import { WorkerThreadsTransport } from "./core/transport/impl/worker-threads";

import { DiagnosticBus } from "./lib/diagnostics";
import {
  DIAGNOSTIC_CHANNEL_PREFIX,
  type QuiryEvents as DiagnosticQuiryEvents,
} from "./interface/diagnostics";

import { fetchDescriptor } from "./lib/helpers";
import { randomBytes } from "node:crypto";

const registry = new Map<string, ServiceImpl>();
const peers = new Map<PeerIdentifier, PeerConnection>();

export interface QuiryEvents {
  "peer-connected": [handle: PeerConnection];
  "peer-disconnected": [handle: PeerConnection, reason?: string];
  shutdown: [reason?: string];
  error: [error: Error];
}

const emitter = new EventEmitter<QuiryEvents>();
/**
 * Module-level diagnostic bus. Carries peer-lifecycle events
 * (`peer:attached`, `peer:detached`). Per-session diagnostics live on
 * `session.diag` (see `Session#diag`). Bridges to `node:diagnostics_channel`
 * under the `quiry:` prefix for external observability tooling.
 */
export const diagnostic = new DiagnosticBus<DiagnosticQuiryEvents>(DIAGNOSTIC_CHANNEL_PREFIX);

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
  return attach(new ChildProcessTransport(subprocess));
}

/** Spawns a worker thread at `filename` and attaches it as a new peer via {@link WorkerThreadsTransport}. */
export function spawn(filename: string | URL, options: NJSWorkerOptions = {}): PeerConnection {
  const worker = new NJSWorker(filename, options);
  return attach(new WorkerThreadsTransport(worker));
}

export function attach<TServices extends ServiceRegistry = {}>(
  transport: Transport<AnyPacket>,
): PeerConnection<TServices> {
  const session = new Session(transport, handleInquiry, {}).open();
  const identifier = randomBytes(4).toString("hex");
  const connection = new PeerConnection(identifier, session);
  peers.set(identifier, connection);

  session.on(
    "terminate",
    (reason?: string) => {
      if (peers.delete(identifier)) {
        emitter.emit("peer-disconnected", connection, reason);
        diagnostic.maybe("peer:detached")?.({ identifier, reason });
      }
    },
    { once: true },
  );

  emitter.emit("peer-connected", connection);
  diagnostic.maybe("peer:attached")?.({ identifier });
  return connection as unknown as PeerConnection<TServices>;
}

export async function detach(identifier: PeerIdentifier, kill: boolean = false): Promise<void> {
  const connection = peers.get(identifier);
  if (!connection) return;

  // `close()` triggers `session.terminate`, which fires the `terminate` listener
  // registered in `attach()`. That listener removes the entry from `peers` and
  // emits `peer-disconnected`, so we don't need to do either of those here.
  await connection.close("detached", !kill);
}

/** Resolve a peer connection by its identifier. */
export function peer(identifier: PeerIdentifier): PeerConnection | undefined {
  return peers.get(identifier);
}

// --------- PUBLIC API: SERVICE REGISTRATION --------- //

type NonCtor<T> = T extends new (...args: any[]) => any ? never : T;

/**
 * Register a service under `name` so it can be later resolved by remote peers.
 * A function is treated as a lazy factory and invoked exactly once on
 * registration. Anything else is registered as-is.
 *
 * Returns the resolved instance so callers can keep a local handle to the
 * same singleton.
 *
 * @throws A {@link QuiryError} `FAILED_PRECONDITION` if `name` is already
 *   registered. Call {@link unexpose} first to replace an existing binding.
 * @throws A {@link QuiryError} `INVALID_ARGUMENT` if the implementation does
 *   not resolve to an object (e.g. a factory returning a primitive).
 */
export function expose<T extends ServiceImpl>(name: string, factory: () => T): T;
export function expose<T extends ServiceImpl>(name: string, value: NonCtor<T>): T;
export function expose(name: string, valueOrFactory: ServiceImpl | (() => ServiceImpl)): ServiceImpl {
  if (registry.has(name)) {
    throw new QuiryError(WireStatus.FAILED_PRECONDITION, `Service "${name}" is already registered`);
  }

  const impl = typeof valueOrFactory === "function" ? valueOrFactory() : valueOrFactory;
  if (impl === null || typeof impl !== "object") {
    throw new QuiryError(
      WireStatus.INVALID_ARGUMENT,
      `Service "${name}" must resolve to an object (got ${impl === null ? "null" : typeof impl})`,
    );
  }

  registry.set(name, impl);
  return impl;
}

/**
 * Remove a service registration. In-flight peer requests against the
 * unexposed service fail with `NOT_FOUND`.
 *
 * @returns `true` if the service was unregistered, `false` otherwise.
 */
export function unexpose(name: string): boolean {
  return registry.delete(name);
}

/** Resolve a registered service by name, or `undefined` if not registered. */
export function get<T extends ServiceImpl = ServiceImpl>(name: string): T | undefined {
  return registry.get(name) as T | undefined;
}

/** Whether a service is registered under `name`. */
export function has(name: string): boolean {
  return registry.has(name);
}

/** Remove every service registration. Peer connections are not affected. */
export function clear(): void {
  registry.clear();
}

// --------- INTERNAL --------- //

function handleInquiry(request: InquiryRequest): ReturnType<InquiryFunc> {
  const context = {
    detail: { query: { service: request.service, property: request.property } },
  };

  const impl = registry.get(request.service);
  if (!impl) throw new QuiryError(WireStatus.NOT_FOUND, `Service ${request.service} not found`, context);
  if (!(request.property in impl)) {
    throw new QuiryError(
      WireStatus.NOT_FOUND,
      `Property ${request.property} does not exist in service ${request.service}`,
      context,
    );
  }

  return makeInquiryDescriptor(impl, request.property);
}

function makeInquiryDescriptor<T = unknown>(impl: object, key: PropertyKey): InquiryDescriptor<T> {
  const [target, descriptor] = fetchDescriptor(impl, key);
  if (!descriptor) {
    throw new ReferenceError(`Property ${String(key)} does not exist in service ${String(impl)}`);
  }

  const isData = "value" in descriptor;
  const isFunction = isData && typeof descriptor.value === "function";
  let boundFn: Function | undefined;

  return {
    get value() {
      return this.get();
    },
    get() {
      if (isData) return isFunction ? (boundFn ??= descriptor.value.bind(impl)) : descriptor.value;
      const value = descriptor.get?.call(impl);
      return typeof value === "function" ? value.bind(impl) : value;
    },
    set(value: T) {
      if (!this.writable) {
        throw new TypeError(`Property ${String(key)} is not writable`);
      }

      if (isData) {
        // descriptor.value = value;
        // Object.defineProperty(impl, key, descriptor);
        Reflect.set(target, key, value, impl);
        return;
      }

      descriptor.set!.call(impl, value);
    },
    // There is no reliable way to check if a property is readonly (TS), so as best-effort,
    // we just check if the descriptor is writable or if the set accessor is not undefined.
    writable: isFunction ? false : isData ? !!descriptor.writable : !!descriptor.set,
    enumerable: !!descriptor.enumerable,
  };
}

export { QuiryError, WorkerThreadsTransport, ChildProcessTransport, WireStatus };
export type { RetryPolicy, RequestControl } from "./interface/protocol";
export * as QuirySymbol from "./core/infra/symbol";
