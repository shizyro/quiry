/**
 * @license Copyright 2026 Shizuka Yashiro
 */

import {
  fork as NJSFork,
  ChildProcess as NJSChildProcess,
  type ForkOptions as NJSForkOptions,
} from "node:child_process";
import { Worker as NJSWorker, type WorkerOptions as NJSWorkerOptions } from "node:worker_threads";

import { EventEmitter } from "node:events";

import { QuiryError } from "./protocol/errors";
import {
  Session,
  type InquiryFunc,
  type InquiryRequest,
  type InquiryDescriptor,
  type SessionOptions,
} from "./core/session";
import { PeerConnection, type PeerIdentifier } from "./internal";

import { WireStatus } from "./protocol/wire";
import type { AnyPacket } from "./protocol/packets";
import type { RemoteImpl, RemoteRegistry } from "./protocol/types";

import type { Transport } from "./core/transport";
import { ChildProcessTransport } from "./core/transport/impl/child-process";
import { WorkerThreadsTransport } from "./core/transport/impl/worker-threads";

import { DiagnosticBus } from "./lib/diagnostics";
import {
  DIAGNOSTIC_CHANNEL_PREFIX,
  type QuiryEvents as DiagnosticQuiryEvents,
} from "./interface/diagnostics";

import { fetchDescriptor } from "./lib/helpers";
import { contextStorage } from "./lib/call-context";
import { randomBytes } from "node:crypto";

const registry = new Map<string, RemoteImpl>();
const peers = new Map<PeerIdentifier, PeerConnection>();

/** Module-level peer lifecycle events. Subscribe via {@link on}. */
export interface QuiryEvents {
  /** A new peer was attached. */
  "peer-connected": [handle: PeerConnection];
  /** A peer's underlying session terminated, gracefully or otherwise. */
  "peer-disconnected": [handle: PeerConnection, reason?: string];
}

const emitter = new EventEmitter<QuiryEvents>();
/**
 * Module-level diagnostic bus. Bridges to `node:diagnostics_channel`
 * under the `quiry:` prefix for external observability tooling.
 */
export const diagnostic: DiagnosticBus<DiagnosticQuiryEvents> = new DiagnosticBus(DIAGNOSTIC_CHANNEL_PREFIX);

/**
 * Subscribe to module-level peer lifecycle events. Returns an unsubscribe function.
 * @see {@link QuiryEvents} for the full event catalog.
 */
export function on<K extends keyof QuiryEvents>(
  event: K,
  listener: (...args: QuiryEvents[K]) => void,
): Unsubscribe {
  emitter.on(event, listener as (...args: unknown[]) => void);
  return () => emitter.off(event, listener as (...args: unknown[]) => void);
}

// --------- PUBLIC API: PERSISTENCE --------- //

const PEER_OPTIONS_KEYS = ["identifier", "creditWindow", "drainTimeout"] as const;
export interface PeerAttachOptions extends SessionOptions {
  identifier?: PeerIdentifier;
}

/**
 * Forks a child (node:child_process) at `filename` and attaches it
 * as a new peer via {@link ChildProcessTransport}.
 */
export function fork<TObjects extends RemoteRegistry = {}>(
  filename: string | URL,
  options: NJSForkOptions & PeerAttachOptions = {},
): PeerConnection<TObjects> {
  const [peer_options, fork_options] = partition(options, PEER_OPTIONS_KEYS);
  const subprocess = NJSFork(filename, fork_options);
  return attach(new ChildProcessTransport(subprocess), peer_options);
}

/**
 * Spawns a worker (node:worker_threads) at `filename` and attaches it
 * as a new peer via {@link WorkerThreadsTransport}.
 */
export function spawn<TObjects extends RemoteRegistry = {}>(
  filename: string | URL,
  options: NJSWorkerOptions & PeerAttachOptions = {},
): PeerConnection<TObjects> {
  const [peer_options, worker_options] = partition(options, PEER_OPTIONS_KEYS);
  const worker = new NJSWorker(filename, worker_options);
  return attach(new WorkerThreadsTransport(worker), peer_options);
}

/**
 * Wrap an existing worker thread or child process in the matching transport and attach it
 * as a new peer. This is the same as constructing {@link ChildProcessTransport} or
 * {@link WorkerThreadsTransport} yourself and calling {@link attach}.
 *
 * @throws A {@link TypeError} if `port` is neither a worker thread nor a child process.
 */
export function wrap<TObjects extends RemoteRegistry = {}>(
  port: NJSWorker | NJSChildProcess,
  options?: PeerAttachOptions,
): PeerConnection<TObjects> {
  if (port instanceof NJSChildProcess) return attach(new ChildProcessTransport(port), options);
  if (port instanceof NJSWorker) return attach(new WorkerThreadsTransport(port), options);

  throw new TypeError("Invalid port; must be a child process or worker thread");
}

/**
 * Open a session over `transport` and register it as a new peer connection.
 * @emits `peer-connected` once the session is open, and `peer-disconnected` when it later terminates.
 */
export function attach<TObjects extends RemoteRegistry = {}>(
  transport: Transport<AnyPacket>,
  options: PeerAttachOptions = {},
): PeerConnection<TObjects> {
  let { identifier, ...rest } = options;
  if (identifier) {
    if (peers.has(identifier))
      throw new QuiryError(
        WireStatus.FAILED_PRECONDITION,
        `Peer with identifier ${identifier} is already linked`,
      );
  } else {
    do {
      identifier = randomBytes(4).toString("hex");
    } while (peers.has(identifier));
  }

  const session = new Session(transport, handleInquiry, rest).open();
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
  return connection as unknown as PeerConnection<TObjects>;
}

/**
 * Close a peer connection by its identifier and remove it from the registry.
 * A no-op if `identifier` doesn't resolve to a known peer.
 */
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

// --------- PUBLIC API: REMOTE REGISTRATION --------- //

type NonCtor<T> = T extends new (...args: any[]) => any ? never : T;

/**
 * Register a remote object under `name` so it can be later resolved by remote peers.
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
export function expose<T extends RemoteImpl>(name: string, factory: () => T): T;
export function expose<T extends RemoteImpl>(name: string, value: NonCtor<T>): T;
export function expose(name: string, valueOrFactory: RemoteImpl | (() => RemoteImpl)): RemoteImpl {
  if (registry.has(name)) {
    throw new QuiryError(WireStatus.FAILED_PRECONDITION, `Remote object "${name}" is already registered`);
  }

  const impl = typeof valueOrFactory === "function" ? valueOrFactory() : valueOrFactory;
  if (impl === null || typeof impl !== "object") {
    throw new QuiryError(
      WireStatus.INVALID_ARGUMENT,
      `Remote identifier "${name}" must resolve to an object (got ${impl === null ? "null" : typeof impl})`,
    );
  }

  registry.set(name, impl);
  return impl;
}

/**
 * Remove a remote object registration. In-flight peer requests against the
 * unexposed object fail with `NOT_FOUND`.
 *
 * @returns `true` if the object was unregistered, `false` otherwise.
 */
export function unexpose(name: string): boolean {
  return registry.delete(name);
}

/** Resolve a registered remote object by name, or `undefined` if not registered. */
export function get<T extends RemoteImpl = RemoteImpl>(name: string): T | undefined {
  return registry.get(name) as T | undefined;
}

/** Whether a remote object is registered under `name`. */
export function has(name: string): boolean {
  return registry.has(name);
}

/** Remove every remote object registration. Peer connections are not affected. */
export function clear(): void {
  registry.clear();
}

// --------- PUBLIC API: UTILITY --------- //

/**
 * Returns the abort signal tied to the remote call currently executing on
 * this async continuation, or `undefined` if called outside of one.
 */
export function signal(): AbortSignal | undefined {
  return contextStorage.getStore()?.signal;
}

// --------- INTERNAL --------- //

function handleInquiry(request: InquiryRequest): ReturnType<InquiryFunc> {
  const context = {
    detail: { query: { object: request.object, property: request.property } },
  };

  const impl = registry.get(request.object);
  if (!impl) throw new QuiryError(WireStatus.NOT_FOUND, `Remote object ${request.object} not found`, context);
  if (!(request.property in impl)) {
    throw new QuiryError(
      WireStatus.NOT_FOUND,
      `Property ${request.property} does not exist in object ${request.object}`,
      context,
    );
  }

  return makeInquiryDescriptor(impl, request.property);
}

function makeInquiryDescriptor<T = unknown>(impl: object, key: PropertyKey): InquiryDescriptor<T> {
  const [target, descriptor] = fetchDescriptor(impl, key);
  if (!descriptor) {
    throw new ReferenceError(`Property ${String(key)} does not exist in object ${String(impl)}`);
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

function partition<T extends object, K extends keyof T>(
  obj: T,
  keys: readonly K[],
): [Pick<T, K>, Omit<T, K>] {
  const picked = {} as Pick<T, K>;
  const omitted = { ...obj } as Omit<T, K>;

  for (const key of keys) {
    if (key in obj) {
      picked[key] = obj[key];
      delete (omitted as any)[key];
    }
  }

  return [picked, omitted];
}

export { QuiryError, WorkerThreadsTransport, ChildProcessTransport, WireStatus, type PeerConnection };
export { type Serializer, type Serializable, registerSerializer } from "./lib/transfer";
export * from "./core/symbols";
