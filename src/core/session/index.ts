import * as Packets from "../../protocol/packets";
import { WireKind, WireStatus } from "../../protocol/wire";
import type { CorrelationId } from "../../protocol/types";

import { EventEmitter } from "node:events";

import { Router } from "../../lib/router";
import { DiagnosticBus } from "../../lib/diagnostics";
import {
  DIAGNOSTIC_CHANNEL_PREFIX,
  type SessionEvents as DiagnosticSessionEvents,
} from "../../interface/diagnostics";

import { QuiryError } from "../../protocol/errors";
import {
  TransportState,
  type Transport,
  type BackpressureSignal,
  type TransportError,
  BackpressureState,
} from "../transport";

import { SessionState } from "./state";
import type { SessionContext } from "./context";
import type { InquiryFunc, InquiryRequest, InquiryDescriptor } from "./inquiry";

export { SessionState };
export type { CallbackProxy, InquiryDescriptor, InquiryFunc, InquiryRequest };

import { InboundRequests } from "./channel/inbound-requests";
import { OutboundRequests } from "./channel/outbound-requests";
import { CallbackBridge, type CallbackProxy } from "./channel/callback-bridge";
import { DrainCoordinator } from "./channel/drain-coordinator";

import { randomBytes } from "node:crypto";

interface InteractiveRouter {
  /**
   * Awaits the next inbound packet matching `predicate` via the session {@link Router}.
   * @throws {@link QuiryError} `DEADLINE_EXCEEDED` on timeout, `ABORTED` on signal — not the raw `Error` strings from {@link Router.wait}.
   */
  wait<K extends Packets.AnyPacket["kind"], P extends Packets.PacketByKind<K>>(
    kind: K,
    predicate?: (packet: Packets.PacketByKind<K>) => packet is P,
    options?: { timeout?: number; signal?: AbortSignal },
  ): Promise<P>;

  /** Passive, persistent listener for a specific packet kind. Matching packets are still forwarded to other consumers. Returns unsubscribe. */
  listen<K extends Packets.AnyPacket["kind"], P extends Packets.PacketByKind<K>>(
    kind: K,
    predicate: (packet: Packets.PacketByKind<K>) => packet is P,
    handler: (packet: P) => void,
  ): Unsubscribe;

  /**
   * Active interceptor for a specific packet kind. When `handler` returns `true` the packet
   * is consumed and not forwarded to the default handler. Returns unsubscribe.
   */
  intercept<K extends Packets.AnyPacket["kind"], P extends Packets.PacketByKind<K>>(
    kind: K,
    predicate: (packet: Packets.PacketByKind<K>) => packet is P,
    handler: (packet: P) => boolean,
  ): Unsubscribe;
}

export interface SessionOptions {
  drainTimeout?: number;
  creditWindow?: number;
}

const DEFAULT_DRAIN_TIMEOUT: number = 5000;
const DEFAULT_CREDIT_WINDOW: number = 100;

/** Public lifecycle events. Domain-level only. */
export interface SessionEvents {
  "state-change": [next: SessionState, prev: SessionState];
  terminate: [reason?: string];
  error: [error: Error];
}

/**
 * Bidirectional RPC session over a {@link Transport}: requests, streams, callbacks, and drain.
 * Incoming routing runs on a {@link Router}; unhandled async errors in packet handlers shut the session down.
 */
export class Session {
  /** Generates a random correlation id. */
  static correlate(): CorrelationId {
    return randomBytes(4).toString("hex") as CorrelationId;
  }

  private readonly emitter = new EventEmitter();
  private readonly router: Router<Packets.AnyPacket>;
  private readonly options: Required<SessionOptions>;

  #state: SessionState = SessionState.CLOSED;
  readonly diagnostic = new DiagnosticBus<DiagnosticSessionEvents>(DIAGNOSTIC_CHANNEL_PREFIX);

  /** A wrapper around the internal router to provide a more convenient API. */
  get channel(): InteractiveRouter {
    return {
      wait: (kind, predicate, options) =>
        this.router
          .wait<any>((packet) => packet.kind === kind && (predicate ? predicate(packet) : true), options)
          .catch((cause: unknown) => {
            throw new QuiryError(WireStatus.ABORTED, "Operation was aborted", { cause });
          }),
      listen: (kind, predicate, handler) =>
        this.router.listen(
          (packet) => packet.kind === kind && (predicate ? predicate(packet) : true),
          handler,
        ),
      intercept: (kind, predicate, handler) =>
        this.router.intercept(
          (packet) => packet.kind === kind && (predicate ? predicate(packet) : true),
          handler,
        ),
    };
  }

  /** Producer-side dispatch + producer-stream lifecycle. */
  readonly inbound: InboundRequests;
  /** Consumer-side RPC: set / get / request / stream and their settlement. */
  readonly outbound: OutboundRequests;
  /** Callback proxy machinery: substitution, restoration, invocation, release. */
  readonly callbacks: CallbackBridge;
  /** Cooperative drain protocol: announce, quiesce, ACK, terminate. */
  readonly coordinator: DrainCoordinator;

  constructor(
    private readonly transport: Transport<Packets.AnyPacket>,
    readonly inquiry: InquiryFunc,
    options: SessionOptions = {},
  ) {
    this.router = new Router(this.transport.receive());
    this.options = {
      drainTimeout: options.drainTimeout ?? DEFAULT_DRAIN_TIMEOUT,
      creditWindow: options.creditWindow ?? DEFAULT_CREDIT_WINDOW,
    };

    const base: Omit<SessionContext, "callbacks"> = {
      diagnostic: this.diagnostic,
      state: () => this.#state,
      send: <P extends Packets.AnyTypedPacket>(
        packet: Omit<P, "id" | "timestamp"> & { id?: CorrelationId },
      ): Promise<CorrelationId> => this.send<P>(packet),
      correlate: () => Session.correlate(),
    };
    this.callbacks = new CallbackBridge(base);

    const ctx: SessionContext = { ...base, callbacks: this.callbacks };
    this.outbound = new OutboundRequests(ctx, this.options.creditWindow);
    this.inbound = new InboundRequests(ctx, inquiry);

    this.coordinator = new DrainCoordinator({
      state: () => this.#state,
      transition: (next) => this.transition(next),
      send: ctx.send,
      diagnostic: this.diagnostic,
      router: {
        wait: <P extends Packets.AnyPacket>(
          predicate: (packet: Packets.AnyPacket) => packet is P,
          options?: { timeout?: number; signal?: AbortSignal },
        ): Promise<P> => this.router.wait<P>(predicate, options),
      },
      transport: { on: (event, handler) => this.transport.on(event, handler) },
      inbound: this.inbound,
      outbound: this.outbound,
      callbacks: this.callbacks,
      terminate: (reason) => this.terminate(reason),
      config: { drainTimeout: this.options.drainTimeout },
    });
  }

  /** Fills source and timestamp, then {@link Session.forward}. */
  send<P extends Packets.AnyTypedPacket>(
    packet: Omit<P, "id" | "timestamp"> & { id?: CorrelationId },
  ): Promise<CorrelationId> {
    return this.forward({
      id: packet.id ?? Session.correlate(),
      timestamp: Date.now(),
      ...packet,
    } as P);
  }

  /** Proxies a function to the session and returns a callback handle. */
  proxy<T extends Function>(fn: T): CallbackProxy<T> {
    return this.callbacks.proxy(fn);
  }

  /** Posts packet to the transport. */
  protected async forward<P extends Packets.AnyPacket>(packet: P): Promise<CorrelationId> {
    // Silently drop sends on a closed session. This can happen during teardown
    // when cleanup code attempts to send RELEASE after the transport is gone.
    if (this.#state !== SessionState.CLOSED) {
      if (this.transport.backpressure.state === BackpressureState.CRITICAL) {
        throw new QuiryError(WireStatus.RESOURCE_EXHAUSTED, "Transport backpressure is critical");
      }

      await this.transport.send(packet);
    }
    return packet.id;
  }

  private transition(next: SessionState): void {
    if (this.#state === next) return;
    const prev = this.#state;
    this.#state = next;
    this.emitter.emit("state-change", next, prev);
    this.diagnostic.maybe("session:state")?.({ prev, next });
  }

  on<K extends keyof SessionEvents>(
    event: K,
    listener: (...args: SessionEvents[K]) => void,
    { once = false }: { once?: boolean } = {},
  ): () => void {
    this.emitter[once ? "once" : "on"](event, listener);
    return () => this.emitter.off(event, listener);
  }

  // --------- PUBLIC API: LIFECYCLE --------- //

  /**
   * Opens transport and starts the receive {@link Router}.
   * @throws {@link QuiryError} `FAILED_PRECONDITION` if not in `closed`, or transport/router errors.
   */
  open() {
    if (this.#state !== SessionState.CLOSED)
      throw new QuiryError(WireStatus.FAILED_PRECONDITION, "Cannot open session in the current state");

    this.transport.on("close", this.onTransportClose);
    this.transport.on("error", this.onTransportError);
    // (error handlers are automatically disposed when the transport is closed)

    this.transport.open();
    // Router runs for the lifetime of the session and is stopped inside teardown.
    void this.router.start(this.routeIncomingPacket.bind(this)).catch((error: unknown) => {
      // The router's source stream errored. Treat as fatal — we can't receive any more packets.
      this.diagnostic.maybe("transport:error")?.({
        kind: "receive",
        message: QuiryError.from(error).message,
      });
      this.terminate();
    });

    this.transition(SessionState.OPEN);
    this.diagnostic.maybe("session:open")?.({});
    return this;
  }

  /**
   * Initiates a cooperative close. Graceful close runs the drain protocol (announces, quiesces,
   * waits for peer ACK); non-graceful skips straight to `terminate`.
   *
   * Multiple concurrent calls collapse onto a single drain promise.
   */
  async close(reason?: string, graceful: boolean = true): Promise<void> {
    if (this.#state === SessionState.CLOSED) return;
    if (!graceful) return this.terminate();
    return this.coordinator.begin("local", reason);
  }

  // --------- INTERNALS: LIFECYCLE --------- //

  private terminate(reason?: string): void {
    if (this.#state === SessionState.CLOSED) return;
    const previous = this.#state;
    this.#state = SessionState.CLOSED;

    // Stop the router; any waiters still pending get rejected with
    // "Stream closed" via the router's own cleanup.
    this.router.stop();

    const error = new QuiryError(WireStatus.ABORTED, "Session draining");
    this.outbound.rejectAll(error);
    // Force unlock activity counters and drain waiters.
    this.inbound.drain();
    this.outbound.drain();
    this.callbacks.drain(error);

    this.emitter.emit("state-change", SessionState.CLOSED, previous);
    this.diagnostic.maybe("session:state")?.({
      prev: previous as "open" | "draining" | "closed",
      next: "closed",
    });
    this.emitter.emit("terminate", reason);
    this.diagnostic.maybe("session:terminate")?.({ reason });

    try {
      this.transport.close(reason);
    } catch {}
  }

  // --------- PUBLIC API: REQUESTS & CALLBACKS --------- //

  readonly set: typeof this.outbound.set = (...args) => this.outbound.set(...args);
  readonly get: typeof this.outbound.get = (...args) => this.outbound.get(...args);

  request(...args: Parameters<typeof this.outbound.request>): Promise<unknown> {
    return this.outbound.request(...args);
  }

  stream(...args: Parameters<typeof this.outbound.stream>): AsyncIterableIterator<unknown> {
    return this.outbound.stream(...args);
  }

  // --------- INTERNALS: ROUTING --------- //

  private routeIncomingPacket(packet: Packets.AnyPacket): void {
    // Each handle* is fire-and-forget (concurrent). Any unhandled throw
    // from them is an internal bug, not a protocol violation — route via
    // the supervisor with `fatal` severity since the session's state may
    // now be inconsistent.
    const attend = (p: PromiseLike<unknown> | unknown): void => {
      Promise.resolve(p).catch((error: unknown) => {
        this.diagnostic.maybe("transport:error")?.({
          kind: "receive",
          message: QuiryError.from(error).message,
        });
        this.terminate();
      });
    };

    switch (packet.kind) {
      case WireKind.REQUEST:
        // Producer-side dispatch + control packets (ABORT/CANCEL).
        attend(this.inbound.handleRequestPacket(packet));
        break;

      case WireKind.RESPONSE:
        // Synchronous routing to the correlation map; async work dispatched inside.
        attend(
          (() => {
            if (!packet.payload.ref) return;

            // Producer-side credit grants belong to the producer's stream loop;
            // everything else is a consumer-side settlement.
            if (packet.type === Packets.ResponseMessageType.STREAM && packet.payload.event === "credit") {
              this.inbound.handleCreditGrant(packet);
              return;
            }

            this.outbound.handleResponsePacket(packet);
          })(),
        );
        break;

      case WireKind.CALLBACK:
        // CBK:INVOKE is concurrent, with each invocation independent of others.
        // CBK:RETURN/RELEASE are handled synchronously (no I/O, just map operations).
        attend(this.callbacks.handleCallbackPacket(packet));
        break;

      case WireKind.SYSTEM:
        // System packets are handled sequentially; ordering matters.
        attend(
          (() => {
            switch (packet.type) {
              case Packets.SystemMessageType.DRAIN:
                this.coordinator.handleSystemDrainPacket(packet);
                break;
              case Packets.SystemMessageType.DRAIN_ACK:
                // Informational only at this stage; the drain coroutine awaits
                // the matching packet via the router's interactive `wait`.
                break;
            }
          })(),
        );
        break;

      default: {
        // Conforming peers don't send this; throwing would kill the
        // router loop, so we surface it via diag and drop.
        const kind = (packet as { kind?: unknown }).kind;
        this.diagnostic.maybe("transport:error")?.({
          kind: "receive",
          message: `Unknown packet kind: ${String(kind)}`,
        });
      }
    }
  }

  // --------- PUBLIC API: STATUS --------- //

  get state(): SessionState {
    return this.#state;
  }

  get status(): SessionStatus {
    return {
      state: this.#state,
      pending: this.outbound.pendingCount,
      streams: this.inbound.streamCount,
      callbacks: this.callbacks.callbackCount,
      invocations: this.callbacks.pendingInvocationCount,
      backpressure: this.transport.backpressure,
      stubs: this.callbacks.remoteStubCount,
    };
  }

  // --------- INTERNALS: EVENT HANDLERS --------- //

  private readonly onTransportClose = (reason?: string): void => {
    if (this.#state === SessionState.CLOSED || this.#state === SessionState.DRAINING) return;
    this.diagnostic.maybe("transport:error")?.({
      kind: "terminate",
      message: reason ?? "transport closed",
    });

    // The transport closed on its own (peer exited, IPC channel dropped)
    // without us going through the drain protocol. Outside of an active
    // drain, nothing else observes this — tear the session down so
    // pending work fails fast and `terminate` fires for peer bookkeeping.
    this.terminate(reason ?? "transport closed");
  };

  private readonly onTransportError = ({ message, kind, cause }: TransportError): void => {
    if (this.#state === SessionState.CLOSED || this.#state === SessionState.DRAINING) return;

    // Map TransportError.kind → WireStatus. Transport errors never escape
    // the transport unclassified — this is the only place that translation
    // happens. The mapped error is exposed on the diag bus; subscribers
    // route it to logs/metrics as they see fit.
    let error: QuiryError;
    switch (kind) {
      case "terminate":
        error = new QuiryError(WireStatus.UNAVAILABLE, message, { cause });
        break;
      case "receive":
        error = new QuiryError(WireStatus.DATA_LOSS, message, { cause });
        break;
      case "send":
        error =
          this.transport.state === TransportState.CLOSED
            ? new QuiryError(WireStatus.UNAVAILABLE, message, { cause })
            : new QuiryError(WireStatus.DATA_LOSS, message, { cause });
        break;
    }

    this.diagnostic.maybe("transport:error")?.({ kind, message: error.message });
    if (this.emitter.listenerCount("error") > 0) this.emitter.emit("error", error);

    // A transport-level error means the channel is no longer usable.
    this.terminate(error.message);
  };
}

export interface SessionStatus {
  readonly state: SessionState;
  readonly pending: number;
  readonly streams: number;
  readonly callbacks: number;
  /** Outstanding remote callback invocations awaiting RETURN. */
  readonly invocations: number;
  /** Stub ids tracked across all in-flight inbound requests. */
  readonly stubs: number;
  readonly backpressure: BackpressureSignal;
}
