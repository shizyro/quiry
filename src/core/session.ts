import EventEmitter from "node:events";

import {
  TransportState,
  type BackpressureSignal,
  type Transport,
  type TransportError,
} from "@/core/transport";
import {
  WireKind,
  WireStatus,
  type CallbackId,
  type CorrelationId,
  type InvocationId,
  type RequestControl,
  type RetryPolicy,
} from "@/interface/base";

import {
  CallbackMessageType,
  RequestMessageType,
  ResponseMessageType,
  SystemMessageType,
  type AbortRequestPacket,
  type AnyCallbackPacket,
  type AnyPacket,
  type AnyRequestPacket,
  type AnyResponsePacket,
  type AnySystemPacket,
  type AnyTypedPacket,
  type CallbackInvokePacket,
  type CallbackReleasePacket,
  type CallbackReturnPacket,
  type CallRequestPacket,
  type CancelRequestPacket,
  type GetRequestPacket,
  type PacketByKind,
  type StreamResponsePacket,
  type SystemDrainAckPacket,
  type SystemDrainPacket,
  type ValueResponsePacket,
} from "@/interface/packets";

import { Router } from "@/lib/router";
import { AsyncQueue } from "@/lib/queue";
import { QuiryError, isRetryableStatus, fromWireError, toWireError } from "@/shared/errors";

import { CallbackRegistry, CallbackScope, isCallbackStub, stub, type Callback } from "@/lib/callbacks";

import { isSerializable, isAnyIterableIterator, clip } from "@/lib/helpers";
import { retryable, timeout, abortable } from "@/lib/utils";
import { InFlightTracker } from "@/lib/tracker";

import { nanoid } from "nanoid";

export type OmitStandardFields<T> = Omit<T, "id" | "from" | "timestamp">;

/**
 * A symbol used to mark a property to substitute the entire object
 * with before sending it to the remote side.
 *
 * Might want to think of a better name for this.
 */
export const Normalized: unique symbol = Symbol("quiry.normalized");

/** Unwraps `[Normalized]` aliases; preserves cycles for a later `isSerializable` rejection instead of blowing the stack. */
function normalize<T = unknown>(value: T, seen?: WeakSet<object>): T {
  if (Object(value) !== value || value === null) return value;
  if (Normalized in (value as object)) return (value as unknown as { [Normalized]: T })[Normalized];
  if (typeof value === "object") {
    // Cycle protection: if we've already seen this node on the current
    // walk, return it unchanged. The cycle will still be present in the
    // returned graph, which is exactly what `isSerializable` relies on
    // to reject it downstream with INVALID_ARGUMENT (rather than the
    // walk blowing the stack with a RangeError).
    seen ??= new WeakSet();
    if (seen.has(value as object)) return value;
    seen.add(value as object);

    if (Array.isArray(value)) return value.map((v) => normalize(v, seen)) as T;
    const result: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value as object)) {
      result[key] = normalize(val, seen);
    }
    return result as T;
  }
  return value;
}

export type InquiryFunc = (
  request: InquiryRequest,
) => Promise<unknown> | AsyncIterableIterator<unknown> | IterableIterator<unknown>;
export type InquiryRequest = Readonly<{
  service: string;
  property: string;
  args: ReadonlyArray<unknown>;
}>;

interface PendingCallRequest<T = unknown> {
  readonly kind: "call";
  resolve: (value: T) => void;
  reject: (error: Error) => void;
  timestamp: number;
  timer?: ReturnType<typeof setTimeout>;
}

interface PendingStreamRequest {
  readonly kind: "stream";
  readonly queue: AsyncQueue<unknown>;
  readonly credit: { remaining: number };
  timer?: ReturnType<typeof setTimeout>;
  seq: number;
}

interface PendingGetRequest<T = unknown> {
  readonly kind: "get";
  resolve: (value: T) => void;
  reject: (error: Error) => void;
}

/**
 * Producer-side state for an in-flight streaming response. Tracks the flow
 * control budget granted by the remote consumer and the cancellation flag so
 * the inquiry iterator can be terminated mid-stream.
 */
interface OutboundStream<T = unknown> {
  /** Chunks we are allowed to send before needing more credit. */
  credit: number;
  /** Pending waiter blocked on credit; resolved with `false` on cancel. */
  waiter: ((ok: boolean) => void) | null;
  cancelled: boolean;
  iterator: IterableIterator<T> | AsyncIterableIterator<T>;
}

interface PendingCallbackInvocation<T = unknown> {
  /** Original request correlation id under which this invocation was issued. */
  readonly ref: CorrelationId;
  resolve: (value: T) => void;
  reject: (error: Error) => void;
  timer?: ReturnType<typeof setTimeout>;
}

export enum SessionState {
  OPEN = "open",
  PEERING = "peering",
  DRAINING = "draining",
  CLOSED = "closed",
}

export interface SessionConfig {
  readonly defaultTimeout?: number;
  readonly defaultRetry?: RetryPolicy;
  readonly drainTimeout?: number;
  /** The number of chunks to prefetch for each streaming request. */
  readonly creditWindow?: number;
}

export interface SessionEvents {
  "state-change": [next: SessionState, prev: SessionState];
  terminate: [reason?: string];
  error: [error: Error];
}

export interface InteractiveRouter {
  /** Passive, persistent listener for a specific packet kind. Matching packets are still forwarded to other consumers. Returns unsubscribe. */
  listen<K extends AnyPacket["kind"], R extends PacketByKind<K>>(
    kind: K,
    predicate: (packet: PacketByKind<K>) => packet is R,
    handler: (packet: R) => void,
  ): Unsubscribe;

  /**
   * Active interceptor for a specific packet kind. When `handler` returns `true` the packet
   * is consumed and not forwarded to the default handler. Returns unsubscribe.
   */
  intercept<K extends AnyPacket["kind"], R extends PacketByKind<K>>(
    kind: K,
    predicate: (packet: PacketByKind<K>) => packet is R,
    handler: (packet: R) => boolean,
  ): Unsubscribe;
}

/**
 * Bidirectional RPC session over a {@link Transport}: handshake, requests, streams, callbacks, and drain.
 * Incoming routing runs on a {@link Router}; unhandled async errors in packet handlers shut the session down.
 */
export class Session {
  private readonly emitter = new EventEmitter();
  #state: SessionState = SessionState.PEERING;

  private readonly config: DeepRequired<SessionConfig>;
  private readonly conveyor: Router<AnyPacket>;

  /** A wrapper around the internal router to provide a more convenient API. */
  get router(): InteractiveRouter {
    return {
      listen: (kind, predicate, handler) =>
        this.conveyor.listen(
          (packet) => packet.kind === kind && (predicate ? predicate(packet) : true),
          handler,
        ),
      intercept: (kind, predicate, handler) =>
        this.conveyor.intercept(
          (packet) => packet.kind === kind && (predicate ? predicate(packet) : true),
          handler,
        ),
    };
  }

  private readonly inbound = new InFlightTracker();
  private readonly outbound = new InFlightTracker();
  private readonly callbacks = new CallbackRegistry();

  readonly #pending_requests = new Map<
    CorrelationId,
    PendingCallRequest | PendingStreamRequest | PendingGetRequest
  >();
  readonly #outbound_streams = new Map<CorrelationId, OutboundStream>();

  readonly #inflight_invocations = new Map<CorrelationId, number>();
  readonly #pending_invocations = new Map<InvocationId, PendingCallbackInvocation>();
  readonly #remote_stubs = new Map<CorrelationId, Set<CallbackId>>();

  readonly #controllers = new Map<CorrelationId, AbortController>();

  constructor(
    private readonly transport: Transport,
    private readonly inquiry: InquiryFunc = () => Promise.resolve(),
    config: SessionConfig = {},
    private readonly logger: Logger | null = null,
  ) {
    this.conveyor = new Router(this.transport.receive());
    this.config = {
      defaultTimeout: config.defaultTimeout ?? 10_000,
      drainTimeout: config.drainTimeout ?? 5000,
      creditWindow: config.creditWindow ?? 100,
      defaultRetry: {
        maxAttempts: config.defaultRetry?.maxAttempts ?? 3,
        delay: config.defaultRetry?.delay ?? 1000,
        backoffStrategy: config.defaultRetry?.backoffStrategy ?? "exponential",
      },
    };
  }

  /** Fills `id`, `from`, `timestamp`, then {@link Session.forward}. */
  send(packet: Omit<AnyTypedPacket, "id" | "from" | "timestamp">): Promise<CorrelationId> {
    return this.forward({
      id: nanoid() as CorrelationId,
      timestamp: Date.now(),
      ...packet,
    } as AnyPacket);
  }

  /**
   * Sends on the transport when not `closed`; otherwise no-ops (returns id) so drain/teardown can still complete.
   */
  protected async forward(packet: AnyPacket): Promise<CorrelationId> {
    // Silently drop sends on a closed session. This can happen during teardown
    // when cleanup code attempts to send RELEASE after the transport is gone.
    if (this.#state !== SessionState.CLOSED) {
      // TODO: account for backpressure
      await this.transport.send(packet);
    } else this.logger?.trace("Attempted to send packet on closed session");
    return packet.id;
  }

  private transition(state: SessionState): void {
    if (this.#state === state) return;
    const prev = this.#state;
    this.#state = state;
    this.emitter.emit("state-change", state, prev);

    this.logger?.debug(`Session state changed to ${state}`);
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
   * Opens transport, performs system handshake, starts the receive {@link Router}.
   * @throws {@link QuiryError} `FAILED_PRECONDITION` if not in `peering`, or handshake/deadline failures.
   */
  open() {
    if (this.#state !== SessionState.PEERING)
      throw new QuiryError(WireStatus.FAILED_PRECONDITION, "Cannot open session in the current state");

    this.transport.on("state-change", (next) => next === "closed" && this.onTransportClose());
    this.transport.on("error", this.onTransportError);
    // (error handlers are automatically disposed when the transport is closed)

    this.transport.attach();
    // Router runs for the lifetime of the session and is stopped inside teardown.
    void this.conveyor.start(this.routeIncomingPacket.bind(this)).catch((error: unknown) => {
      // The router's source stream errored. Treat as fatal — we can't receive any more packets.
      this.logger?.error("Router source stream errored", { error: QuiryError.from(error) });
      this.terminate();
    });
    this.transition(SessionState.OPEN);

    this.logger?.info("Established session connection with peer");
    return this;
  }

  /**
   * Initiates a cooperative close. Graceful close runs the drain protocol (announces, quiesces,
   * waits for peer ACK); non-graceful (or `peering` state) skips straight to `terminate`.
   * Multiple concurrent calls collapse onto a single drain promise.
   */
  async close(reason?: string, graceful: boolean = true): Promise<void> {
    if (this.#state === SessionState.CLOSED) return;
    if (!graceful || this.#state === SessionState.PEERING) return this.terminate();
    return (this.#drain_promise ??= this.performDrain("local", reason));
  }

  // --------- INTERNALS: LIFECYCLE --------- //

  /**
   * Null until drain begins; stays resolved afterward
   * so later callers see "already drained" without re-entering the protocol.
   */
  #drain_promise: Promise<void> | null = null;
  /** Correlation id of the peer's `SYS:DRAIN` packet, if any. */
  #peer_drain_ref: CorrelationId | null = null;
  /** True once our own in-flight work has drained to zero. */
  #drain_quiesced: boolean = false;

  /**
   * A side may only send `DRAIN_ACK` *after* its own in-flight work has
   * finished. Sending earlier would be a lie. Each side's ACK send is
   * independent of receiving the peer's ACK. Both sides quiesce on their
   * own timeline; whoever finishes first ACKs first. If we reversed this,
   * both sides would wait on each other and nothing would ever fire.
   *
   * A `DRAIN` arriving after we've already quiesced is ACKed inline by
   * the handler, so a late-arriving peer DRAIN can't miss the ACK window.
   */
  private async performDrain(
    initiator: "local" | "remote",
    reason: string = "explicit",
    timeout: number = this.config.drainTimeout,
  ): Promise<void> {
    if (this.#state === SessionState.CLOSED) return;
    this.transition(SessionState.DRAINING);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeout);

    // Short-circuit the drain the moment the transport dies under us.
    this.transport.on("state-change", (next) => next === "closed" && controller.abort());

    try {
      // 1. Local initiator announces. Remote initiator stays silent —
      //    its peer has already sent DRAIN and is waiting for our
      //    terminal ACK, which we'll send in step 3 after we quiesce.
      if (initiator === "local") {
        await this.send({
          kind: WireKind.SYSTEM,
          type: SystemMessageType.DRAIN,
          payload: { reason, timeout },
        } satisfies OmitStandardFields<SystemDrainPacket>).catch(() => null);
      }

      // 2. Proactively terminate work that can run past the deadline:
      //    producer-side generators (may emit indefinitely) and the
      //    consumer-side iterators we're no longer going to consume.
      let count = 0;
      for (const stream of this.#outbound_streams.values()) {
        this.cancelOutboundStream(stream);
        count++;
      }

      for (const [ref, entry] of Array.from(this.#pending_requests)) {
        if (entry.kind !== "stream") continue;

        clearTimeout(entry.timer);
        this.#pending_requests.delete(ref);
        this.outbound.exit();
        entry.queue.fail(
          new QuiryError(WireStatus.ABORTED, "Stream aborted by session drain", {
            correlationId: ref,
          }),
        );

        // Tell the producer to stop emitting.
        void this.send({
          kind: WireKind.REQUEST,
          type: RequestMessageType.CANCEL,
          payload: { ref },
        } satisfies OmitStandardFields<CancelRequestPacket>).catch(() => null);
      }

      // 3. Quiesce and ACK in parallel with waiting for the peer's ACK.
      //
      //    `quiesce` waits for our in-flight work; once it resolves, we
      //    send the terminal DRAIN_ACK if the peer's DRAIN ref is known.
      //    Both tasks share the same `drainTimeout` via `controller`.
      const quiesce = Promise.all([this.inbound.idle(), this.outbound.idle()]).then(async () => {
        if (this.#peer_drain_ref) {
          await this.send({
            kind: WireKind.SYSTEM,
            type: SystemMessageType.DRAIN_ACK,
            payload: {
              ref: this.#peer_drain_ref,
              uptime: process.uptime(),
            },
          } satisfies OmitStandardFields<SystemDrainAckPacket>).catch(() => null);
        }
        // Flip the flag *after* the send so the DRAIN handler
        // doesn't fire a duplicate ACK during the await window.
        this.#drain_quiesced = true;
      });

      // Only the initiator of a DRAIN waits for a reciprocal ACK —
      // remote-initiated drains are one-way from the peer's POV.
      const peerAck =
        initiator === "local"
          ? this.conveyor
              .wait<SystemDrainAckPacket>(
                (p) => p.kind === WireKind.SYSTEM && p.type === SystemMessageType.DRAIN_ACK,
                { signal: controller.signal },
              )
              .then(({ id }) => this.logger?.debug(`Received DRAIN_ACK (${clip(id)}) from remote peer`))
              .catch(() => null) // aborted by signal; teardown anyway
          : Promise.resolve();

      await Promise.race([
        Promise.all([quiesce, peerAck]),
        new Promise<never>((_, reject) => {
          if (controller.signal.aborted) return reject("aborted");
          controller.signal.addEventListener("abort", () => reject("aborted"), { once: true });
        }),
      ]);

      // 4. Best-effort release session-scoped callbacks. By now the peer
      //    has signaled it's done, so no further INVOKEs can arrive.
      await this.releaseSessionCallbacks();

      // ...
    } catch (cause: unknown) {
      // Deadline or transport death — we still want to tear down cleanly.
      initiator === "local" && this.logger?.debug("Drain interrupted or timed out; proceeding to teardown");
    } finally {
      clearTimeout(timer);
      this.terminate(reason);
    }
  }

  private terminate(reason?: string): void {
    if (this.#state === SessionState.CLOSED) return;
    const prev = this.#state;
    this.#state = SessionState.CLOSED;

    // Stop the router; any waiters still pending get rejected with
    // "Stream closed" via the router's own cleanup.
    this.conveyor.stop();

    // Reject all pending calls
    this.rejectAllPending(new QuiryError(WireStatus.ABORTED, "Session draining"));

    // Force unlock all activity counters and drain waiters
    this.inbound.drain();
    this.outbound.drain();

    // Clear callback registry (no RELEASE packets; transport is gone)
    this.callbacks.clear();
    this.#inflight_invocations.clear();

    this.emitter.emit("state-change", SessionState.CLOSED, prev);
    this.emitter.emit("terminate", reason);

    try {
      this.transport.dispose();
    } catch {}
  }

  // --------- PUBLIC API: REQUESTS & CALLBACKS --------- //

  async get(service: string, property: string): Promise<unknown> {
    if (this.#state !== SessionState.OPEN) {
      throw new QuiryError(WireStatus.UNAVAILABLE, "Session is not open");
    }

    const correlation = nanoid() as CorrelationId;
    const body = {
      id: correlation,
      kind: WireKind.REQUEST,
      type: RequestMessageType.GET,
      timestamp: Date.now(),
      payload: { service, property },
    } satisfies GetRequestPacket;

    return new Promise((resolve, reject) => {
      const cleanup = (): void => {
        if (this.#pending_requests.delete(correlation)) this.outbound.exit();
      };

      this.#pending_requests.set(correlation, {
        kind: "get",
        resolve: (value: unknown): void => {
          cleanup();
          resolve(value);
        },
        reject: (error: Error): void => {
          cleanup();
          reject(error);
        },
      });

      this.outbound.enter();

      Promise.resolve(this.forward(body)).catch((cause: unknown) => {
        this.logger?.warn(
          `Failed to send packet ${clip(body.id)}: ${cause instanceof Error ? cause.message : String(cause)}`,
        );
        cleanup();
        reject(
          new QuiryError(WireStatus.DATA_LOSS, "Failed to send packet", {
            correlationId: correlation,
            cause,
          }),
        );
      });
    });
  }

  /**
   * Unary RPC with optional retries ({@link isRetryableStatus}), timeout, and `AbortSignal` -> `ABORT` on wire.
   * @throws `UNAVAILABLE` when not `open`; `INVALID_ARGUMENT` when args are not serializable; remote errors as {@link QuiryError} from wire.
   */
  async request(
    service: string,
    method: string,
    args: ReadonlyArray<unknown>,
    control?: Omit<RequestControl, "abortable">,
    signal?: AbortSignal,
  ): Promise<unknown> {
    if (this.#state !== SessionState.OPEN) {
      throw new QuiryError(WireStatus.UNAVAILABLE, "Session is not open", { traceId: control?.traceId });
    }

    const correlation = nanoid() as CorrelationId;
    const substitutes = this.callbacks.substitute(normalize(args), correlation);
    // Ensure arguments can be cloned through port
    if (!isSerializable(substitutes))
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Arguments are not serializable", {
        detail: { args },
      });

    {
      const count = substitutes.reduce<number>((prev, curr) => prev + Number(isCallbackStub(curr)), 0);
      if (count > 0) {
        this.logger?.trace(`Substituted ${count} callbacks for request ${clip(correlation)}`);
      }
    }

    const body = {
      id: correlation,
      kind: WireKind.REQUEST,
      type: RequestMessageType.CALL,
      timestamp: Date.now(),
      payload: {
        service,
        method,
        args: substitutes,
        control: { ...control, abortable: signal instanceof AbortSignal },
      },
    } satisfies CallRequestPacket;

    const timeout = control?.timeout ?? this.config.defaultTimeout;
    const context = { correlationId: correlation, traceId: control?.traceId };

    return retryable(
      async () =>
        new Promise((resolve, reject) => {
          let timer: ReturnType<typeof setTimeout> | null = null;
          let abortHandler: (() => void) | null = null;

          // Idempotent per-attempt cleanup.
          const release = (): void => {
            if (timer) {
              clearTimeout(timer);
              timer = null;
            }
            if (abortHandler && signal) {
              signal.removeEventListener("abort", abortHandler);
              abortHandler = null;
            }
            if (this.#pending_requests.delete(body.id)) {
              this.outbound.exit();
            }
          };

          const fail = (error: Error): void => {
            release();
            reject(error);
          };

          timer = setTimeout(
            () =>
              fail(
                new QuiryError(
                  WireStatus.DEADLINE_EXCEEDED,
                  `Request operation timed out after ${timeout}ms`,
                  { ...context, detail: { timeout } },
                ),
              ),
            timeout,
          );

          this.#pending_requests.set(body.id, {
            kind: "call",
            resolve: (value: unknown): void => {
              release();
              resolve(value);
            },
            reject: fail,
            timestamp: Date.now(),
            timer,
          } satisfies PendingCallRequest);

          this.outbound.enter();

          if (signal) {
            abortHandler = () => {
              void this.send({
                kind: WireKind.REQUEST,
                type: RequestMessageType.ABORT,
                payload: { ref: body.id },
              } satisfies OmitStandardFields<AbortRequestPacket>).catch(() => null);
              fail(new QuiryError(WireStatus.ABORTED, "Request operation cancelled", context));
            };

            if (signal.aborted) return abortHandler();
            signal.addEventListener("abort", abortHandler, { once: true });
          }

          // The session stays up unless the transport itself errors,
          // in which case #on_transport_error will terminate everything.
          Promise.resolve(this.forward(body)).catch((cause: unknown) => {
            this.logger?.warn(
              `Failed to send packet ${clip(body.id)}: ${cause instanceof Error ? cause.message : String(cause)}`,
            );
            fail(new QuiryError(WireStatus.DATA_LOSS, "Failed to send packet", { ...context, cause }));
          });
        }),
      {
        retries: (control?.retry?.maxAttempts ?? this.config.defaultRetry.maxAttempts) - 1,
        initialDelay: control?.retry?.delay ?? this.config.defaultRetry.delay,
        backoffStrategy: control?.retry?.backoffStrategy ?? this.config.defaultRetry.backoffStrategy,
        shouldRetry: (error: unknown) => (error instanceof QuiryError ? isRetryableStatus(error.code) : true),
        signal,
      },
    );
  }

  /**
   * Server-streaming consumer: credit-based flow control; iterator `return`/`throw` sends `CANCEL`.
   * When not `open`, returns an iterator whose `next` rejects with `UNAVAILABLE` (sync throw only for bad args).
   */
  stream(
    service: string,
    method: string,
    args: ReadonlyArray<unknown>,
    control?: Omit<RequestControl, "abortable">,
    signal?: AbortSignal, // TODO: umm... decide on this
  ): AsyncIterableIterator<unknown> {
    if (this.#state !== SessionState.OPEN) {
      const q = new AsyncQueue<unknown>();
      void q.throw(new QuiryError(WireStatus.UNAVAILABLE, "Session is not open"));
      return q;
    }

    const correlation = nanoid() as CorrelationId;
    const substitutes = this.callbacks.substitute(normalize(args), correlation);
    // Ensure arguments can be cloned through port
    if (!isSerializable(substitutes))
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Arguments are not serializable", {
        detail: { args },
      });

    const queue = new AsyncQueue<unknown>();
    // The initial budget we grant the producer. Set synchronously so that
    // chunks arriving before the CALL `.then` callback still decrement the
    // right counter (producer cannot start until it sees the credit grant,
    // but other incoming packets are routed concurrently).
    const credit = { remaining: this.config.creditWindow };
    const context = { correlationId: correlation, traceId: control?.traceId };

    this.logger?.debug(
      `Opening stream ${clip(correlation)} to ${service}.*${method} with a window of ${this.config.creditWindow}`,
    );

    const entry: PendingStreamRequest = { kind: "stream", queue, credit, seq: 0 };
    if (control?.timeout) {
      entry.timer = setTimeout(() => {
        this.logger?.debug(`Stream ${clip(correlation)} timed out after ${control.timeout}ms`);
        this.#pending_requests.delete(correlation);
        this.outbound.exit();
        queue.fail(new QuiryError(WireStatus.DEADLINE_EXCEEDED, "Stream request timed out", context));

        // Best-effort cancel on the producer side so it stops emitting.
        void this.send({
          kind: WireKind.REQUEST,
          type: RequestMessageType.CANCEL,
          payload: { ref: correlation },
        } satisfies OmitStandardFields<CancelRequestPacket>).catch(() => {
          // observable; deadline already fired locally
        });
      }, control.timeout);
    }

    this.#pending_requests.set(correlation, entry);
    this.outbound.enter();

    // Send CALL and initial credit grant. CREDIT is bundled as a separate
    // STREAM response packet referencing the correlation; the producer
    // cannot emit chunks until it observes this grant.
    void (async () => {
      try {
        await this.forward({
          id: correlation,
          kind: WireKind.REQUEST,
          type: RequestMessageType.CALL,
          timestamp: Date.now(),
          payload: { service, method, args: substitutes, control },
        } satisfies CallRequestPacket);

        await this.send({
          kind: WireKind.RESPONSE,
          type: ResponseMessageType.STREAM,
          payload: { event: "credit", ref: correlation, credit: this.config.creditWindow },
        } satisfies OmitStandardFields<StreamResponsePacket>);

        this.logger?.trace(`Stream ${clip(correlation)} initial credit grant sent`);
      } catch (cause: unknown) {
        this.logger?.warn(
          `Failed to initiate stream ${clip(correlation)}: ${cause instanceof Error ? cause.message : String(cause)}`,
        );

        const entry = this.#pending_requests.get(correlation);
        if (entry?.kind === "stream") {
          clearTimeout(entry.timer);
          this.#pending_requests.delete(correlation);
          this.outbound.exit();
          queue.fail(
            new QuiryError(WireStatus.DATA_LOSS, "Failed to initiate stream", { ...context, cause }),
          );
        }
      }
    })();

    const self = this;
    const cleanup = (): void => {
      const entry = self.#pending_requests.get(correlation);
      if (entry?.kind === "stream" && entry.timer) clearTimeout(entry.timer);
      self.#pending_requests.delete(correlation);
      self.outbound.exit();
    };

    return {
      next(): Promise<IteratorResult<unknown>> {
        return queue.next();
      },
      [Symbol.asyncIterator](): AsyncIterableIterator<unknown> {
        return this;
      },
      async return(): Promise<IteratorResult<unknown, undefined>> {
        self.logger?.trace(`Stream ${clip(correlation)} consumer returned; sending CANCEL`);
        // Notify the producer to abort. Fire-and-forget; cleanup is
        // local and must not wait on the wire.
        void self
          .send({
            kind: WireKind.REQUEST,
            type: RequestMessageType.CANCEL,
            payload: { ref: correlation },
          } satisfies OmitStandardFields<CancelRequestPacket>)
          .catch(() => {
            // observable; consumer already abandoned the iterator
          });

        cleanup();
        await queue.return();

        return { value: undefined, done: true };
      },
      async throw(error?: unknown): Promise<IteratorResult<unknown, undefined>> {
        self.logger?.trace(`Stream ${clip(correlation)} consumer threw; sending CANCEL`);
        void self
          .send({
            kind: WireKind.REQUEST,
            type: RequestMessageType.CANCEL,
            payload: { ref: correlation },
          } satisfies OmitStandardFields<CancelRequestPacket>)
          .catch(() => {
            // observable
          });

        cleanup();
        queue.fail(new QuiryError(WireStatus.CANCELLED, "Stream aborted by consumer", { cause: error }));

        return { value: undefined, done: true };
      },
    };
  }

  /**
   * Awaits the next inbound packet matching `kind` (and optional `predicate`) via the session {@link Router}.
   * @throws {@link QuiryError} `DEADLINE_EXCEEDED` on timeout, `ABORTED` on signal — not the raw `Error` strings from {@link Router.wait}.
   */
  async wait<K extends AnyPacket["kind"], R extends PacketByKind<K>>(
    kind: K,
    predicate?: (packet: PacketByKind<K>) => packet is R,
    { timeout, signal }: { timeout?: number; signal?: AbortSignal } = {},
  ): Promise<R> {
    // @ts-expect-error: no plans to type this properly
    return this.conveyor
      .wait((packet) => packet.kind === kind && (predicate ? predicate(packet as R) : true), {
        timeout,
        signal,
      })
      .catch((error: unknown) => {
        if (error instanceof Error && error.message.includes("Timeout")) {
          throw new QuiryError(WireStatus.DEADLINE_EXCEEDED, "Timeout waiting for packet", {
            cause: error,
          });
        }
        throw new QuiryError(WireStatus.ABORTED, "Operation was aborted", { cause: error });
      });
  }

  /** Registers a session-scoped callback stub; pair with {@link Session.release} or `CallbackHandle` dispose. */
  bind<T extends Function>(fn: T): Callback {
    const callback = this.callbacks.register(fn, CallbackScope.STACK);
    this.logger?.debug(`Created callback proxy ${clip(callback)}`);
    return { [stub]: true, id: callback, scope: CallbackScope.STACK } satisfies Callback;
  }

  release(id: CallbackId): boolean {
    const removed = this.callbacks.release(id);
    if (removed) this.logger?.debug(`Released callback proxy ${clip(id)}`);
    return removed;
  }

  /**
   * Test/observability surface: cheap membership check on the local
   * callback registry. Public because tests need a non-polling way to
   * assert the registry's contents without reaching through `status`.
   */
  callable(id: CallbackId): boolean {
    return this.callbacks.get(id) !== undefined;
  }

  // --------- INTERNALS: ROUTING --------- //

  private routeIncomingPacket(packet: AnyPacket): void {
    // Each handle* is fire-and-forget (concurrent). Any unhandled throw
    // from them is an internal bug, not a protocol violation — route via
    // the supervisor with `fatal` severity since the session's state may
    // now be inconsistent.
    const attend = (p: PromiseLike<unknown> | unknown): void => {
      Promise.resolve(p).catch((error: unknown) => {
        this.logger?.error("Unhandled error in packet handler", { error: QuiryError.from(error) });
        this.terminate();
      });
    };

    switch (packet.kind) {
      case WireKind.REQUEST:
        // REQ:CALL is dispatched concurrently with no await.
        attend(this.handleRequestPacket(packet));
        break;

      case WireKind.RESPONSE:
        // Synchronous routing to the correlation map; async work dispatched inside.
        attend(this.handleResponsePacket(packet));
        break;

      case WireKind.CALLBACK:
        // CBK:INVOKE is concurrent, with each invocation independent of others.
        // CBK:RETURN/RELEASE are handled synchronously (no I/O, just map operations).
        attend(this.handleCallbackPacket(packet));
        break;

      case WireKind.SYSTEM:
        // System packets are handled sequentially; ordering matters for handshake and drain.
        attend(this.handleSystemPacket(packet));
        break;

      default: {
        // This shouldn't happen with a conforming peer; ignore.
        // Throwing here would kill the router loop and the rejection would be unhandled.
        const kind = (packet as { kind?: unknown }).kind;
        this.logger?.warn(`Unknown packet kind (${String(kind)}) received`, { detail: { packet } });
      }
    }
  }

  private handleSystemPacket(packet: AnySystemPacket): void {
    switch (packet.type) {
      case SystemMessageType.DRAIN: {
        if (this.#state === SessionState.CLOSED) return;

        // Capture the peer's DRAIN correlation id. The terminal
        // `DRAIN_ACK` sent at the end of `performDrain` will reference
        // this id; sending ACK earlier would violate the protocol's
        // "ACK means I'm done" semantic.
        this.#peer_drain_ref = packet.id;

        if (this.#state === SessionState.DRAINING) {
          // Concurrent drain (or a stray re-send from the peer).
          // If our own work already finished, fire an ACK inline —
          // the condition ACK demands is already satisfied and the
          // quiesce step in `performDrain` won't run again for this
          // new ref.
          if (this.#drain_quiesced) {
            void this.send({
              kind: WireKind.SYSTEM,
              type: SystemMessageType.DRAIN_ACK,
              payload: { ref: packet.id, uptime: process.uptime() },
            } satisfies OmitStandardFields<SystemDrainAckPacket>).catch(() => null);
          }
          return;
        }

        // Handshake hasn't completed — DRAIN before OPEN is nonsense
        // from a conforming peer. Drop.
        if (this.#state !== SessionState.OPEN) return;

        // Remote-initiated drain: run the same coroutine, just without
        // the "announce" step. Our terminal ACK will fire once we
        // quiesce.
        void (this.#drain_promise ??= this.performDrain(
          "remote",
          packet.payload.reason,
          packet.payload.timeout,
        ));

        break;
      }
    }
  }

  private async handleRequestPacket(packet: AnyRequestPacket): Promise<void> {
    if (packet.type === RequestMessageType.ABORT) {
      this.#controllers.get(packet.payload.ref)?.abort();
      return;
    } else if (packet.type === RequestMessageType.CANCEL) {
      // CANCEL bypasses any queue/semaphore — the producer must stop
      // emitting as soon as possible. The cancel is idempotent: missing
      // refs (already completed, already cancelled) are observable, not
      // errors.
      const outbound = this.#outbound_streams.get(packet.payload.ref);
      if (!outbound) return;

      this.cancelOutboundStream(outbound);
      this.logger?.debug(`Cancelled outbound stream ${clip(packet.payload.ref)}`);
      return;
    }

    const context = {
      correlationId: packet.id,
      traceId: "control" in packet.payload ? packet.payload.control?.traceId : undefined,
    };

    this.inbound.run(async () => {
      if (this.#state === SessionState.DRAINING) {
        return await this.send({
          kind: WireKind.RESPONSE,
          type: ResponseMessageType.VALUE,
          payload: {
            ref: packet.id,
            status: WireStatus.CANCELLED,
            error: toWireError(new QuiryError(WireStatus.DRAINING, "Session is draining", context)),
          },
        } satisfies OmitStandardFields<ValueResponsePacket>);
      }

      if (
        "control" in packet.payload &&
        packet.payload.control?.timeout !== undefined &&
        packet.timestamp >= Date.now() + packet.payload.control.timeout
      ) {
        return await this.send({
          kind: WireKind.RESPONSE,
          type: ResponseMessageType.VALUE,
          payload: {
            ref: packet.id,
            status: WireStatus.DEADLINE_EXCEEDED,
            error: toWireError(
              new QuiryError(WireStatus.DEADLINE_EXCEEDED, "Request operation timed out", context),
            ),
          },
        } satisfies OmitStandardFields<ValueResponsePacket>);
      }

      // dispatch the request to the event listener
      // TODO: maybe something like a semaphore to limit the number of concurrent requests

      const request = {
        service: packet.payload.service,
        property: "method" in packet.payload ? packet.payload.method : packet.payload.property,
        args: "args" in packet.payload ? this.restoreStubs(packet.payload.args, packet.id) : [],
      } satisfies InquiryRequest;

      let controller: AbortController | undefined;
      if ("control" in packet.payload && packet.payload.control?.abortable) {
        controller = new AbortController();
        this.#controllers.set(packet.id, controller);
      }

      try {
        const result = this.inquiry(request);

        if (isAnyIterableIterator(result)) {
          // Streaming results must be detected *before* the serialization
          // check: async iterators have a non-plain prototype and would
          // otherwise be rejected as non-serializable. Individual chunks are
          // validated as they are pulled from the iterator.
          await this.streamOutboundResponse(packet.id, result);
        } else {
          const value = await abortable(
            timeout(
              result,
              ("control" in packet.payload && packet.payload.control?.timeout !== undefined
                ? packet.payload.control.timeout
                : this.config.defaultTimeout) -
                (Date.now() - packet.timestamp),
              "Timeout waiting for inquiry response",
            ),
            controller?.signal,
          ).finally(() => controller && this.#controllers.delete(packet.id));

          if (!isSerializable(value))
            throw new QuiryError(WireStatus.INTERNAL, "Response value is not serializable", {
              ...context,
              detail: { value },
            });

          await this.send({
            kind: WireKind.RESPONSE,
            type: ResponseMessageType.VALUE,
            payload: {
              ref: packet.id,
              status: WireStatus.OK,
              result: value,
            },
          } satisfies OmitStandardFields<ValueResponsePacket>);
        }
      } catch (cause: unknown) {
        // The session stays up; the caller learns about the failure via the wire error.
        const error = QuiryError.from(cause, context);
        this.logger?.trace(`Request ${clip(packet.id)} response failed: ${error.message} (${error.code})`);

        await this.send({
          kind: WireKind.RESPONSE,
          type: ResponseMessageType.VALUE,
          payload: {
            ref: packet.id,
            status: error.code as Exclude<WireStatus, typeof WireStatus.OK>,
            error: toWireError(error),
          },
        } satisfies OmitStandardFields<ValueResponsePacket>).catch((reason: unknown) => {
          // If we can't even send the error, something deeper is wrong —
          // the transport itself will emit an error event separately.
          this.logger?.warn(
            `Failed to send error response for request ${clip(packet.id)}: ${reason instanceof Error ? reason.message : String(reason)}`,
          );
        });
      } finally {
        await this.releaseRemoteSubs(packet.id).catch((cause: unknown) => {
          this.logger?.debug(
            `Failed to release remote subs for request ${clip(packet.id)}: ${cause instanceof Error ? cause.message : String(cause)}`,
          );
        });
      }
    });
  }

  private async handleResponsePacket(packet: AnyResponsePacket): Promise<void> {
    if (!packet.payload.ref) {
      // Protocol quirk rather than a real error — log and drop.
      this.logger?.trace(`Received response packet with no reference: ${clip(packet.id)}`);
      return;
    }

    if (packet.type === ResponseMessageType.STREAM) {
      const { ref, event } = packet.payload;

      // route to the outbound stream registry rather than the
      // pending-request map. The two sides of a stream never share state in `#pending`.
      if (event === "credit") {
        const outbound = this.#outbound_streams.get(ref);
        if (!outbound) {
          // Credit for a stream we don't know about (already completed or cancelled). Harmless.
          this.logger?.trace(`Credit grant for unknown stream ${clip(ref)}: ${packet.id}`);
          return;
        }

        outbound.credit += packet.payload.credit;
        this.logger?.trace(
          `Stream ${clip(ref)} received +${packet.payload.credit} credit (total=${outbound.credit})`,
        );

        // Wake the producer loop if it was blocked waiting for credit.
        const waiter = outbound.waiter;
        outbound.waiter = null;
        if (waiter) waiter(true);
        return;
      }

      const entry = this.#pending_requests.get(ref) as PendingStreamRequest | undefined;
      if (!entry) {
        // Stale response — the local side already timed out, cancelled,
        // or otherwise cleared the pending entry.
        return;
      }

      switch (event) {
        case "chunk": {
          if (packet.payload.seq !== entry.seq) {
            this.logger?.warn(
              `Unexpected stream chunk sequence: expected ${entry.seq}, got ${packet.payload.seq}`,
            );
            return;
          }

          entry.seq++;
          entry.credit.remaining--;
          entry.queue.enqueue(packet.payload.chunk);

          // Replenish credit when half the window is consumed.
          if (entry.credit.remaining <= Math.floor(this.config.creditWindow / 2)) {
            const grant = this.config.creditWindow - entry.credit.remaining;
            entry.credit.remaining += grant;

            // Send the delta, not the absolute remaining, and bump the local view up.
            void this.send({
              kind: WireKind.RESPONSE,
              type: ResponseMessageType.STREAM,
              payload: { event: "credit", ref, credit: grant },
            } satisfies OmitStandardFields<StreamResponsePacket>).catch((cause: unknown) => {
              this.logger?.debug(
                `Failed to send credit grant for stream ${clip(ref)}: ${cause instanceof Error ? cause.message : String(cause)}`,
              );
            });
          }

          break;
        }

        case "end": {
          clearTimeout(entry.timer);
          this.#pending_requests.delete(ref);
          this.outbound.exit();

          // Gap detection: the producer reports the next seq it
          // *would* have emitted. If our cursor doesn't match, a
          // chunk was lost in transit.
          if (packet.payload.seq !== entry.seq) {
            this.logger?.warn(
              `Stream ${clip(ref)} gap detected on end (expected=${entry.seq}, got=${packet.payload.seq})`,
            );
            entry.queue.fail(
              new QuiryError(WireStatus.DATA_LOSS, "Stream ended with unexpected sequence (gap detected)", {
                correlationId: ref,
                detail: { expected: entry.seq, actual: packet.payload.seq },
              }),
            );
          } else {
            this.logger?.debug(`Stream ${clip(ref)} ended cleanly (${entry.seq} chunks received)`);
            entry.queue.close();
          }

          break;
        }

        case "error": {
          clearTimeout(entry.timer);
          const error = fromWireError(packet.payload.error);
          this.logger?.debug(
            `Stream ${clip(ref)} errored at seq=${packet.payload.seq}: ${error.message} (${error.code})`,
          );
          entry.queue.fail(error);
          this.#pending_requests.delete(ref);
          this.outbound.exit();

          break;
        }
      }

      return;
    }

    const { ref, status } = packet.payload;
    const entry = this.#pending_requests.get(ref);
    if (!entry) {
      // Stale response — the local side already timed out and cleared the pending entry.
      this.logger?.trace(`Received response for unknown request ${clip(ref)}: ${status}`);
      return;
    }

    clearTimeout("timer" in entry ? entry.timer : undefined);
    this.#pending_requests.delete(ref);
    this.outbound.exit();

    if (entry.kind === "stream") {
      // User tried calling a stream method as a unary call.
      return entry.queue.fail(
        new QuiryError(status, "Cannot call a stream method as a unary call", {
          correlationId: ref,
          detail: { packetId: packet.id },
        }),
      );
    }

    if (status === WireStatus.OK) entry.resolve(packet.payload.result);
    else {
      // Reconstruct the remote error with its full cause chain.
      // The `origin` on the rebuilt error reflects the remote node.
      entry.reject(fromWireError(packet.payload.error));
    }

    this.logger?.debug(
      `Request ${clip(ref)} completed with status ${status}${"timestamp" in entry ? ` in ${Date.now() - entry.timestamp}ms` : ""}`,
    );
  }

  private async handleCallbackPacket(packet: AnyCallbackPacket): Promise<void> {
    switch (packet.type) {
      case CallbackMessageType.INVOKE: {
        const { ref, eid, callback, args } = packet.payload;
        this.#inflight_invocations.set(ref, (this.#inflight_invocations.get(ref) ?? 0) + 1);

        const fn = this.callbacks.get(callback);
        const context = { correlationId: ref };

        try {
          if (!fn) {
            // Callback not found
            this.logger?.warn(`Callback ${callback} not found (${clip(packet.id)})`);

            await this.send({
              kind: WireKind.CALLBACK,
              type: CallbackMessageType.RETURN,
              payload: {
                ref,
                eid,
                callback,
                status: WireStatus.NOT_FOUND,
                error: toWireError(
                  new QuiryError(WireStatus.NOT_FOUND, "Callback not found. Did you release it?", context),
                ),
              },
            } satisfies OmitStandardFields<CallbackReturnPacket>);
            return;
          }

          const result = await fn(...args);
          await this.send({
            kind: WireKind.CALLBACK,
            type: CallbackMessageType.RETURN,
            payload: { ref, eid, callback, status: WireStatus.OK, result },
          } satisfies OmitStandardFields<CallbackReturnPacket>);
        } catch (cause: unknown) {
          // Callback-invoke boundary — non fatal.
          const error = QuiryError.from(cause, context);
          this.logger?.warn(
            `Callback invocation failed for packet ${clip(packet.id)}: ${error.message} (${error.code})`,
          );

          await this.send({
            kind: WireKind.CALLBACK,
            type: CallbackMessageType.RETURN,
            payload: {
              ref,
              eid,
              callback,
              status: error.code as Exclude<WireStatus, typeof WireStatus.OK>,
              error: toWireError(error),
            },
          } satisfies OmitStandardFields<CallbackReturnPacket>).catch((reason: unknown) => {
            this.logger?.warn(
              `Failed to send callback error for packet ${clip(packet.id)}: ${reason instanceof Error ? reason.message : String(reason)}`,
            );
          });
        } finally {
          this.decrementInflightInvocations(ref);
        }

        break;
      }

      case CallbackMessageType.RETURN: {
        const { ref, eid, status } = packet.payload;
        const invocation = this.#pending_invocations.get(eid);
        if (!invocation) {
          // Stale return — the local invocation already timed out and
          // was cleaned up. Observable, not a real error.
          this.logger?.debug(`Received return for unknown invocation ${clip(eid)} (${status})`);
          return;
        }

        clearTimeout(invocation.timer);
        this.#pending_invocations.delete(eid);
        this.outbound.exit();

        if (status === WireStatus.OK) invocation.resolve(packet.payload.result);
        else {
          // Remote callback failed — reject the local promise with the
          // reconstructed error so the caller can handle it.
          invocation.reject(fromWireError(packet.payload.error));
        }

        break;
      }

      case CallbackMessageType.RELEASE: {
        const { ref, callbacks } = packet.payload;
        for (const callback of callbacks) {
          // Missing callback on release is idempotent and expected —
          // e.g. we released it locally first. Observable-only.
          const released = this.callbacks.release(callback);
          if (released) {
            this.logger?.trace(
              ref
                ? `Released callback ${clip(callback)} for packet ${clip(ref)}`
                : `Released out of scope callback ${clip(callback)}`,
            );
          }
        }

        break;
      }
    }
  }

  // --------- INTERNALS: REQUEST HANDLING --------- //

  private rejectAllPending(error: Error): void {
    for (const request of this.#pending_requests.values()) {
      if (request.kind === "stream") request.queue.fail(error);
      else request.reject(error);
    }
    this.#pending_requests.clear();

    for (const invocation of this.#pending_invocations.values()) {
      clearTimeout(invocation.timer);
      invocation.reject(error);
    }
    this.#pending_invocations.clear();

    // Abort any in-flight producer-side streams. This closes their source
    // iterators (so generator `finally` blocks fire) and unblocks the loops
    // waiting on credit, letting them exit cleanly.
    for (const stream of this.#outbound_streams.values()) {
      this.cancelOutboundStream(stream);
    }
    this.#outbound_streams.clear();
  }

  /**
   * Producer side of a server-stream response. Pulls chunks from `iterable` only when the
   * consumer has granted credit, preventing unbounded memory accumulation on a slow consumer.
   */
  private async streamOutboundResponse(
    ref: CorrelationId,
    iterable: IterableIterator<unknown> | AsyncIterableIterator<unknown>,
  ): Promise<void> {
    const stream: OutboundStream = {
      credit: 0,
      waiter: null,
      cancelled: false,
      iterator: iterable,
    };
    this.#outbound_streams.set(ref, stream);

    let seq = 0;
    try {
      for (;;) {
        // Wait for credit before pulling the next chunk. Blocking
        // *before* pulling means a slow consumer doesn't cause us to
        // materialize chunks only to have them queue up in memory.
        while (stream.credit <= 0) {
          if (stream.cancelled) return;
          this.logger?.trace(`Stream ${clip(ref)} waiting for credit (sent=${seq}, credit=${stream.credit})`);
          const granted = await new Promise<boolean>((resolve) => {
            stream.waiter = resolve;
          });
          if (!granted || stream.cancelled) return;
        }

        const result = await iterable.next();
        if (stream.cancelled) return;
        if (result.done) break;

        const chunk = result.value;
        if (!isSerializable(chunk)) {
          throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Stream chunk is not serializable", {
            correlationId: ref,
            detail: { seq },
          });
        }

        stream.credit--;
        await this.send({
          kind: WireKind.RESPONSE,
          type: ResponseMessageType.STREAM,
          payload: { event: "chunk", ref, seq, chunk },
        } satisfies OmitStandardFields<StreamResponsePacket>);

        seq++;
      }

      if (stream.cancelled) return;
      await this.send({
        kind: WireKind.RESPONSE,
        type: ResponseMessageType.STREAM,
        payload: { event: "end", ref, seq },
      } satisfies OmitStandardFields<StreamResponsePacket>);

      this.logger?.trace(`Outbound stream ${clip(ref)} ended (${seq} chunks sent)`);
    } catch (cause: unknown) {
      if (stream.cancelled) return;
      const error = QuiryError.from(cause, { correlationId: ref });
      this.logger?.warn(
        `Outbound stream ${clip(ref)} errored at seq=${seq}: ${error.message} (${error.code})`,
      );

      await this.send({
        kind: WireKind.RESPONSE,
        type: ResponseMessageType.STREAM,
        payload: {
          event: "error",
          ref,
          seq,
          error: toWireError(error, { correlationId: ref }),
        },
      } satisfies OmitStandardFields<StreamResponsePacket>).catch((reason: unknown) => {
        this.logger?.warn(
          `Failed to send stream error for ${clip(ref)}: ${reason instanceof Error ? reason.message : String(reason)}`,
        );
      });
    } finally {
      this.#outbound_streams.delete(ref);
      // Best-effort close the source iterator on any exit path — errors,
      // normal completion, or cancellation. `return()` is idempotent and
      // safe to call on a drained generator.
      if (typeof iterable.return === "function") {
        void Promise.resolve(iterable.return(undefined)).catch(() => {
          // observable
        });
      }
    }
  }

  /**
   * Mark an outbound stream as cancelled and best-effort terminate its
   * source iterator. Called from CANCEL handling on the wire and from
   * session teardown.
   */
  private cancelOutboundStream(stream: OutboundStream): void {
    if (stream.cancelled) return;
    stream.cancelled = true;

    // Release any credit waiter so the streaming loop can exit.
    const waiter = stream.waiter;
    stream.waiter = null;
    if (waiter) waiter(false);

    // Best-effort abort of the underlying source. We swallow errors —
    // the iterator may already be closed or may not implement `return`.
    if (typeof stream.iterator.return === "function") {
      void Promise.resolve(stream.iterator.return(undefined)).catch(() => {
        // observable; the stream is going away anyway
      });
    }
  }

  // --------- INTERNALS: CALLBACK HANDLING --------- //

  private async releaseRemoteSubs(ref: CorrelationId): Promise<void> {
    // Wait for any in-flight invocations to complete before releasing.
    await this.drainInflightInvocations(ref);

    const stubs = this.#remote_stubs.get(ref);
    this.#remote_stubs.delete(ref);
    if (!stubs || stubs.size === 0) return;

    this.logger?.trace(`Sending release yield for packet ${clip(ref)} of ${stubs.size} callbacks`);
    await this.send({
      kind: WireKind.CALLBACK,
      type: CallbackMessageType.RELEASE,
      payload: { ref, callbacks: Array.from(stubs) },
    } satisfies OmitStandardFields<CallbackReleasePacket>);
  }

  private async releaseSessionCallbacks(): Promise<void> {
    const released = this.callbacks.releaseStackScoped();
    if (released.length === 0) return;

    // Best-effort: during drain the transport may already be down.
    await this.send({
      kind: WireKind.CALLBACK,
      type: CallbackMessageType.RELEASE,
      payload: { ref: null, callbacks: released },
    } satisfies OmitStandardFields<CallbackReleasePacket>).catch(() => null);
  }

  /**
   * Polls via `setImmediate` until all in-flight `CBK:INVOKE` packets under `ref` have received
   * their `CBK:RETURN`. Falls through after `defaultTimeout` to avoid blocking indefinitely on
   * an unresponsive peer; remaining callbacks are implicitly released when `callbacks.clear()` fires.
   */
  private drainInflightInvocations(ref: CorrelationId): Promise<void> {
    const remaining = (): number => {
      let n = this.#inflight_invocations.get(ref) ?? 0;
      for (const inv of this.#pending_invocations.values()) inv.ref === ref && n++;
      return n;
    };

    if (remaining() === 0) return Promise.resolve();

    // Attach a hard deadline so we don't busy-loop forever if invocations
    // never complete (e.g. peer gone). On timeout we log at debug and
    // resolve anyway; the callbacks will be implicitly released when
    // `callbacks.clear()` runs at session close.
    const deadline = this.config.defaultTimeout;
    return new Promise<void>((resolve) => {
      const start = Date.now();
      const check = (): void => {
        if (remaining() === 0) return void resolve();
        if (Date.now() - start >= deadline) {
          this.logger?.debug(`Drain inflight deadline hit for ${clip(ref)} with ${remaining()} pending`);
          this.#inflight_invocations.delete(ref);
          return void resolve();
        }

        setImmediate(check);
      };

      setImmediate(check);
    });
  }

  private decrementInflightInvocations(ref: CorrelationId): void {
    const n = this.#inflight_invocations.get(ref) ?? 0;
    n <= 1 ? this.#inflight_invocations.delete(ref) : this.#inflight_invocations.set(ref, n - 1);
  }

  /**
   * Rebuilds the argument list on the receiver side: replaces each {@link Callback} stub
   * found anywhere in the graph with a live async function that sends `CBK:INVOKE`
   * and awaits `CBK:RETURN`.
   *
   * `LOCAL`-scoped stub ids are tracked in `#remote_stubs[ref]` for bulk `CBK:RELEASE`
   * once the owning request completes; `STACK`-scoped stubs survive the request.
   *
   * Walks arrays and plain objects symmetrically with {@link CallbackRegistry.substitute}.
   * Cycles are short-circuited via the `seen` map.
   */
  private restoreStubs(args: ReadonlyArray<unknown>, ref: CorrelationId): ReadonlyArray<unknown> {
    const track = (stub: Callback): CallbackId => {
      if (stub.scope === CallbackScope.STACK) return stub.id;
      let set = this.#remote_stubs.get(ref);
      if (!set) {
        set = new Set();
        this.#remote_stubs.set(ref, set);
      }
      set.add(stub.id);
      return stub.id;
    };

    const seen = new WeakMap<object, unknown>();

    const walk = (value: unknown): unknown => {
      if (isCallbackStub(value)) return this.makeRemoteCallback(track(value), ref, value.scope);
      if (value === null || typeof value !== "object") return value;

      const cached = seen.get(value as object);
      if (cached !== undefined) return cached;

      if (Array.isArray(value)) {
        const result: unknown[] = new Array(value.length);
        seen.set(value as object, result);
        for (let i = 0; i < value.length; i++) result[i] = walk(value[i]);
        return result;
      }

      const proto = Object.getPrototypeOf(value);
      if (proto === Object.prototype || proto === null) {
        const result: Record<string, unknown> = {};
        seen.set(value as object, result);
        for (const [key, val] of Object.entries(value as object)) {
          result[key] = walk(val);
        }
        return result;
      }

      return value;
    };

    return args.map(walk);
  }

  /**
   * Creates an async proxy for a remote callback. Each call sends `CBK:INVOKE` and awaits
   * the `CBK:RETURN` packet. `LOCAL`-scoped invocations are bounded by `defaultTimeout`
   * to prevent leaks from a misbehaving peer; `STACK`-scoped invocations are intentionally
   * unbounded — long-lived event handlers are the very thing they exist for, and capping
   * them at the unary timeout silently breaks that use case.
   *
   * Unobserved rejections are silently caught so fire-and-forget callbacks don't blow up
   * the host with `unhandledRejection`.
   */
  private makeRemoteCallback(
    callback: CallbackId,
    ref: CorrelationId,
    scope: CallbackScope,
  ): (...args: unknown[]) => Promise<unknown> {
    return (...args: unknown[]): Promise<unknown> => {
      const eid = `${callback}:${Date.now()}:${Math.random().toString(36).substring(2, 15)}` as InvocationId;

      const timeout = scope === CallbackScope.STACK ? null : this.config.defaultTimeout;
      return new Promise<unknown>((resolve, reject) => {
        let timer: ReturnType<typeof setTimeout> | undefined;
        if (timeout !== null) {
          // Bound the pending yield with a deadline — a misbehaving peer
          // must not be able to leak promises indefinitely.
          timer = setTimeout(() => {
            this.#pending_invocations.delete(eid);
            this.outbound.exit();

            reject(
              new QuiryError(
                WireStatus.DEADLINE_EXCEEDED,
                `Remote callback did not return within ${timeout}ms`,
                {
                  correlationId: ref,
                  detail: { callback, eid, timeout },
                },
              ),
            );
          }, timeout);
        }

        this.#pending_invocations.set(eid, {
          ref,
          resolve: (value: unknown) => {
            if (timer) clearTimeout(timer);
            resolve(value);
          },
          reject: (error: Error) => {
            if (timer) clearTimeout(timer);
            reject(error);
          },
          timer,
        } satisfies PendingCallbackInvocation);
        this.outbound.enter();

        void this.send({
          kind: WireKind.CALLBACK,
          type: CallbackMessageType.INVOKE,
          payload: { ref, eid, callback, args },
        } satisfies OmitStandardFields<CallbackInvokePacket>).catch((error: unknown) => {
          const pending = this.#pending_invocations.get(eid);
          if (!pending) return;

          if (pending.timer) clearTimeout(pending.timer);
          this.#pending_invocations.delete(eid);
          this.outbound.exit();

          reject(
            new QuiryError(WireStatus.DATA_LOSS, "Failed to send callback invocation", {
              correlationId: ref,
              cause: error,
            }),
          );
        });
      }).catch(() => {
        // Remote callbacks are frequently invoked in fire-and-forget contexts
        // where the caller never observes the return value. Attaching a silent fallback handler
        // marks the rejection as observed so the host process doesn't die from
        // `unhandledRejection` when the remote returns an error.
        this.logger?.debug(`Remote callback rejection (unobserved by default handler): ${clip(eid)}`);
      });
    };
  }

  // --------- PUBLIC API: STATUS --------- //

  get state(): SessionState {
    return this.#state;
  }

  get status(): SessionStatus {
    let stubs = 0;
    for (const set of this.#remote_stubs.values()) stubs += set.size;

    return {
      state: this.#state,
      pending: this.#pending_requests.size,
      streams: this.#outbound_streams.size,
      callbacks: this.callbacks.size,
      invocations: this.#pending_invocations.size,
      stubs,
      backpressure: this.transport.backpressure,
    };
  }

  // --------- INTERNALS: EVENT HANDLERS --------- //

  private readonly onTransportClose = (): void => {
    if (this.#state === SessionState.CLOSED || this.#state === SessionState.DRAINING) return;
    // Cooperative close from the remote side, or our own close() propagating.
    // If we initiated the close (`draining`/`closed`), this is a no-op via #fail's idempotency.
    this.logger?.warn("Transport closed unexpectedly");
  };

  private readonly onTransportError = (error: TransportError): void => {
    if (this.#state === SessionState.CLOSED || this.#state === SessionState.DRAINING) return;
    // Map TransportError.kind → WireStatus. Transport errors never escape the
    // transport unclassified — this is the only place that translation happens.
    this.logger?.error("Transport error", {
      error: (() => {
        switch (error.kind) {
          case "terminate":
            return new QuiryError(WireStatus.PEER_GONE, error.message, { cause: error.cause });
          case "receive":
            return new QuiryError(WireStatus.DATA_LOSS, error.message, { cause: error.cause });
          case "send":
            return this.transport.state === TransportState.CLOSED
              ? new QuiryError(WireStatus.UNAVAILABLE, error.message, { cause: error.cause })
              : new QuiryError(WireStatus.DATA_LOSS, error.message, { cause: error.cause });
        }
      })(),
    });
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
