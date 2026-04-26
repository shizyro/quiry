import EventEmitter from "node:events";

import type { Transport, TransportError } from "@/core/transport";
import { WireKind, WireStatus, type CorrelationId, type NodeId, type RequestControl } from "@/interface/base";
import {
  RequestMessageType,
  ResponseMessageType,
  SystemMessageType,
  type AbortRequestPacket,
  type AnyPacket,
  type AnyRequestPacket,
  type AnyResponsePacket,
  type AnySystemPacket,
  type AnyTypedPacket,
  type CallRequestPacket,
  type HandshakePayload,
  type PacketByKind,
  type SystemDrainAckPacket,
  type SystemDrainPacket,
  type SystemHandshakePacket,
  type ValueResponsePacket,
} from "@/interface/packets";

import { Router } from "@/lib/router";

import { nanoid } from "nanoid";
import { delay, retryable, isSerializable, clip, timeout, abortable } from "@/lib/utils";
import { fromWireError, isRetryableStatus, QuiryError, toWireError } from "@/lib/errors";

import { threadId } from "node:worker_threads";
import { localNodeId } from "@/shared";

export type SessionState = "peering" | "open" | "draining" | "closed";

export interface SessionConfig {
  readonly handshakeTimeout?: number;
  readonly defaultTimeout?: number;
  readonly drainTimeout?: number;
  readonly inquiry?: InquiryFunc;
}

export type OmitStandardFields<T> = Omit<T, "id" | "from" | "timestamp">;

export type InquiryFunc = (request: InquiryRequest) => Promise<unknown>;
export type InquiryRequest = Readonly<{
  id: CorrelationId;
  service: string;
  method: string;
  args: ReadonlyArray<unknown>;
  control?: RequestControl;
}>;

interface PendingCallRequest<T = unknown> {
  resolve: (value: T) => void;
  reject: (error: Error) => void;
  timestamp: number;
  timer?: ReturnType<typeof setTimeout>;
}

export interface SessionEvents {
  "state-change": [state: SessionState];
  handshake: [metadata: HandshakePayload];
  error: [error: Error];
  close: [reason?: string];
}

export class Session extends EventEmitter<SessionEvents> {
  #peer: NodeId | null = null;
  #state: SessionState = "peering";

  private readonly config: Required<SessionConfig>;
  private readonly router: Router<AnyPacket>;

  private readonly inbound = new InFlightTracker();
  private readonly outbound = new InFlightTracker();

  readonly #pending = new Map<CorrelationId, PendingCallRequest>();
  readonly #controllers = new Map<CorrelationId, AbortController>();

  constructor(
    private readonly transport: Transport,
    config: SessionConfig = {},
    private readonly logger: Logger | null = null,
  ) {
    super();

    this.router = new Router(this.transport.receive());
    this.config = {
      handshakeTimeout: config.handshakeTimeout ?? 10_000,
      defaultTimeout: config.defaultTimeout ?? 10_000,
      drainTimeout: config.drainTimeout ?? 5000,
      inquiry: config.inquiry ?? (() => Promise.resolve(undefined)),
    };
  }

  send(packet: Omit<AnyTypedPacket, "id" | "from" | "timestamp">): Promise<CorrelationId> {
    return this.forward({
      id: nanoid() as CorrelationId,
      from: localNodeId,
      timestamp: Date.now(),
      ...packet,
    } as AnyPacket);
  }

  protected async forward(packet: AnyPacket): Promise<CorrelationId> {
    // Silently drop sends on a closed session. This can happen during teardown
    // when cleanup code attempts to send RELEASE after the transport is gone.
    if (this.#state !== "closed") {
      // TODO: account for backpressure
      await this.transport.send(packet);
    } else this.logger?.trace("Attempted to send packet on closed session");
    return packet.id;
  }

  private transition(state: SessionState): void {
    if (this.#state === state) return;
    this.#state = state;
    this.emit("state-change", state);

    this.logger?.debug(`Session state changed to ${state.toUpperCase()}`);
  }

  async wait<K extends AnyPacket["kind"], R extends PacketByKind<K>>(
    kind: K,
    predicate?: (packet: PacketByKind<K>) => packet is R,
    timeout?: number,
  ): Promise<R> {
    // @ts-expect-error - no plans to type this properly
    return this.router
      .wait((packet) => packet.kind === kind && (predicate ? predicate(packet as R) : true), {
        timeout, // TODO: abort signal
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

  /** --------- PUBLIC API: LIFECYCLE --------- */

  async open(): Promise<this> {
    if (this.#state !== "peering")
      throw new QuiryError(
        WireStatus.FAILED_PRECONDITION,
        "Cannot open session that is not in the peering state",
      );

    this.transport.on("state-change", (next) => next === "closed" && this.onTransportClose());
    this.transport.on("error", this.onTransportError);
    // (error handlers are automatically disposed when the transport is closed)

    await this.transport.open();
    await this.performHandshake().catch(async (error: unknown) => {
      // Close the underlying transport so we don't leak the port/worker,
      // then re-throw for the caller to handle.

      //  The supervisor is NOT involved — the session never transitioned out of `peering`.
      await this.transport.close().catch(() => null);
      throw error;
    });

    // Router runs for the lifetime of the session and is stopped inside teardown.
    void this.router.start(this.routeIncomingPacket.bind(this)).catch((error: unknown) => {
      // The router's source stream errored. Treat as fatal — we can't receive any more packets.
      this.logger?.error("Router source stream errored", { error: QuiryError.from(error) });
      void this.terminate();
    });
    this.transition("open");

    this.logger?.info(`Session established ${localNodeId} <-> ${this.#peer}`);
    return this;
  }

  /** Gracefully close the session. */
  async close(force: boolean = false): Promise<void> {
    if (this.#state === "closed") return;
    if (force || this.#state === "peering") return await this.terminate();
    return (this.#drain_promise ??= this.performDrain("local"));
  }

  /** --------- INTERNALS: LIFECYCLE --------- */

  private async performHandshake(): Promise<void> {
    this.logger?.debug(`Performing handshake on node ${localNodeId}`);
    const id = await this.forward({
      id: nanoid() as CorrelationId,
      kind: WireKind.SYSTEM,
      type: SystemMessageType.HANDSHAKE,
      timestamp: Date.now(),
      payload: { nodeId: localNodeId, threadId: threadId, pid: process.pid },
    } satisfies SystemHandshakePacket);

    this.logger?.trace(`Waiting for handshake response (${clip(id)})`);
    // Manually wait for the handshake response instead of using the router since
    // the session shouldn't be able to read any packets before the handshake is complete.
    const feedback = await new Promise<SystemHandshakePacket>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new QuiryError(WireStatus.DEADLINE_EXCEEDED, "Timeout waiting for handshake packet")),
        this.config.handshakeTimeout,
      );

      void (async () => {
        try {
          for await (const packet of this.transport.receive()) {
            if (packet.kind === WireKind.SYSTEM && packet.type === SystemMessageType.HANDSHAKE) {
              clearTimeout(timer);
              resolve(packet);
              return;
            }
          }

          reject(new QuiryError(WireStatus.DATA_LOSS, "No handshake packet received"));
        } catch (error: unknown) {
          clearTimeout(timer);
          reject(QuiryError.from(error));
        }
      })();
    });

    this.#peer = feedback.payload.nodeId;
    this.logger?.debug(`Handshake completed with node ${this.#peer}`);

    this.emit("handshake", feedback.payload);
  }

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
  private async performDrain(initiator: "local" | "remote"): Promise<void> {
    if (this.#state === "closed") return;
    if (this.#state !== "draining") this.transition("draining");

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.config.drainTimeout);

    // Short-circuit the drain the moment the transport dies under us.
    this.transport.on("state-change", (next) => next === "closed" && controller.abort());

    try {
      // Local initiator announces. Remote initiator stays silent —
      // its peer has already sent DRAIN and is waiting for our
      // terminal ACK, which we'll send in step 3 after we quiesce.
      if (initiator === "local") {
        await this.send({
          kind: WireKind.SYSTEM,
          type: SystemMessageType.DRAIN,
          payload: { reason: "explicit", graceful: true },
        } satisfies OmitStandardFields<SystemDrainPacket>).catch(() => null);
      }

      // Quiesce and ACK in parallel with waiting for the peer's ACK.
      //
      // `quiesce` waits for our in-flight work; once it resolves, we
      // send the terminal DRAIN_ACK if the peer's DRAIN ref is known.
      // Both tasks share the same `drainTimeout` via `controller`.
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
          ? this.router
              .wait<SystemDrainAckPacket>(
                (p) => p.kind === WireKind.SYSTEM && p.type === SystemMessageType.DRAIN_ACK,
                { signal: controller.signal },
              )
              .then(({ id }) => this.logger?.debug(`Received DRAIN_ACK ${clip(id)} from remote peer`))
              .catch(() => null) // aborted by signal; teardown anyway
          : Promise.resolve();

      await Promise.race([
        Promise.all([quiesce, peerAck]),
        new Promise<never>((_, reject) => {
          if (controller.signal.aborted) return reject("aborted");
          controller.signal.addEventListener("abort", () => reject("aborted"), { once: true });
        }),
      ]);

      // ...
    } catch (cause: unknown) {
      // Deadline or transport death — we still want to tear down cleanly.
      initiator === "local" && this.logger?.debug("Drain interrupted or timed out; proceeding to teardown");
    } finally {
      clearTimeout(timer);
      await this.terminate();
    }
  }

  private async terminate(): Promise<void> {
    await this.transport.close().catch(() => null);
    this.teardown();
  }

  private teardown(): void {
    if (this.#state === "closed") return;
    this.#state = "closed";

    // Stop the router; any waiters still pending get rejected with
    // "Stream closed" via the router's own cleanup.
    this.router.stop();

    // Reject all pending calls
    this.rejectAllPending(new QuiryError(WireStatus.ABORTED, "Session draining"));

    this.emit("state-change", "closed");
    this.emit("close");
  }

  /** --------- PUBLIC API: REQUESTS --------- */

  async request(
    service: string,
    method: string,
    args: ReadonlyArray<unknown>,
    control?: Omit<RequestControl, "abortable">,
    signal?: AbortSignal,
  ): Promise<unknown> {
    if (this.#state !== "open") {
      throw new QuiryError(WireStatus.UNAVAILABLE, "Session is not open", { traceId: control?.traceId });
    }

    const correlation = nanoid() as CorrelationId;
    // Ensure arguments can be cloned through port
    if (!isSerializable(args))
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Arguments are not serializable", {
        detail: { args },
      });

    const body = {
      id: correlation,
      kind: WireKind.REQUEST,
      type: RequestMessageType.CALL,
      from: localNodeId,
      timestamp: Date.now(),
      payload: { service, method, args, control: { ...control, abortable: signal instanceof AbortSignal } },
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
            if (this.#pending.delete(body.id)) {
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

          this.#pending.set(body.id, {
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
        retries: control?.retry?.maxAttempts ?? 3,
        initialDelay: control?.retry?.delay ?? 1000,
        shouldRetry: (error: unknown) => (error instanceof QuiryError ? isRetryableStatus(error.code) : true),
        signal,
      },
    );
  }

  /** --------- INTERNALS: ROUTING --------- */

  private routeIncomingPacket(packet: AnyPacket): void {
    // Each handle* is fire-and-forget (concurrent). Any unhandled throw
    // from them is an internal bug, not a protocol violation — route via
    // the supervisor with `fatal` severity since the session's state may
    // now be inconsistent.
    const attend = (p: PromiseLike<unknown> | unknown): void => {
      Promise.resolve(p).catch((error: unknown) => {
        this.logger?.error("Unhandled error in packet handler", { error: QuiryError.from(error) });
        void this.terminate();
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
        if (this.#state === "closed") return;

        // Capture the peer's DRAIN correlation id. The terminal
        // `DRAIN_ACK` sent at the end of `performDrain` will reference
        // this id; sending ACK earlier would violate the protocol's
        // "ACK means I'm done" semantic.
        this.#peer_drain_ref = packet.id;

        if (this.#state === "draining") {
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
        if (this.#state !== "open") return;

        // Remote-initiated drain: run the same coroutine, just without
        // the "announce" step. Our terminal ACK will fire once we
        // quiesce.
        void (this.#drain_promise ??= this.performDrain("remote"));

        break;
      }
    }
  }

  private async handleRequestPacket(packet: AnyRequestPacket): Promise<void> {
    if (packet.type === RequestMessageType.ABORT) {
      this.#controllers.get(packet.payload.ref)?.abort();
      return;
    }

    const context = {
      correlationId: packet.id,
      traceId: packet.payload.control?.traceId,
    };

    this.inbound.run(async () => {
      if (this.#state === "draining") {
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
        id: packet.id,
        service: packet.payload.service,
        method: packet.payload.method,
        args: packet.payload.args,
        control: packet.payload.control,
      } satisfies InquiryRequest;

      let controller: AbortController | undefined;
      if (packet.payload.control?.abortable) {
        controller = new AbortController();
        this.#controllers.set(packet.id, controller);
      }

      try {
        const result = this.config.inquiry(request);
        const value = await abortable(
          timeout(
            result,
            (packet.payload.control?.timeout ?? this.config.defaultTimeout) - (Date.now() - packet.timestamp),
            "Timeout waiting for inquiry response",
          ),
          controller?.signal,
        ).finally(() => controller && this.#controllers.delete(packet.id));

        if (!isSerializable(value))
          throw new QuiryError(WireStatus.INTERNAL, "Response value is not serializable", {
            ...context,
            detail: { value },
          });

        return await this.send({
          kind: WireKind.RESPONSE,
          type: ResponseMessageType.VALUE,
          payload: {
            ref: packet.id,
            status: WireStatus.OK,
            result: value,
          },
        } satisfies OmitStandardFields<ValueResponsePacket>);
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
      }
    });
  }

  private async handleResponsePacket(packet: AnyResponsePacket): Promise<void> {
    if (!packet.payload.ref) {
      // Protocol quirk rather than a real error — log and drop.
      this.logger?.debug(`Received response packet with no reference: ${clip(packet.id)}`);
      return;
    }

    const { ref, status } = packet.payload;
    const entry = this.#pending.get(ref);
    if (!entry) {
      // Stale response — the local side already timed out and cleared the pending entry.
      this.logger?.debug(`Received response for unknown request ${clip(ref)}: ${status}`);
      return;
    }

    clearTimeout(entry.timer);
    this.#pending.delete(ref);
    this.outbound.exit();

    if (status === WireStatus.OK) entry.resolve(packet.payload.result);
    else {
      // Reconstruct the remote error with its full cause chain.
      // The `origin` on the rebuilt error reflects the remote node.
      entry.reject(fromWireError(packet.payload.error));
    }

    this.logger?.debug(
      `Request ${clip(ref)} completed with status ${status} in ${Date.now() - entry.timestamp}ms`,
    );
  }

  private rejectAllPending(error: Error): void {
    for (const request of this.#pending.values()) {
      // if (request.timer) clearTimeout(request.timer);
      request.reject(error);
    }
    // this.#pending.clear();
  }

  /** --------- INTERNALS: STATUS --------- */

  get state(): SessionState {
    return this.#state;
  }

  get peer(): NodeId | null {
    return this.#peer;
  }

  get status() {
    return {
      state: this.#state,
      pending: this.#pending.size,
    };
  }

  /** --------- INTERNALS: EVENT HANDLERS --------- */

  private readonly onTransportClose = (): void => {
    if (this.#state === "closed" || this.#state === "draining") return;
    // Cooperative close from the remote side, or our own close() propagating.
    // If we initiated the close (`draining`/`closed`), this is a no-op via #fail's idempotency.
    this.logger?.error("Transport closed unexpectedly", {
      error: new QuiryError(WireStatus.PEER_GONE, "Transport closed unexpectedly", {
        origin: this.#peer ?? undefined,
      }),
    });
  };

  private readonly onTransportError = (error: TransportError): void => {
    if (this.#state === "closed" || this.#state === "draining") return;
    // Map TransportError.kind → WireStatus. Transport errors never escape the
    // transport unclassified — this is the only place that translation happens.
    this.logger?.error("Transport error", {
      error: (() => {
        switch (error.kind) {
          case "terminate":
            return new QuiryError(WireStatus.PEER_GONE, error.message, {
              cause: error.cause,
              origin: this.#peer ?? undefined,
            });
          case "receive":
            return new QuiryError(WireStatus.DATA_LOSS, error.message, { cause: error.cause });
          case "send":
            return this.transport.state === "closed"
              ? new QuiryError(WireStatus.UNAVAILABLE, error.message, { cause: error.cause })
              : new QuiryError(WireStatus.DATA_LOSS, error.message, { cause: error.cause });
        }
      })(),
    });
  };
}

/** Tracks in-flight operations and allows awaiting a "drain to zero" condition. */
class InFlightTracker {
  #inflight: number = 0;
  readonly #resolvers: Array<() => void> = [];

  enter(): void {
    this.#inflight++;
  }

  exit(): void {
    if (this.#inflight <= 0) throw new Error("Tracker underflow (exit without matching enter)");

    this.#inflight--;
    if (this.#inflight === 0) {
      const resolvers = this.#resolvers.splice(0);
      for (const resolve of resolvers) resolve();
    }
  }

  /**
   * Number of currently active operations.
   */
  get active(): number {
    return this.#inflight;
  }

  /**
   * Returns a promise that resolves once the active count reaches zero.
   * If already idle, resolves immediately.
   */
  idle(): Promise<void> {
    if (this.#inflight === 0) return Promise.resolve();
    return new Promise<void>((resolve) => {
      this.#resolvers.push(resolve);
    });
  }

  /**
   * Runs an async function within the barrier, ensuring proper pairing.
   */
  async run<T>(fn: () => Promise<T>): Promise<T> {
    this.enter();
    try {
      return await fn();
    } finally {
      this.exit();
    }
  }

  /**
   * Forces the tracker into an idle state by clearing all in-flight accounting,
   * and resolving all pending idle() waiters.
   */
  drain(): void {
    this.#inflight = 0;
    const resolvers = this.#resolvers.splice(0);
    for (const resolve of resolvers) resolve();
  }
}
