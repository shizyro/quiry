import * as Packets from "../../../interface/packets";
import { WireKind, WireStatus } from "../../../interface/protocol";
import type { CorrelationId } from "../../../interface/types";

import { InFlightTracker } from "../../../lib/tracker";

import { QuiryError, toWireError } from "../../../shared/errors";
import { isAnyIterableIterator, isSerializable } from "../../../lib/helpers";
import { abortable, timeout } from "../../../lib/utils";

import { SessionState } from "../state";
import type { SessionContext } from "../context";
import type { InquiryFunc, InquiryRequest } from "../inquiry";

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
  timestamp: number;
}

export interface InboundRequestsConfig {
  readonly defaultTimeout: number;
  readonly creditWindow: number;
}

/**
 * Producer-side request servicing: dispatches inbound REQUEST packets to
 * the {@link InquiryFunc}, owns the per-request abort controllers, and
 * runs the producer-side stream loop with credit-based flow control.
 *
 * @diagnostics `inquiry:received`, `inquiry:settled`,
 * `stream:open`, `stream:chunk`, `stream:credit-grant`, `stream:end`,
 * `stream:cancel`, `stream:error`.
 */
export class InboundRequests {
  private readonly tracker = new InFlightTracker();
  private readonly controllers = new Map<CorrelationId, AbortController>();

  readonly #outbound_streams = new Map<CorrelationId, OutboundStream>();

  constructor(
    private readonly ctx: SessionContext,
    private readonly inquiry: InquiryFunc,
    private readonly config: InboundRequestsConfig,
  ) {}

  // --------- PACKET HANDLING --------- //

  /**
   * Dispatches an inbound REQUEST. CALL/GET/SET/STREAM are servicing
   * work and run inside the {@link InFlightTracker}; ABORT and CANCEL
   * are control packets handled inline without bumping the tracker.
   */
  handleRequestPacket(packet: Packets.AnyRequestPacket) {
    if (packet.type === Packets.RequestMessageType.ABORT) {
      this.controllers.get(packet.payload.ref)?.abort();
      this.ctx.diagnostic.maybe("inquiry:received")?.({
        ref: packet.id,
        service: "",
        property: "",
        kind: "abort",
      });
      return;
    }

    if (packet.type === Packets.RequestMessageType.CANCEL) {
      // CANCEL bypasses any queue/semaphore — the producer must stop
      // emitting as soon as possible. Idempotent: missing refs are
      // observable, not errors.
      const outbound = this.#outbound_streams.get(packet.payload.ref);
      if (outbound) this.#cancelOutboundStream(outbound, "remote");

      this.ctx.diagnostic.maybe("inquiry:received")?.({
        ref: packet.id,
        service: "",
        property: "",
        kind: "cancel",
      });
      return;
    }

    return this.tracker.run(async () => {
      const startedAt = Date.now();
      const context = {
        correlationId: packet.id,
        traceId: "control" in packet.payload ? packet.payload.control?.traceId : undefined,
      };
      const reqKind: "set" | "get" | "call" =
        packet.type === Packets.RequestMessageType.SET
          ? "set"
          : packet.type === Packets.RequestMessageType.GET
            ? "get"
            : "call";

      this.ctx.diagnostic.maybe("inquiry:received")?.({
        ref: packet.id,
        service: packet.payload.service,
        property: "method" in packet.payload ? packet.payload.method : packet.payload.property,
        kind: reqKind,
      });

      const settled = (status: WireStatus): void => {
        this.ctx.diagnostic.maybe("inquiry:settled")?.({
          ref: packet.id,
          status,
          durationMs: Date.now() - startedAt,
        });
      };

      if (this.ctx.state() === SessionState.DRAINING) {
        await this.ctx.send<Packets.ValueResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.VALUE,
          payload: {
            ref: packet.id,
            status: WireStatus.CANCELLED,
            error: toWireError(new QuiryError(WireStatus.DRAINING, "Session is draining", context)),
          },
        });
        settled(WireStatus.CANCELLED);
        return;
      }

      if (
        "control" in packet.payload &&
        packet.payload.control?.timeout !== undefined &&
        Date.now() - packet.timestamp >= packet.payload.control.timeout
      ) {
        await this.ctx.send<Packets.ValueResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.VALUE,
          payload: {
            ref: packet.id,
            status: WireStatus.DEADLINE_EXCEEDED,
            error: toWireError(
              new QuiryError(WireStatus.DEADLINE_EXCEEDED, "Request operation timed out", context),
            ),
          },
        });
        settled(WireStatus.DEADLINE_EXCEEDED);
        return;
      }

      // dispatch the request to inquiry
      // TODO: maybe something like a semaphore to limit the number of concurrent requests

      const request: InquiryRequest = {
        service: packet.payload.service,
        property: "method" in packet.payload ? packet.payload.method : packet.payload.property,
      };

      let controller: AbortController | undefined;
      if ("control" in packet.payload && packet.payload.control?.abortable) {
        controller = new AbortController();
        this.controllers.set(packet.id, controller);
      }

      try {
        const descriptor = this.inquiry(request);
        let result: unknown;

        switch (packet.type) {
          case Packets.RequestMessageType.SET:
            descriptor.set(packet.payload.value);
            break;

          case Packets.RequestMessageType.GET:
            result = descriptor.value;
            break;

          case Packets.RequestMessageType.CALL: {
            const prop = descriptor.value;
            if (typeof prop !== "function") {
              throw new QuiryError(
                WireStatus.INVALID_ARGUMENT,
                `Property ${request.property} is not a function`,
                context,
              );
            }

            const value = (prop as (...args: unknown[]) => unknown)(
              ...("args" in packet.payload
                ? this.ctx.callbacks.restoreStubs(packet.payload.args, packet.id)
                : []),
            );

            if (isAnyIterableIterator(value)) {
              // Streaming results must be detected *before* the serialization
              // check: async iterators have a non-plain prototype and would
              // otherwise be rejected as non-serializable.
              await this.#streamOutboundResponse(packet.id, value);
              settled(WireStatus.OK);
              return;
            }

            result = await abortable(
              timeout(
                Promise.resolve(value),
                ("control" in packet.payload && packet.payload.control?.timeout !== undefined
                  ? packet.payload.control.timeout
                  : this.config.defaultTimeout) -
                  (Date.now() - packet.timestamp),
                "Timeout waiting for inquiry response",
              ),
              controller?.signal,
            ).finally(() => controller && this.controllers.delete(packet.id));

            break;
          }
        }

        const substituted = this.ctx.callbacks.substitute(result);
        if (!isSerializable(substituted))
          throw new QuiryError(WireStatus.INTERNAL, "Response value is not serializable", {
            ...context,
            detail: { value: substituted },
          });

        await this.ctx.send<Packets.ValueResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.VALUE,
          payload: { ref: packet.id, status: WireStatus.OK, result: substituted },
        });
        settled(WireStatus.OK);
      } catch (cause: unknown) {
        const error = QuiryError.from(cause, context);
        await this.ctx
          .send<Packets.ValueResponsePacket>({
            kind: WireKind.RESPONSE,
            type: Packets.ResponseMessageType.VALUE,
            payload: {
              ref: packet.id,
              status: error.code as Exclude<WireStatus, typeof WireStatus.OK>,
              error: toWireError(error),
            },
          })
          .catch(() => null);
        settled(error.code);
      } finally {
        await this.ctx.callbacks.releaseRemoteStubs(packet.id).catch(() => null);
      }
    });
  }

  /**
   * Producer-side credit grant. Reads `payload.credit` and wakes the
   * blocked credit waiter (if any) so the producer can keep emitting.
   */
  handleCreditGrant(packet: Packets.StreamResponsePacket): void {
    if (packet.payload.event !== "credit") return;
    const { ref, credit } = packet.payload;
    const outbound = this.#outbound_streams.get(ref);
    if (!outbound) return;

    outbound.credit += credit;
    this.ctx.diagnostic.maybe("stream:credit-grant")?.({
      ref,
      delta: credit,
      remaining: outbound.credit,
      direction: "received",
    });

    const waiter = outbound.waiter;
    outbound.waiter = null;
    if (waiter) waiter(true);
  }

  // --------- PUBLIC: LIFECYCLE --------- //

  /**
   * Cancel every active producer stream — used by the drain coordinator
   * before quiescing so long-running generators don't outlive the deadline.
   */
  cancelAllStreams(): void {
    for (const stream of this.#outbound_streams.values()) {
      this.#cancelOutboundStream(stream, "local");
    }
  }

  /** Resolves once the inbound tracker drains to zero. */
  idle(): Promise<void> {
    return this.tracker.idle();
  }

  /** Force-reset for teardown. Existing in-flight `tracker.run` exits will underflow. */
  drain(): void {
    this.tracker.drain();
    this.controllers.clear();
    this.#outbound_streams.clear();
  }

  // --------- PUBLIC: INTROSPECTION --------- //

  get streamCount(): number {
    return this.#outbound_streams.size;
  }

  // --------- INTERNALS --------- //

  /**
   * Producer side of a server-stream response. Pulls chunks from `iterable` only when the
   * consumer has granted credit, preventing unbounded memory accumulation on a slow consumer.
   */
  async #streamOutboundResponse(
    ref: CorrelationId,
    iterable: IterableIterator<unknown> | AsyncIterableIterator<unknown>,
  ): Promise<void> {
    const stream: OutboundStream = {
      credit: 0,
      waiter: null,
      cancelled: false,
      iterator: iterable,
      timestamp: Date.now(),
    };
    this.#outbound_streams.set(ref, stream);
    this.ctx.diagnostic.maybe("stream:open")?.({ ref, window: this.config.creditWindow });

    let seq = 0;
    try {
      for (;;) {
        // Wait for credit before pulling the next chunk. Blocking
        // *before* pulling means a slow consumer doesn't cause us to
        // materialize chunks only to have them queue up in memory.
        while (stream.credit <= 0) {
          if (stream.cancelled) return;
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
        await this.ctx.send<Packets.StreamResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.STREAM,
          payload: { event: "chunk", ref, seq, chunk },
        });
        this.ctx.diagnostic.maybe("stream:chunk")?.({ ref, seq, direction: "sent" });

        seq++;
      }

      if (stream.cancelled) return;
      await this.ctx.send<Packets.StreamResponsePacket>({
        kind: WireKind.RESPONSE,
        type: Packets.ResponseMessageType.STREAM,
        payload: { event: "end", ref, seq },
      });

      this.ctx.diagnostic.maybe("stream:end")?.({ ref, seq, direction: "sent" });
    } catch (cause: unknown) {
      if (stream.cancelled) return;
      const error = QuiryError.from(cause, { correlationId: ref });
      await this.ctx
        .send<Packets.StreamResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.STREAM,
          payload: {
            event: "error",
            ref,
            seq,
            error: toWireError(error, { correlationId: ref }),
          },
        })
        .catch(() => null);

      this.ctx.diagnostic.maybe("stream:error")?.({ ref, seq, status: error.code });
    } finally {
      this.#outbound_streams.delete(ref);
      // Best-effort close the source iterator on any exit path — errors,
      // normal completion, or cancellation. `return()` is idempotent and
      // safe to call on a drained generator.
      if (typeof iterable.return === "function") {
        void Promise.resolve(iterable.return(undefined)).catch(() => {});
      }
    }
  }

  /**
   * Mark an outbound stream as cancelled and best-effort terminate its
   * source iterator. Called by inbound CANCEL handling and by drain.
   */
  #cancelOutboundStream(stream: OutboundStream, source: "local" | "remote"): void {
    if (stream.cancelled) return;
    stream.cancelled = true;

    // Find the ref for diagnostic emit.
    let ref: CorrelationId | undefined;
    for (const [k, v] of this.#outbound_streams) {
      if (v === stream) {
        ref = k;
        break;
      }
    }
    if (ref) this.ctx.diagnostic.maybe("stream:cancel")?.({ ref, source });

    // Release any credit waiter so the streaming loop can exit.
    const waiter = stream.waiter;
    stream.waiter = null;
    if (waiter) waiter(false);

    // Best-effort abort of the underlying source. We swallow errors —
    // the iterator may already be closed or may not implement `return`.
    if (typeof stream.iterator.return === "function") {
      void Promise.resolve(stream.iterator.return(undefined)).catch(() => {
        // Observable; the stream is going away anyway.
      });
    }
  }
}
