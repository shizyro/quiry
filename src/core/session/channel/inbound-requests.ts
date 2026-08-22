import * as Packets from "../../../protocol/packets";
import { WireKind, WireStatus } from "../../../protocol/wire";

import { InFlightTracker } from "../../../lib/tracker";
import { contextStorage } from "../../../lib/call-context";

import type { CorrelationId } from "../../../protocol/types";
import { QuiryError, toWireError } from "../../../protocol/errors";
import { isAnyIterableIterator, isSerializable } from "../../../lib/helpers";

import * as Transform from "./wire-transform";

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

/**
 * Represents a pending operation on an inbound request. Used to track
 * and potentially control pending operations.
 */
interface PendingOperation {
  controller: AbortController;
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

  readonly #outbound_streams = new Map<CorrelationId, OutboundStream>();
  readonly #pending_operations = new Map<CorrelationId, PendingOperation>();

  constructor(
    private readonly ctx: SessionContext,
    private readonly inquiry: InquiryFunc,
  ) {}

  // --------- PACKET HANDLING --------- //

  /**
   * Dispatches an inbound REQUEST. CALL/GET/SET/STREAM are servicing
   * work and run inside the {@link InFlightTracker}; ABORT and CANCEL
   * are control packets handled inline without bumping the tracker.
   */
  async handleRequestPacket(packet: Packets.AnyRequestPacket): Promise<void> {
    if (packet.type === Packets.RequestMessageType.ABORT) {
      this.#pending_operations.get(packet.payload.ref)?.controller.abort();
      this.ctx.diagnostic.maybe("inquiry:received")?.({
        ref: packet.id,
        object: "",
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
        object: "",
        property: "",
        kind: "cancel",
      });
      return;
    }

    return this.tracker.run(async () => {
      const startedAt = Date.now();
      const context = { cid: packet.id };
      const reqKind: "set" | "get" | "call" =
        packet.type === Packets.RequestMessageType.SET
          ? "set"
          : packet.type === Packets.RequestMessageType.GET
            ? "get"
            : "call";

      this.ctx.diagnostic.maybe("inquiry:received")?.({
        ref: packet.id,
        object: packet.payload.object,
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
            status: WireStatus.DRAINING,
            error: toWireError(new QuiryError(WireStatus.DRAINING, "Session is draining", context)),
          },
        });
        settled(WireStatus.DRAINING);
        return;
      }

      // dispatch the request to inquiry
      // TODO: maybe something like a semaphore to limit the number of concurrent requests

      const request: InquiryRequest = {
        object: packet.payload.object,
        property: "method" in packet.payload ? packet.payload.method : packet.payload.property,
      };

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

            const controller = new AbortController();
            this.#pending_operations.set(packet.id, { controller });

            const restored = Transform.fromWire(packet.payload.args, this.ctx.callbacks, packet.id);
            const value = contextStorage.run({ signal: controller.signal }, () =>
              (prop as (...args: unknown[]) => unknown)(...("args" in packet.payload ? restored : [])),
            );

            if (isAnyIterableIterator(value)) {
              // Streaming results must be detected *before* the serialization
              // check: async iterators have a non-plain prototype and would
              // otherwise be rejected as non-serializable.
              await this.#streamOutboundResponse(packet.id, value);
              this.#pending_operations.delete(packet.id);
              settled(WireStatus.OK);
              return;
            }

            result = await abortable(Promise.resolve(value), controller.signal).finally(() =>
              this.#pending_operations.delete(packet.id),
            );

            break;
          }
        }

        const transformed = Transform.toWire(result, this.ctx.callbacks);
        if (!isSerializable(transformed))
          throw new QuiryError(WireStatus.DATA_LOSS, "Response value is not serializable", {
            ...context,
            detail: { value: transformed },
          });

        await this.ctx.send<Packets.ValueResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.VALUE,
          payload: { ref: packet.id, status: WireStatus.OK, result: transformed },
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

    // No need to go through; tracker already finalized.
    this.#outbound_streams.clear();
    this.#pending_operations.clear();
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
    cid: CorrelationId,
    iterable: IterableIterator<unknown> | AsyncIterableIterator<unknown>,
  ): Promise<void> {
    const stream: OutboundStream = {
      credit: 0,
      waiter: null,
      cancelled: false,
      iterator: iterable,
      timestamp: Date.now(),
    };
    this.#outbound_streams.set(cid, stream);

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

        const chunk = Transform.fromWire(result.value, this.ctx.callbacks, cid);
        if (!isSerializable(chunk)) {
          throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Stream chunk is not serializable", {
            cid,
            detail: { seq },
          });
        }

        stream.credit--;
        await this.ctx.send<Packets.StreamResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.STREAM,
          payload: { event: "chunk", ref: cid, seq, chunk },
        });
        this.ctx.diagnostic.maybe("stream:chunk")?.({ ref: cid, seq, direction: "sent" });

        seq++;
      }

      if (stream.cancelled) return;
      await this.ctx.send<Packets.StreamResponsePacket>({
        kind: WireKind.RESPONSE,
        type: Packets.ResponseMessageType.STREAM,
        payload: { event: "end", ref: cid, seq },
      });

      this.ctx.diagnostic.maybe("stream:end")?.({ ref: cid, seq, direction: "sent" });
    } catch (cause: unknown) {
      if (stream.cancelled) return;
      const error = QuiryError.from(cause, { cid });
      await this.ctx
        .send<Packets.StreamResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.STREAM,
          payload: {
            event: "error",
            ref: cid,
            seq,
            error: toWireError(error, { cid }),
          },
        })
        .catch(() => null);

      this.ctx.diagnostic.maybe("stream:error")?.({ ref: cid, seq, status: error.code });
    } finally {
      this.#outbound_streams.delete(cid);
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

    // Find the correlation ID for diagnostic emit.
    let cid: CorrelationId | undefined;
    for (const [k, v] of this.#outbound_streams) {
      if (v === stream) {
        cid = k;
        break;
      }
    }
    if (cid) this.ctx.diagnostic.maybe("stream:cancel")?.({ ref: cid, source });

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

/** A wrapper around a promise that rejects with an abort signal. */
export function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal?.aborted) return Promise.reject(new QuiryError(WireStatus.ABORTED, "Operation was aborted"));

  return new Promise<T>((resolve, reject) => {
    const abortHandler = () => reject(new QuiryError(WireStatus.ABORTED, "Operation was aborted"));
    signal?.addEventListener("abort", abortHandler);

    promise
      .then(resolve)
      .catch(reject)
      .finally(() => {
        signal?.removeEventListener("abort", abortHandler);
      });
  });
}
