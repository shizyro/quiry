import * as Packets from "../../../protocol/packets";
import { WireKind, WireStatus } from "../../../protocol/wire";
import type { CorrelationId } from "../../../protocol/types";

import { AsyncQueue } from "../../../lib/queue";
import { InFlightTracker } from "../../../lib/tracker";

import { fromWireError, QuiryError } from "../../../protocol/errors";
import { isSerializable, unwrapSerialized } from "../../../lib/helpers";

import { SessionState } from "../state";
import type { SessionContext } from "../context";

interface RequestDiagnosticContext {
  readonly object: string;
  readonly property: string;
  readonly timestamp: number;
}

interface PendingGetRequest<T = unknown> extends RequestDiagnosticContext {
  readonly kind: "get";
  resolve: (value: T) => void;
  reject: (error: Error) => void;
}

interface PendingSetRequest<T = unknown> extends RequestDiagnosticContext {
  readonly kind: "set";
  resolve: () => void;
  reject: (error: Error) => void;
}

interface PendingCallRequest<T = unknown> extends RequestDiagnosticContext {
  readonly kind: "call";
  resolve: (value: T) => void;
  reject: (error: Error) => void;
  timestamp: number;
}

interface PendingStreamRequest extends RequestDiagnosticContext {
  readonly kind: "stream";
  readonly queue: AsyncQueue<unknown>;
  readonly credit: { remaining: number };
  seq: number;
}

type PendingRequest = PendingGetRequest | PendingSetRequest | PendingCallRequest | PendingStreamRequest;

/**
 * Owns the pending requests correlation map keyed by outbound packet id. Every
 * outbound request bumps the {@link InFlightTracker} so the drain coordinator
 * can wait for them to settle. Stream chunks accumulate in a per-request
 * {@link AsyncQueue} with credit-based flow control: half-window depletion
 * triggers a CREDIT grant back to the producer.
 *
 * @diagnostics `request:sent`, `request:settled`, `request:retry`,
 * `request:abort`, `stream:open`, `stream:chunk`, `stream:credit-grant`,
 * `stream:end`, `stream:error`, `stream:cancel`.
 */
export class OutboundRequests {
  private readonly tracker = new InFlightTracker();
  readonly #pending = new Map<CorrelationId, PendingRequest>();

  constructor(
    private readonly ctx: SessionContext,
    private readonly window: number,
  ) {}

  // --------- PUBLIC: REQUESTS --------- //

  async set(object: string, property: string, value: unknown): Promise<true> {
    if (this.ctx.state() !== SessionState.OPEN) {
      throw new QuiryError(WireStatus.UNAVAILABLE, "Session is not open");
    }

    const cid = this.ctx.correlate();
    const substitute = this.ctx.callbacks.substitute(unwrapSerialized(value), cid);
    if (!isSerializable(substitute))
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Value is not serializable");

    const startedAt = Date.now();
    return new Promise<true>((resolve, reject) => {
      const cleanup = (): void => {
        if (this.#pending.delete(cid)) this.tracker.exit();
      };

      this.#pending.set(cid, {
        kind: "set",
        object,
        property,
        timestamp: startedAt,
        resolve: () => {
          cleanup();
          resolve(true);
        },
        reject: (error: Error) => {
          cleanup();
          reject(error);
        },
      });
      this.tracker.enter();
      this.ctx.diagnostic.maybe("request:sent")?.({
        ref: cid,
        object,
        property,
        kind: "set",
      });

      void this.ctx
        .send<Packets.SetRequestPacket>({
          id: cid,
          kind: WireKind.REQUEST,
          type: Packets.RequestMessageType.SET,
          payload: { object, property, value: substitute },
        })
        .catch((cause: unknown) => {
          cleanup();
          reject(new QuiryError(WireStatus.DATA_LOSS, "Failed to send packet", { cid, cause }));
        });
    });
  }

  async get(object: string, property: string): Promise<unknown> {
    if (this.ctx.state() !== SessionState.OPEN) {
      throw new QuiryError(WireStatus.UNAVAILABLE, "Session is not open");
    }

    const cid = this.ctx.correlate();
    const startedAt = Date.now();
    return new Promise<unknown>((resolve, reject) => {
      const cleanup = (): void => {
        if (this.#pending.delete(cid)) this.tracker.exit();
      };

      this.#pending.set(cid, {
        kind: "get",
        object,
        property,
        timestamp: startedAt,
        resolve: (value: unknown) => {
          cleanup();
          resolve(value);
        },
        reject: (error: Error) => {
          cleanup();
          reject(error);
        },
      });
      this.tracker.enter();
      this.ctx.diagnostic.maybe("request:sent")?.({
        ref: cid,
        object,
        property,
        kind: "get",
      });

      void this.ctx
        .send<Packets.GetRequestPacket>({
          id: cid,
          kind: WireKind.REQUEST,
          type: Packets.RequestMessageType.GET,
          payload: { object, property },
        })
        .catch((cause: unknown) => {
          cleanup();
          reject(new QuiryError(WireStatus.DATA_LOSS, "Failed to send packet", { cid, cause }));
        });
    });
  }

  /**
   * Unary IPC request over the wire. Can be aborted via `AbortSignal`.
   */
  async request(
    object: string,
    method: string,
    args: ReadonlyArray<unknown>,
    signal?: AbortSignal,
  ): Promise<unknown> {
    if (this.ctx.state() !== SessionState.OPEN) {
      throw new QuiryError(WireStatus.UNAVAILABLE, "Session is not open");
    }

    const cid = this.ctx.correlate();
    const substitutes = this.ctx.callbacks.substitute(unwrapSerialized(args), cid);
    // Ensure arguments can be cloned through port.
    if (!isSerializable(substitutes))
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Arguments are not serializable", {
        detail: { args },
      });

    const payload = {
      object,
      method,
      args: substitutes,
    } satisfies Packets.CallRequestPacket["payload"];
    const context = { cid };

    return new Promise<unknown>((resolve, reject) => {
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
        if (this.#pending.delete(cid)) this.tracker.exit();
      };

      const fail = (error: Error): void => {
        release();
        reject(error);
      };

      this.#pending.set(cid, {
        kind: "call",
        object,
        property: method,
        timestamp: Date.now(),
        resolve: (value: unknown) => {
          release();
          resolve(value);
        },
        reject: fail,
      } satisfies PendingCallRequest);
      this.tracker.enter();

      this.ctx.diagnostic.maybe("request:sent")?.({
        ref: cid,
        object,
        property: method,
        kind: "call",
      });

      if (signal) {
        abortHandler = async () => {
          this.ctx.diagnostic.maybe("request:abort")?.({ ref: cid });
          await this.ctx
            .send<Packets.AbortRequestPacket>({
              kind: WireKind.REQUEST,
              type: Packets.RequestMessageType.ABORT,
              payload: { ref: cid },
            })
            .catch(() => null);
          fail(new QuiryError(WireStatus.ABORTED, "Request operation cancelled", context));
        };

        if (signal.aborted) return abortHandler();
        signal.addEventListener("abort", abortHandler, { once: true });
      }

      // The session stays up unless the transport itself errors.
      Promise.resolve(
        this.ctx.send<Packets.CallRequestPacket>({
          id: cid,
          kind: WireKind.REQUEST,
          type: Packets.RequestMessageType.CALL,
          payload,
        }),
      ).catch((cause: unknown) => {
        fail(new QuiryError(WireStatus.DATA_LOSS, "Failed to send packet", { ...context, cause }));
      });
    });
  }

  /**
   * Server-streaming consumer: credit-based flow control; iterator `return`/`throw` sends `CANCEL`.
   * When not `open`, returns an iterator whose `next` rejects with `UNAVAILABLE` (sync throw only for bad args).
   */
  stream(
    object: string,
    method: string,
    args: ReadonlyArray<unknown>,
    signal?: AbortSignal,
  ): AsyncIterableIterator<unknown> {
    if (this.ctx.state() !== SessionState.OPEN) {
      const q = new AsyncQueue<unknown>();
      void q.throw(new QuiryError(WireStatus.UNAVAILABLE, "Session is not open"));
      return q;
    }

    const cid = this.ctx.correlate();
    const substitutes = this.ctx.callbacks.substitute(unwrapSerialized(args), cid);
    // Ensure arguments can be cloned through port.
    if (!isSerializable(substitutes))
      throw new QuiryError(WireStatus.INVALID_ARGUMENT, "Arguments are not serializable", {
        detail: { args },
      });

    const queue = new AsyncQueue<unknown>();
    // The initial budget we grant the producer. Set synchronously so that
    // chunks arriving before the CALL `.then` callback still decrement the
    // right counter (producer cannot start until it sees the credit grant,
    // but other incoming packets are routed concurrently).
    const credit = { remaining: this.window };
    const context = { cid };

    const entry: PendingStreamRequest = {
      kind: "stream",
      object,
      property: method,
      timestamp: Date.now(),
      queue,
      credit,
      seq: 0,
    };

    const cancel = (error: Error): void => {
      this.#pending.delete(cid);
      this.tracker.exit();
      queue.fail(error);

      this.ctx.diagnostic.maybe("stream:cancel")?.({ ref: cid, source: "local" });
      void this.ctx
        .send<Packets.CancelRequestPacket>({
          kind: WireKind.REQUEST,
          type: Packets.RequestMessageType.CANCEL,
          payload: { ref: cid },
        })
        .catch(() => {
          // Observable; deadline already fired locally.
        });
    };

    if (signal) {
      const abortHandler = () => {
        cancel(new QuiryError(WireStatus.ABORTED, "Stream request cancelled", context));
        signal!.removeEventListener("abort", abortHandler);
      };
      if (signal.aborted) abortHandler();
      signal.addEventListener("abort", abortHandler);
    }

    this.#pending.set(cid, entry);
    this.tracker.enter();

    this.ctx.diagnostic.maybe("request:sent")?.({
      ref: cid,
      object,
      property: method,
      kind: "stream",
    });
    this.ctx.diagnostic.maybe("stream:open")?.({
      ref: cid,
      window: this.window,
    });

    // Send CALL and initial credit grant. CREDIT is bundled as a separate
    // STREAM response packet referencing the correlation; the producer
    // cannot emit chunks until it observes this grant.
    void (async () => {
      try {
        await this.ctx.send<Packets.CallRequestPacket>({
          id: cid,
          kind: WireKind.REQUEST,
          type: Packets.RequestMessageType.CALL,
          payload: { object, method, args: substitutes },
        });

        await this.ctx.send<Packets.StreamResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.STREAM,
          payload: { event: "credit", ref: cid, credit: this.window },
        });

        this.ctx.diagnostic.maybe("stream:credit-grant")?.({
          ref: cid,
          delta: this.window,
          remaining: this.window,
          direction: "sent",
        });
      } catch (cause: unknown) {
        const e = this.#pending.get(cid);
        if (e?.kind === "stream") {
          this.#pending.delete(cid);
          this.tracker.exit();
          queue.fail(
            new QuiryError(WireStatus.DATA_LOSS, "Failed to initiate stream", { ...context, cause }),
          );
        }
      }
    })();

    const cleanup = (): void => {
      void this.ctx
        .send<Packets.CancelRequestPacket>({
          kind: WireKind.REQUEST,
          type: Packets.RequestMessageType.CANCEL,
          payload: { ref: cid },
        })
        .catch(() => {
          // Observable; consumer already abandoned the iterator.
        });

      const e = this.#pending.get(cid);
      if (this.#pending.delete(cid)) this.tracker.exit();
    };

    return {
      next(): Promise<IteratorResult<unknown>> {
        return queue.next();
      },
      [Symbol.asyncIterator](): AsyncIterableIterator<unknown> {
        return this;
      },
      async return(): Promise<IteratorResult<unknown, undefined>> {
        cleanup();
        await queue.return();
        return { value: undefined, done: true };
      },
      async throw(error?: unknown): Promise<IteratorResult<unknown, undefined>> {
        cleanup();
        queue.fail(new QuiryError(WireStatus.CANCELLED, "Stream aborted by consumer", { cause: error }));
        return { value: undefined, done: true };
      },
    };
  }

  // --------- PACKET HANDLING --------- //

  /**
   * Routes consumer-side RESPONSE packets to their pending entry.
   *
   * Producer-side STREAM events (credit grants for outbound producer streams)
   * are NOT handled here — the orchestrator dispatches those to
   * {@link InboundRequests} which owns the producer state.
   */
  handleResponsePacket(packet: Packets.AnyResponsePacket) {
    if (packet.type === Packets.ResponseMessageType.STREAM) {
      const entry = this.#pending.get(packet.payload.ref) as PendingStreamRequest | undefined;
      if (!entry) return; // stale; producer side or already cleaned up

      switch (packet.payload.event) {
        case "chunk":
          this.#handleStreamChunk(packet.payload, entry);
          return;
        case "end":
          this.#handleStreamEnd(packet.payload, entry);
          return;
        case "error":
          this.#handleStreamError(packet.payload, entry);
          return;
        // "credit" is producer-side and the orchestrator routes it elsewhere.
        case "credit":
          return;
      }

      // Fallthrough; not supposed to happen.
      return;
    }

    const { ref, status } = packet.payload;
    const entry = this.#pending.get(ref);
    if (!entry) return; // stale — local already settled (timeout, abort, etc.)

    this.#pending.delete(ref);
    this.tracker.exit();

    if (entry.kind === "stream") {
      // User tried streaming a unary method.
      entry.queue.fail(
        new QuiryError(WireStatus.FAILED_PRECONDITION, "Cannot stream a unary method.", { cid: ref }),
      );

      this.ctx.diagnostic.maybe("request:settled")?.({
        ref,
        status,
        durationMs: Date.now() - entry.timestamp,
      });
      return;
    }

    if (status === WireStatus.OK) {
      entry.resolve(this.ctx.callbacks.restoreStubs(packet.payload.result));
    } else {
      // Reconstruct the remote error with its full cause chain.
      // The `origin` on the rebuilt error reflects the remote node.
      entry.reject(fromWireError(packet.payload.error));
    }

    this.ctx.diagnostic.maybe("request:settled")?.({
      ref,
      status,
      durationMs: Date.now() - entry.timestamp,
    });
  }

  #handleStreamChunk({ ref, seq, chunk }: Packets.StreamChunkPayload, entry: PendingStreamRequest): void {
    if (seq !== entry.seq) {
      // Out-of-order chunk — drop and let the gap surface on stream:end.
      return;
    }

    entry.seq++;
    entry.credit.remaining--;
    entry.queue.enqueue(chunk);

    this.ctx.diagnostic.maybe("stream:chunk")?.({ ref, seq, direction: "received" });

    // Replenish credit when half the window is consumed.
    if (entry.credit.remaining <= Math.floor(this.window / 2)) {
      const grant = this.window - entry.credit.remaining;
      entry.credit.remaining += grant;

      // Send the delta, not the absolute remaining, and bump the local view up.
      void this.ctx
        .send<Packets.StreamResponsePacket>({
          kind: WireKind.RESPONSE,
          type: Packets.ResponseMessageType.STREAM,
          payload: { event: "credit", ref, credit: grant },
        })
        .catch(() => {});

      this.ctx.diagnostic.maybe("stream:credit-grant")?.({
        ref,
        delta: grant,
        remaining: entry.credit.remaining,
        direction: "sent",
      });
    }
  }

  #handleStreamEnd({ ref, seq }: Packets.StreamEndPayload, entry: PendingStreamRequest): void {
    this.#pending.delete(ref);
    this.tracker.exit();

    // Gap detection: the producer reports the next seq it
    // *would* have emitted. If our cursor doesn't match, a
    // chunk was lost in transit.
    if (seq !== entry.seq) {
      // Producer's reported "next seq" doesn't match — a chunk was lost.
      entry.queue.fail(
        new QuiryError(WireStatus.DATA_LOSS, "Stream ended with unexpected sequence (gap detected)", {
          cid: ref,
          detail: { expected: entry.seq, actual: seq },
        }),
      );

      this.ctx.diagnostic.maybe("stream:error")?.({ ref, seq, status: WireStatus.DATA_LOSS });
    } else {
      entry.queue.close();
      this.ctx.diagnostic.maybe("stream:end")?.({ ref, seq, direction: "received" });
    }

    this.ctx.diagnostic.maybe("request:settled")?.({
      ref,
      status: WireStatus.OK,
      durationMs: Date.now() - entry.timestamp,
    });
  }

  #handleStreamError(
    { ref, seq, error: cause }: Packets.StreamErrorPayload,
    entry: PendingStreamRequest,
  ): void {
    const error = fromWireError(cause);
    entry.queue.fail(error);
    this.#pending.delete(ref);
    this.tracker.exit();

    this.ctx.diagnostic.maybe("stream:error")?.({ ref, seq, status: error.code });
    this.ctx.diagnostic.maybe("request:settled")?.({
      ref,
      status: error.code,
      durationMs: Date.now() - entry.timestamp,
    });
  }

  // --------- PUBLIC: LIFECYCLE --------- //

  /**
   * Pre-emptively cancel every pending stream and notify the producer side.
   * Called by the drain coordinator before quiescing — streams may run
   * indefinitely and would otherwise block the deadline.
   */
  cancelStreams(reason: string = "drained"): void {
    for (const [cid, entry] of Array.from(this.#pending)) {
      if (entry.kind !== "stream") continue;

      this.#pending.delete(cid);
      this.tracker.exit();
      entry.queue.fail(new QuiryError(WireStatus.ABORTED, `Stream aborted by session ${reason}`, { cid }));

      this.ctx.diagnostic.maybe("stream:cancel")?.({ ref: cid, source: "local" });
      void this.ctx
        .send<Packets.CancelRequestPacket>({
          kind: WireKind.REQUEST,
          type: Packets.RequestMessageType.CANCEL,
          payload: { ref: cid },
        })
        .catch(() => {});
    }
  }

  /** Reject every outstanding pending request. Called from `Session.terminate`. */
  rejectAll(error: Error): void {
    for (const request of this.#pending.values()) {
      if (request.kind === "stream") request.queue.fail(error);
      else request.reject(error);
    }
    this.#pending.clear();
  }

  /** Resolves once the in-flight tracker drains to zero. */
  idle(): Promise<void> {
    return this.tracker.idle();
  }

  /** Force-reset the tracker. Used during teardown. */
  drain(): void {
    this.tracker.drain();
  }

  // --------- PUBLIC: INTROSPECTION --------- //

  /** Total outstanding outbound requests (unary + stream consumer entries). */
  get pendingCount(): number {
    return this.#pending.size;
  }

  /** Number of consumer-side stream requests currently subscribed. */
  get streamCount(): number {
    let n = 0;
    for (const entry of this.#pending.values()) if (entry.kind === "stream") n++;
    return n;
  }
}
