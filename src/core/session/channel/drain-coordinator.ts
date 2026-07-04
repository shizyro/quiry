import * as Packets from "../../../protocol/packets";
import { WireKind } from "../../../protocol/wire";
import type { CorrelationId } from "../../../protocol/types";

import type { DiagnosticBus } from "../../../lib/diagnostics";
import type { SessionEvents } from "../../../interface/diagnostics";

import { SessionState } from "../state";
import type { SessionContext } from "../context";

/**
 * Minimal router surface the coordinator needs in order to wait for the
 * peer's terminal `DRAIN_ACK`.
 */
export interface DrainRouter {
  wait<P extends Packets.AnyPacket>(
    predicate: (p: Packets.AnyPacket) => p is P,
    options?: { signal?: AbortSignal },
  ): Promise<P>;
}

/**
 * Minimal transport surface — the coordinator subscribes to `close` so a
 * dying transport short-circuits the drain timeout.
 */
export interface DrainTransport {
  on(event: "close", handler: (reason?: string) => void): unknown;
}

export interface DrainCoordinatorDeps {
  /** Live session state — read on every check so we observe the latest value. */
  readonly state: () => SessionState;
  /** Drives the OPEN -> DRAINING transition. CLOSED is set by `terminate`. */
  readonly transition: (next: SessionState) => void;
  readonly send: SessionContext["send"];
  readonly diagnostic: DiagnosticBus<SessionEvents>;
  readonly router: DrainRouter;
  readonly transport: DrainTransport;
  readonly inbound: { cancelAllStreams(): void; idle(): Promise<void> };
  readonly outbound: { cancelStreams(reason: string): void; idle(): Promise<void> };
  readonly callbacks: { idle(): Promise<void>; releaseSessionCallbacks(): void };
  /**
   * Tear-down hook. Runs in `finally` so failure of the protocol (timeout,
   * transport death, exception) still produces a clean session close.
   */
  readonly terminate: (reason?: string) => void;
  readonly config: { drainTimeout: number };
}

/**
 * Owns the cooperative drain protocol.
 *
 * Idempotent: concurrent callers join the same in-flight promise. A `DRAIN`
 * packet that arrives mid-protocol is folded in — its ref is captured and
 * an inline ACK fires if we've already quiesced (no double-quiesce).
 *
 * @diagnostics `drain:phase` for `announced`, `quiesced`, `acked`,
 * and `timeout` so external tooling can build a drain timeline.
 */
export class DrainCoordinator {
  /** Null until drain begins; stays resolved after so re-entry sees "already drained". */
  #drain_promise: Promise<void> | null = null;
  /** Correlation id of the peer's `SYS:DRAIN`, used as the ACK ref. */
  #peer_drain_ref: CorrelationId | null = null;
  /** Set after our own quiesce step completes — gates inline ACK. */
  #drain_quiesced: boolean = false;

  constructor(private readonly deps: DrainCoordinatorDeps) {}

  /**
   * Begin (or join) a drain. Returns the in-flight protocol promise.
   * Resolves after `terminate`, regardless of success or timeout.
   */
  begin(
    initiator: "local" | "remote",
    reason: string = "drained",
    timeout: number = this.deps.config.drainTimeout,
  ): Promise<void> {
    return (this.#drain_promise ??= this.performDrain(initiator, reason, timeout));
  }

  private async performDrain(initiator: "local" | "remote", reason: string, timeout: number): Promise<void> {
    this.deps.transition(SessionState.DRAINING);
    this.deps.diagnostic.maybe("drain:phase")?.({ initiator, phase: "announced" });

    const controller = new AbortController();
    const timer = setTimeout(() => {
      this.deps.diagnostic.maybe("drain:phase")?.({ initiator, phase: "timeout" });
      controller.abort();
    }, timeout);

    // Short-circuit the drain the moment the transport dies under us.
    this.deps.transport.on("close", () => controller.abort());

    try {
      // 1. Local initiator announces. Remote initiator stays silent —
      //    its peer has already sent DRAIN and is waiting for our
      //    terminal ACK, which we'll send in step 3 after we quiesce.
      if (initiator === "local") {
        await this.deps
          .send<Packets.SystemDrainPacket>({
            kind: WireKind.SYSTEM,
            type: Packets.SystemMessageType.DRAIN,
            payload: { reason, timeout },
          })
          .catch(() => {});
      }

      // 2. Proactively terminate work that can run past the deadline:
      //    producer-side generators (may emit indefinitely) and the
      //    consumer-side iterators we're no longer going to consume.
      this.deps.inbound.cancelAllStreams();
      this.deps.outbound.cancelStreams("drain");

      // 3. Quiesce and ACK in parallel with waiting for the peer's ACK.
      //
      //    `quiesce` waits for our in-flight work; once it resolves, we
      //    send the terminal DRAIN_ACK if the peer's DRAIN ref is known.
      //    Both tasks share the same `drainTimeout` via `controller`.
      const quiesce = Promise.all([
        this.deps.inbound.idle(),
        this.deps.outbound.idle(),
        this.deps.callbacks.idle(),
      ]).then(async () => {
        if (this.#peer_drain_ref) {
          await this.deps
            .send<Packets.SystemDrainAckPacket>({
              kind: WireKind.SYSTEM,
              type: Packets.SystemMessageType.DRAIN_ACK,
              payload: { ref: this.#peer_drain_ref, uptime: process.uptime() },
            })
            .catch(() => null);
        }
        // Flip *after* the send so a late DRAIN handler doesn't fire a
        // duplicate ACK during our await window.
        this.#drain_quiesced = true;
        this.deps.diagnostic.maybe("drain:phase")?.({ initiator, phase: "quiesced" });
      });

      // Only the initiator awaits a reciprocal ACK from the peer.
      const peerAck =
        initiator === "local"
          ? this.deps.router
              .wait<Packets.SystemDrainAckPacket>(
                (p): p is Packets.SystemDrainAckPacket =>
                  p.kind === WireKind.SYSTEM && p.type === Packets.SystemMessageType.DRAIN_ACK,
                { signal: controller.signal },
              )
              .then(() => {
                this.deps.diagnostic.maybe("drain:phase")?.({ initiator, phase: "acked" });
              })
              .catch(() => null)
          : Promise.resolve();

      await Promise.race([
        Promise.all([quiesce, peerAck]),
        new Promise<never>((_, reject) => {
          if (controller.signal.aborted) return reject(new Error("aborted"));
          controller.signal.addEventListener("abort", () => reject(new Error("aborted")), {
            once: true,
          });
        }),
      ]);

      // 4. Best-effort release session-scoped callbacks. By now the peer
      //    has signaled it's done, so no further INVOKEs can arrive.
      await this.deps.callbacks.releaseSessionCallbacks();
    } catch {
      // Deadline or transport death — terminate fires unconditionally.
    } finally {
      clearTimeout(timer);
      this.deps.terminate(reason);
    }
  }

  /**
   * Process an inbound `SYS:DRAIN` packet. Captures the ref, fires an
   * inline terminal ACK if our own quiesce already finished, otherwise
   * starts a remote-initiated drain.
   */
  handleSystemDrainPacket(packet: Packets.SystemDrainPacket): void {
    const state = this.deps.state();
    if (state === SessionState.CLOSED) return;

    // Capture the peer's DRAIN packet id for correlation.
    this.#peer_drain_ref = packet.id;

    if (state === SessionState.DRAINING) {
      // Concurrent drain (or a stray re-send). If our work already
      // finished, fire ACK inline — the quiesce step won't run again
      // for this newly-arrived ref.
      if (this.#drain_quiesced) {
        void this.deps
          .send<Packets.SystemDrainAckPacket>({
            kind: WireKind.SYSTEM,
            type: Packets.SystemMessageType.DRAIN_ACK,
            payload: { ref: packet.id, uptime: process.uptime() },
          })
          .catch(() => {});
      }
      return;
    }

    // DRAIN before OPEN is nonsense from a conforming peer. Drop.
    if (state !== SessionState.OPEN) return;

    // Remote-initiated drain: run the same coroutine, just without
    // the "announce" step. Our terminal ACK will fire once we
    // quiesce.
    void this.begin("remote", packet.payload.reason, packet.payload.timeout);
  }

  /** Whether `begin` has been called (drain in progress or finished). */
  get inProgress(): boolean {
    return this.#drain_promise !== null;
  }
}
