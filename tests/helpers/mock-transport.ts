import EventEmitter from "node:events";

import type { AnyPacket } from "@/interface/packets";
import type { Transport, TransportError, BackpressureSignal, ConnectionState } from "@/core/transport";

import { PacketQueue } from "@/core/transport/packet-queue";

interface MockEvents {
  "state-change": [next: ConnectionState, prev: ConnectionState];
  backpressure: [signal: BackpressureSignal];
  error: [error: TransportError];
}

/**
 * An in-memory transport used for testing the session layer without spawning
 * worker threads. Two instances are linked via {@link pairTransports} so
 * packets posted on one surface on the other's receive stream.
 *
 * Packets are deep-cloned across the boundary with `structuredClone` to
 * mirror the real `MessagePort`-based transport semantics: receivers can't
 * mutate the sender's copy, and non-cloneable values blow up the same way.
 *
 * The inbound buffer uses {@link PacketQueue} (generator-based iteration)
 * rather than a raw `AsyncQueue`. That matters because the session creates
 * more than one iterator on `receive()` — one transient during handshake,
 * one durable for the router — and a raw `AsyncQueue` would close the
 * shared queue the moment the handshake iterator's `return()` was called.
 */
export class MockTransport implements Transport {
  readonly #emitter = new EventEmitter();
  readonly #inbound = new PacketQueue();

  #peer: MockTransport | null = null;
  #state: ConnectionState = "connecting";

  get state(): ConnectionState {
    return this.#state;
  }

  get backpressure(): BackpressureSignal {
    return { state: "ok", depth: 0 };
  }

  private transition(next: ConnectionState): void {
    if (next === this.#state) return;
    const prev = this.#state;
    this.#state = next;
    this.#emitter.emit("state-change", next, prev);
  }

  async open(): Promise<void> {
    this.transition("open");
  }

  async close(): Promise<void> {
    if (this.#state === "closed") return;
    this.transition("closed");
    this.#inbound.close();
    this.#peer = null;
  }

  async send(packet: AnyPacket): Promise<void> {
    if (this.#state !== "open") {
      throw new Error(`MockTransport cannot send: state=${this.#state}`);
    }
    if (!this.#peer) throw new Error("MockTransport has no peer");

    // Deliver asynchronously so send() resolves before the peer observes
    // the packet. This mirrors postMessage semantics and prevents
    // reentrancy if a receiver calls back into send() synchronously.
    const cloned = structuredClone(packet);
    queueMicrotask(() => {
      if (this.#peer && this.#peer.#state === "open") this.#peer.#deliver(cloned);
    });
  }

  #deliver(packet: AnyPacket): void {
    // Defensive: a race with close() could still race the state check on
    // the sending side. PacketQueue already drops silently on `#closed`,
    // so this is belt-and-suspenders.
    if (this.#state !== "open") return;
    this.#inbound.enqueue(packet);
  }

  receive(): AsyncIterableIterator<AnyPacket> {
    return this.#inbound[Symbol.asyncIterator]();
  }

  on<K extends keyof MockEvents>(event: K, listener: (...args: MockEvents[K]) => void): () => void {
    this.#emitter.on(event, listener as (...args: unknown[]) => void);
    return () => this.#emitter.off(event, listener as (...args: unknown[]) => void);
  }

  /** @internal test helper — hooks a second transport as this one's counterpart. */
  _link(peer: MockTransport): void {
    this.#peer = peer;
  }
}

/**
 * Build a linked pair of transports suitable for driving two {@link Session}
 * instances against one another.
 */
export function pairTransports(): [MockTransport, MockTransport] {
  const a = new MockTransport();
  const b = new MockTransport();
  a._link(b);
  b._link(a);
  return [a, b];
}
