import {
  type Transport,
  type TransportError,
  type BackpressureSignal,
  TransportState,
  BackpressureState,
} from "~/core/transport";

import { DeferredQueue } from "~/lib/queue";
import type { AnyPacket } from "~/interface/packets";

interface MockEvents {
  open: [];
  close: [reason?: string];
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
 * The inbound buffer uses {@link DeferredQueue} (generator-based iteration)
 * rather than a raw `AsyncQueue`. That matters because the session creates
 * more than one iterator on `receive()` — one transient during handshake,
 * one durable for the router — and a raw `AsyncQueue` would close the
 * shared queue the moment the handshake iterator's `return()` was called.
 */
export class MockTransport implements Transport {
  readonly #listeners = new Map<keyof MockEvents, Set<(...args: unknown[]) => void>>();
  readonly #inbound = new DeferredQueue<AnyPacket>();

  #peer: MockTransport | null = null;
  #state: TransportState = TransportState.CLOSED;

  get state(): TransportState {
    return this.#state;
  }

  get backpressure(): BackpressureSignal {
    return { state: BackpressureState.OK, depth: 0 };
  }

  private transition(next: TransportState, reason?: string): void {
    if (next === this.#state) return;
    this.#state = next;
    if (next === TransportState.OPEN) this.emit("open");
    if (next === TransportState.CLOSED) this.emit("close", reason);
  }

  open(): void {
    this.transition(TransportState.OPEN);
  }

  close(reason?: string): void {
    if (this.#state === TransportState.CLOSED) return;
    const peer = this.#peer;
    this.#peer = null;
    this.transition(TransportState.CLOSED, reason);
    this.#inbound.close();

    // Mirror real worker-thread / child-process semantics: the remote
    // side observes a close event when its peer goes away. Without this
    // the producer would keep running its outbound streams forever
    // after a force-close on the consumer.
    if (peer && peer.state === TransportState.OPEN) {
      queueMicrotask(() => peer.close(reason));
    }
  }

  async send(packet: AnyPacket): Promise<void> {
    if (this.#state !== TransportState.OPEN) {
      throw new Error(`MockTransport cannot send: state=${this.#state}`);
    }
    if (!this.#peer) throw new Error("MockTransport has no peer");

    // Deliver asynchronously so send() resolves before the peer observes
    // the packet. This mirrors postMessage semantics and prevents
    // reentrancy if a receiver calls back into send() synchronously.
    const cloned = structuredClone(packet);
    queueMicrotask(() => {
      if (this.#peer && this.#peer.#state === TransportState.OPEN) this.#peer.#deliver(cloned);
    });
  }

  #deliver(packet: AnyPacket): void {
    // Defensive: a race with close() could still race the state check on
    // the sending side. DeferredQueue already drops silently on `#closed`,
    // so this is belt-and-suspenders.
    if (this.#state !== TransportState.OPEN) return;
    this.#inbound.enqueue(packet);
  }

  receive(): AsyncIterableIterator<AnyPacket> {
    return this.#inbound[Symbol.asyncIterator]();
  }

  private emit<K extends keyof MockEvents>(event: K, ...args: MockEvents[K]): void {
    this.#listeners.get(event)?.forEach((listener) => void listener(...args));
  }

  on<K extends keyof MockEvents>(event: K, listener: (...args: MockEvents[K]) => void): () => void {
    const handle = listener as (...args: unknown[]) => void;
    this.#listeners.set(event, new Set([...(this.#listeners.get(event) ?? []), handle]));
    return () => this.#listeners.get(event)?.delete(handle);
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
