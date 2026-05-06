import EventEmitter from "node:events";
import type { AnyPacket } from "@/interface/packets";

import {
  TransportError,
  TransportState,
  type Transport,
  type TransportEvents,
  type BackpressureSignal,
  BackpressureState,
} from ".";

import { PacketQueue } from "./lib/packet-queue";
import { isWirePacket } from "@/lib/helpers";

const BACKPRESSURE_HIGH = 100; // packets
const BACKPRESSURE_CRITICAL = 500;

/**
 * Base transport class for most transport implementations.
 *
 * Provides a common interface for sending and receiving packets,
 * as well as managing the transport state and backpressure.
 */
export abstract class BaseTransport implements Transport {
  private readonly emitter = new EventEmitter<TransportEvents>();
  protected readonly queue = new PacketQueue(16);

  #state: TransportState = TransportState.CLOSED;
  #depth: number = 0;

  get state(): TransportState {
    return this.#state;
  }

  protected transition(next: TransportState): void {
    if (next === this.#state) return;
    const prev = this.#state;
    this.#state = next;
    this.emitter.emit("state-change", next, prev);
  }

  async send(packet: AnyPacket): Promise<void> {
    if (this.#state !== "open") {
      throw new TransportError("Packet cannot be sent, transport is not open", { kind: "send" });
    }

    const transferables = collectTransferables(packet);
    this.#depth++;

    try {
      await this.post(packet, transferables);
    } catch (error: unknown) {
      throw new TransportError("Failed to send packet", { kind: "send", cause: error });
    } finally {
      this.#depth--;
    }
  }

  /**
   * Subclasses call this to post a message on their specific port type.
   */
  protected abstract post(packet: AnyPacket, transferables: Transferable[]): void | PromiseLike<void>;

  receive(): AsyncIterableIterator<AnyPacket> {
    return this.queue[Symbol.asyncIterator]();
  }

  abstract attach(): void;
  abstract dispose(): void;

  /**
   * Enqueues a decoded message from the underlying channel.
   * Non-packets are dropped (no throw) so a malformed message cannot tear down the transport.
   */
  protected read(value: unknown): void {
    if (!isWirePacket(value)) {
      //// throw new TransportError("Invalid wire packet", { kind: "receive", cause: value });
      return;
    }
    this.queue.enqueue(value);
  }

  protected terminate(reason: TransportError | string, cause?: unknown): void {
    const error =
      reason instanceof TransportError ? reason : new TransportError(reason, { kind: "terminate", cause });
    this.emitter.emit("error", error);
    this.queue.close();
    this.transition(TransportState.CLOSED);
    this.cleanup();
  }

  protected cleanup(): void {
    this.emitter.removeAllListeners();
  }

  /**
   * Messages are synchronous and enqueued immediately in the V8 message queue.
   * We track out own send queue depth to provide a useful backpressure signal to session layer.
   */
  get backpressure(): BackpressureSignal {
    return {
      state:
        this.#depth >= BACKPRESSURE_CRITICAL
          ? BackpressureState.CRITICAL
          : this.#depth >= BACKPRESSURE_HIGH
            ? BackpressureState.HIGH
            : BackpressureState.OK,
      depth: this.#depth,
    };
  }

  on<K extends keyof TransportEvents>(event: K, listener: (...args: TransportEvents[K]) => void): () => void {
    this.emitter.on(event, listener as (...args: unknown[]) => void);
    return () => this.emitter.off(event, listener as (...args: unknown[]) => void);
  }
}

/**
 * Recursively walks a packet to find array buffers and message ports that
 * should be transferred (zero-copy) rather than cloned.
 */
function collectTransferables(value: unknown, seen = new Set<object>()): Transferable[] {
  if (value === null || typeof value !== "object") return [];
  if (seen.has(value as object)) return [];
  seen.add(value as object);

  if (value instanceof ArrayBuffer) return [value];
  if (value instanceof MessagePort) return [value];

  // Typed arrays and their underlying array buffers
  if (ArrayBuffer.isView(value)) return value.buffer instanceof ArrayBuffer ? [value.buffer] : [];

  return Object.values(value).flatMap((item) => collectTransferables(item, seen));
}
