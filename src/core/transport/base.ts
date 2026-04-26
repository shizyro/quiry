import EventEmitter from "node:events";
import type { AnyPacket } from "@/interface/packets";

import {
  TransportError,
  type Transport,
  type TransportEvents,
  type BackpressureSignal,
  type ConnectionState,
} from ".";

import { PacketQueue } from "./packet-queue";
import { isWirePacket } from "@/lib/utils";

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

  #state: ConnectionState = "connecting";
  #depth: number = 0;

  get state(): ConnectionState {
    return this.#state;
  }

  protected transition(next: ConnectionState): void {
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
  protected abstract post(packet: AnyPacket, transferables: Transferable[]): void | Promise<void>;

  receive(): AsyncIterableIterator<AnyPacket> {
    return this.queue[Symbol.asyncIterator]();
  }

  abstract open(): Promise<void>;
  abstract close(): Promise<void>;

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
    this.transition("closed");
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
        this.#depth >= BACKPRESSURE_CRITICAL ? "critical" : this.#depth >= BACKPRESSURE_HIGH ? "high" : "ok",
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
