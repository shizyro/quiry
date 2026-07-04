import {
  TransportState,
  TransportError,
  type Transport,
  type TransportEvents,
  type BackpressureSignal,
  BackpressureState,
} from ".";

import { DeferredQueue } from "../../lib/queue";
import { isWirePacket, collectTransferables } from "../../lib/helpers";
import type { AnyPacket } from "../../protocol/packets";

const BACKPRESSURE_HIGH: number = 100; // packets
const BACKPRESSURE_CRITICAL: number = 500;

/**
 * Base transport class for most transport implementations.
 *
 * Provides a common interface for sending and receiving packets,
 * as well as managing the transport state and backpressure.
 */
export abstract class BaseTransport implements Transport<AnyPacket> {
  protected readonly queue = new DeferredQueue<AnyPacket>(16);
  private readonly listeners = new Map<keyof TransportEvents, Set<(...args: unknown[]) => void>>();

  #state: TransportState = TransportState.CLOSED;
  #depth: number = 0;

  get state(): TransportState {
    return this.#state;
  }

  open(): void {
    if (this.state !== TransportState.CLOSED)
      throw new TransportError("Cannot attach transport that is not in the closed state");
    this.attach();

    this.#state = TransportState.OPEN;
    this.emit("open");
  }

  close(reason?: string): void {
    if (this.state === TransportState.CLOSED) return;
    this.dispose();

    this.queue.close();
    this.#state = TransportState.CLOSED;
    this.emit("close", reason ?? "explicit");
    this.cleanup();
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

  private emit<K extends keyof TransportEvents>(event: K, ...args: TransportEvents[K]): void {
    this.listeners.get(event)?.forEach((listener) => void listener(...args));
  }

  on<K extends keyof TransportEvents>(event: K, listener: (...args: TransportEvents[K]) => void): () => void {
    const handle = listener as (...args: unknown[]) => void;
    this.listeners.set(event, new Set([...(this.listeners.get(event) ?? []), handle]));
    return () => this.listeners.get(event)?.delete(handle);
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
    if (isWirePacket(value)) this.queue.enqueue(value);
  }

  protected terminate(message: string, cause?: unknown): void {
    this.emit("error", new TransportError(message, { kind: "terminate", cause }));

    this.queue.close();
    this.#state = TransportState.CLOSED;
    this.emit("close", message);
    this.cleanup();
  }

  protected cleanup(): void {
    this.listeners.clear();
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
}
