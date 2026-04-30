import type { AnyPacket } from "@/interface/packets";

export type ConnectionState = "connecting" | "open" | "draining" | "closed";
export type BackpressureState = "ok" | "high" | "critical";

export interface BackpressureSignal {
  readonly state: BackpressureState;
  readonly depth: number;
}

export type BackpressureSnapshot = BackpressureSignal & {
  readonly updatedAt: number;
};

export interface TransportOptions {
  // Nothing here, yet.
}

export interface TransportEvents {
  "state-change": [next: ConnectionState, prev: ConnectionState];
  error: [error: TransportError];
}

export interface Transport {
  readonly state: ConnectionState;
  send(packet: AnyPacket): Promise<void>;

  /**
   * Pull-based, the session drives consumption.
   * Naturally applies backpressure. If the consumer stops pulling,
   * the transport can signal the sender to slow down.
   */
  receive(): AsyncIterableIterator<AnyPacket>;
  readonly backpressure: BackpressureSignal;

  open(): Promise<void>;
  close(): Promise<void>;

  on<K extends keyof TransportEvents>(event: K, listener: (...args: TransportEvents[K]) => void): () => void;
}

export type TransportErrorKind = "send" | "receive" | "terminate";

export interface TransportErrorOptions extends ErrorOptions {
  readonly kind?: TransportErrorKind;
}

export class TransportError extends Error {
  readonly kind: TransportErrorKind;
  override readonly cause?: unknown;

  constructor(message: string, opts: TransportErrorOptions = {}) {
    super(message);

    this.name = "TransportError";
    this.kind = opts.kind ?? "send";
    this.cause = opts.cause;
  }
}
