export enum TransportState {
  OPEN = "open",
  CLOSED = "closed",
}

export interface TransportOptions {
  // Nothing here, yet.
}

export interface TransportEvents {
  open: [];
  close: [reason?: string];
  error: [error: TransportError];
}

export interface Transport<T = unknown> {
  readonly state: TransportState;
  send(packet: T): Promise<void>;

  /**
   * Pull-based, the session drives consumption.
   * Naturally applies backpressure. If the consumer stops pulling,
   * the transport can signal the sender to slow down.
   */
  receive(): AsyncIterableIterator<T>;
  readonly backpressure: BackpressureSignal;

  open(): void;
  close(reason?: string): void;

  on<K extends keyof TransportEvents>(event: K, listener: (...args: TransportEvents[K]) => void): Unsubscribe;
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
    this.kind = opts.kind ?? "receive";
    this.cause = opts.cause;
  }
}

export enum BackpressureState {
  OK = "ok",
  HIGH = "high",
  CRITICAL = "critical",
}

export interface BackpressureSignal {
  readonly state: BackpressureState;
  readonly depth: number;
}

export type BackpressureSnapshot = Omit<BackpressureSignal, "state"> & {
  readonly state: keyof typeof BackpressureState;
  readonly updatedAt: number;
};
