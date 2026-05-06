import { Worker, isMainThread, parentPort } from "node:worker_threads";
import type { MessagePort, Transferable } from "node:worker_threads";

import { BaseTransport } from "./base";
import { TransportError, TransportState, type TransportOptions } from ".";

export interface WorkerThreadsTransportOptions extends TransportOptions {
  readonly worker?: Worker;
}

/**
 * Dual-mode transport for `worker_threads`. In the main thread, requires `opts.worker`;
 * in a worker thread it automatically binds to `parentPort`. Both sides exchange
 * packets via `postMessage` / `message` using the same underlying channel.
 */
export class WorkerThreadsTransport extends BaseTransport {
  private readonly port: Worker | MessagePort;

  constructor(opts: WorkerThreadsTransportOptions = {}) {
    super();

    if (isMainThread) {
      if (!opts.worker) throw new TypeError("Worker is required in main thread");
      if (!(opts.worker instanceof Worker)) throw new TypeError("Worker instance is required");
      this.port = opts.worker;
    } else {
      if (!parentPort) throw new TypeError("parentPort is null — ensure this is running in a worker thread");
      this.port = parentPort;
    }
  }

  attach(): void {
    if (this.state !== TransportState.CLOSED) {
      throw new TransportError("Cannot attach transport that is not in the closed state");
    }

    this.port.on("message", this.onPortMessage);
    this.port.on("error", this.onPortError);

    if (this.port instanceof Worker) {
      this.port.on("exit", this.onPortExit);
    } else this.port.on("close", this.onPortClose);

    this.transition(TransportState.OPEN);
  }

  dispose(): void {
    if (this.state === TransportState.CLOSED) return;

    // Remove all listeners
    this.port.off("message", this.onPortMessage);
    this.port.off("error", this.onPortError);

    if (this.port instanceof Worker) {
      this.port.off("exit", this.onPortExit);
    } else this.port.off("close", this.onPortClose);

    this.queue.close();
    this.transition(TransportState.CLOSED);
    this.cleanup();
  }

  protected post(packet: unknown, transferables: Transferable[]): void {
    if (transferables.length > 0) {
      this.port.postMessage(packet, transferables);
    } else {
      // @ts-expect-error - postMessage without transferables is supported
      this.port.postMessage(packet);
    }
  }

  // ...

  private readonly onPortMessage = (value: unknown): void => this.read(value);

  private readonly onPortError = (error: Error): void => {
    this.terminate("Worker thread errored", error);
  };

  private readonly onPortExit = (code: number): void => {
    if (this.state === TransportState.CLOSED) return;
    // Code 0 is a clean, cooperative shutdown (e.g. `process.exit(0)`); anything else
    // is abnormal and must be surfaced to the session as `terminated`.
    if (code === 0) return void this.dispose();
    this.terminate(`Worker thread exited with code ${code}`);
  };

  private readonly onPortClose = (): void => {
    if (this.state === TransportState.CLOSED) return;
    // The port was closed by the remote — we cannot send or receive further.
    this.terminate("Message port closed by remote");
  };
}
