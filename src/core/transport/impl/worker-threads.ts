import { Worker, isMainThread, parentPort } from "node:worker_threads";
import type { MessagePort, Transferable } from "node:worker_threads";

import { BaseTransport } from "../base";
import { TransportState } from "../index";

/**
 * Dual-mode transport for `worker_threads`. In the main thread, requires `opts.worker`;
 * in a worker thread it automatically binds to `parentPort`. Both sides exchange
 * packets via `postMessage` / `message` using the same underlying channel.
 */
export class WorkerThreadsTransport extends BaseTransport {
  private readonly port: Worker | MessagePort;

  constructor(worker?: Worker) {
    super();

    if (isMainThread) {
      if (!worker) throw new TypeError("Worker is required in main thread");
      if (!(worker instanceof Worker)) throw new TypeError("Worker instance is required");
      this.port = worker;
    } else {
      if (!parentPort) throw new TypeError("parentPort is null — ensure this is running in a worker thread");
      this.port = parentPort;
    }
  }

  attach(): void {
    this.port.on("message", this.onPortMessage);
    this.port.on("error", this.onPortError);

    if (this.port instanceof Worker) {
      this.port.on("exit", this.onPortExit);
    } else this.port.on("close", this.onPortClose);
  }

  dispose(): void {
    this.port.off("message", this.onPortMessage);
    this.port.off("error", this.onPortError);

    if (this.port instanceof Worker) {
      this.port.off("exit", this.onPortExit);
    } else this.port.off("close", this.onPortClose);
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
    /**
     * Code 0 is a clean cooperative shutdown (e.g. `process.exit(0)`).
     * However, code 1 is an explicit termination from the remote; anything else
     * is abnormal and must be surfaced to the session as `terminated`.
     */
    if (code === 0) return void this.close();
    else if (code === 1) return void this.close("terminated");
    this.terminate(`Worker thread exited with code ${code}`);
  };

  private readonly onPortClose = (): void => {
    if (this.state === TransportState.CLOSED) return;
    // The port was closed by the remote — we cannot send or receive further.
    this.terminate("Message port closed by remote");
  };
}
