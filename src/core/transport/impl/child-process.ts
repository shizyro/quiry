import { ChildProcess, type Serializable } from "node:child_process";

import { BaseTransport } from "../base";
import { TransportState } from "..";

/**
 * Dual-mode transport for `child_process.fork`. In the parent process, requires `opts.child`;
 * in the forked child it wraps `process` directly.
 */
export class ChildProcessTransport extends BaseTransport {
  private readonly port: ChildProcess | NodeJS.Process;

  constructor(child?: ChildProcess) {
    super();

    const isChildProcess = typeof process.send === "function";
    if (!isChildProcess) {
      if (!child) throw new TypeError("Child process instance is required");
      if (!(child instanceof ChildProcess)) throw new TypeError("Child process instance is required");
      this.port = child;
    } else {
      this.port = process;
    }
  }

  attach(): void {
    this.port.on("message", this.onPortMessage);
    this.port.on("error", this.onPortError);

    if (this.port instanceof ChildProcess) {
      this.port.on("exit", this.onPortExit);
      this.port.on("disconnect", this.onPortDisconnect);
    } else {
      // Child side — detect parent disconnect.
      process.on("disconnect", this.onPortDisconnect);
    }
  }

  dispose(): void {
    this.port.off("message", this.onPortMessage);
    this.port.off("error", this.onPortError);

    if (this.port instanceof ChildProcess) {
      this.port.off("exit", this.onPortExit);
      this.port.off("disconnect", this.onPortDisconnect);
    } else process.off("disconnect", this.onPortDisconnect);
  }

  protected post(packet: unknown): void {
    if (this.port instanceof ChildProcess) {
      this.port.send(packet as Serializable);
    } else process.send?.(packet);
  }

  // ...

  private readonly onPortMessage = (value: unknown): void => this.read(value);

  private readonly onPortError = (error: Error): void => {
    this.terminate("Child process errored", error);
  };

  private readonly onPortExit = (code: number | null): void => {
    if (this.state === TransportState.CLOSED) return;
    if (code === 0) return void this.close();
    this.terminate(`Child process exited with code ${code ?? -1}`);
  };

  private readonly onPortDisconnect = (): void => {
    if (this.state === TransportState.CLOSED) return;
    this.terminate("IPC channel disconnected");
  };
}
