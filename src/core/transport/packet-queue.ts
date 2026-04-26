import { Queue } from "@/lib/queue";
import type { AnyPacket } from "@/interface/packets";

interface Deferred {
  resolve: (value?: unknown) => void;
  reject: (error: Error) => void;
}

/** A queue wrapper for deferred pull-based consumption of packets. */
export class PacketQueue extends Queue<AnyPacket> implements AsyncIterable<AnyPacket> {
  #waiting: Deferred | null = null;
  #closed: boolean = false;

  override enqueue(item: AnyPacket): void {
    if (this.#closed) return;
    super.enqueue(item);
    this.#waiting?.resolve();
    this.#waiting = null;
  }

  close(): void {
    if (this.#closed) return;
    this.clear();
    this.#closed = true;
    this.#waiting?.resolve();
    this.#waiting = null;
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<AnyPacket> {
    while (true) {
      while (this.size > 0) yield this.dequeue()!;
      if (this.#closed) return;

      await new Promise((resolve, reject) => {
        this.#waiting = { resolve, reject };
      });
    }
  }
}
