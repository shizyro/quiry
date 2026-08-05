import * as Quiry from "~";

/**
 * A tiny job that ticks on an interval, and can be paused, resumed, or
 * re-timed — all by writing to its plain fields, not by calling methods.
 */
class Ticker {
  /** A plain mutable field, no getter/setter needed. */
  paused: boolean = false;
  /** Also just a field. Written to from the remote side to slow/speed up ticking. */
  interval: number = 200;

  private count = 0;
  /** Ticks up to 12 times, skipping ticks entirely while `paused` is true. */
  async *run(): AsyncGenerator<number> {
    while (this.count < 12) {
      await new Promise((resolve) => setTimeout(resolve, this.interval));
      if (this.paused) continue;
      yield ++this.count;
    }
  }
}

Quiry.attach(new Quiry.WorkerThreadsTransport());
Quiry.expose("ticker", new Ticker());

// This example only demos one round of pause/resume, so once the main side
// closes the connection, there's nothing left for this thread to do.
Quiry.on("peer-disconnected", () => process.exit(0));

export type { Ticker };
