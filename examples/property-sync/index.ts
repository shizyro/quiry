/**
 * Property writes travel across the boundary the same way calls do. This example
 * proves it by pausing and re-timing a remote job purely through assignment, and
 * watching the effect show up on the consuming side.
 *
 * The write is fire-and-forget from the caller's side (it doesn't await the round
 * trip), but it lands before any subsequent call that depends on it, because
 * requests to the same peer are delivered in order.
 */

import * as Quiry from "~";
import { log, join } from "../shared";

import type { Ticker } from "./worker";

async function main() {
  const peer = Quiry.spawn(join(import.meta.dirname, "worker.ts"));
  const ticker = peer.remote<Ticker>("ticker");

  /**
   * Data properties are proxied exactly like accessors. `ticker proxy.paused` resolves
   * the current value, and `ticker.paused = value` forwards the assignment.
   */
  log(`[initial state] paused: ${await ticker.paused}, interval: ${await ticker.interval}`);

  /**
   * Consume the stream and pausing concurrently: the generator is driven by
   * `for await`, while a separate task mutates `paused`/`intervalMs` on a timer.
   */
  const consuming = (async () => {
    for await (const tick of ticker.run()) {
      log(`tick #${tick}`);
    }
  })();

  // Wait for the first few ticks to complete.
  await new Promise((resolve) => setTimeout(resolve, 450));
  log("--- pausing ---");
  ticker.paused = Promise.resolve(true); // (wrap in a promise to bypass type-check)
  log(`[paused state] paused: ${await ticker.paused}, interval: ${await ticker.interval}`);

  // No ticks should be logged during this window; the write already landed.
  await new Promise((resolve) => setTimeout(resolve, 1350));

  log("--- speeding up and resuming ---");
  ticker.interval = Promise.resolve(80);
  ticker.paused = Promise.resolve(false);
  await consuming;

  log(`[final state] paused: ${await ticker.paused}, interval: ${await ticker.interval}`);
  await peer.close();
}

main().catch(console.error);
