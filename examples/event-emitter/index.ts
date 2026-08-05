import * as Quiry from "~";
import { log, join } from "../shared";

import type { Counter } from "./child";

async function main() {
  const peer = Quiry.fork(join(import.meta.dirname, "child.ts"));
  const counter = peer.remote<Counter>("counter");

  // Create event handler proxies
  const onReset = peer.callback(() => log("[ev: reset]"));
  const onIncrement = peer.callback((prev: number, next: number) => {
    log(`[ev: increment] (${prev} -> ${next})`);
  });

  // Subscribe to events
  await counter.on("reset", onReset);
  await counter.on("increment", onIncrement);

  await counter.increment(); // 0 -> 1
  await counter.increment(); // 1 -> 2
  await counter.increment(); // 2 -> 3

  await counter.reset();

  await counter.increment(); // 0 -> 1
  log(`final count: ${await counter.value}`);

  await peer.close();
}

main().catch(console.error);
