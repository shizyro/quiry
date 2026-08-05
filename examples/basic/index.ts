import * as Quiry from "~";
import { log, join } from "../shared";

import type { Registry } from "./worker";

async function main() {
  const peer = Quiry.spawn<Registry>(join(import.meta.dirname, "worker.ts"));
  const solver = peer.remote("solver");

  // [unary calls]
  await peer.remote("greeter").greet("World"); // > "Hello, World!"
  const result = await solver.evaluate(1);

  // [object property access]
  log(`result: ${result} (precision: ${await solver.precision})`);

  // [iterators]
  const iterations = [];
  for await (const value of solver.iterate(result)) {
    iterations.push(value);
    if (iterations.length >= 10) break; // propagates the break to the caller
  }
  log(`iterations: [${iterations.slice(0, 3).join(", ")}, ..., ${iterations.slice(-3).join(", ")}]`);

  // [functional arguments]
  void peer.remote("timer").delay(() => {
    log(">> Hello! This is called from remote side, one second later.");
  }, 1000); // inline callbacks are released once the remote call is settled

  // [functional return values]
  /**
   * Returned function stubs are automatically "released" from remote peer once
   * they are garbage collected on the caller's side.
   *
   * In this example, `.within` returns a predicate function that could be used
   * just like any other. Note that it is reflected as asynchronous, and should
   * be used as such.
   */
  const predicate = await solver.within(0.1);
  const samples = [0.05, 0.1, 0.15, -0.08, -0.3, 0.4];
  const results = await Promise.all(samples.map(predicate));
  const filtered = samples.filter((_, indx) => results[indx]);
  log(samples, "->", filtered);

  /**
   * Closing the peer will release all resources and close the connection.
   *
   * However, this does not immediately cancel pending remote calls, but rather
   * waits for them to complete before settling. This is indefinite by default,
   * but can be bound by a configurable `drainTimeout` option.
   */
  await peer.close();
}

main().catch(console.error);
