import * as Quiry from "~/index";
import type { ExampleRegistry } from "./index";

function log(...data: any[]) {
  console.log(`\u001b[2m${new Date().toISOString()}\u001b[22m`, ...data);
}

async function main() {
  const peer = Quiry.attach<ExampleRegistry>(new Quiry.WorkerThreadsTransport());
  void peer.remote("greeter").greet("World");

  const math = peer.remote("math");
  const result = await math.multiply(3, 4); // unary calls
  log(`3 x 4 = ${result}`);
  // object property access
  log(`PI (remote): ${await math.pi}`);

  const received = [];
  // async iterator streaming
  for await (const number of peer.remote("math").prime()) {
    received.push(number);
    if (number > 100) break;
  }
  log(`Stream results: [${received.slice(0, 3).join(", ")}, ..., ${received.slice(-3).join(", ")}]`);

  // support for functional arguments
  void peer.remote("timer").delay(() => {
    log("Hello, from the other side! One second later!");
  }, 1000); // inline callbacks are released when the remote call settles

  const events = peer.remote("events");
  {
    // long-lived callback - must be manually released with the `.release()` method,
    // or be automatically disposed out of scope with the `using` (TC39) keyword.
    const handle = peer.callback((query?: string) => {
      log(`Scoped callback invoked with query: ${query}`);
    });

    await events.on("foo", handle);
    log(`Event names: ${(await events.eventNames).join(", ")}`); // remote getters

    await new Promise((resolve) => setTimeout(resolve, 2000));
    await events.emit("foo", "You shall pass!"); // <-
  }

  const predicate = await peer.remote("math").threshold(50);
  const scores = [15, 42, 68, 91, 33];
  const results = await Promise.all(scores.map(predicate));
  const filtered = scores.filter((_, index) => results[index]);
  log(scores, "->", filtered);

  // ... also supports functions that are deeply nested in return values
  const file = await peer.remote("file").open[Quiry.control](AbortSignal.timeout(5000))("data.txt"); // controlled remote calls
  log("\n\t", await file.read(123));
  await file.close();
}

void main();
