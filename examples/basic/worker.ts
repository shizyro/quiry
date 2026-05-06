import Quiry, { WorkerThreadsTransport } from "@/";
import type { ExampleRegistry } from "./index";

function log(message: string) {
  console.log(`\u001b[2m${new Date().toISOString()}\u001b[22m ${message}`);
}

async function main() {
  const peer = Quiry.attach<ExampleRegistry>(new WorkerThreadsTransport());
  void peer.service("greeter").greet("World");

  const math = peer.service("math");
  const result = await math.multiply(3, 4); // unary calls
  log(`3 x 4 = ${result}`);
  // object property access
  log(`PI (remote): ${await math.pi}`);

  const received = [];
  // async iterator streaming
  for await (const number of peer.service("math").prime()) {
    received.push(number);
    if (number > 100) break;
  }
  log(`Stream results: [${received.splice(0, 3).join(", ")}, ..., ${received.splice(-3).join(", ")}]`);

  // support for functional arguments
  void peer.service("timer").delay(() => {
    log("Hello, from the other side! One second later!");
  }, 1000); // callbacks are automatically "released" on the remote side after invocation

  const events = peer.service("events");
  {
    // long-lived callback - must be manually released with the `.release()` method,
    // or be automatically disposed out of scope with the `using` (TC39) keyword.
    await using handle = peer.callback((query?: string) => {
      log(`Scoped callback invoked with query: ${query}`);
    });

    events.on("test", handle);
    log(`Event names: ${await events.eventNames}`); // remote getters

    await new Promise((resolve) => setTimeout(resolve, 2000)).then(() =>
      events.emit("test", "You shall pass!"),
    );
  }
}

void main();
