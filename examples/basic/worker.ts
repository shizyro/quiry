import { QuiryClient } from "@/core/client";
import { WorkerThreadsTransport } from "@/core/transport/worker-threads";
import type { AppRegistry } from "./provider";

async function main() {
  const client = await new QuiryClient<AppRegistry>(new WorkerThreadsTransport()).open();

  await (async () => {
    void client.service("greeter").greet("World");

    const math = client.service("math");
    await math.add(1, 2).then((result) => console.log(`1 + 2 = ${result}`));
    const result = await math.multiply(3, 4);
    console.log(`3 x 4 = ${result}`);

    const received = [];
    for await (const number of client.service("math").count(0, 10)) {
      received.push(number);
      if (received.length > 5) break; // cancel the stream
    }
    console.log(`Stream Result: ${received.join(", ")}`);

    // support for functional arguments
    void client.service("timer").delay(() => {
      console.log("\n\tHello, from the other side! One second later!\n");
    }, 1000); // callbacks are automatically "released" on the remote side after invocation

    const events = client.service("events");
    {
      // long-lived callback - must be manually released with the `.release()` method,
      // or be automatically disposed out of scope with the `using` (TC39) keyword.
      await using handle = client.callback((query?: string) => {
        console.log(`\n\tScoped callback invoked with query: ${query}\n`);
      });

      events
        .on("test", handle)
        .catch((error: unknown) =>
          console.log(
            "Couldn't add callback to events:",
            error instanceof Error ? error.message : String(error),
          ),
        );

      await new Promise((resolve) => setTimeout(resolve, 2000)).then(() =>
        events.emit("test", "You shall pass!"),
      );
    }

    // ...
  })()
    .then(() => client.session.close())
    .catch(console.error);
}

void main();
