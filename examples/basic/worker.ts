import { QuiryClient } from "@/core/client";
import { WorkerThreadsTransport } from "@/core/transport/worker-threads";
import type { AppRegistry } from "./provider";

async function main() {
  const client = await new QuiryClient<AppRegistry>(new WorkerThreadsTransport()).open();

  await (async () => {
    void client.service("greeter").greet("World");

    const math = client.service("math");
    await math.add(1, 2).then((result) => console.log(`1 + 2 = ${result}`));
    await math.multiply(3, 4).then((result) => console.log(`3 x 4 = ${result}`));

    // ...
  })()
    .then(() => client.session.close())
    .catch(console.error);
}

void main();
