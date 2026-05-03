import { isMainThread } from "node:worker_threads";
import { join } from "node:path";

async function bootstrap() {
  const broker = (await import("./provider")).default;
  process.on("SIGINT", async () => {
    console.error("SIGINT received; shutting down...");
    await broker.shutdown();
    process.exit(0);
  });

  broker.on("peer-disconnected", (handle) => {
    console.log(`\u001b[105m Worker ${handle.id} disconnected \u001b[49m`);
  });

  await broker.spawn(join(__dirname, "worker.ts"));
}

if (isMainThread) {
  console.clear();
  void bootstrap();
}
