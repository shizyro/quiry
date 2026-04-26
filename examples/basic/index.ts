import { isMainThread } from "node:worker_threads";
import { join } from "node:path";

async function bootstrap() {
  const broker = (await import("./provider")).default;
  process.on("SIGINT", async () => {
    console.error("SIGINT received; shutting down...");
    await broker.shutdown();
    process.exit(0);
  });

  await broker.spawn(join(__dirname, "worker.ts"));
}

if (isMainThread) {
  console.clear();
  void bootstrap();
}
