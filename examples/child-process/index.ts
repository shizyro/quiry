import * as Quiry from "~";
import { log, join } from "../shared";

import type { Compressor } from "./child";

/**
 * Child processes' default IPC serialization mode mangles buffer/typed-array
 * payloads into plain objects; it never round-trips through V8's structured-clone
 * machinery the way worker threads do.
 *
 * Setting serialization to "advanced" switches the underlying channel to that
 * richer format, which is required in this example since we are exchanging real
 * buffer instances.
 */

async function main() {
  const peer = Quiry.fork(join(import.meta.dirname, "child.ts"), { serialization: "advanced" });

  const compressor = peer.remote<Compressor>("compressor");
  const original = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ".repeat(20);

  const compressed = await compressor.gzip(original);
  log(`compressed ${original.length} chars into ${compressed.length} bytes`);

  const restored = await compressor.gunzip(compressed);
  log(`round-trip successful: ${restored === original ? "OK" : "FAIL"}`);

  await peer.close();
}

main().catch(console.error);
