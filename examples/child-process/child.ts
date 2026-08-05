import * as Quiry from "~";
import { gzipSync, gunzipSync } from "node:zlib";

/**
 * A compression service, offloaded to its own OS process rather than a
 * worker thread; CPU-heavy zlib calls block whichever thread runs them,
 * and process isolation also means a crash here can't take the caller
 * down with it.
 */
class Compressor {
  gzip(input: string): NonSharedBuffer {
    return gzipSync(Buffer.from(input, "utf8"));
  }

  gunzip(input: Buffer): string {
    return gunzipSync(input).toString("utf8");
  }
}

Quiry.attach(new Quiry.ChildProcessTransport());
Quiry.expose("compressor", new Compressor());

Quiry.on("peer-disconnected", () => process.exit(0));

export type { Compressor };
