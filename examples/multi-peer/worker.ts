import * as Quiry from "~";
import { createHash } from "node:crypto";

class Compute {
  /** Deliberately a little slow, so dispatching across a pool is visibly worth it. */
  hash(input: string, rounds: number = 200_000): string {
    let digest = input;
    for (let i = 0; i < rounds; i++) {
      digest = createHash("sha256").update(digest).digest("hex");
    }
    return digest;
  }
}

Quiry.attach(new Quiry.WorkerThreadsTransport());
Quiry.expose("compute", new Compute());

Quiry.on("peer-disconnected", () => process.exit(0));

export type { Compute };
