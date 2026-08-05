import * as Quiry from "~";
import { log, join } from "../shared";

import { Worker } from "node:worker_threads";
import type { Compute } from "./worker";

interface PoolMember {
  readonly worker: Worker;
  readonly peer: Quiry.PeerConnection;
}

const POOL_SIZE = 3;

async function main() {
  Quiry.on("peer-connected", (handle) => log(`[${handle.identifier}] connected`));
  Quiry.on("peer-disconnected", (handle, reason) =>
    log(`[${handle.identifier}] disconnected (${reason ?? "no reason"})`),
  );

  const pool: PoolMember[] = Array.from({ length: POOL_SIZE }, (_, indx): PoolMember => {
    const worker = new Worker(join(import.meta.dirname, "worker.ts"));
    return { worker, peer: Quiry.wrap(worker, { identifier: `worker-${indx}` }) };
  });

  // Generate a list of jobs to dispatch across the pool.
  const jobs = Array.from({ length: POOL_SIZE * 3 }, (_) => Math.random().toString(36).substring(2, 11));

  const start = Date.now();
  const results = await Promise.all(
    jobs.map(async (job, i) => {
      const { peer } = pool[i % pool.length]!; // (round-robin dispatch across the pool)
      return peer
        .remote<Compute>("compute")
        .hash(job)
        .then((digest) => ({ job, digest }));
    }),
  );

  log(`hashed ${results.length} jobs across ${pool.length} peers in ${Date.now() - start}ms`);
  for (const { job, digest } of results) {
    log(`${job} -> ${digest.slice(0, 16)}[:${digest.length - 16}]`);
  }

  /**
   * Here, I'm demonstrating different methods of shutting down the peers. You can
   * use any of the following, but in most cases, the first two are preferred.
   */
  const [first, second, third] = pool;

  // Graceful shutdown; runs the drain protocol and removes the peer from the module registry.
  await first!.peer.close();
  await Quiry.detach(second!.peer.identifier); // (does the same as `peer.close()`)

  // Explicit hard kill; skips the drain protocol and terminates the underlying worker thread.
  await third!.worker.terminate();
}

main().catch(console.error);
