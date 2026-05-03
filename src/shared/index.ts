import { randomBytes } from "node:crypto";
import type { NodeId } from "@/interface/base";

/** A hex string used to identify a node in the network. Unique per process/thread. */
export const localNodeId = randomBytes(4).toString("hex") as NodeId;
