import crypto from "node:crypto";
import type { NodeId } from "@/interface/base";

/** A hex string used to identify a node in the network. Unique per process/thread. */
export const localNodeId = crypto.randomBytes(4).toString("hex") as NodeId;
