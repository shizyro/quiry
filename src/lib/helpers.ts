import type { AnyPacket } from "@/interface/packets";
import type { Serializable } from "node:child_process";

export function isSerializable(value: unknown, seen = new WeakSet<object>()): value is Serializable {
  if (value === null || value === undefined) return true;
  const t = typeof value;
  if (t === "string" || t === "number" || t === "boolean" || t === "bigint") return true;
  if (t === "function" || t === "symbol") return false;
  if (t === "object") {
    if (seen.has(value as object)) return false;
    seen.add(value as object);
    if (Array.isArray(value)) return value.every((v) => isSerializable(v, seen));
    // Accept plain objects only; typed arrays / ArrayBuffer / MessagePort
    // are transferable but not typically useful in error detail.
    const proto = Object.getPrototypeOf(value);
    if (proto !== Object.prototype && proto !== null) return false;
    return Object.values(value as Record<string, unknown>).every((v) => isSerializable(v, seen));
  }
  return false;
}

export function isPlainObject(value: unknown): value is object {
  if (value === null || typeof value !== "object") return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

export function isWirePacket(value: unknown): value is AnyPacket {
  if (typeof value !== "object" || value === null) return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.id === "string" &&
    typeof v.kind === "string" &&
    typeof v.timestamp === "number" &&
    "payload" in v
  );
}

export function getMemoryUsage() {
  const usage = process.memoryUsage();
  return {
    heapUsed: usage.heapUsed,
    heapTotal: usage.heapTotal,
    external: usage.external,
    rss: usage.rss,
  };
}

/** A utility function that clips a string to a given length. Used for logging long IDs. */
export function clip(text: string, length: number = 8): string {
  return text.slice(0, length) + (text.length > length ? `[:${text.length - length}]` : "");
}
