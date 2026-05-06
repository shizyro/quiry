import type { AnyPacket } from "@/interface/packets";

/**
 * Whether `value` is safe for structured clone / typical IPC payloads (plain data only).
 * Typed arrays, `ArrayBuffer`, and ports are rejected here — use transfer lists for those.
 */
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

export function isPrimitive(value: unknown): value is Primitive {
  return (
    typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null
  );
}

export function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    "then" in value &&
    typeof (value as PromiseLike<unknown>).then === "function"
  );
}

export function isAnyIterableIterator(
  value: unknown,
): value is IterableIterator<unknown> | AsyncIterableIterator<unknown> {
  return (
    typeof value === "object" && value !== null && (Symbol.iterator in value || Symbol.asyncIterator in value)
  );
}

export function isPlainObject(value: unknown): value is object {
  if (value === null || typeof value !== "object") return false;
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** Shape check for session routing; intentionally loose vs full packet typing. */
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

/** @deprecated */
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
