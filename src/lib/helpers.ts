import type { AnyPacket } from "../interface/packets";

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

/**
 * Recursively walks a packet to find array buffers and message ports that
 * should be transferred (zero-copy) rather than cloned.
 */
export function collectTransferables(value: unknown, seen = new Set<object>()): Transferable[] {
  if (value === null || typeof value !== "object") return [];
  if (seen.has(value as object)) return [];
  seen.add(value as object);

  if (value instanceof ArrayBuffer) return [value];
  if (value instanceof MessagePort) return [value];

  // Typed arrays and their underlying array buffers
  if (ArrayBuffer.isView(value)) return value.buffer instanceof ArrayBuffer ? [value.buffer] : [];

  return Object.values(value).flatMap((item) => collectTransferables(item, seen));
}
