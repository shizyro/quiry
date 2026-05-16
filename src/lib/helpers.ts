import type { AnyPacket } from "../interface/packets";
import * as QuirySymbol from "../core/infra/symbol";

/**
 * Whether `value` can survive a structured-clone hop across a thread or
 * process boundary. Used as a fast pre-check before handing payloads.
 */
export function isSerializable(value: unknown, seen = new WeakSet<object>()): value is Serializable {
  if (value === null || value === undefined) return true;

  const t = typeof value;
  if (t === "string" || t === "number" || t === "boolean" || t === "bigint") return true;
  if (t !== "object") return false; // function, symbol

  // Atomic cloneable host types.
  if (value instanceof Date) return true;
  if (value instanceof RegExp) return true;
  if (value instanceof Error) return true;
  if (value instanceof ArrayBuffer) return true;
  if (typeof SharedArrayBuffer !== "undefined" && value instanceof SharedArrayBuffer) return true;
  if (ArrayBuffer.isView(value)) return true; // typed arrays, DataView, Node Buffer
  // `URL` are notably not accepted. Despite being part of the HTML spec's
  // cloneable set, Node's `structuredClone` doesn't recognize WHATWG URL
  // objects. Pass `url.href` instead.

  // Track membership on the *active recursion path* so a true
  // cycle is rejected, but a value shared by reference across
  // sibling branches still passes.
  if (seen.has(value as object)) return false;
  seen.add(value as object);
  try {
    if (value instanceof Map) {
      for (const [k, v] of value) {
        if (!isSerializable(k, seen) || !isSerializable(v, seen)) return false;
      }
      return true;
    }
    if (value instanceof Set) {
      for (const v of value) if (!isSerializable(v, seen)) return false;
      return true;
    }
    if (Array.isArray(value)) return value.every((v) => isSerializable(v, seen));

    // Plain objects only past this point.
    // Class instances with custom prototypes are rejected.
    const proto = Object.getPrototypeOf(value);
    if (proto !== Object.prototype && proto !== null) return false;
    return Object.values(value as Record<string, unknown>).every((v) => isSerializable(v, seen));
  } finally {
    seen.delete(value as object);
  }
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
    typeof value === "object" &&
    value !== null &&
    // fix: check for `next`, so arrays don't get caught.
    typeof (value as { next?: unknown }).next === "function" &&
    (typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === "function" ||
      typeof (value as { [Symbol.asyncIterator]?: unknown })[Symbol.asyncIterator] === "function")
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

export function fetchDescriptor(
  target: object,
  key: PropertyKey,
): [object, PropertyDescriptor] | [null, undefined] {
  let self: object | null = target;
  while (self) {
    const descriptor = Object.getOwnPropertyDescriptor(self, key);
    if (descriptor) return [self, descriptor];
    self = Object.getPrototypeOf(self);
  }
  return [null, undefined];
}

/**
 * Strips `[QuirySymbol.serialize]` aliases recursively.
 *
 * Cycles are preserved (not unwrapped) so the downstream serialization
 * check rejects them with INVALID_ARGUMENT instead of blowing the stack.
 */
export function unwrapSerialized<T = unknown>(value: T, seen?: WeakSet<object>): T {
  if (Object(value) !== value || value === null) return value;
  if (QuirySymbol.serialize in (value as object)) {
    return (value as unknown as { [QuirySymbol.serialize]: T })[QuirySymbol.serialize];
  }
  if (typeof value === "object") {
    seen ??= new WeakSet();
    if (seen.has(value as object)) return value;
    seen.add(value as object);

    if (Array.isArray(value)) return value.map((v) => unwrapSerialized(v, seen)) as T;
    const result: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value as object)) {
      result[key] = unwrapSerialized(val, seen);
    }
    return result as T;
  }
  return value;
}
