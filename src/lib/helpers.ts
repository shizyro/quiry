import type { AnyPacket } from "../protocol/packets";

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

/**
 * Native types structured clone already reconstructs correctly on its own,
 * so we leave them untouched, never walked into or substituted.
 */
export function isOpaqueCloneable(v: unknown): boolean {
  return v instanceof Date || v instanceof RegExp || v instanceof ArrayBuffer || ArrayBuffer.isView(v);
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
 * This is how independent transforms (e.g. class-instance serialization,
 * callback substitution) plug into the same {@link rebuild} pass.
 */
export type StepTransformer = (node: object, ctx: WalkContext) => { value: unknown } | undefined;

export interface WalkContext {
  /** Recursively applies the same walk (and all combined steps) to a nested value. */
  walk(value: unknown): unknown;
  /**
   * Remembers `replacement` as the result for `original`, so that if the
   * same object is reached again elsewhere in the graph, the walk returns
   * this replacement instead of processing it twice. Call this *before*
   * recursing into `original`'s own data if that data can reference
   * `original` itself (self-reference), or *after* building `replacement`
   * otherwise.
   */
  cache(original: object, replacement: unknown): void;
}

/**
 * Walks and deep-clones a value, giving each transformer a chance to
 * intercept and rebuild a node before the default array/plain object
 * walk logic runs.
 *
 * Transformers are tried in order at every node (arrays, plain objects,
 * and any other object type); the first one to return a value other than
 * `undefined` wins and that value is used as the rebuilt node, and no
 * further transformers or default handling run for that node.
 */
export function rebuild<T>(value: T, ...transformers: StepTransformer[]): T {
  const seen = new WeakMap<object, unknown>();
  const ctx: WalkContext = {
    walk,
    cache: (original, replacement) => seen.set(original, replacement),
  };

  function walk(block: unknown): unknown {
    if (block === null || Object(block) !== block) return block;
    if (isOpaqueCloneable(block)) return block;

    const cached = seen.get(block as object);
    if (cached !== undefined) return cached;

    for (const step of transformers) {
      const result = step(block as object, ctx);
      if (result !== undefined) return result.value;
    }

    if (typeof block !== "object") return block; // e.g. a function no step handled

    if (Array.isArray(block)) {
      const result: unknown[] = new Array(block.length);
      seen.set(block, result);
      for (let i = 0; i < block.length; i++) result[i] = walk(block[i]);
      return result;
    }

    if (isPlainObject(block)) {
      const result: Record<string, unknown> = {};
      seen.set(block, result);
      for (const [key, val] of Object.entries(block)) result[key] = walk(val);
      return result;
    }

    return block;
  }

  return walk(value) as T;
}
