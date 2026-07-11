import { isPlainObject } from "./helpers";
import * as QuirySymbol from "../core/symbols";

/**
 * Plain-data marker for a marshalled value. Used to differentiate
 * between regular objects and serialized ones crossing the wire.
 */
const MARSHAL_MARKER = "__quiry.marshal" as const;

interface MarshalEnvelope<T = unknown> {
  readonly [MARSHAL_MARKER]: string;
  data: T;
}

function isSerializedEnvelope<T>(value: unknown): value is MarshalEnvelope<T> {
  return typeof value === "object" && value !== null && MARSHAL_MARKER in value;
}

/**
 * Native types structured clone already reconstructs correctly on its own,
 * so we leave them untouched, never walked into or substituted.
 */
function isOpaqueCloneable(v: unknown): boolean {
  return v instanceof Date || v instanceof RegExp || v instanceof ArrayBuffer || ArrayBuffer.isView(v);
}

export interface Serializer<TInstance = unknown, TWire = unknown> {
  readonly tag?: string;
  serialize(value: TInstance): TWire;
  deserialize(value: TWire): TInstance;
}

function isValidSerializer(v: unknown): v is Serializer<unknown, unknown> {
  return (
    typeof v === "object" &&
    v !== null &&
    typeof (v as any).serialize === "function" &&
    typeof (v as any).deserialize === "function"
  );
}

interface RegistryEntry extends Required<Serializer<unknown, unknown>> {
  ctor: Function;
}

const registryById = new Map<string, RegistryEntry>();
const registryByCtor = new Map<Function, RegistryEntry>();

export function marshal<T>(value: T): T {
  const seen = new WeakMap<object, unknown>();
  const walk = (block: unknown): unknown => {
    if (block === null || Object(block) !== block) return block;
    if (isOpaqueCloneable(block)) return block; // structured clone preserves identity for these itself

    if (QuirySymbol.override in (block as object)) {
      return (block as unknown as { [QuirySymbol.override]: T })[QuirySymbol.override];
    }
    if (typeof block !== "object") return block;

    const cached = seen.get(block);
    if (cached !== undefined) return cached; // shared refs & cycles through containers we rebuild

    const entry = registryByCtor.get(block.constructor);
    if (entry) {
      const placeholder = { [MARSHAL_MARKER]: entry.tag, data: undefined } as MarshalEnvelope;
      seen.set(block, placeholder); // register identity BEFORE recursing — enables self-reference
      placeholder.data = walk(entry.serialize(block));
      return placeholder;
    }

    if (Array.isArray(block)) {
      const result: unknown[] = new Array(block.length);
      seen.set(block as object, result);
      for (let i = 0; i < block.length; i++) result[i] = walk(block[i]);
      return result;
    }

    if (isPlainObject(block)) {
      const result: Record<string, unknown> = {};
      seen.set(block, result);
      for (const [key, val] of Object.entries(block)) {
        result[key] = walk(val);
      }
      return result;
    }

    // Anything else: an instance with no registered serializer.
    return block;
  };

  return walk(value) as T;
}

export function restore<T>(value: T): T {
  const seen = new WeakMap<object, unknown>();
  const walk = (block: unknown): unknown => {
    if (block === null || typeof block !== "object") return block;
    if (isOpaqueCloneable(block)) return block;

    const cached = seen.get(block);
    if (cached !== undefined) return cached;

    if (isSerializedEnvelope(block)) {
      const entry = registryById.get(block[MARSHAL_MARKER]);
      if (!entry) {
        throw new TypeError(
          `Unknown serializer id "${block[MARSHAL_MARKER]}". Is the class imported (not type-only) and registered on this side?`,
        );
      }
      const instance = entry.deserialize(walk(block.data));
      seen.set(block, instance); // set AFTER construction
      return instance;
    }

    if (Array.isArray(block)) {
      const result: unknown[] = new Array(block.length);
      seen.set(block, result);
      for (let i = 0; i < block.length; i++) result[i] = walk(block[i]);
      return result;
    }

    if (isPlainObject(block)) {
      const result: Record<string, unknown> = {};
      seen.set(block, result);
      for (const [k, v] of Object.entries(block)) {
        result[k] = walk(v);
      }
      return result;
    }

    return block; // host object or already-native type we don't special-case
  };

  return walk(value) as T;
}

export function registerSerializer(ctor: Function, moduleUrl: string): void {
  const config = (ctor as any)[QuirySymbol.serialize];
  if (!isValidSerializer(config)) {
    throw new TypeError(`${ctor.name}: malformed or missing Quiry.serialize`);
  }

  const tag = config.tag ?? `${moduleUrl}:${ctor.name}`;
  const existingById = registryById.get(tag);
  if (existingById && existingById.ctor !== ctor) {
    throw new TypeError(`Serializer id "${tag}" already registered by a different class`);
  }

  const entry: RegistryEntry = {
    tag,
    ctor,
    serialize: config.serialize,
    deserialize: config.deserialize,
  };
  registryById.set(tag, entry);
  registryByCtor.set(ctor, entry);
}
