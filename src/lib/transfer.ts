import type { StepTransformer } from "./helpers";
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

export const transform: StepTransformer = (block, { walk, cache }) => {
  if (QuirySymbol.override in block) {
    return { value: (block as { [QuirySymbol.override]: unknown })[QuirySymbol.override] };
  }
  if (typeof block !== "object") return undefined; // e.g. a function — not this step's concern

  const entry = registryByCtor.get((block as { constructor: Function }).constructor);
  if (!entry) return undefined;

  const placeholder = { [MARSHAL_MARKER]: entry.tag, data: undefined } as MarshalEnvelope;
  cache(block, placeholder); // register identity BEFORE recursing (enables self-reference)
  placeholder.data = walk(entry.serialize(block));
  return { value: placeholder };
};

export const restore: StepTransformer = (block, { walk, cache }) => {
  if (!(MARSHAL_MARKER in block)) return undefined;
  const entry = registryById.get(block[MARSHAL_MARKER] as string);
  if (!entry) {
    throw new TypeError(
      `Unknown serializer id "${block[MARSHAL_MARKER]}". Is the class imported (not type-only) and registered on this side?`,
    );
  }
  const instance = entry.deserialize(walk((block as MarshalEnvelope).data));
  cache(block, instance); // set AFTER construction
  return { value: instance };
};

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
