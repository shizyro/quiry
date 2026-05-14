import { isSerializable } from "~/lib/helpers";

/**
 * `isSerializable` is the fast pre-check that gates payloads before they hit
 * the transport. Anything that passes must also pass `structuredClone`.
 */
describe("serialization safety check", () => {
  const cloneable = (value: unknown): boolean => {
    try {
      structuredClone(value);
      return true;
    } catch {
      return false;
    }
  };

  describe("accepts cloneable values", () => {
    const fixtures: ReadonlyArray<readonly [string, unknown]> = [
      ["null", null],
      ["undefined", undefined],
      ["empty string", ""],
      ["non-empty string", "hello"],
      ["zero", 0],
      ["negative number", -42],
      ["NaN", NaN],
      ["Infinity", Infinity],
      ["true", true],
      ["false", false],
      ["bigint zero", BigInt(0)], // no bigint literals; lost when target is before ES2020
      ["large bigint", BigInt(1_234_567_890_123_456)],
      ["empty array", []],
      ["mixed array", [1, "x", null, true, [2, 3]]],
      ["empty plain object", {}],
      ["null-prototype object", Object.create(null)],
      ["nested plain object", { a: 1, b: { c: [1, 2, "x"] } }],
      ["Date now", new Date()],
      ["Date epoch", new Date(0)],
      ["RegExp", /foo/gi],
      ["Error", new Error("boom")],
      ["TypeError", new TypeError("bad type")],
      ["Error with cause", new Error("outer", { cause: new Error("inner") })],
      ["empty Map", new Map()],
      [
        "Map of primitives",
        new Map<string, number>([
          ["a", 1],
          ["b", 2],
        ]),
      ],
      ["Map with object key/value", new Map<object, unknown>([[{ k: 1 }, [1, 2]]])],
      ["empty Set", new Set()],
      ["Set of primitives", new Set([1, 2, 3])],
      ["Set with objects", new Set([{ x: 1 }, { y: 2 }])],
      ["ArrayBuffer", new ArrayBuffer(16)],
      ["Uint8Array", new Uint8Array([1, 2, 3])],
      ["Float64Array", new Float64Array([1.5, 2.5])],
      ["DataView", new DataView(new ArrayBuffer(8))],
      ["Node Buffer", Buffer.from("hi")],
    ];

    it.each(fixtures)("%s", (_, value) => {
      expect(isSerializable(value)).toBe(true);
    });

    it("never accepts a value structuredClone would reject (no false positives)", () => {
      for (const [label, value] of fixtures) {
        // `isSerializable` is allowed to be stricter, but anything it accepts
        // here must round-trip through structured clone without throwing.
        expect(cloneable(value), `structuredClone rejected fixture "${label}"`).toBe(true);
      }
    });
  });

  describe("rejects non-cloneable values", () => {
    const fixtures: ReadonlyArray<readonly [string, unknown]> = [
      ["arrow function", () => 1],
      ["named function", function f() {}],
      ["Symbol", Symbol("x")],
      ["WeakMap", new WeakMap()],
      ["WeakSet", new WeakSet()],
      ["Promise", Promise.resolve(1)],
      ["object with function value", { fn: () => 1 }],
      ["array containing a function", [() => 1]],
      ["function nested deep in object", { a: { b: { fn: () => 1 } } }],
      ["function in nested array", [[[[() => 1]]]]],
      ["Map with function value", new Map<string, unknown>([["k", () => 1]])],
      ["Map with symbol key", new Map<unknown, number>([[Symbol("k"), 1]])],
      ["Set with Symbol", new Set([Symbol("x")])],
      ["Set with function", new Set([() => 1])],
      [
        "arbitrary class instance",
        new (class {
          x = 1;
        })(),
      ],
      // URL is part of the HTML spec's cloneable set but Node's
      // structuredClone doesn't recognize it. Documented carve-out.
      ["URL (Node gap)", new URL("https://example.com/path?q=1")],
    ];

    it.each(fixtures)("%s", (_, value) => {
      expect(isSerializable(value)).toBe(false);
    });
  });

  describe("cycle handling — stricter than structured clone by design", () => {
    it("rejects a self-referential plain object", () => {
      const obj: Record<string, unknown> = { a: 1 };
      obj.self = obj;
      expect(isSerializable(obj)).toBe(false);
      // structuredClone *can* handle this; we reject so the rest of the
      // pipeline doesn't have to defend against cyclic graphs.
      expect(cloneable(obj)).toBe(true);
    });

    it("rejects a self-referential array", () => {
      const arr: unknown[] = [1];
      arr.push(arr);
      expect(isSerializable(arr)).toBe(false);
    });

    it("rejects a self-referential Map", () => {
      const m = new Map<string, unknown>();
      m.set("self", m);
      expect(isSerializable(m)).toBe(false);
    });

    it("rejects a self-referential Set", () => {
      const s = new Set<unknown>();
      s.add(s);
      expect(isSerializable(s)).toBe(false);
    });

    it("accepts a value shared by reference across sibling branches", () => {
      // Not a cycle — `shared` doesn't reference itself. The old guard
      // misread this as a cycle the second time it was visited.
      const shared = { a: 1 };
      expect(isSerializable([shared, shared])).toBe(true);
      expect(isSerializable({ x: shared, y: shared })).toBe(true);
      expect(
        isSerializable(
          new Map<string, object>([
            ["a", shared],
            ["b", shared],
          ]),
        ),
      ).toBe(true);
    });
  });
});
