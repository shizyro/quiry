import { WireStatus } from "~/protocol/wire";

import { PeerConnection } from "~/internal";
import { openSessionPair, type SessionPair } from "./helpers/session-pair";

/**
 * Tests for the user-facing surface of the framework.
 */
describe("Peer service proxy", () => {
  let pair: SessionPair | null = null;
  let peer: PeerConnection | null = null;

  const setup = (
    impl: Record<string, unknown>,
    setMutator?: (prop: string, value: unknown) => void,
  ): PeerConnection => {
    pair = openSessionPair({
      producerInquiry: ({ property }) => {
        const value = impl[property];
        return {
          value,
          get: () => impl[property],
          set: (v: unknown) => {
            if (setMutator) setMutator(property, v);
            else impl[property] = v;
          },
          writable: !!setMutator || true,
        };
      },
    });
    peer = new PeerConnection("test", pair.consumer);
    return peer;
  };

  afterEach(async () => {
    if (peer) {
      await peer.close("test").catch(() => null);
      peer = null;
    }
    if (pair) {
      await pair.close(false).catch(() => null);
      pair = null;
    }
  });

  it("awaiting a property reads through GET", async () => {
    const svc = setup({ version: "1.2.3" }).service<{ version: string }>("svc");
    await expect(svc.version).resolves.toBe("1.2.3");
  });

  it("assigning to a property triggers an async SET on the peer", async () => {
    const writes: Array<[string, unknown]> = [];
    const svc = setup({ counter: 0 }, (prop, v) => writes.push([prop, v])).service<{ counter: number }>(
      "svc",
    );

    svc.counter = Promise.resolve(42);
    // Set is fire-and-forget at the syntactic level; the proxy awaits internally.
    await vi.waitFor(() => expect(writes).toEqual([["counter", 42]]), { interval: 10 });
  });

  it("calling a method as a function returns its result via the call path", async () => {
    const svc = setup({
      greet: (name: string) => `hello, ${name}`,
    }).service<{ greet: (n: string) => Promise<string> }>("svc");

    await expect(svc.greet("world")).resolves.toBe("hello, world");
  });

  it("iterating a method's return value reads through the stream path", async () => {
    const svc = setup({
      range: function* (start: number, end: number) {
        for (let i = start; i < end; i++) yield i;
      },
    }).service<{ range: (s: number, e: number) => Generator<number> }>("svc");

    const out: number[] = [];
    for await (const n of svc.range(0, 4)) out.push(n);
    expect(out).toEqual([0, 1, 2, 3]);
  });

  it("a handle that already committed to call cannot also be iterated", async () => {
    const svc = setup({ now: () => Date.now() }).service<{ now: () => Promise<number> }>("svc");

    const handle = svc.now();
    await handle; // commit to call mode

    // @ts-expect-error - expected to throw
    expect(() => handle[Symbol.asyncIterator]().next()).toThrow(/already been committed as a unary call/);
  });

  it("a handle that already committed to stream cannot also be awaited", async () => {
    const svc = setup({
      seq: function* () {
        yield 1;
      },
    }).service<{ seq: () => Generator<number> }>("svc");

    const handle = svc.seq();
    const it = handle[Symbol.asyncIterator]();
    await it.next();

    // The then-trap throws synchronously when the handle has already
    // been committed to streaming — the user observation is a thrown
    // error, not a rejected promise.

    // @ts-expect-error - expected to throw
    expect(() => handle.then((v) => v)).toThrow(/already been committed as a stream/);
  });

  it("callback() rejects non-functions", () => {
    const conn = setup({});
    expect(() => conn.callback(42 as unknown as Function)).toThrow(
      expect.objectContaining({ code: WireStatus.INVALID_ARGUMENT }),
    );
  });

  it("callback() refuses to double-wrap an already-bound handle", () => {
    const conn = setup({});
    const handle = conn.callback(() => 1);
    expect(() => conn.callback(handle as unknown as Function)).toThrow(
      expect.objectContaining({ code: WireStatus.INVALID_ARGUMENT }),
    );
  });
});
