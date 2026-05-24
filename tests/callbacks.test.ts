import { WireStatus } from "~/interface/protocol";
import { openSessionPair, type SessionPair } from "./helpers/session-pair";

import type { Callback } from "~/core/session";
import type { Remote } from "~/interface/transformers";

import * as QuirySymbol from "~/core/symbol";

import { runGCPressure } from "./helpers/garbage-collection";
import type { RemoteCallback } from "~/core/infra/channel/callback-bridge";

/**
 * Tests for the Session's support of callback functions as
 * arguments in request payloads.
 */
describe("Session callbacks", () => {
  let pair: SessionPair | null = null;

  afterEach(async () => {
    if (pair) {
      await pair.close().catch(() => null);
      pair = null;
    }
  });

  it("a functional argument is invoked remotely with the right args, and its return value reaches the producer", async () => {
    let observed: unknown = null;
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (cb: (x: number) => Promise<number>) => {
          observed = await cb(7);
          return "ok";
        },
      }),
    });

    const local = vi.fn((x: number) => x * 2);
    await expect(pair.consumer.request("svc", "_", [local])).resolves.toBe("ok");
    expect(local).toHaveBeenCalledWith(7);
    expect(observed).toBe(14);
  });

  it("substitution walks into objects and arrays nested in arguments", async () => {
    const ticks: number[] = [];
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (config: { listeners: Array<{ on: (n: number) => Promise<void> }> }) => {
          for (const l of config.listeners) await l.on(1);
          for (const l of config.listeners) await l.on(2);
          return null;
        },
      }),
    });

    await pair.consumer.request("svc", "_", [
      { listeners: [{ on: (n: number) => ticks.push(n) }, { on: (n: number) => ticks.push(n * 10) }] },
    ]);

    expect(ticks).toEqual([1, 10, 2, 20]);
  });

  it("a session-scoped callback released locally before invocation rejects", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (cb: () => Promise<unknown>) => {
          // Yield long enough for the consumer's release() below to ride
          // the wire before the producer dispatches INVOKE.
          await new Promise((r) => setTimeout(r, 20));
          return cb();
        },
      }),
    });

    const cb = pair.consumer.proxy(() => "should-not-fire");
    const promise = pair.consumer.request("svc", "_", [cb]);
    cb[QuirySymbol.release]();

    await expect(promise).rejects.toMatchObject({ code: WireStatus.NOT_FOUND });
  });

  it("a session-scoped proxy is reusable across multiple requests", async () => {
    const fn = vi.fn((x: number) => x + 100);
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (cb: (x: number) => Promise<number>, x: number) => cb(x),
      }),
    });

    const cb = pair.consumer.proxy(fn);
    await expect(pair.consumer.request("svc", "_", [cb, 1])).resolves.toBe(101);
    await expect(pair.consumer.request("svc", "_", [cb, 2])).resolves.toBe(102);
    expect(fn).toHaveBeenCalledTimes(2);

    cb[QuirySymbol.release]();
  });

  it("producer waits for in-flight callback invocations to settle before sending RELEASE", async () => {
    // Without this guarantee, the consumer could drop the local callback
    // mid-execution because RELEASE arrived while the callback was still running.
    let resolveCallback: (() => void) | null = null;
    let invocationFinished = false;

    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (cb: () => Promise<void>) => {
          // Fire-and-forget: producer returns before the callback settles.
          void cb().finally(() => {
            invocationFinished = true;
          });
          // Yield once so the INVOKE packet hits the wire before the
          // producer's `finally` runs releaseRemoteSubs.
          await new Promise((r) => setTimeout(r, 5));
          return "ok";
        },
      }),
    });

    await pair.consumer.request("svc", "_", [
      () =>
        new Promise<void>((resolve) => {
          resolveCallback = resolve;
        }),
    ]);

    // Request has resolved on the consumer side, but the producer is
    // parked waiting for the callback to settle. RELEASE has not been
    // sent yet, so the consumer's callback is still registered.
    // await new Promise((r) => setTimeout(r, 25));
    expect(invocationFinished).toBe(false);

    resolveCallback!();
    await vi.waitFor(() => expect(invocationFinished).toBe(true));
  });

  it("call-scoped callbacks are reaped on both sides after the owning request settles", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (cb: () => Promise<unknown>) => cb(),
      }),
    });

    await pair.consumer.request("svc", "_", [() => "x"]);

    // Both ends should converge to zero: consumer drops the registry
    // entry on RELEASE; producer drops the remote stub in its `finally`.
    await vi.waitFor(() => {
      expect(pair!.consumer.status.callbacks).toBe(0);
      expect(pair!.producer.status.stubs).toBe(0);
    });
  });

  it("session-scoped stubs survive RELEASE round-trips for unrelated local callbacks", async () => {
    const sessionFn = vi.fn(() => "session");
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (session: () => Promise<string>, local: () => Promise<string>) => {
          return await Promise.all([session(), local()]);
        },
      }),
    });

    const listenerFn = vi.fn();
    pair.consumer.diagnostic.once("callback:release", ({ id }) => listenerFn(id));

    const cb = pair.consumer.proxy(sessionFn);
    const result = await pair.consumer.request("svc", "_", [cb, () => "local"]);
    expect(result).toEqual(["session", "local"]);

    // Wait for the RELEASE for the SESSION callback; the CALL one must remain.
    await vi.waitFor(() => expect(pair!.consumer.status.callbacks).toBe(1));
    expect(listenerFn).toHaveBeenCalledTimes(1);
    expect(listenerFn).not.toHaveBeenCalledWith(cb[QuirySymbol.identifier]);
  });

  it("a callback that throws on the consumer side rejects the proxy with the error", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (cb: () => Promise<unknown>) => {
          return await cb();
        },
      }),
    });

    const promise = pair.consumer.request("svc", "_", [
      () => {
        throw new Error("nope");
      },
    ]);
    await expect(promise).rejects.toMatchObject({ message: "nope" });
  });

  it("inquiries that return a function are passed as callback proxies", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async () => () => "ok",
      }),
    });

    const func = (await pair.consumer.request("svc", "_", [])) as RemoteCallback;
    expect(typeof func).toBe("function");
    expect(func[QuirySymbol.identifier]).toBeDefined();
    expect(await func()).toBe("ok");

    func[QuirySymbol.release]();
  });

  it("object returns are walked through and their functions are substituted as callback proxies", async () => {
    const fn = vi.fn(() => ({ first: () => 1, second: () => 2, deep: { third: () => 3 } }));
    pair = openSessionPair({
      producerInquiry: () => ({
        value: () => fn(),
      }),
    });

    const result = (await pair.consumer.request("svc", "_", [])) as Remote<ReturnType<typeof fn>>;
    expect(typeof result).toBe("object");
    expect(typeof result.first).toBe("function");
    expect(typeof result.deep.third).toBe("function");

    expect(await result.first()).toBe(1);
    expect(await result.second()).toBe(2);
    expect(await result.deep.third()).toBe(3);

    // Release the proxies on the producer side; this is were they are defined.
    pair.producer.callbacks.releaseSessionCallbacks();
    await vi.waitFor(() => expect(pair!.producer.status.callbacks).toBe(0), { timeout: 100, interval: 10 });
  });

  describe("automatic callback cleanup", () => {
    const handleGC = () => {
      if (typeof globalThis.gc === "function") {
        return [vi.fn(), () => globalThis.gc!()];
      }

      const controller = new AbortController();
      return [
        vi.fn(() => controller.abort()),
        () => {
          // Stimulate GC pressure to trigger the release.
          runGCPressure({ signal: controller.signal });
        },
      ];
    };

    it("a local proxy is released through TC39 explicit resource management", async () => {
      pair = openSessionPair({
        producerInquiry: () => ({
          value: async (cb: () => Promise<string>) => cb(),
        }),
      });

      const listenerFn = vi.fn();
      pair.consumer.diagnostic.once("callback:release", listenerFn);

      {
        using handle = pair.consumer.proxy(() => "ok");
        await expect(pair.consumer.request("svc", "_", [handle])).resolves.toBe("ok");
      }

      // A tight timeout to avoid flakiness in CI.
      await vi.waitFor(() => expect(pair!.consumer.status.callbacks).toBe(0), { timeout: 100, interval: 10 });
      expect(listenerFn).toHaveBeenCalledWith(expect.objectContaining({ reason: "explicit" }));
    });

    it("a local proxy is unregistered automatically when reclaimed", async () => {
      pair = openSessionPair({
        producerInquiry: () => ({
          value: async (cb: () => Promise<string>) => cb(),
        }),
      });

      // Defined on consumer side.
      let handle = pair.consumer.proxy(() => "ok") as RemoteCallback | null;
      const cbId = String(handle![QuirySymbol.identifier]);

      await expect(pair.consumer.request("svc", "_", [handle])).resolves.toBe("ok");
      handle = null; // Force the handle to be garbage collected.

      const [followUp, triggerGC] = handleGC();

      pair.consumer.diagnostic.once("callback:release", followUp);
      triggerGC();

      await vi.waitFor(() => expect(pair!.consumer.status.callbacks).toBe(0), {
        timeout: 5000,
        interval: 10,
      });
      expect(followUp).toHaveBeenCalledTimes(1); // This is in case of double release; should be only once.
      expect(followUp).toHaveBeenCalledWith({ id: cbId, reason: "gc" });
    });

    it("a remote proxy is released automatically when collected on the consumer side", async () => {
      pair = openSessionPair({
        producerInquiry: () => ({
          value: () => () => "ok",
        }),
      });

      let handle = (await pair.consumer.request("svc", "_", [])) as Callback | null;
      const cbId = String(handle![QuirySymbol.identifier]);

      expect(await handle!()).toBe("ok");
      handle = null; // Force the handle to be garbage collected.

      const [followUp, triggerGC] = handleGC();

      // Check producer side for the release.
      pair.producer.diagnostic.once("callback:release", followUp);
      triggerGC();

      await vi.waitFor(() => expect(pair!.producer.status.callbacks).toBe(0), {
        timeout: 5000,
        interval: 10,
      });
      expect(followUp).toHaveBeenCalledTimes(1);
      expect(followUp).toHaveBeenCalledWith({ id: cbId, reason: "remote-gc" });
    });
  });
});
