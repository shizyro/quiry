import { WireStatus } from "~/interface/protocol";
import { openSessionPair, type SessionPair } from "./helpers/session-pair";

import * as QuirySymbol from "~/core/infra/symbol";

/**
 * Tests for the Session's support of callback functions as arguments in request payloads.
 */
describe("Session callbacks", () => {
  let pair: SessionPair | null = null;

  afterEach(async () => {
    if (pair) {
      await pair.close().catch(() => null);
      pair = null;
    }
  });

  describe("local (request-scoped) callbacks", () => {
    it("a function passed in args is invoked from the producer's inquiry and returns its value", async () => {
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
      await expect(pair.consumer.request("svc", "invoke", [local])).resolves.toBe("ok");

      expect(local).toHaveBeenCalledTimes(1);
      expect(local).toHaveBeenCalledWith(7);
      expect(observed).toBe(14);
    });

    it("a callback nested one level inside a plain-object arg is invoked too", async () => {
      const ticks: number[] = [];
      pair = openSessionPair({
        producerInquiry: () => ({
          value: async (config: { name: string; tick: (n: number) => Promise<void> }) => {
            expect(config.name).toBe("alpha");
            expect(config.tick).toBeTypeOf("function");
            await config.tick(1);
            await config.tick(2);
            return null;
          },
        }),
      });

      await pair.consumer.request("svc", "tick", [
        {
          name: "alpha",
          tick: (n: number) => {
            ticks.push(n);
          },
        },
      ]);

      expect(ticks).toEqual([1, 2]);
    });

    it("multiple callbacks in the same request resolve independently with the right args", async () => {
      const a = vi.fn((x: number) => x + 1);
      const b = vi.fn((x: number) => x * 10);
      pair = openSessionPair({
        producerInquiry: () => ({
          value: async (
            cbA: (x: number) => Promise<number>,
            cbB: (x: number) => Promise<number>,
            num: number,
          ) => {
            return await Promise.all([cbA(num), cbB(num)]);
          },
        }),
      });

      await expect(pair.consumer.request("svc", "invoke", [a, b, 5])).resolves.toEqual([6, 50]);
      expect(a).toHaveBeenCalledWith(5);
      expect(b).toHaveBeenCalledWith(5);
    });

    it("substitutes are tracked under the request's correlation id", async () => {
      pair = openSessionPair({
        producerInquiry: () => ({
          value: () => new Promise<never>(() => {}), // hangs forever
        }),
      });

      const fn = (): number => 1;
      const promise = pair.consumer.request("svc", "never", [fn], {
        timeout: 60_000,
        retry: { maxAttempts: 0 },
      });

      // The substitute call happens synchronously inside `request()`,
      // so the registry already shows the CALL entry before the
      // promise has had a chance to roundtrip on the wire.
      expect(pair.consumer.status.callbacks).toBe(1);

      // Force-close to abort the hanging request — the CALL callback
      // is still in the registry because no RELEASE has been sent.
      await pair.consumer.close("explicit", false);
      await expect(promise).rejects.toMatchObject({ code: WireStatus.ABORTED });

      // teardown wipes the whole registry.
      expect(pair.consumer.status.callbacks).toBe(0);

      // Skip the graceful close on the orphaned producer — its peer
      // is gone, the DRAIN ACK will never arrive.
      void pair.close(false);
    });

    it("does not leave the producer-side stub map populated after the request settles", async () => {
      pair = openSessionPair({
        producerInquiry: () => ({
          value: async (cb: () => Promise<void>) => {
            await cb();
            return null;
          },
        }),
      });

      await pair.consumer.request("svc", "_", [() => {}]);
      // The producer tracks remote stubs for the duration of the
      // inbound request and clears them in the `finally` block before
      // sending RELEASE. Wait for that side to settle.
      await vi.waitFor(() => expect(pair!.producer.status.stubs).toBe(0));
    });

    it("waits for in-flight callback invocations to settle before sending RELEASE", async () => {
      // The producer kicks off a callback but doesn't await it before
      // returning. `releaseRemoteSubs` must drain the pending invocation
      // before letting RELEASE go out, otherwise the consumer would
      // drop the callback while it's still running and the next
      // invocation would race the release.
      let resolveCallback: (() => void) | null = null;
      let invocationFinished = false;

      pair = openSessionPair({
        producerInquiry: () => ({
          value: async (cb: () => Promise<void>) => {
            // Fire the callback without awaiting; the consumer will
            // not finish executing it until the test resolves
            // `resolveCallback`.
            void cb().finally(() => {
              invocationFinished = true;
            });
            // Yield so the INVOKE packet hits the wire before we
            // return — otherwise the producer's `finally` runs before
            // the proxy has even registered the pending invocation
            // and `drainInflightInvocations` sees nothing to wait on.
            await new Promise((r) => setTimeout(r, 5));
            return "done";
          },
        }),
      });

      await pair.consumer.request("svc", "finish", [
        () =>
          new Promise<void>((resolve) => {
            resolveCallback = resolve;
          }),
      ]);

      // The request itself has resolved on the consumer side, but the
      // producer is still parked in `drainInflightInvocations` waiting
      // for the in-flight callback to settle. Give a beat to verify
      // RELEASE hasn't fired yet.
      await new Promise((r) => setTimeout(r, 25));
      expect(pair.consumer.status.callbacks).toBe(1);
      expect(invocationFinished).toBe(false);

      // Unblock the callback — drain completes, RELEASE goes out.
      resolveCallback!();
      await vi.waitFor(() => expect(invocationFinished).toBe(true), { timeout: 1000 });
      expect(pair.consumer.status.callbacks).toBe(0);
    });
  });

  describe("stack-scoped callbacks", () => {
    it("a bound stub is reusable across multiple requests", async () => {
      const fn = vi.fn((x: number) => x + 100);
      pair = openSessionPair({
        producerInquiry: () => ({
          value: async (cb: (x: number) => Promise<number>, x: number) => {
            return await cb(x);
          },
        }),
      });

      const cb = pair.consumer.proxy(fn);
      await expect(pair.consumer.request("svc", "_", [cb, 1])).resolves.toBe(101);
      await expect(pair.consumer.request("svc", "_", [cb, 2])).resolves.toBe(102);

      expect(fn).toHaveBeenCalledTimes(2);
    });

    it.skip("stack-scoped stubs survive RELEASE round-trips for unrelated local callbacks", async () => {
      const stackFn = vi.fn(() => "stack");
      pair = openSessionPair({
        producerInquiry: () => ({
          value: async (stack: () => Promise<string>, local: () => Promise<string>) => {
            return await Promise.all([stack(), local()]);
          },
        }),
      });

      const listenerFn = vi.fn();
      // @ts-expect-error; ignore.
      const unsubscribe = pair.consumer.on("callback-released", listenerFn);

      const cb = pair.consumer.proxy(stackFn);
      const result = await pair.consumer.request("svc", "all", [cb, () => "local"]);
      expect(result).toEqual(["stack", "local"]);

      // Wait for the RELEASE for the SESSION callback; the CALL one must remain.
      await vi.waitFor(() => expect(pair!.consumer.status.callbacks).toBe(1));
      expect(listenerFn).toHaveBeenCalledTimes(1);
      expect(listenerFn).not.toHaveBeenCalledWith(cb.id);
      unsubscribe();
    });

    it("invoking a stack-scoped callback after local rejects with NOT_FOUND", async () => {
      const observations: unknown[] = [];
      pair = openSessionPair({
        producerInquiry: () => ({
          value: async (cb: () => Promise<unknown>) => {
            // Yield so the consumer's release(...) below has time to run
            // before the producer dispatches the INVOKE.
            await new Promise((r) => setTimeout(r, 20));
            observations.push(await cb());
            return null;
          },
        }),
      });

      const cb = pair.consumer.proxy(() => "should-not-fire");
      const promise = pair.consumer.request("svc", "_", [cb]);
      cb[QuirySymbol.release]();

      await expect(promise).rejects.toMatchObject({ code: WireStatus.NOT_FOUND });
    });

    describe("invocation argument handling", () => {
      it("forwards positional args verbatim to the local callback", async () => {
        const seen: unknown[][] = [];
        pair = openSessionPair({
          producerInquiry: () => ({
            value: async (cb: (...args: unknown[]) => Promise<void>) => {
              await cb(1, "two", { three: 3 }, [4, 5]);
              return null;
            },
          }),
        });

        await pair.consumer.request("svc", "_", [
          (...args: unknown[]) => {
            seen.push(args);
          },
        ]);

        expect(seen).toEqual([[1, "two", { three: 3 }, [4, 5]]]);
      });

      it("the callback's return value flows back to the producer's await", async () => {
        let received: unknown = null;
        pair = openSessionPair({
          producerInquiry: () => ({
            value: async (cb: () => Promise<unknown>) => {
              received = await cb();
              return null;
            },
          }),
        });

        await pair.consumer.request("svc", "_", [() => ({ ok: true, value: 7 })]);
        expect(received).toEqual({ ok: true, value: 7 });
      });

      it("a callback that throws on the consumer side rejects the proxy with the error", async () => {
        pair = openSessionPair({
          producerInquiry: () => ({
            value: async (cb: () => Promise<unknown>) => {
              return await cb();
            },
          }),
        });

        const promise = pair.consumer.request("svc", "invoke", [
          () => {
            throw new Error("nope");
          },
        ]);
        await expect(promise).rejects.toMatchObject({ message: "nope" });
      });

      it("the producer's outbound counter stays balanced when the callback returns", async () => {
        pair = await openSessionPair({
          producerInquiry: () => ({
            value: async (cb: () => Promise<unknown>) => {
              await cb();
              return null;
            },
          }),
        });

        await pair.consumer.request("svc", "_", [() => "ok"]);
        // After RELEASE, no remote invocation should be pending on either side.
        await vi.waitFor(() => {
          expect(pair!.producer.status.invocations).toBe(0);
          expect(pair!.consumer.status.invocations).toBe(0);
        });
      });
    });
  });
});
