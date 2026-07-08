import { WireStatus } from "~/protocol/wire";
import { QuiryError } from "~/protocol/errors";

import { openSessionPair, type SessionPair, type MockInquiryFunc } from "./helpers/session-pair";
import { contextStorage } from "~/lib/call-context";

/**
 * Request correlation, argument routing, error fidelity,
 * lifecycle gates, and request control.
 */
describe("Session requests", () => {
  let pair: SessionPair | null = null;

  afterEach(async () => {
    if (pair) {
      await pair.close().catch(() => null);
      pair = null;
    }
  });

  // Tests below are NOT a method-by-method coverage matrix; each one
  // is named by the user-visible guarantee it protects.

  it("delivers arguments verbatim and returns the inquiry's value", async () => {
    const producerInquiry = vi.fn<MockInquiryFunc>(() => ({
      value: vi.fn((...args: unknown[]) => ({ echoed: args })),
    }));
    pair = openSessionPair({ producerInquiry });

    const result = await pair.consumer.request("orders", "create", [
      "sku-1",
      3,
      { gift: true, tags: ["birthday"] },
    ]);

    expect(result).toEqual({ echoed: ["sku-1", 3, { gift: true, tags: ["birthday"] }] });

    const seen = producerInquiry.mock.calls[0]![0];
    expect(seen).toMatchObject({ object: "orders", property: "create" });
  });

  it("keeps concurrent responses strictly correlated", async () => {
    // The producer randomizes per-call latency so responses arrive
    // out-of-order on the wire; correlated routing must reassemble them.
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (n: number) => {
          await new Promise((r) => setTimeout(r, Math.random() * 5));
          return n * 2;
        },
      }),
    });

    const inputs = Array.from({ length: 25 }, (_, i) => i);
    const results = await Promise.all(inputs.map((n) => pair!.consumer.request("svc", "double", [n])));

    expect(results).toEqual(inputs.map((n) => n * 2));
  });

  it("rebuilds a thrown error on the caller side with code, message, detail, and cause", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: () => {
          throw new QuiryError(WireStatus.NOT_FOUND, "missing", {
            detail: { id: 42 },
            cause: new QuiryError(WireStatus.DATA_LOSS, "stale snapshot"),
          });
        },
      }),
    });

    const error = await pair.consumer.request("svc", "_", []).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(QuiryError);
    expect(error).toMatchObject({
      code: WireStatus.NOT_FOUND,
      message: "missing",
      detail: { id: 42 },
    });
    expect((error as QuiryError).cause).toMatchObject({
      code: WireStatus.DATA_LOSS,
      message: "stale snapshot",
    });
  });

  it("promotes a native error to INTERNAL without losing the message", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: () => {
          throw new TypeError("bad input");
        },
      }),
    });

    const error = await pair.consumer.request("svc", "_", []).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(QuiryError);
    expect((error as QuiryError).code).toBe(WireStatus.INTERNAL);
    expect((error as QuiryError).message).toBe("bad input");
  });

  it("rejects non-serializable arguments before any wire traffic", async () => {
    const producerInquiry = vi.fn<MockInquiryFunc>(() => ({ value: () => null }));
    pair = openSessionPair({ producerInquiry });

    await expect(pair.consumer.request("svc", "_", [Symbol("x")])).rejects.toMatchObject({
      code: WireStatus.INVALID_ARGUMENT,
    });

    // await new Promise((r) => setTimeout(r, 25));
    expect(producerInquiry).not.toHaveBeenCalled();
  });

  it("rejects with INTERNAL when the producer returns a value that can't survive structured clone", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({ value: () => Symbol("nope") }),
    });

    const error = await pair.consumer.request("svc", "_", []).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(QuiryError);
    expect((error as QuiryError).code).toBe(WireStatus.INTERNAL);
  });

  describe("request control", () => {
    it("rejects with after explicit timeout", async () => {
      pair = openSessionPair({
        producerInquiry: () => ({ value: () => new Promise<never>(() => {}) }),
      });

      const start = Date.now();
      const error = await pair.consumer
        .request("svc", "_", [], AbortSignal.timeout(20))
        .catch((e: unknown) => e);
      const elapsed = Date.now() - start;

      expect((error as QuiryError).code).toBe(WireStatus.ABORTED);
      // A wide window: tight enough to fail if the timer was lost, loose
      // enough to survive CI scheduler jitter.
      expect(elapsed).toBeGreaterThanOrEqual(10);
      expect(elapsed).toBeLessThan(100);
    });

    it("aborts a parked request and clears its pending slot", async () => {
      pair = openSessionPair({
        producerInquiry: () => ({ value: () => new Promise<never>(() => {}) }),
      });

      const ac = new AbortController();
      const promise = pair.consumer.request("svc", "_", [], ac.signal);

      // await new Promise((r) => setTimeout(r, 15));
      expect(pair.consumer.status.pending).toBe(1);
      ac.abort();

      await expect(promise).rejects.toMatchObject({ code: WireStatus.ABORTED });
      expect(pair.consumer.status.pending).toBe(0);
    });

    it("rejects synchronously when the signal is already aborted", async () => {
      const producerInquiry = vi.fn<MockInquiryFunc>(() => ({ value: () => "never" }));
      pair = openSessionPair({ producerInquiry });

      const ac = new AbortController();
      ac.abort();

      await expect(pair.consumer.request("svc", "_", [], ac.signal)).rejects.toMatchObject({
        code: WireStatus.ABORTED,
      });

      expect(pair.consumer.status.pending).toBe(0);

      // await new Promise((r) => setTimeout(r, 15));
      expect(producerInquiry).not.toHaveBeenCalled();
    });

    it("a force-close drops all pending requests", async () => {
      pair = openSessionPair({
        producerInquiry: () => ({ value: () => new Promise<never>(() => {}) }),
      });

      const promises = Array.from({ length: 5 }, () =>
        pair!.consumer.request("svc", "_", [], AbortSignal.timeout(60_000)),
      );

      // await new Promise((r) => setTimeout(r, 20));
      expect(pair.consumer.status.pending).toBe(5);
      await pair.consumer.close("explicit", false);

      for (const p of promises) {
        await expect(p).rejects.toMatchObject({ code: WireStatus.ABORTED });
      }
      expect(pair.consumer.status.pending).toBe(0);

      // Peer is orphaned; its DRAIN_ACK will never come. Force tear-down.
      void pair.close(false);
    });
  });

  it("a request issued during DRAIN rejects before reaching the producer", async () => {
    const producerSeen = vi.fn();
    pair = openSessionPair({
      producerInquiry: () => ({
        value: () => {
          producerSeen();
          return "ok";
        },
      }),
    });

    const drain = pair.consumer.close(); // synchronous transition to draining
    expect(pair.consumer.state).not.toBe("open");

    await expect(pair.consumer.request("svc", "_", [])).rejects.toMatchObject({
      code: WireStatus.UNAVAILABLE,
    });
    expect(producerSeen).not.toHaveBeenCalled();

    await drain.catch(() => {});
  });

  it("exposes an ambient abort signal that fires when the caller aborts", async () => {
    let observed: AbortSignal | undefined;
    pair = openSessionPair({
      producerInquiry: () => ({
        value: () =>
          new Promise((resolve) => {
            observed = contextStorage.getStore()?.signal;
            observed?.addEventListener("abort", () => resolve("stopped"), { once: true });
          }),
      }),
    });

    const ac = new AbortController();
    const promise = pair.consumer.request("svc", "_", [], ac.signal);
    await new Promise((r) => setTimeout(r, 10));
    ac.abort();

    await expect(promise).rejects.toMatchObject({ code: WireStatus.ABORTED });
    expect(observed?.aborted).toBe(true);
  });
});
