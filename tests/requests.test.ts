import { Session, type InquiryFunc, type InquiryRequest } from "@/core/session";
import { WireStatus, type TraceId } from "@/interface/base";
import { QuiryError } from "@/shared/errors";

import { pairTransports } from "./helpers/mock-transport";
import { openSessionPair, type SessionPair } from "./helpers/session-pair";

/**
 * The full request surface lives on `Session.request`. These tests exercise it
 * end-to-end through a pair of in-memory sessions linked by a mock transport,
 * with a few low-level cases that bypass the helper to drive states the
 * helper can't reach (peering, closed).
 */
describe("Session requests", () => {
  let pair: SessionPair | null = null;

  afterEach(async () => {
    if (pair) {
      await pair.close().catch(() => null);
      pair = null;
    }
  });

  describe("successful invocation", () => {
    it("resolves with the value the inquiry returns", async () => {
      pair = await openSessionPair({
        producerInquiry: async () => ({ ok: true }),
      });

      await expect(pair.consumer.request("svc", "ping", [])).resolves.toEqual({ ok: true });
    });

    it("forwards service, method, and args verbatim to the inquiry", async () => {
      const inquiry = vi.fn<InquiryFunc>(async () => null);
      pair = await openSessionPair({ producerInquiry: inquiry });

      await pair.consumer.request("orders", "create", ["sku-1", 3, { gift: true }]);

      expect(inquiry).toHaveBeenCalledTimes(1);
      const arg = inquiry.mock.calls[0]![0] as InquiryRequest;
      expect(arg.service).toBe("orders");
      expect(arg.method).toBe("create");
      expect(arg.args).toEqual(["sku-1", 3, { gift: true }]);
      expect(typeof arg.id).toBe("string");
    });

    it("propagates control.traceId on the wire", async () => {
      let captured: string | undefined;
      pair = await openSessionPair({
        producerInquiry: async (req) => {
          captured = req.control?.traceId;
          return null;
        },
      });

      const traceId = "trace-abc" as unknown as TraceId;
      await pair.consumer.request("_", "_", [], { traceId });

      expect(captured).toBe("trace-abc");
    });

    it("returns a variety of serializable value shapes unchanged", async () => {
      const fixtures: any[] = [
        null,
        undefined,
        0,
        -1,
        "string",
        true,
        false,
        [],
        [1, 2, [3, 4]],
        { nested: { deeply: { value: "x" } } },
      ];

      pair = await openSessionPair({
        producerInquiry: async (req) => req.args[0],
      });

      for (const value of fixtures) {
        await expect(pair.consumer.request("_", "echo", [value])).resolves.toEqual(value);
      }
    });

    it("handles many concurrent requests without crossing responses", async () => {
      pair = await openSessionPair({
        producerInquiry: async (req) => {
          // small per-call jitter to interleave responses on the wire
          await new Promise((r) => setTimeout(r, Math.random() * 10));
          return (req.args[0] as number) * 2;
        },
      });

      const values = Array.from({ length: 20 }, (_, i) => i);
      const results = await Promise.all(values.map((v) => pair!.consumer.request("_", "double", [v])));

      expect(results).toEqual(values.map((v) => v * 2));
    });
  });

  describe("argument validation", () => {
    it("rejects with INVALID_ARGUMENT when args contain a symbol or circular reference", async () => {
      pair = await openSessionPair({});

      await expect(pair.consumer.request("_", "_", [Symbol("x")])).rejects.toMatchObject({
        code: WireStatus.INVALID_ARGUMENT,
      });

      const circular: Record<string, unknown> = {};
      circular.self = circular;
      await expect(pair.consumer.request("_", "_", [circular])).rejects.toMatchObject({
        code: WireStatus.INVALID_ARGUMENT,
      });
    });

    it("does not invoke the producer's inquiry when args fail validation", async () => {
      const inquiry = vi.fn<InquiryFunc>(async () => null);
      pair = await openSessionPair({ producerInquiry: inquiry });

      await expect(pair.consumer.request("_", "_", [Symbol("x")])).rejects.toBeInstanceOf(QuiryError);
      // Give the wire a beat just in case something erroneously got dispatched.
      await new Promise((r) => setTimeout(r, 25));
      expect(inquiry).not.toHaveBeenCalled();
    });
  });

  describe("session lifecycle", () => {
    it("rejects with UNAVAILABLE when the session is still in 'peering'", async () => {
      const [tA] = pairTransports();
      const session = new Session(tA);
      // never opened — state is "peering"

      const err = await session.request("_", "_", []).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.UNAVAILABLE);
    });

    it("rejects with UNAVAILABLE once the session has begun draining", async () => {
      pair = await openSessionPair({});

      // close() returns the in-flight drain promise; state flips to
      // "draining" synchronously before the first internal await.
      const drainPromise = pair.consumer.close();
      expect(pair.consumer.state).not.toBe("open");

      await expect(pair.consumer.request("_", "_", [])).rejects.toMatchObject({
        code: WireStatus.UNAVAILABLE,
      });

      await drainPromise.catch(() => {});
    });

    it("rejects pending requests with ABORTED when the session is force-closed", async () => {
      pair = await openSessionPair({
        // hangs forever so the consumer's pending entry stays outstanding
        producerInquiry: () => new Promise<never>(() => {}),
      });

      const promise = pair.consumer.request("_", "_", [], {
        timeout: 60_000,
        retry: { maxAttempts: 0 },
      });

      // Let the request settle into pending before we tear the session down.
      await new Promise((r) => setTimeout(r, 25));
      expect(pair.consumer.status.pending).toBe(1);

      await pair.consumer.close("explicit", false); // force teardown

      await expect(promise).rejects.toMatchObject({ code: WireStatus.ABORTED, message: "Session draining" });
      expect(pair.consumer.status.pending).toBe(0);

      // Peer is gone, the DRAIN ACK will never arrive.
      void pair.close(false);
    });
  });

  describe("timeout", () => {
    it("rejects with DEADLINE_EXCEEDED when the explicit timeout elapses", async () => {
      pair = await openSessionPair({
        producerInquiry: () => new Promise<never>(() => {}),
      });

      const start = Date.now();
      const err = await pair.consumer
        .request("_", "_", [], { timeout: 80, retry: { maxAttempts: 0 } })
        .catch((e: unknown) => e);
      const elapsed = Date.now() - start;

      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.DEADLINE_EXCEEDED);
      expect(elapsed).toBeGreaterThanOrEqual(70);
      expect(elapsed).toBeLessThan(1000);
    });

    it("does not time out a request that completes within the budget", async () => {
      pair = await openSessionPair({
        producerInquiry: async () => {
          await new Promise((r) => setTimeout(r, 30));
          return "ok";
        },
      });

      await expect(pair.consumer.request("_", "_", [], { timeout: 500 })).resolves.toBe("ok");
    });
  });

  describe("retry policy", () => {
    it("retries on a retryable error and resolves once the producer recovers", async () => {
      let attempts = 0;
      pair = await openSessionPair({
        producerInquiry: async () => {
          attempts++;
          if (attempts < 3) {
            throw new QuiryError(WireStatus.UNAVAILABLE, "retry me");
          }
          return "ok";
        },
      });

      await expect(
        pair.consumer.request("_", "_", [], { retry: { maxAttempts: 5, delay: 1 } }),
      ).resolves.toBe("ok");
      expect(attempts).toBe(3);
    });

    it("does not retry a non-retryable error", async () => {
      let attempts = 0;
      pair = await openSessionPair({
        producerInquiry: async () => {
          attempts++;
          throw new QuiryError(WireStatus.INVALID_ARGUMENT, "no");
        },
      });

      await expect(
        pair.consumer.request("_", "_", [], { retry: { maxAttempts: 5, delay: 1 } }),
      ).rejects.toMatchObject({ code: WireStatus.INVALID_ARGUMENT });
      expect(attempts).toBe(1);
    });

    it("propagates the final error after retries are exhausted", async () => {
      let attempts = 0;
      pair = await openSessionPair({
        producerInquiry: async () => {
          attempts++;
          throw new QuiryError(WireStatus.UNAVAILABLE, "still down");
        },
      });

      await expect(
        pair.consumer.request("_", "_", [], { retry: { maxAttempts: 2, delay: 1 } }),
      ).rejects.toMatchObject({
        code: WireStatus.UNAVAILABLE,
        message: "still down",
      });
      // `retryable` interprets `maxAttempts` as additional retries on top of the
      // initial call, so 2 means: 1 initial + 2 retries = 3 calls total.
      expect(attempts).toBe(3);
    });

    it("respects a custom delay between retries", async () => {
      let attempts = 0;
      const stamps: number[] = [];
      pair = await openSessionPair({
        producerInquiry: async () => {
          stamps.push(Date.now());
          attempts++;
          throw new QuiryError(WireStatus.UNAVAILABLE, "nope");
        },
      });

      await expect(
        pair.consumer.request("_", "_", [], { retry: { maxAttempts: 1, delay: 60 } }),
      ).rejects.toMatchObject({ code: WireStatus.UNAVAILABLE });

      expect(attempts).toBe(2);
      // First retry uses the configured delay.
      // (exponential backoff with k=0 means `delay * 2^0 = delay`)
      expect(stamps[1]! - stamps[0]!).toBeGreaterThanOrEqual(50);
    });

    it("aborts during retry backoff without waiting for the next attempt", async () => {
      let attempts = 0;
      pair = await openSessionPair({
        producerInquiry: async () => {
          attempts++;
          throw new QuiryError(WireStatus.UNAVAILABLE, "down");
        },
      });

      const ac = new AbortController();
      const promise = pair.consumer.request(
        "_",
        "_",
        [],
        {
          retry: { maxAttempts: 5, delay: 1000 },
        },
        ac.signal,
      );

      // Wait long enough for the first attempt to fail and to
      // schedule its (1s) backoff, then abort.
      await new Promise((r) => setTimeout(r, 60));
      ac.abort();

      const start = Date.now();
      await expect(promise).rejects.toMatchObject({ code: WireStatus.ABORTED });
      expect(Date.now() - start).toBeLessThan(200);
      expect(attempts).toBe(1);
    });
  });

  describe("abort signal", () => {
    it("rejects in-flight requests with ABORTED when the signal fires", async () => {
      pair = await openSessionPair({
        producerInquiry: () => new Promise<never>(() => {}),
      });

      const ac = new AbortController();
      const promise = pair.consumer.request(
        "_",
        "_",
        [],
        {
          timeout: 60_000,
          retry: { maxAttempts: 0 },
        },
        ac.signal,
      );

      // Make sure the request is parked in pending before aborting,
      // so we exercise the abort listener and not a faster code path.
      await new Promise((r) => setTimeout(r, 20));
      expect(pair.consumer.status.pending).toBe(1);
      ac.abort();

      await expect(promise).rejects.toMatchObject({ code: WireStatus.ABORTED });
      expect(pair.consumer.status.pending).toBe(0);
    });

    it("aborting after a successful resolution is a no-op", async () => {
      pair = await openSessionPair({
        producerInquiry: async () => "done",
      });

      const ac = new AbortController();
      await expect(pair.consumer.request("_", "_", [], {}, ac.signal)).resolves.toBe("done");

      // The abort listener must have been detached when the request settled
      expect(() => ac.abort()).not.toThrow();
      await new Promise((r) => setTimeout(r, 5));
      expect(pair.consumer.status.pending).toBe(0);
    });

    it("rejects synchronously when the signal is already aborted", async () => {
      pair = await openSessionPair({
        producerInquiry: async () => "should never run",
      });

      const ac = new AbortController();
      ac.abort();

      await expect(
        pair.consumer.request("_", "_", [], { retry: { maxAttempts: 0 } }, ac.signal),
      ).rejects.toMatchObject({ code: WireStatus.ABORTED });
      expect(pair.consumer.status.pending).toBe(0);
    });

    it("clears every pending slot when many requests are aborted at once", async () => {
      let respond: ((value: string) => void) | null = null;
      pair = await openSessionPair({
        producerInquiry: (req) =>
          new Promise<string>((resolve) => {
            // Only the last request gets a real responder, the others hang
            // forever so that aborts are the only way they settle.
            if ((req.args[0] as string) === "live") respond = resolve;
          }),
      });

      const aborts = Array.from({ length: 10 }, () => new AbortController());
      const aborted = aborts.map((ac) =>
        pair!.consumer.request(
          "_",
          "_",
          ["hang"],
          {
            timeout: 60_000,
            retry: { maxAttempts: 0 },
          },
          ac.signal,
        ),
      );
      const live = pair.consumer.request("_", "_", ["live"], {
        timeout: 60_000,
        retry: { maxAttempts: 0 },
      });

      await new Promise((r) => setTimeout(r, 25));
      expect(pair.consumer.status.pending).toBe(11);

      for (const ac of aborts) ac.abort();
      for (const p of aborted) {
        await expect(p).rejects.toMatchObject({ code: WireStatus.ABORTED });
      }

      // The live request is unaffected by the surrounding aborts; the
      // outbound tracker is balanced enough to deliver its response.
      expect(pair.consumer.status.pending).toBe(1);
      respond!("done");
      await expect(live).resolves.toBe("done");
      expect(pair.consumer.status.pending).toBe(0);
    });
  });

  describe("error propagation", () => {
    it("rebuilds a thrown error on the caller side with the same code/message", async () => {
      pair = await openSessionPair({
        producerInquiry: async () => {
          throw new QuiryError(WireStatus.NOT_FOUND, "no such record", {
            detail: { id: 42 },
          });
        },
      });

      const err = await pair.consumer
        .request("_", "_", [], { retry: { maxAttempts: 0 } })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(QuiryError);
      expect(err).toMatchObject({
        code: WireStatus.NOT_FOUND,
        message: "no such record",
        detail: { id: 42 },
      });
    });

    it("promotes a thrown native Error to INTERNAL", async () => {
      pair = await openSessionPair({
        producerInquiry: async () => {
          throw new Error("boom");
        },
      });

      const err = await pair.consumer
        .request("_", "_", [], { retry: { maxAttempts: 0 } })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.INTERNAL);
      expect((err as QuiryError).message).toBe("boom");
    });

    it("rejects with INTERNAL when the producer returns a non-serializable value", async () => {
      pair = await openSessionPair({
        producerInquiry: async () => () => 42,
      });

      const err = await pair.consumer
        .request("_", "_", [], { retry: { maxAttempts: 0 } })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.INTERNAL);
    });

    it("preserves the cause chain across the wire boundary", async () => {
      pair = await openSessionPair({
        producerInquiry: async () => {
          const root = new QuiryError(WireStatus.DATA_LOSS, "root cause");
          throw new QuiryError(WireStatus.INTERNAL, "wrapper", { cause: root });
        },
      });

      const err = await pair.consumer
        .request("_", "_", [], { retry: { maxAttempts: 0 } })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).cause).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).cause).toMatchObject({
        code: WireStatus.DATA_LOSS,
        message: "root cause",
      });
    });

    it("tags the rebuilt error with the remote node as `origin`", async () => {
      let producerNodeId: string | null = null;
      pair = await openSessionPair({
        producerInquiry: async () => {
          throw new QuiryError(WireStatus.INTERNAL, "kaboom");
        },
      });

      // The producer's `peer` field on the consumer side is whatever the
      // producer announced during handshake. It also matches the origin of
      // any errors produced on the producer side.
      producerNodeId = pair.consumer.peer;

      const err = await pair.consumer
        .request("_", "_", [], { retry: { maxAttempts: 0 } })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).origin).toBe(producerNodeId);
    });
  });
});
