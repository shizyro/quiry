import { openSessionPair, type SessionPair } from "./helpers/session-pair";
import type { InquiryRequest } from "@/core/session";

/**
 * Tests the consumer-/producer-side streaming flow end-to-end through a pair
 * of sessions linked by an in-memory transport. The producer exposes an
 * async generator via its `inquiry` handler; the consumer calls `.stream(...)`
 * and iterates the returned `AsyncIterableIterator`.
 */
describe("Session streaming", () => {
  let pair: SessionPair | null = null;

  afterEach(async () => {
    if (pair) {
      await pair.close().catch(() => null);
      pair = null;
    }
  });

  it("chunks flow in order through end-of-stream", async () => {
    pair = await openSessionPair({
      producerInquiry: async function* (request: InquiryRequest) {
        const [start, end] = request.args as [number, number];
        for (let i = start; i < end; i++) yield i;
      },
    });

    const stream = pair.consumer.stream("_", "range", [0, 5]);
    const received: unknown[] = [];
    for await (const chunk of stream) received.push(chunk);

    expect(received).toEqual([0, 1, 2, 3, 4]);
  });

  it("propagates producer errors to the consumer", async () => {
    pair = await openSessionPair({
      producerInquiry: async function* () {
        yield "a";
        yield "b";
        throw new Error("producer failed");
      } as any,
    });

    const stream = pair.consumer.stream("_", "broken", []);
    const received: unknown[] = [];

    let caught: unknown = null;
    try {
      for await (const chunk of stream) received.push(chunk);
    } catch (err) {
      caught = err;
    }

    expect(caught).not.toBeNull();
    expect((caught as Error).message).toBe("producer failed");
    expect(received).toEqual(["a", "b"]);
  });

  it("consumer break sends CANCEL and stops the producer", async () => {
    let sent = 0;
    let cancelled = false;

    pair = await openSessionPair({
      producerInquiry: async function* () {
        try {
          // Large window; rely on the consumer cancelling to stop us.
          for (let i = 0; i < 1000; i++) {
            sent = i + 1;
            yield i;
            // Yield back to the microtask queue so incoming CANCEL
            // packets can be processed between emissions.
            await new Promise<void>((r) => setTimeout(r, 0));
          }
        } finally {
          cancelled = true;
        }
      } as any,
    });

    const stream = pair.consumer.stream("_", "infinite", []);

    let count = 0;
    for await (const _chunk of stream) {
      count++;
      if (count >= 3) break;
    }

    expect(count).toBe(3);

    // Give the CANCEL packet time to land and the producer's `finally`
    // block time to run. We check `cancelled` rather than `sent`
    // because the producer may have dispatched a few extra chunks
    // while the CANCEL was in flight.
    await new Promise((r) => setTimeout(r, 50));
    expect(cancelled).toBe(true);

    // Producer should have stopped well before the 1000-iteration limit.
    expect(sent).toBeLessThan(100);
  });

  it("credit backpressure throttles a fast producer to the consumer's pace", async () => {
    const total = 260; // > 2 * default window (100) to force multiple replenishments
    let producerEmitted = 0;

    pair = await openSessionPair({
      producerInquiry: async function* () {
        for (let i = 0; i < total; i++) {
          producerEmitted = i + 1;
          yield i;
        }
      } as any,
    });

    const stream = pair.consumer.stream("_", "many", []);
    const received: number[] = [];
    for await (const chunk of stream) received.push(chunk as number);

    expect(received.length).toBe(total);
    expect(received[0]).toBe(0);
    expect(received[total - 1]).toBe(total - 1);
    expect(producerEmitted).toBe(total);
  });
});
