import { WireStatus } from "~/interface/protocol";
import { QuiryError } from "~/shared/errors";

import { openSessionPair, type SessionPair } from "./helpers/session-pair";

/**
 * Tests the consumer-/producer-side streaming flow end-to-end through a pair
 * of sessions linked by an in-memory transport.
 */
describe("Session streams", () => {
  let pair: SessionPair | null = null;

  afterEach(async () => {
    if (pair) {
      await pair.close().catch(() => null);
      pair = null;
    }
  });

  it("delivers a sync-generator's chunks in order and terminates cleanly", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: function* (start: number, end: number) {
          for (let i = start; i < end; i++) yield i;
        },
      }),
    });

    const received: number[] = [];
    for await (const chunk of pair.consumer.stream("svc", "range", [0, 5])) {
      received.push(chunk as number);
    }

    expect(received).toEqual([0, 1, 2, 3, 4]);
  });

  it("delivers an async-generator's chunks in order across microtask boundaries", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async function* () {
          for (let i = 0; i < 4; i++) {
            await new Promise((r) => setTimeout(r, 1));
            yield `item-${i}`;
          }
        },
      }),
    });

    const received: unknown[] = [];
    for await (const chunk of pair.consumer.stream("svc", "tick", [])) received.push(chunk);

    expect(received).toEqual(["item-0", "item-1", "item-2", "item-3"]);
  });

  it("propagates a mid-stream producer throw AFTER chunks already delivered", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: function* () {
          yield "a";
          yield "b";
          throw new QuiryError(WireStatus.INTERNAL, "boom");
        },
      }),
    });

    const received: unknown[] = [];
    let caught: unknown = null;
    try {
      for await (const chunk of pair.consumer.stream("svc", "broken", [])) received.push(chunk);
    } catch (e) {
      caught = e;
    }

    expect(received).toEqual(["a", "b"]);
    expect(caught).toBeInstanceOf(QuiryError);
    expect((caught as QuiryError).message).toBe("boom");
  });

  it("consumer break sends CANCEL and the producer's finally runs", async () => {
    let cancelled = false;
    let sent = 0;
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async function* () {
          try {
            for (let i = 0; i < 1000; i++) {
              sent = i + 1;
              yield i;
              // Yield to the microtask queue so the incoming CANCEL
              // gets dispatched between emissions.
              await new Promise<void>((r) => setTimeout(r, 0));
            }
          } finally {
            cancelled = true;
          }
        },
      }),
    });

    let count = 0;
    for await (const _ of pair.consumer.stream("svc", "infinite", [])) {
      if (++count >= 3) break;
    }

    // The cancel rides the wire; give it a moment to land.
    await new Promise((r) => setTimeout(r, 30));
    expect(cancelled).toBe(true);
    // The producer should stop well before its 1000-iteration cap.
    expect(sent).toBeLessThan(100);
  });

  it("credit-based backpressure throttles a fast producer to the consumer's pace", async () => {
    // Window is set explicitly to a value smaller than `total` so the
    // producer MUST block on credit replenishment to make progress.
    const window = 20;
    const total = 70;
    let producerEmitted = 0;

    pair = openSessionPair({
      config: { creditWindow: window },
      producerInquiry: () => ({
        value: function* () {
          for (let i = 0; i < total; i++) {
            producerEmitted = i + 1;
            yield i;
          }
        },
      }),
    });

    const received: number[] = [];
    for await (const chunk of pair.consumer.stream("svc", "many", [])) {
      received.push(chunk as number);
    }

    expect(received).toHaveLength(total);
    expect(received[0]).toBe(0);
    expect(received[total - 1]).toBe(total - 1);
    expect(producerEmitted).toBe(total);
  });

  it("concurrent streams do not cross chunks", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        // Two services emit distinguishable, non-overlapping ranges.
        value: function* (offset: number) {
          for (let i = 0; i < 25; i++) yield offset + i;
        },
      }),
    });

    const collect = async (name: string, offset: number): Promise<number[]> => {
      const out: number[] = [];
      for await (const c of pair!.consumer.stream("svc", name, [offset])) out.push(c as number);
      return out;
    };

    const [a, b, c] = await Promise.all([collect("a", 0), collect("b", 1000), collect("c", 2000)]);
    expect(a).toEqual(Array.from({ length: 25 }, (_, i) => i));
    expect(b).toEqual(Array.from({ length: 25 }, (_, i) => 1000 + i));
    expect(c).toEqual(Array.from({ length: 25 }, (_, i) => 2000 + i));
  });

  it("a force-close mid-stream rejects the iterator and leaves no producer-side stream", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async function* () {
          for (let i = 0; i < 1000; i++) {
            yield i;
            await new Promise<void>((r) => setTimeout(r, 5));
          }
        },
      }),
    });

    const iter = pair.consumer.stream("svc", "infinite", [])[Symbol.asyncIterator]();
    // Pull two chunks so the producer is mid-emission.
    await iter.next();
    await iter.next();

    await pair.consumer.close("explicit", false);

    // The next pull must reject promptly; the consumer's pending entry
    // is gone, and the producer side will see its outbound stream
    // cancelled via its own `terminate`.
    await expect(iter.next()).rejects.toMatchObject({ code: WireStatus.ABORTED });

    // Let CANCEL/teardown propagate before checking the producer side.
    await vi.waitFor(() => expect(pair!.producer.status.streams).toBe(0));
    void pair.close(false);
  });
});
