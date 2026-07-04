import { WireStatus } from "~/protocol/wire";
import { Session, SessionState } from "~/core/session";

import { pairTransports, type MockTransport } from "./helpers/mock-transport";
import { openSessionPair, defaultInquiryDescriptor, type SessionPair } from "./helpers/session-pair";
import type { DrainInitiator, DrainPhase } from "~/interface/diagnostics";

describe("Session lifecycle", () => {
  let pair: SessionPair | null = null;

  afterEach(async () => {
    if (pair) {
      await pair.close(false).catch(() => null);
      pair = null;
    }
  });

  it("a graceful close on one side terminates both sides cooperatively", async () => {
    pair = openSessionPair();
    expect(pair.consumer.state).toBe(SessionState.OPEN);
    expect(pair.producer.state).toBe(SessionState.OPEN);

    await pair.consumer.close("explicit");

    expect(pair.consumer.state).toBe(SessionState.CLOSED);
    // The producer transitions in response to receiving DRAIN; give it
    // a moment to ACK and tear down.
    await vi.waitFor(() => expect(pair!.producer.state).toBe(SessionState.CLOSED));
  });

  it("in-flight requests resolve before drain completes", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({
        value: async (n: number) => {
          await new Promise((r) => setTimeout(r, 25));
          return n;
        },
      }),
    });

    const pending = Promise.all([
      pair.consumer.request("svc", "_", [1]),
      pair.consumer.request("svc", "_", [2]),
      pair.consumer.request("svc", "_", [3]),
    ]);

    // Kick off drain while requests are still parked on the producer.
    const drain = pair.consumer.close();

    await expect(pending).resolves.toEqual([1, 2, 3]);
    await expect(drain).resolves.toBeUndefined();
  });

  it("a force-close skips the drain protocol and rejects pending work", async () => {
    pair = openSessionPair({
      producerInquiry: () => ({ value: () => new Promise<never>(() => {}) }),
    });

    const promise = pair.consumer.request("svc", "_", [], { retry: { maxAttempts: 0 } });
    await new Promise((r) => setTimeout(r, 15));

    // No DRAIN goes out under force-close. We can't easily assert the
    // *absence* of a packet, but we CAN assert the visible effects:
    // synchronous CLOSED transition, ABORTED rejection of pending work.
    await pair.consumer.close("crash", false);
    expect(pair.consumer.state).toBe(SessionState.CLOSED);
    await expect(promise).rejects.toMatchObject({ code: WireStatus.ABORTED });
  });

  it("a drain whose peer never ACKs terminates after the configured timeout", async () => {
    // Build a session whose peer has no Session attached at all, so its
    // DRAIN packet is forever ignored. The drain coroutine must fall
    // through to its timeout path and tear down without hanging.
    const [tA, _tB] = pairTransports();
    const session = new Session(tA, () => defaultInquiryDescriptor({ value: () => null }), {
      drainTimeout: 80,
    }).open();

    const start = Date.now();
    await session.close("timeout-test");
    const elapsed = Date.now() - start;

    expect(session.state).toBe(SessionState.CLOSED);
    // Should be roughly the configured timeout, not the 5s default.
    expect(elapsed).toBeGreaterThanOrEqual(60);
    expect(elapsed).toBeLessThan(1000);
  });

  it("concurrent drain from both sides still terminates both cleanly", async () => {
    pair = openSessionPair();

    // Both sides race to initiate drain simultaneously. The inline-ACK
    // path in DrainCoordinator must prevent a deadlock where each side
    // waits for the other.
    const [a, b] = await Promise.all([
      pair.consumer.close("a").then(() => "a-done"),
      pair.producer.close("b").then(() => "b-done"),
    ]);

    expect([a, b]).toEqual(["a-done", "b-done"]);
    expect(pair.consumer.state).toBe(SessionState.CLOSED);
    expect(pair.producer.state).toBe(SessionState.CLOSED);
  });

  it("transport closing mid-drain shortcuts the timeout and still terminates", async () => {
    const [tA, tB] = pairTransports();
    tA.open();
    tB.open();
    const session = new Session(tA, () => defaultInquiryDescriptor({ value: () => null }), {
      drainTimeout: 5000, // intentionally long — we expect transport close to short-circuit
    }).open();

    const phases: string[] = [];
    session.diagnostic.on("drain:phase", (p) => phases.push(p.phase));

    const drain = session.close("transport-died");
    // Kill the peer transport mid-drain. The mock propagates close to
    // the local side; the drain coordinator's close handler aborts and
    // teardown runs without waiting for the configured 5s timeout.
    setTimeout(() => (tB as MockTransport).close("peer-crash"), 25);

    const start = Date.now();
    await drain;
    const elapsed = Date.now() - start;

    expect(session.state).toBe(SessionState.CLOSED);
    expect(phases).not.toContain("timeout");
    expect(elapsed).toBeLessThan(500);
  });
});
