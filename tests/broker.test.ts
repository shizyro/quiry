import { Broker, type PeerHandle } from "@/core/broker";
import { Session } from "@/core/session";

import { HeartbeatStatus, WireKind, WireStatus } from "@/interface/base";
import { SystemMessageType, type SystemHeartbeatPacket } from "@/interface/packets";
import { QuiryError } from "@/shared/errors";

import { pairTransports } from "./helpers/mock-transport";
import { makeMockPeer, type MockPeer } from "./helpers/peer";

/**
 * Bring up a broker with one mock peer attached. Both sides are driven on the
 * same event loop, so the worker session and `broker.attach` must run
 * concurrently — sequential opens would deadlock on the handshake.
 */
async function attachOnePeer(
  broker: Broker<any>,
  options: Parameters<typeof makeMockPeer>[0] = {},
): Promise<{ peer: PeerHandle; mock: MockPeer }> {
  const mock = makeMockPeer(options);
  const [, peer] = await Promise.all([mock.workerSession.open(), broker.attach(mock.brokerSide)]);
  return { peer, mock };
}

describe("The broker class", () => {
  let broker: Broker<any>;
  let teardown: Array<() => Promise<any> | any> = [];

  beforeEach(() => {
    broker = new Broker();
    teardown = [];
  });

  afterEach(async () => {
    for (const fn of teardown) await Promise.resolve(fn()).catch(() => null);
    await broker.shutdown().catch(() => null);
  });

  describe("expose / delete", () => {
    it("`expose` registers a service and returns the broker for chaining", () => {
      const result = broker.expose("svc", { ping: () => "pong" });
      expect(result).toBe(broker);
      expect(broker.status.services).toContain("svc");
    });

    it("`expose` rejects duplicate names with FAILED_PRECONDITION", () => {
      broker.expose("svc", { ping: () => "pong" });
      expect(() => broker.expose("svc", { ping: () => "alt" })).toThrow(QuiryError);
      try {
        broker.expose("svc", {});
      } catch (e) {
        expect(e).toBeInstanceOf(QuiryError);
        expect((e as QuiryError).code).toBe(WireStatus.FAILED_PRECONDITION);
        expect((e as QuiryError).detail).toMatchObject({ service: "svc" });
      }
    });

    it("`delete` removes a service from the registry", () => {
      broker.expose("svc", { ping: () => "pong" });
      broker.delete("svc");
      expect(broker.status.services).not.toContain("svc");
    });
  });

  describe("peer attachment", () => {
    it("opens the session, completes identify, and registers the peer", async () => {
      const events: PeerHandle[] = [];
      broker.on("peer-connected", (h) => events.push(h));

      const { peer, mock } = await attachOnePeer(broker, { label: "alpha" });
      teardown.push(() => mock.close());

      expect(peer.label).toBe("alpha");
      expect(peer.session).toBeInstanceOf(Session);
      expect(peer.session.isConnected()).toBe(true);
      expect(peer.info.health.status).toBe(HeartbeatStatus.HEALTHY);
      expect(broker.status.peers).toBe(1);
      expect(broker.peer(peer.id)).toBe(peer);
      expect(events).toHaveLength(1);
      expect(events[0]).toBe(peer);
    });

    it("rejects a duplicate node id with FAILED_PRECONDITION and tears the new session down", async () => {
      const { peer, mock: first } = await attachOnePeer(broker);
      teardown.push(() => first.close());

      // Second mock peer reuses the same `localNodeId` (same process), so the
      // broker must reject it as a duplicate.
      const second = makeMockPeer();
      const open = second.workerSession.open();

      const err = await broker.attach(second.brokerSide).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.FAILED_PRECONDITION);
      expect((err as QuiryError).detail).toMatchObject({ peerId: peer.id });

      await open.catch(() => null);
      // The broker closes the duplicate session in-flight; let the worker
      // settle so we don't leak its drain promise.
      await second.close().catch(() => null);

      // Original peer is still registered.
      expect(broker.peer(peer.id)).toBe(peer);
      expect(broker.status.peers).toBe(1);
    });

    it("propagates an identify-timeout failure and closes the session before rethrowing", async () => {
      const slow = new Broker({ identifyTimeout: 60 });
      teardown.push(() => slow.shutdown());

      const mock = makeMockPeer({ skipIdentifyAck: true });
      teardown.push(() => mock.close());

      const open = mock.workerSession.open();
      const err = await Promise.all([open, slow.attach(mock.brokerSide).catch((e: unknown) => e)]).then(
        ([, e]) => e,
      );

      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.DEADLINE_EXCEEDED);
      expect(slow.status.peers).toBe(0);

      // The worker's session sees the broker tear its transport down.
      await vi.waitFor(() => expect(mock.workerSession.state).not.toBe("open"));
    });

    it("starts the heartbeat monitor when the first peer attaches", async () => {
      const fast = new Broker({ heartbeat: { checkInterval: 5000 } });
      teardown.push(() => fast.shutdown());

      const setIntervalSpy = vi.spyOn(global, "setInterval");
      const mock = makeMockPeer();
      teardown.push(() => mock.close());

      const [, _peer] = await Promise.all([mock.workerSession.open(), fast.attach(mock.brokerSide)]);

      expect(setIntervalSpy).toHaveBeenCalledWith(expect.any(Function), 5000);
      setIntervalSpy.mockRestore();
    });
  });

  describe("inquiry routing", () => {
    it("dispatches `service.method` calls to the registered impl with `this` bound", async () => {
      const impl = {
        value: 42,
        getValue(): number {
          return this.value;
        },
        echo(payload: unknown): unknown {
          return payload;
        },
      };
      broker.expose("svc", impl);

      const { mock } = await attachOnePeer(broker);
      teardown.push(() => mock.close());

      await expect(mock.workerSession.request("svc", "getValue", [])).resolves.toBe(42);
      await expect(mock.workerSession.request("svc", "echo", ["ok"])).resolves.toEqual("ok");
    });

    it("forwards the original argument list verbatim to the registered method", async () => {
      const fn = vi.fn(async (a: number, b: number) => a + b);
      broker.expose("math", { add: fn });

      const { mock } = await attachOnePeer(broker);
      teardown.push(() => mock.close());

      await expect(mock.workerSession.request("math", "add", [2, 3])).resolves.toBe(5);
      expect(fn).toHaveBeenCalledTimes(1);
      expect(fn).toHaveBeenCalledWith(2, 3);
    });

    it("rejects unknown services with NOT_FOUND", async () => {
      const { mock } = await attachOnePeer(broker);
      teardown.push(() => mock.close());

      const err = await mock.workerSession
        .request("missing", "anything", [], { retry: { maxAttempts: 0 } })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.NOT_FOUND);
      expect((err as QuiryError).message).toContain("missing");
    });

    it("rejects unknown methods on a known service with NOT_FOUND", async () => {
      broker.expose("svc", { hello: () => "world" });
      const { mock } = await attachOnePeer(broker);
      teardown.push(() => mock.close());

      const err = await mock.workerSession
        .request("svc", "missing", [], { retry: { maxAttempts: 0 } })
        .catch((e: unknown) => e);

      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.NOT_FOUND);
      expect((err as QuiryError).message).toContain("missing");
      expect((err as QuiryError).message).toContain("svc");
    });
  });

  describe("peer detachment", () => {
    it("closes the peer session and removes it from the registry", async () => {
      const { peer, mock } = await attachOnePeer(broker);
      teardown.push(() => mock.close(false));

      const events: Array<[PeerHandle, string?]> = [];
      broker.on("peer-disconnected", (h, r) => events.push([h, r]));

      await broker.detach(peer.id);

      expect(broker.peer(peer.id)).toBeUndefined();
      expect(broker.status.peers).toBe(0);
      expect(events.length).toBeGreaterThanOrEqual(1);
      expect(events[0]![0]).toBe(peer);
    });

    it("stops the heartbeat monitor when the last peer detaches", async () => {
      const clearSpy = vi.spyOn(global, "clearInterval");
      const { peer, mock } = await attachOnePeer(broker);
      teardown.push(() => mock.close());

      await broker.detach(peer.id);
      expect(clearSpy).toHaveBeenCalled();
      clearSpy.mockRestore();
    });
  });

  describe("session-driven disconnects", () => {
    it("removes the peer and emits `peer-disconnected` when the worker session drains", async () => {
      const { peer, mock } = await attachOnePeer(broker);

      const events: Array<[PeerHandle, string?]> = [];
      broker.on("peer-disconnected", (h, r) => events.push([h, r]));

      // Graceful close drives the drain protocol on both sides; the broker's
      // session terminates as a result and triggers the registry cleanup.
      // (MockTransport doesn't propagate a force-close to its peer, so a
      // non-graceful close on one side leaves the other side oblivious.)
      await mock.workerSession.close("worker-side-close", true);

      await vi.waitFor(() => expect(broker.peer(peer.id)).toBeUndefined());
      expect(events.length).toBeGreaterThanOrEqual(1);
      expect(events[0]![0]).toBe(peer);
    });
  });

  describe("heartbeat tracking", () => {
    it("resets `heartbeat.missed` and updates `health.status` on each incoming heartbeat", async () => {
      const { peer, mock } = await attachOnePeer(broker);
      teardown.push(() => mock.close());

      // Pretend we missed a few before.
      peer.info.heartbeat.missed = 2;

      await mock.workerSession.send({
        kind: WireKind.SYSTEM,
        type: SystemMessageType.HEARTBEAT,
        payload: { status: HeartbeatStatus.HEALTHY, metrics: { uptime: 123 } },
      } satisfies Omit<SystemHeartbeatPacket, "id" | "from" | "timestamp">);

      // Wait directly on the value the heartbeat handler resets — checking
      // `info.heartbeat.last` is unreliable because it's initialized to
      // `Date.now()` at attach time and may collide with the test's clock
      // sample, satisfying the predicate before the handler runs.
      await vi.waitFor(() => expect(peer.info.heartbeat.missed).toBe(0));
      expect(peer.info.health.status).toBe(HeartbeatStatus.HEALTHY);
      expect(peer.info.health.metrics).toMatchObject({ uptime: 123 });
    });

    it("emits `peer-health` with the previous status when the worker reports a status change", async () => {
      const { peer, mock } = await attachOnePeer(broker);
      teardown.push(() => mock.close());

      const seen: Array<[PeerHandle, HeartbeatStatus]> = [];
      broker.on("peer-health", (h, prev) => seen.push([h, prev]));

      await mock.workerSession.send({
        kind: WireKind.SYSTEM,
        type: SystemMessageType.HEARTBEAT,
        payload: { status: HeartbeatStatus.DEGRADED },
      } satisfies Omit<SystemHeartbeatPacket, "id" | "from" | "timestamp">);

      await vi.waitFor(() => expect(seen.length).toBeGreaterThanOrEqual(1));
      expect(seen[0]).toEqual([peer, HeartbeatStatus.HEALTHY]);
      expect(peer.info.health.status).toBe(HeartbeatStatus.DEGRADED);
    });

    it("detaches a peer that has missed too many heartbeats", async () => {
      // Aggressive timing so the test runs quickly: each missed-heartbeat
      // window is `timeout`, and we need `maxMissed` of them before detach.
      const fast = new Broker({
        heartbeat: { interval: 100, timeout: 25, maxMissed: 2, checkInterval: 30 },
      });
      teardown.push(() => fast.shutdown());

      const mock = makeMockPeer();
      teardown.push(() => mock.close().catch(() => null));
      const [, peer] = await Promise.all([mock.workerSession.open(), fast.attach(mock.brokerSide)]);

      const events: Array<[PeerHandle, string?]> = [];
      fast.on("peer-disconnected", (h, r) => events.push([h, r]));

      await vi.waitFor(() => expect(fast.peer(peer.id)).toBeUndefined(), { timeout: 1000 });
      expect(events).toContainEqual([peer, "heartbeat missed"]);
    });
  });

  describe("session lifecycle integration", () => {
    it("keeps `Session` instances dedicated per peer (no cross-talk)", async () => {
      // Attach one, detach, attach a second to verify state isolation.
      const { peer: first, mock: m1 } = await attachOnePeer(broker);
      await broker.detach(first.id);
      // The old session must be torn down before we drop our reference.
      await vi.waitFor(() => expect(first.session.state).not.toBe("open"));
      await m1.close().catch(() => null);

      const { peer: second, mock: m2 } = await attachOnePeer(broker);
      teardown.push(() => m2.close().catch(() => null));

      expect(second.session).not.toBe(first.session);
      expect(second.session.state).toBe("open");
    });
  });

  describe("transport that fails handshake", () => {
    it("surfaces the handshake failure from `attach` without registering a peer", async () => {
      // Build a plain transport that never gets a peer — handshake will time out.
      const [tA] = pairTransports();
      const orphan = new Broker({ session: { handshakeTimeout: 50 } });
      teardown.push(() => orphan.shutdown());

      const err = await orphan.attach(tA).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.DEADLINE_EXCEEDED);
      expect(orphan.status.peers).toBe(0);
    });
  });
});
