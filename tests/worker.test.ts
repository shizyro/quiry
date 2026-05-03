import { Worker } from "@/core/client";
import { Session, type InquiryFunc, type InquiryRequest } from "@/core/session";

import { WireKind, WireStatus } from "@/interface/base";
import { SystemMessageType, type AnyPacket, type SystemHeartbeatPacket } from "@/interface/packets";
import { QuiryError } from "@/shared/errors";

import { type MockTransport, pairTransports } from "./helpers/mock-transport";
import { makeMockHost, type MockHost } from "./helpers/peer";

describe("The worker class", () => {
  let teardown: Array<() => Promise<any> | any> = [];

  /** Open a worker against a freshly built mock host. Both sides run concurrently to avoid handshake deadlock. */
  async function openWorker(
    options: {
      workerConfig?: ConstructorParameters<typeof Worker>[1];
      hostInquiry?: InquiryFunc;
      heartbeatInterval?: number;
    } = {},
  ): Promise<{ worker: Worker<any>; host: MockHost }> {
    const host = makeMockHost({
      inquiry: options.hostInquiry,
      heartbeatInterval: options.heartbeatInterval,
    });

    const worker = new Worker(host.workerSide, options.workerConfig);
    await Promise.all([worker.open(), host.open()]);

    teardown.push(() => worker.shutdown());
    teardown.push(() => host.close());

    return { worker, host };
  }

  beforeEach(() => {
    teardown = [];
  });

  afterEach(async () => {
    for (const fn of teardown) await Promise.resolve(fn()).catch(() => null);
  });

  describe("open flow", () => {
    it("sends an IDENTIFY_ACK referencing the host's IDENTIFY", async () => {
      const host = makeMockHost();
      teardown.push(() => host.close());
      const worker = new Worker(host.workerSide, { label: "worker-1" });
      teardown.push(() => worker.shutdown());

      const [, ack] = await Promise.all([Promise.all([worker.open(), host.open()]), host.identifyAck]);

      const packet = ack as { payload: { ref: string; label?: string } };
      expect(packet.payload.label).toBe("worker-1");
      expect(packet.payload.ref).toEqual(expect.any(String));
    });

    it("emits `host-connected` once the handshake completes", async () => {
      const host = makeMockHost();
      teardown.push(() => host.close());

      const worker = new Worker(host.workerSide);
      teardown.push(() => worker.shutdown());

      const seen: Array<{ id: string }> = [];
      worker.on("host-connected", (h) => seen.push({ id: h.id }));

      await Promise.all([worker.open(), host.open()]);
      expect(seen).toHaveLength(1);
    });
  });

  describe("heartbeat", () => {
    it("starts a periodic heartbeat using the host's advertised interval", async () => {
      const setSpy = vi.spyOn(global, "setInterval");
      await openWorker({ heartbeatInterval: 4321 });

      // Find the call whose delay matches what the host advertised.
      const found = setSpy.mock.calls.find(([, delay]) => delay === 4321);
      expect(found).toBeDefined();
      setSpy.mockRestore();
    });

    it("emits HEARTBEAT packets carrying the configured metrics payload", async () => {
      const beats: SystemHeartbeatPacket[] = [];
      const host = makeMockHost();
      host.hostSession.intercept(
        WireKind.SYSTEM,
        (p): p is SystemHeartbeatPacket => p.type === SystemMessageType.HEARTBEAT,
        (packet) => {
          beats.push(packet);
          return true;
        },
      );

      const worker = new Worker(host.workerSide, {
        heartbeat: {
          intervalOverride: 30,
          metrics: () => ({ uptime: 7, custom: { ticks: 1 } }),
        },
      });
      teardown.push(() => worker.shutdown());
      teardown.push(() => host.close());

      await Promise.all([worker.open(), host.open()]);
      await vi.waitFor(() => expect(beats.length).toBeGreaterThanOrEqual(1), { timeout: 1500 });
      expect(beats[0]!.payload.metrics).toMatchObject({ uptime: 7, custom: { ticks: 1 } });
    });
  });

  describe("calls & streams", () => {
    it("`call` forwards args verbatim to `Session.request`", async () => {
      const { worker } = await openWorker({
        hostInquiry: async (req: InquiryRequest) => ({ s: req.service, m: req.method, a: req.args }),
      });

      await expect(worker.call("svc", "do", "x", 1)).resolves.toEqual({
        s: "svc",
        m: "do",
        a: ["x", 1],
      });
    });

    it("`call` peels a trailing object with `timeout`/`retries`/`signal` as `RequestControl`", async () => {
      const { worker } = await openWorker({
        hostInquiry: async (req: InquiryRequest) => req.args,
      });

      // The `opts` object is NOT a positional arg — the splitter peels it.
      await expect(worker.call("svc", "fn", "a", "b", { timeout: 1234 })).resolves.toEqual(["a", "b"]);
      // ...but a final non-control object stays positional.
      await expect(worker.call("svc", "fn", { not: "control" })).resolves.toEqual([{ not: "control" }]);
    });

    it("`stream` returns an iterable iterator over remote chunks", async () => {
      const { worker } = await openWorker({
        hostInquiry: async function* (req: InquiryRequest) {
          for (const v of req.args as number[]) yield v * 10;
        },
      });

      const chunks: number[] = [];
      for await (const chunk of worker.stream("svc", "iter", 1, 2, 3)) {
        chunks.push(chunk as number);
      }
      expect(chunks).toEqual([10, 20, 30]);
    });

    it("propagates remote errors to `call`", async () => {
      const { worker } = await openWorker({
        hostInquiry: async () => {
          throw new QuiryError(WireStatus.NOT_FOUND, "no such record");
        },
      });

      const err = await worker.call("svc", "fail", { retry: { maxAttempts: 0 } }).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(QuiryError);
      expect((err as QuiryError).code).toBe(WireStatus.NOT_FOUND);
    });
  });

  describe("service proxy", () => {
    it("awaits → unary call (CALL path)", async () => {
      const { worker } = await openWorker({
        hostInquiry: async (req: InquiryRequest) => ({ s: req.service, m: req.method }),
      });

      type Reg = { svc: { ping(): Promise<{ s: string; m: string }> } };
      const proxy = (worker as Worker<Reg>).service("svc");
      await expect(proxy.ping()).resolves.toEqual({ s: "svc", m: "ping" });
    });

    it("iterates → server-stream (STREAM path)", async () => {
      const { worker } = await openWorker({
        hostInquiry: async function* (_req: InquiryRequest) {
          yield "a";
          yield "b";
          yield "c";
        },
      });

      const chunks: string[] = [];
      for await (const chunk of (worker as Worker<{ svc: { tail(): AsyncIterableIterator<string> } }>)
        .service("svc")
        .tail())
        chunks.push(chunk);
      expect(chunks).toEqual(["a", "b", "c"]);
    });

    it("commits to one mode — awaiting after iterating throws", async () => {
      const { worker } = await openWorker({
        hostInquiry: async function* () {
          yield 1;
        },
      });

      const handle = (worker as Worker<{ svc: { dual(): AsyncIterableIterator<number> } }>)
        .service("svc")
        .dual();

      // Engage as a stream.
      const iter = handle[Symbol.asyncIterator]();
      await iter.next();

      // Now awaiting must throw — already committed to STREAM.
      expect(() => (handle as unknown as Promise<unknown>).then(() => null)).toThrow(
        /already been committed as a stream/,
      );
      await iter.return?.();
    });

    it("commits to one mode — iterating after awaiting throws", async () => {
      const { worker } = await openWorker({ hostInquiry: async () => 42 });

      const handle = (worker as Worker<{ svc: { dual(): Promise<number> } }>).service("svc").dual();
      await handle;

      expect(() => (handle as unknown as AsyncIterableIterator<number>)[Symbol.asyncIterator]()).toThrow(
        /already been committed as a unary call/,
      );
    });

    it("auto-commits to call mode when the handle is dropped without engagement", async () => {
      const calls: InquiryRequest[] = [];
      const { worker } = await openWorker({
        hostInquiry: async (req: InquiryRequest) => {
          calls.push(req);
          return null;
        },
      });

      const service = (worker as Worker<{ svc: { fire(arg: string): Promise<null> } }>).service("svc");
      service.fire("yo"); // not awaited, not iterated

      // Microtask flushes the auto-trigger; give the wire a beat.
      await vi.waitFor(() => expect(calls).toHaveLength(1));
      expect(calls[0]!.method).toBe("fire");
      expect(calls[0]!.args).toEqual(["yo"]);
    });
  });

  describe("controlled services", () => {
    it("applies the `RequestControl` to all calls made through the returned proxy", async () => {
      const seen: InquiryRequest[] = [];
      const { worker } = await openWorker({
        hostInquiry: async (req: InquiryRequest) => {
          seen.push(req);
          return null;
        },
      });

      const proxy = (worker as Worker<{ svc: { go(): Promise<null> } }>).controlled("svc", {
        traceId: "trace-xyz" as never,
      });

      await proxy.go();
      expect(seen).toHaveLength(1);
      expect(seen[0]!.control?.traceId).toBe("trace-xyz");
    });
  });

  describe("callback handles", () => {
    it("returns a function that retains the original call signature", async () => {
      const { worker } = await openWorker();

      const handle = worker.callback((x: number) => x + 1);
      expect(typeof handle).toBe("function");
      expect(handle(2)).toBe(3);
    });

    it("`release()` and `Symbol.dispose` both detach the callback from the session registry", async () => {
      const { worker } = await openWorker();
      const session = worker.host!.session;

      const a = worker.callback(() => "a");
      expect(session.status.callbacks).toBe(1);
      expect(a.release()).toBe(true);
      expect(session.status.callbacks).toBe(0);
      // Subsequent release calls return `false` (already released).
      expect(a.release()).toBe(false);

      const b = worker.callback(() => "b");
      expect(session.status.callbacks).toBe(1);
      b[Symbol.dispose]();
      expect(session.status.callbacks).toBe(0);
    });

    it("automatic release on scope exit (TC39 `using` keyword)", async () => {
      const { worker } = await openWorker({
        hostInquiry: async ({ args }) => {
          const cb = args[0] as () => Promise<string>;
          return await cb();
        },
      });
      const session = worker.host!.session;

      {
        using callback = worker.callback(() => "ok");
        await expect(worker.call("svc", "cb", callback)).resolves.toBe("ok");
        expect(session.status.callbacks).toBe(1);
      }
      expect(session.status.callbacks).toBe(0);
    });
  });

  describe("shutdown", () => {
    it("closes the session, stops the heartbeat, and emits `shutdown`", async () => {
      const clearSpy = vi.spyOn(global, "clearInterval");
      const { worker, host } = await openWorker();
      teardown.push(() => host.close().catch(() => null));

      const reasons: Array<string | undefined> = [];
      worker.on("shutdown", (r) => reasons.push(r));

      await worker.shutdown("test-reason");

      expect(reasons).toEqual(["test-reason"]);
      expect(clearSpy).toHaveBeenCalled();
      expect(worker.host?.session.state).not.toBe("open");
      clearSpy.mockRestore();
    });
  });

  describe("host disconnect", () => {
    it("emits `host-disconnected` when the host-side session drains", async () => {
      const { worker, host } = await openWorker();
      teardown.push(() => worker.shutdown().catch(() => null));

      const seen: Array<unknown> = [];
      worker.on("host-disconnected", (h, r) => seen.push({ id: h.id, reason: r }));

      // Graceful close so the DRAIN packet propagates and the worker side
      // actually transitions to terminated. (MockTransport doesn't notify
      // its peer on a force-close.)
      await host.close(true);
      await vi.waitFor(() => expect(seen.length).toBeGreaterThanOrEqual(1));
    });
  });
});
