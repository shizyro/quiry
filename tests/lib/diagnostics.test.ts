import { subscribe as dcSubscribe, unsubscribe as dcUnsubscribe } from "node:diagnostics_channel";
import { randomBytes } from "node:crypto";

import { DiagnosticBus } from "~/lib/diagnostics";

type Events = {
  ping: { readonly value: number };
  pong: { readonly text: string };
  empty: Record<string, never>;
};

/** Fresh prefix per call so dc channels don't bleed across tests. */
function uniquePrefix(): string {
  return `test::${randomBytes(3).toString("hex")}`;
}

describe("diagnostics bus", () => {
  describe("local fan-out", () => {
    it("has() returns false when nothing is attached", () => {
      const bus = new DiagnosticBus<Events>();
      expect(bus.has("ping")).toBe(false);
      expect(bus.listenerCount("ping")).toBe(0);
    });

    it("on() delivers the payload to the registered listener", () => {
      const bus = new DiagnosticBus<Events>();
      const seen: Events["ping"][] = [];

      bus.on("ping", (p) => seen.push(p));
      bus.emit("ping", { value: 1 });
      bus.emit("ping", { value: 2 });

      expect(seen).toEqual([{ value: 1 }, { value: 2 }]);
    });

    it("fan-out invokes every listener in registration order", () => {
      const bus = new DiagnosticBus<Events>();
      const order: string[] = [];

      bus.on("ping", () => order.push("a"));
      bus.on("ping", () => order.push("b"));
      bus.on("ping", () => order.push("c"));
      bus.emit("ping", { value: 0 });

      expect(order).toEqual(["a", "b", "c"]);
    });

    it("has() flips true once a listener is attached and false again after detach", () => {
      const bus = new DiagnosticBus<Events>();
      const unsubscribe = bus.on("ping", () => {});

      expect(bus.has("ping")).toBe(true);
      expect(bus.listenerCount("ping")).toBe(1);

      unsubscribe();
      expect(bus.has("ping")).toBe(false);
      expect(bus.listenerCount("ping")).toBe(0);
    });

    it("off() detaches the listener", () => {
      const bus = new DiagnosticBus<Events>();
      const fn = vi.fn();

      bus.on("ping", fn);
      bus.off("ping", fn);
      bus.emit("ping", { value: 0 });

      expect(fn).not.toHaveBeenCalled();
    });

    it("off() for an unknown listener is a no-op", () => {
      const bus = new DiagnosticBus<Events>();
      expect(() => bus.off("ping", () => {})).not.toThrow();
    });

    it("isolated event names", () => {
      const bus = new DiagnosticBus<Events>();
      const onPing = vi.fn();
      const onPong = vi.fn();

      bus.on("ping", onPing);
      bus.on("pong", onPong);
      bus.emit("ping", { value: 7 });

      expect(onPing).toHaveBeenCalledTimes(1);
      expect(onPong).not.toHaveBeenCalled();
    });
  });

  describe("once", () => {
    it("fires the listener exactly once and detaches itself", () => {
      const bus = new DiagnosticBus<Events>();
      const fn = vi.fn();

      bus.once("ping", fn);
      bus.emit("ping", { value: 1 });
      bus.emit("ping", { value: 2 });

      expect(fn).toHaveBeenCalledTimes(1);
      expect(fn).toHaveBeenCalledWith({ value: 1 });
      expect(bus.listenerCount("ping")).toBe(0);
    });

    it("can be cancelled before it fires", () => {
      const bus = new DiagnosticBus<Events>();
      const fn = vi.fn();

      const cancel = bus.once("ping", fn);
      cancel();
      bus.emit("ping", { value: 1 });

      expect(fn).not.toHaveBeenCalled();
    });

    it("a once handler that throws still detaches itself", () => {
      const bus = new DiagnosticBus<Events>();
      const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
      try {
        bus.once("ping", () => {
          throw new Error("boom");
        });

        // The first emit fires-and-detaches; the second must find no listener.
        bus.emit("ping", { value: 1 });
        bus.emit("ping", { value: 2 });

        expect(bus.listenerCount("ping")).toBe(0);
        expect(warn).toHaveBeenCalledTimes(1);
      } finally {
        warn.mockRestore();
      }
    });
  });

  describe("listener error isolation", () => {
    it("a throwing listener does not stop sibling listeners", () => {
      const bus = new DiagnosticBus<Events>();
      const warn = vi.spyOn(process, "emitWarning").mockImplementation(() => {});
      const before = vi.fn();
      const after = vi.fn();

      try {
        bus.on("ping", before);
        bus.on("ping", () => {
          throw new Error("middle threw");
        });
        bus.on("ping", after);

        bus.emit("ping", { value: 1 });

        expect(before).toHaveBeenCalledTimes(1);
        expect(after).toHaveBeenCalledTimes(1);
        expect(warn).toHaveBeenCalledTimes(1);
        expect(warn.mock.calls[0]![0]).toMatch(/Diagnostic listener for "ping" threw: middle threw/);
      } finally {
        warn.mockRestore();
      }
    });
  });

  describe("clear", () => {
    it("removes every local listener across every event", () => {
      const bus = new DiagnosticBus<Events>();
      const onPing = vi.fn();
      const onPong = vi.fn();

      bus.on("ping", onPing);
      bus.on("pong", onPong);
      bus.clear();
      bus.emit("ping", { value: 0 });
      bus.emit("pong", { text: "x" });

      expect(onPing).not.toHaveBeenCalled();
      expect(onPong).not.toHaveBeenCalled();
      expect(bus.listenerCount("ping")).toBe(0);
      expect(bus.listenerCount("pong")).toBe(0);
    });
  });

  describe("maybe (gated emit)", () => {
    it("returns undefined when nobody is listening", () => {
      const bus = new DiagnosticBus<Events>();
      expect(bus.maybe("ping")).toBeUndefined();
    });

    it("returns a working emitter when at least one local listener is attached", () => {
      const bus = new DiagnosticBus<Events>();
      const fn = vi.fn();

      bus.on("ping", fn);
      const emit = bus.maybe("ping");
      expect(emit).toBeTypeOf("function");
      emit!({ value: 7 });

      expect(fn).toHaveBeenCalledWith({ value: 7 });
    });

    it("the optional-chain pattern does not evaluate the payload when off", () => {
      const bus = new DiagnosticBus<Events>();
      let payloadBuilds = 0;
      const buildPayload = (): Events["ping"] => {
        payloadBuilds++;
        return { value: 1 };
      };

      bus.maybe("ping")?.(buildPayload());
      expect(payloadBuilds).toBe(0);

      bus.on("ping", () => {});
      bus.maybe("ping")?.(buildPayload());
      expect(payloadBuilds).toBe(1);
    });

    it("returns the same cached function across repeated calls", () => {
      const bus = new DiagnosticBus<Events>();
      bus.on("ping", () => {});

      const a = bus.maybe("ping");
      const b = bus.maybe("ping");
      expect(a).toBe(b);
    });

    it("caches per-event-name independently", () => {
      const bus = new DiagnosticBus<Events>();
      bus.on("ping", () => {});
      bus.on("pong", () => {});

      const emitPing = bus.maybe("ping");
      const emitPong = bus.maybe("pong");
      expect(emitPing).not.toBe(emitPong);
    });

    it("returns undefined again once every listener detaches", () => {
      const bus = new DiagnosticBus<Events>();
      const off = bus.on("ping", () => {});

      expect(bus.maybe("ping")).toBeTypeOf("function");
      off();
      expect(bus.maybe("ping")).toBeUndefined();
    });

    it("a hoisted emitter still delivers to listeners attached after maybe()", () => {
      // The cached emitter routes through `emit()`, which always re-checks
      // the live observer set. So a long-lived emitter handle stays correct
      // across subscription churn.
      const bus = new DiagnosticBus<Events>();
      const first = vi.fn();
      const second = vi.fn();

      bus.on("ping", first);
      const emit = bus.maybe("ping")!;
      bus.on("ping", second);
      emit({ value: 3 });

      expect(first).toHaveBeenCalledWith({ value: 3 });
      expect(second).toHaveBeenCalledWith({ value: 3 });
    });

    it("flips on for dc subscribers without any local listener", () => {
      const prefix = uniquePrefix();
      const bus = new DiagnosticBus<Events>(prefix);
      const received: unknown[] = [];
      const handler = (m: unknown) => received.push(m);

      expect(bus.maybe("ping")).toBeUndefined();

      dcSubscribe(`${prefix}:ping`, handler);
      try {
        const emit = bus.maybe("ping");
        expect(emit).toBeTypeOf("function");
        emit!({ value: 5 });
      } finally {
        dcUnsubscribe(`${prefix}:ping`, handler);
      }

      expect(received).toEqual([{ value: 5 }]);
    });
  });

  describe("payload typing at runtime", () => {
    it("delivers the same object reference (no defensive copy)", () => {
      const bus = new DiagnosticBus<Events>();
      const payload = { value: 42 };
      let received: Events["ping"] | null = null;

      bus.on("ping", (p) => {
        received = p;
      });
      bus.emit("ping", payload);

      // Bus is a thin fan-out; receivers see the same reference.
      // dc subscribers across thread boundaries get structured-cloned copies,
      // but in-process listeners do not. Documented contract.
      expect(received).toBe(payload);
    });

    it("supports an empty payload shape", () => {
      const bus = new DiagnosticBus<Events>();
      const fn = vi.fn();

      bus.on("empty", fn);
      bus.emit("empty", {});

      expect(fn).toHaveBeenCalledWith({});
    });
  });

  describe("node:diagnostics_channel bridge", () => {
    it("has() reflects a dc subscriber even when no local listeners are attached", () => {
      const prefix = uniquePrefix();
      const bus = new DiagnosticBus<Events>(prefix);
      const handler = () => {};

      expect(bus.has("ping")).toBe(false);
      dcSubscribe(`${prefix}:ping`, handler);
      try {
        expect(bus.has("ping")).toBe(true);
      } finally {
        dcUnsubscribe(`${prefix}:ping`, handler);
      }

      expect(bus.has("ping")).toBe(false);
    });

    it("emit() publishes payloads on the bridged dc channel", () => {
      const prefix = uniquePrefix();
      const bus = new DiagnosticBus<Events>(prefix);
      const received: unknown[] = [];
      const handler = (message: unknown) => received.push(message);

      dcSubscribe(`${prefix}:ping`, handler);
      try {
        bus.emit("ping", { value: 1 });
        bus.emit("ping", { value: 2 });
      } finally {
        dcUnsubscribe(`${prefix}:ping`, handler);
      }

      expect(received).toEqual([{ value: 1 }, { value: 2 }]);
    });

    it("delivers to both local listeners and dc subscribers", () => {
      const prefix = uniquePrefix();
      const bus = new DiagnosticBus<Events>(prefix);
      const local: unknown[] = [];
      const remote: unknown[] = [];
      const handler = (message: unknown) => remote.push(message);

      bus.on("ping", (p) => local.push(p));
      dcSubscribe(`${prefix}:ping`, handler);
      try {
        bus.emit("ping", { value: 9 });
      } finally {
        dcUnsubscribe(`${prefix}:ping`, handler);
      }

      expect(local).toEqual([{ value: 9 }]);
      expect(remote).toEqual([{ value: 9 }]);
    });

    it("a throwing dc subscriber does not affect local listeners", async () => {
      // dc's contract: subscriber throws are re-thrown on `process.nextTick`,
      // which surfaces as an uncaught exception. We don't try to suppress
      // that — it's how external dc consumers learn about their own bugs.
      // But we DO guarantee the synchronous fan-out is unaffected: every
      // local listener fires, and the bus does not propagate the error to
      // the caller of `emit`.
      const prefix = uniquePrefix();
      const bus = new DiagnosticBus<Events>(prefix);
      const local = vi.fn();
      const handler = () => {
        throw new Error("dc subscriber blew up");
      };

      // Swallow the next-tick rethrow so the test runner doesn't fail on it.
      const swallow = (err: unknown): void => {
        if (!(err instanceof Error) || err.message !== "dc subscriber blew up") throw err;
      };
      process.on("uncaughtException", swallow);

      bus.on("ping", local);
      dcSubscribe(`${prefix}:ping`, handler);
      try {
        expect(() => bus.emit("ping", { value: 0 })).not.toThrow();
        // Yield a tick so dc can surface its re-throw before we tear down.
        await new Promise<void>((resolve) => setImmediate(resolve));
      } finally {
        dcUnsubscribe(`${prefix}:ping`, handler);
        process.off("uncaughtException", swallow);
      }

      expect(local).toHaveBeenCalledTimes(1);
      expect(local).toHaveBeenCalledWith({ value: 0 });
    });

    it("bus without a prefix never touches diagnostics_channel", () => {
      const bus = new DiagnosticBus<Events>(); // no prefix
      const handler = vi.fn();

      // Subscribe to a name we know the bus is NOT bridging to. The bus has no
      // prefix, so it cannot publish anywhere on dc. The dc subscriber stays
      // silent.
      dcSubscribe("quiry-test-unused:ping", handler);
      try {
        bus.emit("ping", { value: 1 });
      } finally {
        dcUnsubscribe("quiry-test-unused:ping", handler);
      }

      expect(handler).not.toHaveBeenCalled();
    });
  });
});
