import { Session, type InquiryFunc, type OmitStandardFields } from "@/core/session";
import { WireKind } from "@/interface/base";
import {
  SystemMessageType,
  type SystemIdentifyAckPacket,
  type SystemIdentifyPacket,
} from "@/interface/packets";

import { type MockTransport, pairTransports } from "./mock-transport";

export interface MockPeerOptions {
  readonly inquiry?: InquiryFunc;
  readonly label?: string;
  readonly skipIdentifyAck?: boolean;
}

export interface MockPeer {
  readonly brokerSide: MockTransport;
  readonly workerSession: Session;
  readonly identify: Promise<SystemIdentifyPacket>;
  readonly close: (graceful?: boolean) => Promise<void>;
}

/**
 * Builds a half-pair the {@link Broker} can attach to: a paired transport whose
 * worker-facing end is bound to a real {@link Session} that intercepts IDENTIFY
 * and (by default) replies with IDENTIFY_ACK.
 *
 * The interceptor is registered before the session is opened so that there's no
 * race against the broker's identify-send: as soon as the router begins routing
 * post-handshake, the IDENTIFY packet is consumed inline and the ACK is sent.
 */
export function makeMockPeer(options: MockPeerOptions = {}): MockPeer {
  const [brokerSide, workerSide] = pairTransports();
  const session = new Session(workerSide, options.inquiry);

  let resolve: (packet: SystemIdentifyPacket) => void = () => {};
  const identify = new Promise<SystemIdentifyPacket>((r) => (resolve = r));

  session.intercept(
    WireKind.SYSTEM,
    (p): p is SystemIdentifyPacket => p.type === SystemMessageType.IDENTIFY,
    (packet) => {
      resolve(packet);
      if (!options.skipIdentifyAck) {
        void session
          .send({
            kind: WireKind.SYSTEM,
            type: SystemMessageType.IDENTIFY_ACK,
            payload: { ref: packet.id, label: options.label },
          } satisfies OmitStandardFields<SystemIdentifyAckPacket>)
          .catch(() => null);
      }
      return true;
    },
  );

  return {
    brokerSide,
    workerSession: session,
    identify,
    close: async (graceful: boolean = true) => {
      await session.close("explicit", graceful).catch(() => null);
    },
  };
}

export interface MockHostOptions {
  readonly inquiry?: InquiryFunc;
  readonly heartbeatInterval?: number;
  readonly skipIdentify?: boolean;
}

export interface MockHost {
  readonly workerSide: MockTransport;
  readonly hostSession: Session;
  readonly identifyAck: Promise<unknown>;
  readonly open: (label?: string) => Promise<void>;
  readonly close: (graceful?: boolean) => Promise<void>;
}

/**
 * Inverts {@link makeMockPeer}: a host-side {@link Session} that performs the
 * Broker's identify protocol so a real {@link Worker} can be opened against it.
 *
 * Call `open()` concurrently with `worker.open()` to avoid handshake deadlock.
 */
export function makeMockHost(options: MockHostOptions = {}): MockHost {
  const [workerSide, hostSide] = pairTransports();
  const session = new Session(hostSide, options.inquiry);

  let ackResolve: (packet: unknown) => void = () => {};
  const identifyAck = new Promise<unknown>((r) => (ackResolve = r));

  session.intercept(
    WireKind.SYSTEM,
    (p): p is SystemIdentifyAckPacket => p.type === SystemMessageType.IDENTIFY_ACK,
    (packet) => {
      ackResolve(packet);
      return true;
    },
  );

  return {
    workerSide,
    hostSession: session,
    identifyAck,
    open: async () => {
      await session.open();
      if (options.skipIdentify) return;
      await session.send({
        kind: WireKind.SYSTEM,
        type: SystemMessageType.IDENTIFY,
        payload: { heartbeatInterval: options.heartbeatInterval },
      } satisfies OmitStandardFields<SystemIdentifyPacket>);
    },
    close: async (graceful: boolean = true) => {
      await session.close("explicit", graceful).catch(() => null);
    },
  };
}
