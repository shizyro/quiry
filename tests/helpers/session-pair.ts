import { Session, type InquiryFunc, type SessionConfig } from "@/core/session";
import { pairTransports } from "./mock-transport";

export type SessionPair = { producer: Session; consumer: Session; close: (force?: boolean) => Promise<void> };

/**
 * Opens a pair of peered {@link Session} instances linked via the in-memory
 * {@link MockTransport}. Both sessions complete their handshakes before the
 * pair is returned.
 *
 * The caller provides the inquiry handlers for each side; the "producer" side
 * typically hosts a service that returns an async iterator, the "consumer"
 * side is the one that calls `.stream(...)`.
 */
export async function openSessionPair(options: {
  producerInquiry?: InquiryFunc;
  consumerInquiry?: InquiryFunc;
  config?: Partial<SessionConfig>;
}): Promise<SessionPair> {
  const [tA, tB] = pairTransports();

  const producer = new Session(tA, options.producerInquiry, {
    ...options.config,
  });
  const consumer = new Session(tB, options.consumerInquiry, {
    ...options.config,
  });

  // Both sessions must call `open()` concurrently — each one sends a
  // handshake packet and waits for the peer's, so sequential opens would
  // deadlock waiting on a handshake the other side hasn't sent yet.
  await Promise.all([producer.open(), consumer.open()]);

  return {
    producer,
    consumer,
    close: async (graceful: boolean = true) => {
      // Close sequentially so each session transitions to its own
      // "closed" state before its transport emits the state-change
      // event — avoids the session interpreting its own close as a
      // peer-initiated crash.
      await producer.close("explicit", graceful).catch(() => {});
      await consumer.close("explicit", graceful).catch(() => {});
    },
  };
}
