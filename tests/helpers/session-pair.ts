import {
  type InquiryDescriptor,
  Session,
  type InquiryFunc,
  type SessionConfig,
  type InquiryRequest,
} from "~/core/session";
import { pairTransports } from "./mock-transport";

export type SessionPair = { producer: Session; consumer: Session; close: (force?: boolean) => Promise<void> };

export const defaultInquiryDescriptor = (overrides: Partial<InquiryDescriptor> = {}): InquiryDescriptor => ({
  value: undefined,
  get: () => undefined,
  set: () => undefined,
  enumerable: false,
  writable: false,
  ...overrides,
});

export type MockInquiryFunc = (request: InquiryRequest) => Partial<InquiryDescriptor>;

/**
 * Opens a pair of peered {@link Session} instances linked via the in-memory
 * {@link MockTransport}.
 *
 * The caller provides the inquiry handlers for each side; the "producer" side
 * typically hosts a service that returns an async iterator, the "consumer"
 * side is the one that calls `.stream(...)`.
 */
export function openSessionPair(
  options: {
    producerInquiry?: MockInquiryFunc;
    consumerInquiry?: MockInquiryFunc;
    config?: Partial<SessionConfig>;
  } = {},
): SessionPair {
  const [tA, tB] = pairTransports();

  const producer = new Session(
    tA,
    (request) => defaultInquiryDescriptor(options.producerInquiry?.(request)),
    {
      ...options.config,
    },
  );
  const consumer = new Session(
    tB,
    (request) => defaultInquiryDescriptor(options.consumerInquiry?.(request)),
    {
      ...options.config,
    },
  );

  producer.open();
  consumer.open();

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
