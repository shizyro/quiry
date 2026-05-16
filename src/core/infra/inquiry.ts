/**
 * Producer-side dispatch contract. The session calls {@link InquiryFunc}
 * for every inbound REQUEST packet and translates the returned descriptor
 * into the appropriate response (value, stream, or error).
 */
export type InquiryFunc = (request: InquiryRequest) => InquiryDescriptor;

export interface InquiryRequest {
  readonly service: string;
  readonly property: string;
}

/**
 * Unified descriptor for reading and writing a resolved property.
 *
 * This descriptor intentionally presents a normalized interface rather than
 * mirroring native JavaScript property descriptors. In particular, `get` and
 * `set` are always provided, even when the underlying property is a data
 * property, accessor, method, or inherited member.
 */
export interface InquiryDescriptor<T = unknown> {
  value: T;
  get: () => T;
  set: (value: T) => void;
  enumerable: boolean;
  writable: boolean;
}
