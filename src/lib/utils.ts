import { WireStatus } from "../protocol/wire";
import { QuiryError } from "../protocol/errors";

// Timing utilities

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** @throws {@link QuiryError} `ABORTED` when `signal` aborts; rejects immediately if already aborted. */
export function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal?.aborted) return Promise.reject(new QuiryError(WireStatus.ABORTED, "Operation was aborted"));

  return new Promise<T>((resolve, reject) => {
    const abortHandler = () => reject(new QuiryError(WireStatus.ABORTED, "Operation was aborted"));
    signal?.addEventListener("abort", abortHandler);

    promise
      .then(resolve)
      .catch(reject)
      .finally(() => {
        signal?.removeEventListener("abort", abortHandler);
      });
  });
}
