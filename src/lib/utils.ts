import { QuiryError } from "./errors";
import { WireStatus } from "@/interface/base";

// Timing utilities

export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Abort a promise after a timeout. */
export function timeout<T>(promise: Promise<T>, ms: number, message?: string): Promise<T> {
  return Promise.race([
    promise,
    delay(ms).then(() => {
      throw new QuiryError(WireStatus.DEADLINE_EXCEEDED, message ?? `Operation timed out after ${ms}ms`);
    }),
  ]);
}

export function retryable<T>(
  fn: () => Promise<T>,
  options: {
    retries?: number;
    initialDelay?: number;
    backoffStrategy?: "linear" | "exponential";
    shouldRetry?: (error: Error) => boolean;
    /**
     * Aborting interrupts an active backoff window — the next attempt is
     * dispatched immediately and is expected to observe the signal and fail
     * through its own error path. The signal does not synthesize a rejection
     * by itself, so the caller's chosen abort error semantics are preserved.
     */
    signal?: AbortSignal;
  } = {},
): Promise<T> {
  const {
    retries = 3,
    initialDelay = 1000,
    backoffStrategy = "exponential",
    shouldRetry = () => true,
    signal,
  } = options;

  return new Promise<T>((resolve, reject) => {
    let attempt = 0;
    let timer: ReturnType<typeof setTimeout> | null = null;
    let abortHandler: (() => void) | null = null;

    const detach = (): void => {
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      if (abortHandler && signal) {
        signal.removeEventListener("abort", abortHandler);
        abortHandler = null;
      }
    };

    const tryOnce = async (): Promise<void> => {
      try {
        const result = await fn();
        detach();
        resolve(result);
      } catch (error: unknown) {
        attempt++;
        if (signal?.aborted || attempt > retries || !shouldRetry(error as Error)) {
          detach();
          return reject(error);
        }

        timer = setTimeout(
          () => {
            timer = null;
            void tryOnce();
          },
          backoffStrategy === "exponential"
            ? initialDelay * Math.pow(2, attempt - 1)
            : initialDelay * attempt,
        );
      }
    };

    if (signal) {
      abortHandler = () => {
        // If a backoff is in flight, jump to the next attempt immediately
        // so it can observe the aborted signal through `fn` and reject. If
        // no timer is active, an attempt is currently running; its own
        // abort handling will surface the cancellation.
        if (timer) {
          clearTimeout(timer);
          timer = null;
          void tryOnce();
        }
      };
      signal.addEventListener("abort", abortHandler);
    }

    void tryOnce();
  });
}

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
