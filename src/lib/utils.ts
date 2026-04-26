import { QuiryError } from "./errors";
import { WireStatus } from "@/interface/base";
import type { AnyPacket } from "@/interface/packets";

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

// Helpers

export function isSerializable(value: unknown, seen = new WeakSet<object>()): boolean {
  if (value === null || value === undefined) return true;
  const t = typeof value;
  if (t === "string" || t === "number" || t === "boolean" || t === "bigint") return true;
  if (t === "function" || t === "symbol") return false;
  if (t === "object") {
    if (seen.has(value as object)) return false;
    seen.add(value as object);
    if (Array.isArray(value)) return value.every((v) => isSerializable(v, seen));
    // Accept plain objects only; typed arrays / ArrayBuffer / MessagePort
    // are transferable but not typically useful in error detail.
    const proto = Object.getPrototypeOf(value);
    if (proto !== Object.prototype && proto !== null) return false;
    return Object.values(value as Record<string, unknown>).every((v) => isSerializable(v, seen));
  }
  return false;
}

export function isPlainObject(value: unknown): value is object {
  return value !== null && typeof value === "object" && Object.getPrototypeOf(value) === null;
}

export function isWirePacket(value: unknown): value is AnyPacket {
  if (typeof value !== "object" || value === null) return false;
  const v = value as Record<string, unknown>;
  return (
    typeof v.id === "string" &&
    typeof v.kind === "string" &&
    typeof v.timestamp === "number" &&
    "payload" in v
  );
}

export function getMemoryUsage() {
  const usage = process.memoryUsage();
  return {
    heapUsed: usage.heapUsed,
    heapTotal: usage.heapTotal,
    external: usage.external,
    rss: usage.rss,
  };
}

/** A utility function that clips a string to a given length. Used for logging long IDs. */
export function clip(text: string, length: number = 8): string {
  return text.slice(0, length) + (text.length > length ? `[:${text.length - length}]` : "");
}
