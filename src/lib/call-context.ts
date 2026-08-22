import { AsyncLocalStorage } from "node:async_hooks";

/**
 * Ambient context for the single producer-side call currently executing
 * on this async continuation chain.
 */
export interface CallContext {
  /** Fires if the caller peer aborts the request. */
  readonly signal: AbortSignal;
}

export const contextStorage: AsyncLocalStorage<CallContext> = new AsyncLocalStorage();
