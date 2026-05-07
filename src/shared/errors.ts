/**
 * The "layers" here aren't a DDD stack; errors travel across a thread boundary,
 * not up through abstractions. The design is therefore split on that axis into
 * local (in-process), wire (transferable), and boundary mappers (serialization).
 */

import { inspect } from "node:util";

import { WireStatus, type WireError } from "../interface/protocol";
import type { CorrelationId, TraceId } from "../interface/types";
import { isSerializable } from "../lib/helpers";

export type NonOkWireStatus = Exclude<WireStatus, typeof WireStatus.OK>;
export type RetryableWireStatus =
  | WireStatus.UNAVAILABLE
  | WireStatus.RESOURCE_EXHAUSTED
  | WireStatus.OVERLOADED
  | WireStatus.PEER_GONE;

const RETRYABLE_CODES = new Set<WireStatus>([
  WireStatus.UNAVAILABLE,
  WireStatus.RESOURCE_EXHAUSTED,
  WireStatus.OVERLOADED,
  WireStatus.PEER_GONE,
]);

export function isRetryableStatus(code: WireStatus): code is RetryableWireStatus {
  return RETRYABLE_CODES.has(code);
}

export interface TraceableErrorOptions {
  /** Structured, safe context. Non-serializable values are stripped at the wire boundary. */
  readonly detail?: Record<string, unknown>;
  /** Native cause chain; preserved locally, serialized at the wire boundary. */
  readonly cause?: unknown;
  /** The request this error belongs to, if applicable. */
  readonly correlationId?: CorrelationId;
  /** End-to-end trace identifier propagated via `RequestControl.trace`. */
  readonly traceId?: TraceId;
  /** Override the original stack (used when reconstructing from wire). */
  readonly stack?: string;
}

/** Maximum depth for the `cause` chain when serializing to/from wire. */
export const MAX_CAUSE_DEPTH = 3;

/**
 * The single in-process error class for quiry. Carries a structured status `code`
 * plus diagnostic context for both local handling and wire transport.
 */
export class QuiryError extends Error {
  readonly code: WireStatus;
  readonly retryable: boolean;

  readonly correlationId?: CorrelationId;
  readonly traceId?: TraceId;

  readonly detail?: Record<string, unknown>;
  override readonly cause?: unknown;

  constructor(code: WireStatus, message: string, opts: TraceableErrorOptions = {}) {
    super(message);

    this.name = "QuiryError";
    this.code = code;
    this.retryable = isRetryableStatus(code);
    this.correlationId = opts.correlationId;
    this.traceId = opts.traceId;
    this.detail = opts.detail;
    this.cause = opts.cause;

    // Hide the error constructor itself from the stack trace so callers
    // see their own call site at the top rather than framework internals.
    // Explicit `stack` overrides (used when rebuilding from wire) win.
    if (opts.stack) this.stack = opts.stack;
    else if (typeof Error.captureStackTrace === "function") {
      Error.captureStackTrace(this, new.target ?? QuiryError);
    }
  }

  /**
   * Normalize any thrown value into a {@link QuiryError}.
   *
   * If the input is already a {@link QuiryError}, it is augmented with the provided context
   * without losing its original code or cause chain. A native error is promoted
   * to {@link WireStatus.INTERNAL}, and its existing `cause` chain (if any) is preserved,
   * but the native error is **not** re-attached as its own cause. That avoids a phantom chain
   * of identical wrapper/wrappee pairs that serialize into noisy nested payloads.
   */
  static from(
    error: unknown,
    ctx: Pick<TraceableErrorOptions, "correlationId" | "traceId"> = {},
  ): QuiryError {
    if (error instanceof QuiryError) {
      // Only augment context; do not rewrap.
      if (ctx.correlationId && !error.correlationId) {
        return new QuiryError(error.code, error.message, {
          detail: error.detail,
          cause: error.cause,
          correlationId: ctx.correlationId,
          traceId: error.traceId ?? ctx.traceId,
          stack: error.stack,
        });
      }
      return error;
    }

    if (error instanceof Error) {
      // Promote the native error's own `cause` chain (if any) rather than
      // attaching the native error to itself — that avoids a self-referential
      // chain of identical error wrappers after serialization.
      return new QuiryError(WireStatus.INTERNAL, error.message, {
        ...ctx,
        cause: error.cause,
        stack: error.stack,
      });
    }

    return new QuiryError(WireStatus.INTERNAL, String(error), { ...ctx });
  }

  /**
   * Compact, human-readable representation for `console.log` / `util.inspect`.
   * Avoids re-inspecting the full cause chain (Node's default dumps each level
   * with source snippets, producing walls of near-duplicate output).
   */
  [inspect.custom](_depth: number, _opts: unknown, _inspectFn: typeof inspect): string {
    const meta: string[] = [`code=${WireStatus[this.code] ?? this.code}`];
    if (this.correlationId) meta.push(`ref=${this.correlationId}`);
    if (this.traceId) meta.push(`trace=${this.traceId}`);
    if (this.retryable) meta.push("retryable");

    const header = `\u001b[91m${this.name}: ${this.message} [${meta.join(", ")}]\u001b[39m`;
    const stack = this.stack?.split("\n").slice(1).join("\n") ?? "";
    const detail =
      this.detail && Object.keys(this.detail).length > 0
        ? `\n\tdetail: ${inspect(this.detail, { colors: false, depth: 2, breakLength: Infinity })}`
        : "";
    const cause = this.cause !== undefined ? `\n\tcaused by: ${describe(this.cause)}` : "";

    return `${header}${stack ? "\n" + stack : ""}${detail}${cause}`;
  }
}

function describe(cause: unknown): string {
  if (cause instanceof QuiryError) {
    const bits: string[] = [`${cause.name}: ${cause.message}`];
    bits.push(`(code=${WireStatus[cause.code] ?? cause.code})`);
    if (cause.cause !== undefined) bits.push(`\n\t\tcaused by: ${describe(cause.cause)}`);
    return bits.join(" ");
  }

  if (cause instanceof Error) {
    const nested = (cause as Error & { cause?: unknown }).cause;
    const head = `${cause.name}: ${cause.message}`;
    return nested !== undefined ? `${head}\n\t\tcaused by: ${describe(nested)}` : head;
  }

  return String(cause);
}

/**
 * Serialize a {@link QuiryError} (or any thrown value) into a transferable {@link WireError}.
 * Walks the `cause` chain up to `MAX_CAUSE_DEPTH`.
 *
 * Non-serializable values in `detail` are dropped so the result is always structured-clone-safe.
 */
export function toWireError(
  error: unknown,
  ctx: Pick<TraceableErrorOptions, "correlationId" | "traceId"> = {},
): WireError {
  const build = (err: QuiryError, depth: number): WireError => {
    const details = {
      status: (err.code === WireStatus.OK ? WireStatus.INTERNAL : err.code) as NonOkWireStatus,
      message: err.message,
      correlationId: err.correlationId,
      traceId: err.traceId,
      detail: err.detail ? sanitize(err.detail) : undefined,
      stack: err.stack,
      cause: undefined,
      timestamp: Date.now(),
    } satisfies WireError;

    if (depth + 1 >= MAX_CAUSE_DEPTH || err.cause === undefined) return details;

    // Promote whatever cause we have into an QuiryError and recurse. `QuiryError.from`
    // deliberately does NOT wrap a native error around itself, so the chain terminates
    // naturally once we hit a leaf cause with no further `cause` of its own.
    const nested = err.cause instanceof QuiryError ? err.cause : QuiryError.from(err.cause);
    return { ...details, cause: build(nested, depth + 1) };
  };

  const err = QuiryError.from(error, ctx);
  return build(err, 0);
}

/**
 * Best-effort structured-clone safety check. Drops keys whose values cannot
 * survive `postMessage` (functions, symbols, circular refs, etc.).
 */
function sanitize(detail: Record<string, unknown>): Record<string, unknown> {
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(detail)) {
    if (isSerializable(value)) output[key] = value;
  }
  return output;
}

/**
 * Reconstruct a {@link QuiryError} from a {@link WireError}.
 *
 * Rebuilds the cause chain as nested {@link QuiryError} instances.
 * The rebuilt error's `origin` is the remote node, so local log lines can be told aport
 * from the originating side. The remote `stack` is re-attached (with a header line identifying
 * the origin node) so the caller sees where the error was actually thrown, not where we rebuilt it.
 */
export function fromWireError(error: WireError): QuiryError {
  const cause = error.cause ? fromWireError(error.cause) : undefined;
  const stack = error.stack
    ? `${QuiryError.constructor.name}: ${error.message}\n\t[remote origin]\n${stripStackHeader(error.stack)}`
    : undefined;

  return new QuiryError(error.status, error.message, {
    correlationId: error.correlationId,
    traceId: error.traceId,
    detail: error.detail,
    cause,
    stack,
  });
}

/** Drop the leading `Error: <message>` line from a stack string; keep only the frames. */
function stripStackHeader(stack: string): string {
  const newline = stack.indexOf("\n");
  if (newline === -1) return "";
  return stack.slice(newline + 1);
}

/**
 * Capture the caller's synchronous stack at an async entry point. The returned
 * frames are intended to be attached to any error that eventually rejects out
 * of the async operation, so the consumer sees where *they* engaged with it —
 * not just where the error happened to be constructed on another thread.
 *
 * `skip` is the function whose frame (and everything below) should be omitted.
 */
export function captureCallerStack(skip?: Function): string {
  const holder: { stack?: string } = {};
  if (typeof Error.captureStackTrace === "function") {
    Error.captureStackTrace(holder, skip);
  } else holder.stack = new Error().stack;
  return stripStackHeader(holder.stack ?? "");
}

const CALLER_MARKER = "\t--- caller ---";

/**
 * Graft a pre-captured caller-site stack onto an error's own stack. Used so
 * async failures show both the remote origin frames AND the local engagement
 * point. Idempotent — a second call with the same frames is a no-op.
 */
export function attachCallerStack(error: unknown, callerFrames: string): void {
  if (!(error instanceof Error) || !callerFrames) return;
  const existing = error.stack ?? `${error.name}: ${error.message}`;
  if (existing.includes(CALLER_MARKER)) return;
  error.stack = `${existing}\n${CALLER_MARKER}\n${callerFrames}`;
}
