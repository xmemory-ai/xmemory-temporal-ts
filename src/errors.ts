/**
 * Translate xmemory API errors into Temporal failures.
 *
 * Temporal owns retries, so this is the one place an `XmemoryAPIError` becomes an
 * `ApplicationFailure` with a retryability verdict.
 *
 * Rules: branch on `.code`, not the HTTP status; an unrecognized code never raises
 * and keeps its own type, so a newer server cannot break this client mid-deploy,
 * while whether to retry it comes from the status — a 401 or 404 is terminal
 * whatever the code says; never echo the raw exception string, which can embed
 * internal hostnames.
 */

import { ApplicationFailure } from '@temporalio/common';
import { XmemoryAPIError } from 'xmemory';
import { applicationFailure, ownOnly } from './defaults';
import {
  TYPE_AUTH_FAILED,
  TYPE_BAD_OPTIONS,
  TYPE_BAD_REQUEST,
  TYPE_DAILY_QUOTA_EXCEEDED,
  TYPE_DEADLINE_EXPIRED,
  TYPE_MONTHLY_QUOTA_EXCEEDED,
  TYPE_NOT_BOUND,
  TYPE_NOT_FOUND,
  TYPE_NO_DEADLINE,
  TYPE_QUOTA_EXCEEDED,
  TYPE_RATE_LIMITED,
  TYPE_SCHEMA_REJECTED,
  TYPE_SERVER_ERROR,
  TYPE_UNAVAILABLE,
  TYPE_UNKNOWN,
  TYPE_WRITE_FAILED,
  TYPE_WRITE_NOT_FOUND,
  TYPE_WRITE_TIMEOUT,
} from './names';

// Re-exported from the client-free leaf. They are a public contract for
// `RetryPolicy.nonRetryableErrorTypes`, so renaming one is a breaking change, and
// living in `names.ts` keeps the client out of the workflow bundle.
export {
  TYPE_AUTH_FAILED,
  TYPE_BAD_OPTIONS,
  TYPE_BAD_REQUEST,
  TYPE_DAILY_QUOTA_EXCEEDED,
  TYPE_DEADLINE_EXPIRED,
  TYPE_MONTHLY_QUOTA_EXCEEDED,
  TYPE_NOT_BOUND,
  TYPE_NOT_FOUND,
  TYPE_NO_DEADLINE,
  TYPE_QUOTA_EXCEEDED,
  TYPE_RATE_LIMITED,
  TYPE_SCHEMA_REJECTED,
  TYPE_SERVER_ERROR,
  TYPE_UNAVAILABLE,
  TYPE_UNKNOWN,
  TYPE_WRITE_FAILED,
  TYPE_WRITE_NOT_FOUND,
  TYPE_WRITE_TIMEOUT,
};

export const NON_RETRYABLE_TYPES: readonly string[] = [
  TYPE_MONTHLY_QUOTA_EXCEEDED,
  TYPE_QUOTA_EXCEEDED,
  TYPE_AUTH_FAILED,
  TYPE_NOT_FOUND,
  TYPE_BAD_REQUEST,
  TYPE_SCHEMA_REJECTED,
  TYPE_WRITE_FAILED,
  TYPE_WRITE_NOT_FOUND,
  TYPE_WRITE_TIMEOUT,
  TYPE_BAD_OPTIONS,
  TYPE_NOT_BOUND,
  TYPE_NO_DEADLINE,
];

/**
 * A server-supplied code, if this package recognises it.
 *
 * The code outlives the request in the Worker log and in failure `details`, which
 * Temporal persists in the clear — and a shape test is no defence, since a leaked
 * key looks exactly like an identifier. So only codes this module branches on are
 * echoed; the rest become a fixed marker, not a digest (an unsalted hash of a
 * low-entropy value is recoverable, and leaks equality). Matching uses the raw value.
 */
const UNRECOGNIZED = '<unrecognized>';

function safeCode(code: string | null | undefined): string | null {
  if (typeof code !== 'string') return null;
  return KNOWN_CODES.has(code) ? code : UNRECOGNIZED;
}

// Fixed, history-safe messages. Never include the raw exception string.
const MESSAGES: Record<string, string> = {
  [TYPE_UNAVAILABLE]: 'xmemory is unreachable',
  [TYPE_SERVER_ERROR]: 'xmemory returned a server error',
  [TYPE_RATE_LIMITED]: 'xmemory rate-limited the request',
  [TYPE_DAILY_QUOTA_EXCEEDED]: 'xmemory daily quota exceeded',
  [TYPE_MONTHLY_QUOTA_EXCEEDED]: 'xmemory monthly quota exceeded',
  [TYPE_QUOTA_EXCEEDED]: 'xmemory quota exceeded',
  [TYPE_AUTH_FAILED]: 'xmemory rejected the credentials',
  [TYPE_NOT_FOUND]: 'xmemory resource not found',
  [TYPE_BAD_REQUEST]: 'xmemory rejected the request as invalid',
  [TYPE_SCHEMA_REJECTED]: 'xmemory rejected the schema change',
  [TYPE_UNKNOWN]: 'xmemory returned an unrecognized error',
};

// 9999-12-31T23:59:59Z, where protobuf's Timestamp ends: a retry scheduled past it
// cannot be represented, and the server refuses the failure that carries it.
const MAX_TIMESTAMP_MS = 253_402_300_799_000;

const RETRYABLE_CODES = new Set(['INTERNAL_ERROR', 'SERVICE_UNAVAILABLE']);
const AUTH_CODES = new Set(['UNAUTHORIZED', 'FORBIDDEN']);
const BAD_REQUEST_CODES = new Set(['VALIDATION_ERROR', 'INVALID_INPUT', 'ALREADY_EXISTS', 'CONFLICT']);
// A queued write that exhausted its own retry budget; retrying cannot help.
const EXHAUSTED_CODES = new Set(['MAX_RETRIES_EXCEEDED']);
const SCHEMA_CODES = new Set([
  'stale_proposal_version',
  'stale_schema_version',
  'dependency_closure_failed',
  'destructive_confirmation_required',
  'non_additive_change_requires_plan',
  'migration_not_found',
  'instance_not_initialised',
]);

/**
 * The server's pacing hint in milliseconds, or `undefined` when Temporal cannot
 * carry it. Never clamped — the Activity's own policy bounds the retrying.
 *
 * Sent as a number, not a `<n>s` string: the string form goes through a duration
 * parser that rejects exponent notation. Dropped only when unusable — `Infinity`,
 * past `MAX_SAFE_INTEGER` milliseconds where the int64 conversion saturates, or a
 * delay landing past protobuf's 9999-12-31 Timestamp ceiling, which the server
 * rejects when the failure is reported.
 */
// Every code this module branches on. Nothing else is ever echoed.
const KNOWN_CODES = new Set<string>([
  ...RETRYABLE_CODES,
  ...AUTH_CODES,
  ...BAD_REQUEST_CODES,
  ...EXHAUSTED_CODES,
  ...SCHEMA_CODES,
  'QUOTA_EXCEEDED',
  'RATE_LIMITED',
  'NOT_FOUND',
]);

function retryDelayMs(err: XmemoryAPIError): number | undefined {
  // `ownOnly`: an inherited `retry_after_seconds` would be a hint nobody sent.
  const detail = ownOnly({ ...((err.details ?? {}) as Record<string, unknown>) });
  // Each source judged on its own: preferring the header and validating afterwards
  // discarded a good structured hint whenever the header was unusable.
  for (const hint of [err.retryAfter, detail.retry_after_seconds]) {
    const ms = usableDelayMs(hint);
    if (ms !== undefined) return ms;
  }
  return undefined;
}

/** A hint in milliseconds, or `undefined` when Temporal could not carry it. */
function usableDelayMs(hint: unknown): number | undefined {
  if (typeof hint !== 'number' || hint <= 0) return undefined;
  const ms = Math.round(hint * 1000);
  if (!Number.isSafeInteger(ms) || ms < 1) return undefined;
  if (ms > MAX_TIMESTAMP_MS - Date.now()) return undefined;
  return ms;
}

function quotaVerdict(err: XmemoryAPIError): { type: string; retryable: boolean } {
  // Own properties only: an inherited `kind` would promote a terminal quota failure.
  const detail = ownOnly({ ...((err.details ?? {}) as Record<string, unknown>) });
  const kind = detail.kind;
  // A daily window resets within hours and is worth retrying; a monthly one is not.
  // An unknown kind falls back to non-retryable.
  if (kind === 'daily_quota_exceeded') return { type: TYPE_DAILY_QUOTA_EXCEEDED, retryable: true };
  if (kind === 'monthly_quota_exceeded') return { type: TYPE_MONTHLY_QUOTA_EXCEEDED, retryable: false };
  return { type: TYPE_QUOTA_EXCEEDED, retryable: false };
}

function verdictFromStatus(status: number | undefined): { type: string; retryable: boolean } {
  if (status === undefined) return { type: TYPE_UNAVAILABLE, retryable: true };
  if (status === 408 || status >= 500) return { type: TYPE_SERVER_ERROR, retryable: true };
  if (status === 429) return { type: TYPE_RATE_LIMITED, retryable: true };
  if (status === 401 || status === 403) return { type: TYPE_AUTH_FAILED, retryable: false };
  if (status === 404) return { type: TYPE_NOT_FOUND, retryable: false };
  if (status >= 400) return { type: TYPE_BAD_REQUEST, retryable: false };
  return { type: TYPE_UNKNOWN, retryable: true };
}

function build(
  type: string,
  retryable: boolean,
  code: string | null = null,
  status: number | null = null,
  delayMs?: number,
): ApplicationFailure {
  return applicationFailure({
    message: MESSAGES[type] ?? 'xmemory request failed',
    type,
    nonRetryable: !retryable,
    // `retryAfterSeconds` rides in details as well as in nextRetryDelay: the SDK
    // encodes the latter for the server's retry scheduling but drops it when
    // decoding the failure back into workflow code, where the durable-write loop
    // needs it to pace its own polling. Details survive that round trip.
    details: [{ code, status, ...(delayMs !== undefined ? { retryAfterSeconds: delayMs / 1000 } : {}) }],
    nextRetryDelay: retryable && delayMs !== undefined ? delayMs : undefined,
  });
}

/**
 * Whether a non-API error is a `fetch` network failure rather than a bug.
 *
 * Undici raises `TypeError('fetch failed', { cause })` for DNS failures,
 * refused connections, and resets; a programming TypeError carries no `cause`.
 * An `AbortError` from the client's own timeout is a `DOMException`, so it does
 * not reach here.
 */
function isNetworkError(err: unknown): boolean {
  // `Object.hasOwn`, not a plain read: an inherited `cause` would make a programming
  // TypeError look like a transient blip.
  return err instanceof TypeError && Object.hasOwn(err, 'cause');
}

/**
 * Whether this is the client's own API error. `instanceof` alone is not enough:
 * xmemory ships CJS and ESM builds, so a handle from the other half throws a
 * different class object and an auth failure looked like a transport blip.
 */
function isApiError(err: unknown): err is XmemoryAPIError {
  return err instanceof XmemoryAPIError || (err instanceof Error && err.name === 'XmemoryAPIError');
}

/** Map any client-raised error onto a Temporal `ApplicationFailure`. */
export function toApplicationFailure(err: unknown): ApplicationFailure {
  if (!isApiError(err)) {
    // Not an API error: a transport failure (retryable) or a deterministic
    // client-side error (non-retryable — a retry replays the same input). The
    // raw message is never echoed (it may embed internal transport details).
    //
    // `fetch` rejects a network failure (DNS, refused, reset) with a TypeError,
    // and the client does not wrap it, so a bare `instanceof TypeError` would
    // make a transient blip permanently fatal. Undici sets `cause` on those and
    // never on a programming TypeError, which is the discriminator.
    if (isNetworkError(err)) return build(TYPE_UNAVAILABLE, true);
    if (err instanceof TypeError || err instanceof RangeError || err instanceof SyntaxError) {
      return build(TYPE_BAD_REQUEST, false);
    }
    return build(TYPE_UNAVAILABLE, true);
  }

  const { code, status } = err;
  let verdict: { type: string; retryable: boolean };

  if (code === 'QUOTA_EXCEEDED') verdict = quotaVerdict(err);
  else if (code === 'RATE_LIMITED') verdict = { type: TYPE_RATE_LIMITED, retryable: true };
  else if (code && RETRYABLE_CODES.has(code)) verdict = { type: TYPE_SERVER_ERROR, retryable: true };
  else if (code && AUTH_CODES.has(code)) verdict = { type: TYPE_AUTH_FAILED, retryable: false };
  else if (code === 'NOT_FOUND') verdict = { type: TYPE_NOT_FOUND, retryable: false };
  else if (code && BAD_REQUEST_CODES.has(code)) verdict = { type: TYPE_BAD_REQUEST, retryable: false };
  else if (code && EXHAUSTED_CODES.has(code)) verdict = { type: TYPE_WRITE_FAILED, retryable: false };
  else if (code && SCHEMA_CODES.has(code)) verdict = { type: TYPE_SCHEMA_REJECTED, retryable: false };
  else if (code) {
    // Almost certainly a newer server: keep the type so the gap is findable, but
    // take retryability from the status — a 401 is terminal whatever the code says.
    // No status at all still gets the benefit of the doubt.
    console.warn(`xmemory returned an unrecognized error code ${JSON.stringify(safeCode(code))} (HTTP ${status})`);
    verdict = { type: TYPE_UNKNOWN, retryable: verdictFromStatus(status).retryable };
  } else {
    verdict = verdictFromStatus(status);
  }

  const delay = verdict.retryable ? retryDelayMs(err) : undefined;
  return build(verdict.type, verdict.retryable, safeCode(code), status ?? null, delay);
}
