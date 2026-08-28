/**
 * Activity name constants, in a leaf module with no client imports.
 *
 * The workflow bundle imports these; keeping them here (rather than in
 * `activities.ts`, which imports the xmemory client) keeps the client out of
 * the workflow sandbox bundle.
 */

export const ACTIVITY_READ = 'xmemory_read';
export const ACTIVITY_WRITE = 'xmemory_write';
export const ACTIVITY_WRITE_START = 'xmemory_write_start';
export const ACTIVITY_WRITE_STATUS = 'xmemory_write_status';

// Job-level write outcomes raised by the durable poll loop (workflow.ts). A
// public `RetryPolicy.nonRetryableErrorTypes` contract; kept in this client-free
// leaf so the workflow bundle can import them without pulling the client in, and
// re-exported from errors.ts so the full type= set lives in one logical place.
export const TYPE_WRITE_FAILED = 'XmemoryWriteFailed';
export const TYPE_WRITE_NOT_FOUND = 'XmemoryWriteNotFound';
export const TYPE_WRITE_TIMEOUT = 'XmemoryWriteTimeout';
// A caller's own durable-write options are unusable. Non-retryable: the same
// arguments would be rejected identically on every attempt.
export const TYPE_BAD_OPTIONS = 'XmemoryBadOptions';

// Stable `type` strings — a public contract for
// `RetryPolicy.nonRetryableErrorTypes`. Renaming one is a breaking change.
export const TYPE_UNAVAILABLE = 'XmemoryUnavailable';
export const TYPE_SERVER_ERROR = 'XmemoryServerError';
export const TYPE_RATE_LIMITED = 'XmemoryRateLimited';
export const TYPE_DAILY_QUOTA_EXCEEDED = 'XmemoryDailyQuotaExceeded';
export const TYPE_MONTHLY_QUOTA_EXCEEDED = 'XmemoryMonthlyQuotaExceeded';
export const TYPE_QUOTA_EXCEEDED = 'XmemoryQuotaExceeded';
export const TYPE_AUTH_FAILED = 'XmemoryAuthFailed';
export const TYPE_NOT_FOUND = 'XmemoryNotFound';
export const TYPE_BAD_REQUEST = 'XmemoryBadRequest';
export const TYPE_SCHEMA_REJECTED = 'XmemorySchemaRejected';
export const TYPE_NOT_BOUND = 'XmemoryNotBound';
// Worker-side misconfiguration raised by activities.ts, kept distinct from
// NotBound because the remedy differs: one is a missing plugin registration, the
// other an activity scheduled with neither close timeout.
export const TYPE_NO_DEADLINE = 'XmemoryNoDeadline';
// The Activity's deadline is already spent. Retryable: Temporal decides whether
// another attempt still fits, and failing here only avoids a doomed request.
export const TYPE_DEADLINE_EXPIRED = 'XmemoryDeadlineExpired';
export const TYPE_UNKNOWN = 'XmemoryUnknown';
// Re-export the durable-write outcome types (defined in the client-free leaf) so
// the full `type=` set is reachable from one module.
