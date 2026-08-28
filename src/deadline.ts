import { MAX_DURATION_MS } from './defaults';

/**
 * A total wall-clock bound for a client call.
 *
 * The xmemory client's `timeoutMs` stops applying once response headers arrive, so
 * a stalled body can outlive the Activity deadline. Racing a timer caps the total.
 */
export class DeadlineExceededError extends Error {
  constructor(ms: number) {
    super(`xmemory call exceeded its ${ms}ms client deadline`);
    this.name = 'DeadlineExceededError';
  }
}

export function withDeadline<T>(promise: Promise<T>, ms: number): Promise<T> {
  // Refused rather than passed to setTimeout, which turns anything above 2**31-1
  // into 1ms — the deadline would fire immediately instead of never.
  if (!Number.isFinite(ms) || ms <= 0 || ms > MAX_DURATION_MS) {
    return Promise.reject(new RangeError(`xmemory client deadline must be 1..${MAX_DURATION_MS}ms, got ${ms}`));
  }
  let timer: ReturnType<typeof setTimeout>;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new DeadlineExceededError(ms)), ms);
  });
  return Promise.race([promise, deadline]).finally(() => clearTimeout(timer)) as Promise<T>;
}

export interface DeadlineInfo {
  readonly startToCloseTimeoutMs: number;
  readonly scheduleToCloseTimeoutMs: number;
  readonly scheduledTimestampMs: number;
}

/**
 * What is left of this Activity's deadline, or `null` if it has none. Zero or
 * negative once spent.
 *
 * The smaller of the two bounds — a short schedule-to-close expires the attempt
 * before a longer start-to-close would, and the larger figure would let a write
 * commit after Temporal gave up. Schedule-to-close counts from the service's own
 * `scheduledTimestampMs`; `Math.max(0, ...)` stops a lagging Worker clock, or a
 * time-skipping test server, from *lengthening* the budget.
 *
 * `elapsedInAttemptMs` is for a caller running partway through the Activity, and
 * applies to start-to-close only — schedule-to-close already contains that time.
 * Time spent *before* the Activity function is not measurable without a
 * Worker-wide interceptor, which this plugin does not install.
 */
export function activityBudgetMs(info: DeadlineInfo, elapsedInAttemptMs = 0): number | null {
  const bounds: number[] = [];
  if (info.startToCloseTimeoutMs > 0) bounds.push(info.startToCloseTimeoutMs - elapsedInAttemptMs);
  if (info.scheduleToCloseTimeoutMs > 0) {
    bounds.push(info.scheduleToCloseTimeoutMs - Math.max(0, Date.now() - info.scheduledTimestampMs));
  }
  return bounds.length > 0 ? Math.min(...bounds) : null;
}
