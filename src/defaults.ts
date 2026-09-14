/**
 * Default activity budgets and the client-margin rule.
 *
 * A dependency-free leaf on purpose: workflow code imports these defaults, so
 * this module must never reach for `process.env` or the xmemory client the way
 * `config.ts` does.
 */

/** Default `startToClose` budgets, in milliseconds. */
export interface XmemoryTimeouts {
  readMs: number;
  writeMs: number;
  writeStartMs: number;
  writeStatusMs: number;
}

/**
 * The defaults `xmemoryForWorkflow()` applies when a workflow does not pass its
 * own. The workflow owns the real budget: whatever it sets is what Temporal
 * enforces, and what each activity derives its client timeout from.
 */
export const DEFAULT_TIMEOUTS: XmemoryTimeouts = {
  readMs: 120_000,
  writeMs: 180_000,
  writeStartMs: 30_000,
  writeStatusMs: 30_000,
};

/**
 * How far below its Temporal deadline each call's client timeout sits. Inverted,
 * Temporal could abandon an enqueue that still succeeds server-side, and a later
 * durable-write retry would queue it twice.
 */
export const DEFAULT_CLIENT_MARGIN_MS = 5_000;

/**
 * The longest a Node timer can hold (24.8 days).
 *
 * `setTimeout` silently turns anything larger into 1ms. Client budgets are capped
 * at it, and `writeDurable` refuses loop options past it before its enqueue.
 */
export const MAX_DURATION_MS = 2_147_483_647;

/**
 * Client budget for an activity whose Temporal deadline is `activityMs`.
 *
 * Always strictly below it, so the client gives up first and the failure is an
 * attributable xmemory error rather than an opaque activity timeout. A budget at
 * or under the margin gets a proportional one instead; there is no floor, which
 * would hand back more time than Temporal is giving.
 *
 * Only meaningful above timer resolution: a 1ms deadline yields 0.8ms, which
 * `setTimeout` rounds back to 1ms — the same instant Temporal uses.
 *
 * Capped at `MAX_DURATION_MS`, because the client arms a `setTimeout` with this
 * value and Node fires anything larger after 1ms. The cap only applies to a deadline
 * longer than that, so the client still gives up first.
 */
export function clientTimeoutMs(activityMs: number, marginMs: number = DEFAULT_CLIENT_MARGIN_MS): number {
  if (!Number.isFinite(activityMs) || activityMs <= 0) {
    throw new RangeError(`activityMs must be a positive, finite number, got ${activityMs}`);
  }
  const margin = Number.isFinite(marginMs) && marginMs > 0 ? marginMs : 0;
  const budgetMs = margin === 0 || activityMs <= margin ? activityMs * 0.8 : activityMs - margin;
  return Math.min(budgetMs, MAX_DURATION_MS);
}
