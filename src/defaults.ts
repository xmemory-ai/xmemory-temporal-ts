import { ApplicationFailure } from '@temporalio/common';

/**
 * Default activity budgets and the client-margin rule.
 *
 * A dependency-free leaf on purpose: workflow code imports these defaults, so
 * this module must never reach for `process.env` or the xmemory client the way
 * `config.ts` does.
 */

/**
 * A copy of `value` with no prototype, so an absent field reads as absent.
 *
 * A plain object answers a missing field from `Object.prototype`, which lets
 * prototype pollution supply options nobody passed. Shallow: use `ownDeep` for
 * nested data.
 */
export function ownOnly<T extends object>(value: T): T {
  return Object.assign(Object.create(null) as T, value);
}

/**
 * A deep own-only copy, for data forwarded to the client — scopes, mutations.
 * Nested objects keep their own prototypes, so `ownOnly` alone is not enough:
 * an inherited `relationsScope` or `allow_bulk_delete` widens what the call does.
 *
 * Depth-bounded so a deeply nested payload fails as a bad option rather than as a
 * stack overflow, which would reach Temporal untyped and retryable.
 */
export const MAX_OWN_DEEP_DEPTH = 64;

export class TooDeepError extends Error {
  constructor() {
    super(`value nests deeper than ${MAX_OWN_DEEP_DEPTH} levels`);
    this.name = 'TooDeepError';
  }
}

export function ownDeep<T>(value: T, depth = 0): T {
  if (depth > MAX_OWN_DEEP_DEPTH) throw new TooDeepError();
  if (Array.isArray(value)) return value.map((item) => ownDeep(item, depth + 1)) as T;
  if (typeof value === 'object' && value !== null) {
    const copy = Object.create(null) as Record<string, unknown>;
    for (const key of Object.keys(value)) {
      copy[key] = ownDeep((value as Record<string, unknown>)[key], depth + 1);
    }
    return copy as T;
  }
  return value;
}

/**
 * `ApplicationFailure.create` with prototype-proof options.
 *
 * The SDK reads fields off the object it is given, `cause` among them, and an
 * inherited one would be serialized into workflow history.
 */
export function applicationFailure(options: Parameters<typeof ApplicationFailure.create>[0]): ApplicationFailure {
  return ApplicationFailure.create(ownOnly({ ...options }));
}

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
 * Upper bound on any duration this plugin accepts (24.8 days).
 *
 * `setTimeout` silently turns anything larger into 1ms, and the service rejects
 * oversized durations only when it builds the command — after the enqueue.
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
 */
export function clientTimeoutMs(activityMs: number, marginMs: number = DEFAULT_CLIENT_MARGIN_MS): number {
  if (!Number.isFinite(activityMs) || activityMs <= 0) {
    throw new RangeError(`activityMs must be a positive, finite number, got ${activityMs}`);
  }
  const margin = Number.isFinite(marginMs) && marginMs > 0 ? marginMs : 0;
  if (margin === 0 || activityMs <= margin) return activityMs * 0.8;
  return activityMs - margin;
}
