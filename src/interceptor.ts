/**
 * Opt-in auto-capture of activity results into xmemory.
 *
 * An *activity* interceptor, not a workflow one: workflow interceptors re-run on
 * every replay, so I/O there breaks determinism.
 *
 * Guardrails: `project` decides what to remember, `sampleRate` bounds fan-out,
 * capture is clamped to what is left of the activity's deadline, and a capture
 * failure never fails that activity.
 */

import { Context } from '@temporalio/activity';
import type {
  ActivityExecuteInput,
  ActivityInboundCallsInterceptor,
  Next,
} from '@temporalio/worker';
import type { InstanceHolder } from './activities';
import type { XmemoryConfig } from './config';
import { activityBudgetMs, type DeadlineInfo, withDeadline } from './deadline';
import { DEFAULT_CLIENT_MARGIN_MS, ownOnly } from './defaults';

// Never capture our own writes, or capture recurses. This also skips a user
// activity named `xmemory_*` — see the README's auto-capture section.
const OWN_ACTIVITY_PREFIX = 'xmemory_';

export interface AutoCaptureConfig {
  /**
   * What to remember from a completed activity, or `undefined` to skip it.
   *
   * No default: raw payloads are JSON blobs the extraction engine cannot use.
   */
  project: (activityName: string, result: unknown) => string | undefined | null;
  /** Fraction of eligible activities to capture, in [0, 1]. Defaults to 1. */
  sampleRate?: number;
  extractionLogic?: 'fast' | 'deep';
  /**
   * Ceiling (ms) a capture may add to the wrapped activity. Lowered further when
   * less than this is left of the activity's deadline.
   */
  captureTimeoutMs?: number;
}

/**
 * Milliseconds capture may take, or `null` when it must be skipped.
 *
 * Capture spends the wrapped Activity's deadline, so an Activity that has nearly
 * used its budget would be pushed past it and retried, discarding a result it had
 * already produced.
 */
export function captureBudgetMs(remainingMs: number, ceilingMs: number, marginMs: number): number | null {
  // A margin of zero or less would leave no completion gap, or hand capture more
  // time than the Activity has left. Fall back to a proportional reserve.
  const usable = Number.isFinite(marginMs) && marginMs > 0 ? remainingMs - marginMs : remainingMs * 0.8;
  if (usable <= 0 || !(ceilingMs > 0)) return null;
  return Math.min(ceilingMs, usable);
}

export function createAutoCaptureInterceptor(
  holder: InstanceHolder,
  config: XmemoryConfig,
  autoCapture: AutoCaptureConfig,
): ActivityInboundCallsInterceptor {
  // Validated at plugin construction; this only resolves the default.
  const sampleRate = autoCapture.sampleRate ?? 1;
  const extractionLogic = autoCapture.extractionLogic ?? 'fast';
  const ceilingMs = autoCapture.captureTimeoutMs ?? 5000;
  const marginMs = config.clientMarginMs ?? DEFAULT_CLIENT_MARGIN_MS;

  return {
    async execute(input: ActivityExecuteInput, next: Next<ActivityInboundCallsInterceptor, 'execute'>) {
      const started = performance.now();
      const result = await next(input);
      try {
        await maybeCapture(result, started);
      } catch {
        // Capture must never fail the wrapped activity: errors and timeouts alike.
        console.warn('xmemory auto-capture skipped; the wrapped activity is unaffected');
      }
      return result;
    },
  };

  async function maybeCapture(result: unknown, started: number): Promise<void> {
    const info = Context.current().info;
    const name = info.activityType;
    if (name.startsWith(OWN_ACTIVITY_PREFIX)) return;
    if (!shouldSample()) return;
    // Budget before projecting: `project` is the caller's code and can itself be
    // slow enough to push the Activity past its deadline.
    const budgetMs = captureBudgetMs(remainingMs(info, started), ceilingMs, marginMs);
    if (budgetMs === null) return;
    const text = autoCapture.project(name, result);
    if (!text) return;
    if (typeof text !== 'string') {
      // `writeAsync` is overloaded on its first argument, so an array would be
      // applied as mutations rather than remembered. Skipped, like every capture
      // problem: it never affects the wrapped Activity.
      console.warn(`xmemory auto-capture skipped: project() returned a ${typeof text}, expected a string`);
      return;
    }
    // Re-checked, because the projector just spent some of the same budget.
    const enqueueMs = captureBudgetMs(remainingMs(info, started), ceilingMs, marginMs);
    if (enqueueMs === null) return;
    // Enqueue, not a full write: waiting for extraction would add its latency to
    // the activity's budget. Two bounds because `timeoutMs` stops at headers and
    // `withDeadline` caps the total.
    // Null-prototype, like every object handed to the client: an inherited `scope`
    // or `diffEngine` would otherwise ride along on the capture.
    await withDeadline(
      holder.get().writeAsync(text, ownOnly({ extractionLogic, timeoutMs: enqueueMs })),
      enqueueMs,
    );
  }

  /**
   * What is left of the wrapped Activity's deadline, or zero when it has none.
   *
   * `started` is stamped before the Activity runs, so the elapsed figure covers it
   * and every interceptor inside this one. Handed to `activityBudgetMs` rather than
   * subtracted here, since only the start-to-close bound needs it.
   */
  function remainingMs(info: DeadlineInfo, started: number): number {
    return activityBudgetMs(info, performance.now() - started) ?? 0;
  }

  function shouldSample(): boolean {
    if (sampleRate >= 1) return true;
    if (sampleRate <= 0) return false;
    // Keyed on the run as well as the activity: `activityId` restarts at "1" in
    // every workflow, so hashing it alone bucketed them all together.
    const info = Context.current().info;
    const id = `${info.workflowExecution?.runId ?? ''}/${info.activityId}`;
    let hash = 0;
    for (let i = 0; i < id.length; i++) hash = (hash * 31 + id.charCodeAt(i)) >>> 0;
    // A million buckets, so a rate below 1/1000 still works. Low bits: this
    // accumulator mixes them well, its high bits barely move across similar ids.
    return (hash % 1_000_000) / 1_000_000 < sampleRate;
  }
}
