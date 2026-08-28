/**
 * Workflow definitions used by the integration tests.
 *
 * Registered with the worker via `workflowsPath` (this file). It imports only
 * the workflow-safe surface of the package.
 */

import { xmemoryForWorkflow } from '../src/workflow';

export async function readWorkflow(query: string): Promise<unknown> {
  const mem = xmemoryForWorkflow();
  return (await mem.read(query)).readerResult;
}

export async function writeWorkflow(text: string): Promise<string> {
  const mem = xmemoryForWorkflow();
  return (await mem.write(text)).writeId;
}

// The opt-in retry path (for deterministic-PK schemas).
export async function optInRetryWriteWorkflow(text: string): Promise<string> {
  const mem = xmemoryForWorkflow({ writeRetryPolicy: { initialInterval: '1s', maximumAttempts: 3 } });
  return (await mem.write(text)).writeId;
}

export async function durableWriteWorkflow(text: string): Promise<string> {
  const mem = xmemoryForWorkflow();
  const out = await mem.writeDurable(text, { pollIntervalMs: 1_000, maxWaitMs: 15 * 60_000 });
  return out.writeStatus;
}

/** Poll interval far longer than the wait, so the first sleep would overrun it. */
export async function longIntervalDurableWriteWorkflow(text: string): Promise<string> {
  const mem = xmemoryForWorkflow();
  const out = await mem.writeDurable(text, { pollIntervalMs: 30_000, maxWaitMs: 10_000 });
  return out.writeStatus;
}

/** A short wait with a cadence that fits inside it. */
export async function shortWaitDurableWriteWorkflow(text: string): Promise<string> {
  const mem = xmemoryForWorkflow();
  const out = await mem.writeDurable(text, { pollIntervalMs: 1_000, maxWaitMs: 10_000 });
  return out.writeStatus;
}

/** Durable write whose timing options come straight from the test. */
export async function badOptionsDurableWriteWorkflow(options: Record<string, number | undefined>): Promise<string> {
  const mem = xmemoryForWorkflow();
  const out = await mem.writeDurable('remember', options as never);
  return out.writeStatus;
}

/** An unusable status budget, which the poll loop must reject before enqueuing. */
export async function badStatusTimeoutDurableWriteWorkflow(timeoutMs: number): Promise<string> {
  const mem = xmemoryForWorkflow({ writeStatusTimeout: timeoutMs });
  const out = await mem.writeDurable('remember');
  return out.writeStatus;
}

/** A wait shorter than a server hint, so the pacing decision is observable. */
export async function briefWaitDurableWriteWorkflow(text: string): Promise<string> {
  const mem = xmemoryForWorkflow();
  const out = await mem.writeDurable(text, { pollIntervalMs: 2_000, maxWaitMs: 1_000 });
  return out.writeStatus;
}

export async function readThenWriteWorkflow(text: string): Promise<string> {
  const mem = xmemoryForWorkflow();
  await mem.read('before');
  return (await mem.write(text)).writeId;
}

export async function doubleWriteWorkflow(text: string): Promise<number> {
  const mem = xmemoryForWorkflow();
  await mem.write(`${text} (1)`);
  await mem.write(`${text} (2)`);
  return 2;
}

/** A poll policy the SDK's own compiler refuses, on the enqueue-then-poll path. */
export async function badPollPolicyDurableWriteWorkflow(maximumAttempts: number): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { maximumAttempts } });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/** A coefficient the SDK compiles but the service refuses. */
export async function serviceRejectedPollPolicyWorkflow(backoffCoefficient: number): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { backoffCoefficient } });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/** An attempt count the SDK compiles but the int32 wire format cannot carry. */
export async function overflowPollPolicyWorkflow(maximumAttempts: number): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { maximumAttempts } });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/** Unlimited poll retries, which Temporal spells `Infinity`. Must be left alone. */
export async function unlimitedPollPolicyWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { maximumAttempts: Number.POSITIVE_INFINITY } });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/** An interval the SDK compiles but the service refuses. */
export async function negativeIntervalPollPolicyWorkflow(initialInterval: number): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { initialInterval } });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/** A non-string in the reserved-type list, reachable from JavaScript callers. */
export async function nonStringErrorTypePollPolicyWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { nonRetryableErrorTypes: [1 as never] } });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/** A positive interval below one nanosecond, which encodes as no delay at all. */
export async function subNanosecondIntervalPollPolicyWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { initialInterval: 1e-7 } });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/** A month-long retry interval: the service keeps it, so it must be accepted. */
export async function longIntervalPollPolicyWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { initialInterval: 30 * 24 * 60 * 60_000, maximumAttempts: 1 } });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/** An initial interval whose *derived* maximum (100x) overflows what Temporal holds. */
export async function derivedMaxIntervalPollPolicyWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { initialInterval: 3 * 365 * 24 * 60 * 60_000 } });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/** An unusable read policy: no side effect, but the Workflow Task would loop. */
export async function badReadPolicyWorkflow(maximumAttempts: number): Promise<unknown> {
  const mem = xmemoryForWorkflow({ readRetryPolicy: { maximumAttempts } });
  return (await mem.read('q')).readerResult;
}

/** A durable write whose polls treat a server error as terminal. */
export async function nonRetryableStatusDurableWriteWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({
    pollRetryPolicy: { maximumAttempts: 3, nonRetryableErrorTypes: ['XmemoryServerError'] },
  });
  return (await mem.writeDurable('remember', { pollIntervalMs: 1_000, maxWaitMs: 30_000 })).writeStatus;
}

/** A read with a total bound across retries, not just per attempt. */
export async function boundedReadWorkflow(): Promise<unknown> {
  const mem = xmemoryForWorkflow({ totalTimeout: '5s', readTimeout: '2s' });
  return (await mem.read('q')).readerResult;
}

/** A retry policy whose reserved-type list is not a list at all. */
export async function nonArrayErrorTypesWorkflow(value: unknown): Promise<unknown> {
  const mem = xmemoryForWorkflow({ readRetryPolicy: { nonRetryableErrorTypes: value as never } });
  return (await mem.read('q')).readerResult;
}

/** A durable write under a total bound tighter than the loop's own poll budget. */
export async function totalBoundedDurableWriteWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ totalTimeout: '5s', writeStatusTimeout: '30s' });
  return (await mem.writeDurable('remember', { pollIntervalMs: 1_000, maxWaitMs: 60_000 })).writeStatus;
}

/** A retry policy that is not a policy object at all. */
export async function malformedPolicyContainerWorkflow(value: unknown): Promise<string> {
  const mem = xmemoryForWorkflow({ writeRetryPolicy: value as never });
  return (await mem.write('remember')).writeId;
}

/** Options arriving as JSON `null` rather than omitted. */
export async function nullOptionsWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow(null as never);
  return (await mem.write('remember', null as never)).writeId;
}

/** A non-string in the text or query slot, as JSON permits. */
export async function nonStringTextWorkflow(value: unknown): Promise<string> {
  const mem = xmemoryForWorkflow();
  return (await mem.write(value as never)).writeId;
}

export async function nonStringQueryWorkflow(value: unknown): Promise<unknown> {
  const mem = xmemoryForWorkflow();
  return (await mem.read(value as never)).readerResult;
}

/**
 * A durable write whose poll policy is mutated after the enqueue.
 *
 * The caller keeps its own reference, and its code runs between our awaits.
 */
export async function mutatedPollPolicyWorkflow(): Promise<string> {
  const policy = { maximumAttempts: 1 };
  const mem = xmemoryForWorkflow({ pollRetryPolicy: policy, writeStatusTimeout: '5s' });
  const pending = mem.writeDurable('remember', { pollIntervalMs: 1_000, maxWaitMs: 10_000 });
  policy.maximumAttempts = 0; // invalid: Temporal refuses to schedule with this
  return (await pending).writeStatus;
}

/** Content in the activity summary, switched on with a truthy string. */
export async function stringSummaryFlagWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ includeContentInSummary: 'false' as never });
  return (await mem.write('SECRET memory text')).writeId;
}

/**
 * A durable write under a workflow that polluted its own sandbox prototype.
 *
 * Workflow code runs in its own context, so this is where such pollution has to
 * come from — the caller's own code, or a library in their bundle.
 */
export async function pollutedPolicyDurableWriteWorkflow(): Promise<string> {
  (Object.prototype as Record<string, unknown>).nonRetryableErrorTypes = ['XmemoryServerError'];
  try {
    const mem = xmemoryForWorkflow();
    return (await mem.writeDurable('remember', { pollIntervalMs: 1_000, maxWaitMs: 30_000 })).writeStatus;
  } finally {
    delete (Object.prototype as Record<string, unknown>).nonRetryableErrorTypes;
  }
}

/** Options supplied as something that is not an options object. */
export async function badOptionsContainerWorkflow(value: unknown): Promise<string> {
  const mem = xmemoryForWorkflow();
  return (await mem.writeDurable('remember', value as never)).writeStatus;
}

/** A durable write called with no text at all. */
export async function noTextDurableWriteWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow();
  return (await (mem.writeDurable as (t?: string) => Promise<{ writeStatus: string }>)()).writeStatus;
}

/** A non-string write id on the bare status poll. */
export async function nonStringWriteIdWorkflow(value: unknown): Promise<string> {
  const mem = xmemoryForWorkflow();
  return (await mem.writeStatus(value as never)).writeStatus;
}

/** An options object carrying its own `text`, which must not win over the argument. */
export async function optionsOverrideTextWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow();
  return (await mem.write('INTENDED', { text: 'OVERRIDE' } as never)).writeId;
}

/** A valid write policy that simply never says how many attempts: unlimited. */
export async function unboundedWritePolicyWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ writeRetryPolicy: { initialInterval: '1s', backoffCoefficient: 2 } });
  return (await mem.write('remember')).writeId;
}

/** A write policy with a plausible typo: Temporal ignores it and retries forever. */
export async function typoWritePolicyWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ writeRetryPolicy: { maximumAttempt: 1 } as never });
  return (await mem.write('remember')).writeId;
}

/** A duration string no parser accepts, supplied where a duration is expected. */
export async function badDurationDurableWriteWorkflow(timeout: string): Promise<string> {
  const mem = xmemoryForWorkflow({ writeStatusTimeout: timeout as never });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}

/**
 * A durable write whose polls never retry internally, so a `writeStatus` count
 * means loop iterations rather than Temporal's retries within one.
 */
export async function singleAttemptDurableWriteWorkflow(text: string): Promise<string> {
  const mem = xmemoryForWorkflow({ pollRetryPolicy: { maximumAttempts: 1 } });
  const out = await mem.writeDurable(text, { pollIntervalMs: 1_000, maxWaitMs: 15 * 60_000 });
  return out.writeStatus;
}

/**
 * A read whose timeout is not a representable duration.
 *
 * `Infinity` is a literal here rather than an argument: JSON cannot carry it, so
 * Temporal would deliver `null` and the `??` default would quietly take over.
 */
export async function infiniteReadTimeoutWorkflow(): Promise<unknown> {
  const mem = xmemoryForWorkflow({ readTimeout: Number.POSITIVE_INFINITY });
  return (await mem.read('q')).readerResult;
}

export async function badReadTimeoutWorkflow(timeout: unknown): Promise<unknown> {
  const mem = xmemoryForWorkflow({ readTimeout: timeout as never });
  return (await mem.read('q')).readerResult;
}

/** A bare status poll whose own timeout is unusable. */
export async function badStatusTimeoutPollWorkflow(timeout: unknown): Promise<string> {
  const mem = xmemoryForWorkflow({ writeStatusTimeout: timeout as never });
  return (await mem.writeStatus('w1')).writeStatus;
}

/** A durable write whose status timeout is larger than any duration can carry. */
export async function oversizedStatusTimeoutWorkflow(timeoutMs: number): Promise<string> {
  const mem = xmemoryForWorkflow({ writeStatusTimeout: timeoutMs });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}
