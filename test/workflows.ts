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

/** A durable write whose polls treat a server error as terminal. */
export async function nonRetryableStatusDurableWriteWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({
    writeStatusRetry: { attempts: 3, nonRetryableErrorTypes: ['XmemoryServerError'] },
  });
  return (await mem.writeDurable('remember', { pollIntervalMs: 1_000, maxWaitMs: 30_000 })).writeStatus;
}

/** A read with a total bound across retries, not just per attempt. */
export async function boundedReadWorkflow(): Promise<unknown> {
  const mem = xmemoryForWorkflow({ totalTimeout: '5s', readTimeout: '2s' });
  return (await mem.read('q')).readerResult;
}

/** A durable write under a total bound tighter than the loop's own poll budget. */
export async function totalBoundedDurableWriteWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ totalTimeout: '5s', writeStatusTimeout: '30s' });
  return (await mem.writeDurable('remember', { pollIntervalMs: 1_000, maxWaitMs: 60_000 })).writeStatus;
}

/** Poll options Temporal could never be given, refused before anything is scheduled. */
export async function badWriteStatusRetryWorkflow(retry: unknown): Promise<string> {
  const mem = xmemoryForWorkflow({ writeStatusRetry: retry as never });
  return (await mem.writeDurable('remember', { pollIntervalMs: 1_000, maxWaitMs: 30_000 })).writeStatus;
}

/** A durable write whose polls use custom, valid retry pacing. */
export async function tunedPollDurableWriteWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow({ writeStatusRetry: { attempts: 4, intervalMs: 2_000, maxIntervalMs: 8_000 } });
  return (await mem.writeDurable('remember', { pollIntervalMs: 1_000, maxWaitMs: 30_000 })).writeStatus;
}

/** An options object carrying its own `text`, which must not win over the argument. */
export async function optionsOverrideTextWorkflow(): Promise<string> {
  const mem = xmemoryForWorkflow();
  return (await mem.write('INTENDED', { text: 'OVERRIDE' } as never)).writeId;
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
  const mem = xmemoryForWorkflow({ writeStatusRetry: { attempts: 1 } });
  const out = await mem.writeDurable(text, { pollIntervalMs: 1_000, maxWaitMs: 15 * 60_000 });
  return out.writeStatus;
}

/** A durable write whose status timeout is larger than any duration can carry. */
export async function oversizedStatusTimeoutWorkflow(timeoutMs: number): Promise<string> {
  const mem = xmemoryForWorkflow({ writeStatusTimeout: timeoutMs });
  return (await mem.writeDurable('remember', { maxWaitMs: 60_000 })).writeStatus;
}
