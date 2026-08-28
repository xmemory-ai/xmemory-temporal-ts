/**
 * Workflow-level tests: read/write round-trips, durable-write polling, and the
 * replay side-effects check with a sensitivity control.
 *
 * `maxCachedWorkflows: 0` evicts the workflow after every task, forcing replay
 * from history — the condition under which a non-replay-safe implementation
 * would duplicate its activities. We assert both at the history level
 * (ActivityTaskScheduled count) and the ledger level (the fake's call count).
 */

import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import path from 'node:path';
import { TestWorkflowEnvironment } from '@temporalio/testing';
import { Worker } from '@temporalio/worker';
import type { WorkflowHandle } from '@temporalio/client';
import { XmemoryPlugin } from '../src/plugin';
import { FakeXmemoryInstance, apiError } from './fakes';
import {
  doubleWriteWorkflow,
  durableWriteWorkflow,
  badOptionsDurableWriteWorkflow,
  badOptionsContainerWorkflow,
  badPollPolicyDurableWriteWorkflow,
  badReadPolicyWorkflow,
  boundedReadWorkflow,
  derivedMaxIntervalPollPolicyWorkflow,
  longIntervalPollPolicyWorkflow,
  malformedPolicyContainerWorkflow,
  nonStringQueryWorkflow,
  nonStringTextWorkflow,
  mutatedPollPolicyWorkflow,
  noTextDurableWriteWorkflow,
  pollutedPolicyDurableWriteWorkflow,
  nonStringWriteIdWorkflow,
  stringSummaryFlagWorkflow,
  optionsOverrideTextWorkflow,
  typoWritePolicyWorkflow,
  unboundedWritePolicyWorkflow,
  nullOptionsWorkflow,
  totalBoundedDurableWriteWorkflow,
  nonArrayErrorTypesWorkflow,
  nonRetryableStatusDurableWriteWorkflow,
  negativeIntervalPollPolicyWorkflow,
  nonStringErrorTypePollPolicyWorkflow,
  subNanosecondIntervalPollPolicyWorkflow,
  overflowPollPolicyWorkflow,
  serviceRejectedPollPolicyWorkflow,
  unlimitedPollPolicyWorkflow,
  badDurationDurableWriteWorkflow,
  badReadTimeoutWorkflow,
  infiniteReadTimeoutWorkflow,
  badStatusTimeoutPollWorkflow,
  oversizedStatusTimeoutWorkflow,
  badStatusTimeoutDurableWriteWorkflow,
  briefWaitDurableWriteWorkflow,
  longIntervalDurableWriteWorkflow,
  shortWaitDurableWriteWorkflow,
  singleAttemptDurableWriteWorkflow,
  readThenWriteWorkflow,
  readWorkflow,
  optInRetryWriteWorkflow,
  writeWorkflow,
} from './workflows';

const WORKFLOWS_PATH = require.resolve('./workflows');
const TASK_QUEUE = 'xmemory-test';

let env: TestWorkflowEnvironment;

before(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping();
});

after(async () => {
  await env?.teardown();
});

function plugin(fake: FakeXmemoryInstance): XmemoryPlugin {
  return new XmemoryPlugin({ instanceId: 'inst-1' }, { instance: fake });
}

async function worker(fake: FakeXmemoryInstance, maxCachedWorkflows?: number): Promise<Worker> {
  return Worker.create({
    connection: env.nativeConnection,
    taskQueue: TASK_QUEUE,
    workflowsPath: WORKFLOWS_PATH,
    plugins: [plugin(fake)],
    ...(maxCachedWorkflows !== undefined ? { maxCachedWorkflows } : {}),
  });
}

async function scheduledCount(handle: WorkflowHandle): Promise<number> {
  const history = await handle.fetchHistory();
  return (history.events ?? []).filter((e) => e.activityTaskScheduledEventAttributes != null).length;
}

test('read round-trips through an activity', async () => {
  const fake = new FakeXmemoryInstance('Alice likes tea');
  const w = await worker(fake);
  const result = await w.runUntil(
    env.client.workflow.execute(readWorkflow, { taskQueue: TASK_QUEUE, workflowId: `wf-${Date.now()}-r`, args: ['q?'] }),
  );
  assert.equal(result, 'Alice likes tea');
  assert.equal(fake.count('read'), 1);
});

test('write round-trips through an activity', async () => {
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake);
  const writeId = await w.runUntil(
    env.client.workflow.execute(writeWorkflow, { taskQueue: TASK_QUEUE, workflowId: `wf-${Date.now()}-w`, args: ['remember'] }),
  );
  assert.equal(writeId, 'w1');
  assert.equal(fake.count('write'), 1);
});

test('write is at-most-once by default', async () => {
  // Writes default to at-most-once: even a retryable 500 is NOT retried (a
  // lost-response retry could duplicate — PK extraction is non-deterministic).
  const fake = new FakeXmemoryInstance();
  fake.failWriteTimes(5, apiError({ status: 500 }));
  const w = await worker(fake);
  await assert.rejects(() =>
    w.runUntil(
      env.client.workflow.execute(writeWorkflow, { taskQueue: TASK_QUEUE, workflowId: `wf-${Date.now()}-amo`, args: ['x'] }),
    ),
  );
  assert.equal(fake.count('write'), 1);
});

test('write retries when opted in', async () => {
  // The opt-in path: a workflow that sets a retryable writeRetryPolicy retries a
  // transient 500 to success.
  const fake = new FakeXmemoryInstance();
  fake.failWriteTimes(2, apiError({ status: 500 }));
  const w = await worker(fake);
  const writeId = await w.runUntil(
    env.client.workflow.execute(optInRetryWriteWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-optin`,
      args: ['x'],
    }),
  );
  assert.equal(fake.count('write'), 3);
  assert.equal(writeId, 'w1');
});

test('durable write polls to completion', async () => {
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['queued', 'processing', 'extracting', 'completed']);
  const w = await worker(fake);
  const status = await w.runUntil(
    env.client.workflow.execute(durableWriteWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-d`,
      args: ['remember'],
    }),
  );
  assert.equal(status, 'completed');
  assert.equal(fake.count('writeAsync'), 1);
  assert.equal(fake.count('writeStatus'), 4);
});

async function runDurable(fake: FakeXmemoryInstance, singleAttempt = false): Promise<string> {
  const w = await worker(fake);
  return w.runUntil(
    env.client.workflow.execute(singleAttempt ? singleAttemptDurableWriteWorkflow : durableWriteWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
      args: ['remember'],
    }),
  );
}

// A workflow failure surfaces as WorkflowFailedError; the ApplicationFailure
// (with our `type`) is somewhere down the `.cause` chain.
// Flatten message + details across the whole cause chain: everything here is
// what Temporal writes to cleartext history.
function failureText(err: unknown): string {
  const parts: unknown[] = [];
  let cur: unknown = err;
  while (cur) {
    const f = cur as { message?: string; details?: unknown; cause?: unknown };
    parts.push(f.message, f.details);
    cur = f.cause;
  }
  return JSON.stringify(parts);
}

function hasFailureType(err: unknown, type: string): boolean {
  let cur: unknown = err;
  while (cur) {
    if ((cur as { type?: string }).type === type) return true;
    cur = (cur as { cause?: unknown }).cause;
  }
  return false;
}

test('durable write fails on FAILED status', async () => {
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['processing', 'failed'], 'extractor exploded at internal-db.local');
  await assert.rejects(() => runDurable(fake), (e: unknown) => hasFailureType(e, 'XmemoryWriteFailed'));
});

test('a failed durable write keeps the server detail out of history', async () => {
  // Failure details are persisted in the clear, so the detail belongs in the
  // worker log only. Nothing the server said may appear anywhere on the failure.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['processing', 'failed'], 'extractor exploded at internal-db.local');
  await assert.rejects(
    () => runDurable(fake),
    (e: unknown) => {
      assert.doesNotMatch(failureText(e), /internal-db\.local|extractor exploded/);
      return true;
    },
  );
});

test('durable write fails immediately on NOT_FOUND', async () => {
  // `writeAsync` is transactional, so the id it returned is always queryable. A
  // not_found means the write is genuinely gone: fail on the first poll rather
  // than masking a backend that violated that contract.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['not_found']);
  await assert.rejects(() => runDurable(fake), (e: unknown) => hasFailureType(e, 'XmemoryWriteNotFound'));
  assert.equal(fake.count('writeStatus'), 1);
});

test('durable write rides through an unrecognized status (keeps polling)', async () => {
  // An unknown status must be non-terminal, not a fatal error.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['indexing', 'indexing', 'completed']);
  const status = await runDurable(fake);
  assert.equal(status, 'completed');
  assert.equal(fake.count('writeStatus'), 3);
});

test('durable write times out when never terminal', async () => {
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['processing']); // repeats, never terminal
  await assert.rejects(() => runDurable(fake), (e: unknown) => hasFailureType(e, 'XmemoryWriteTimeout'));
});

test('single write is scheduled exactly once under forced replay', async () => {
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake, 0);
  const wfId = `wf-${Date.now()}-replay1`;
  await w.runUntil(env.client.workflow.execute(writeWorkflow, { taskQueue: TASK_QUEUE, workflowId: wfId, args: ['remember'] }));
  const handle = env.client.workflow.getHandle(wfId);
  assert.equal(await scheduledCount(handle), 1);
  assert.equal(fake.count('write'), 1);
});

test('two ops scheduled once each under forced replay', async () => {
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake, 0);
  const wfId = `wf-${Date.now()}-replay2`;
  await w.runUntil(
    env.client.workflow.execute(readThenWriteWorkflow, { taskQueue: TASK_QUEUE, workflowId: wfId, args: ['remember'] }),
  );
  const handle = env.client.workflow.getHandle(wfId);
  assert.equal(await scheduledCount(handle), 2);
  assert.equal(fake.count('read'), 1);
  assert.equal(fake.count('write'), 1);
});

test('sensitivity: two writes report two under the same harness', async () => {
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake, 0);
  const wfId = `wf-${Date.now()}-double`;
  await w.runUntil(
    env.client.workflow.execute(doubleWriteWorkflow, { taskQueue: TASK_QUEUE, workflowId: wfId, args: ['remember'] }),
  );
  const handle = env.client.workflow.getHandle(wfId);
  assert.equal(await scheduledCount(handle), 2);
  assert.equal(fake.count('write'), 2);
});


test('the wait never overruns maxWaitMs', async () => {
  // maxWaitMs bounds the whole thing, polls included. Giving the last poll a
  // fresh status budget instead would let a 10s wait run for 10s + that budget.
  // The poll must actually spend its budget for that to show up as elapsed time,
  // so it is rate-limited into retrying.
  const fake = new FakeXmemoryInstance();
  fake.failStatusAlways(apiError({ status: 429, code: 'RATE_LIMITED', retryAfter: 5 }));
  const w = await worker(fake);
  const workflowId = `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`;
  await assert.rejects(
    () =>
      w.runUntil(
        env.client.workflow.execute(longIntervalDurableWriteWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId,
          args: ['remember'],
        }),
      ),
    (e: unknown) => hasFailureType(e, 'XmemoryWriteTimeout'),
  );

  const history = await env.client.workflow.getHandle(workflowId).fetchHistory();
  const times = (history.events ?? [])
    .map((ev) => Number(ev.eventTime?.seconds ?? 0))
    .filter((secs) => secs > 0);
  const elapsed = Math.max(...times) - Math.min(...times);
  assert.ok(elapsed <= 11, `ran ${elapsed}s for a 10s maxWaitMs`);
});

test('a retry hint longer than the wait is not second-guessed', async () => {
  // The server asked for an hour and only fifteen minutes remain, so another poll
  // would arrive before it is willing to answer. Wait the wait out and report the
  // timeout rather than re-polling on our own cadence.
  const fake = new FakeXmemoryInstance();
  fake.failStatusAlways(apiError({ status: 429, code: 'RATE_LIMITED', retryAfter: 3600 }));
  // Single-attempt polls, so the count is the number of loop iterations and not
  // however many Activity retries happened to fit inside one.
  await assert.rejects(() => runDurable(fake, true), (e: unknown) => hasFailureType(e, 'XmemoryWriteTimeout'));
  assert.equal(fake.count('writeStatus'), 1, 'polled again before the server said it would answer');
});

test('a fast poll does not end the wait', async () => {
  // Bounding a poll by the remaining wait must not be read as "this is the last
  // one": a status that comes back immediately leaves the whole wait available,
  // and the write may well complete on the next poll.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['processing', 'completed']);
  const w = await worker(fake);
  const out = await w.runUntil(
    env.client.workflow.execute(shortWaitDurableWriteWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
      args: ['remember'],
    }),
  );

  assert.equal(out, 'completed');
  assert.equal(fake.count('writeStatus'), 2);
});

test('a late completion is still observed', async () => {
  // A cadence longer than the wait must not mean "look once and give up": the
  // write can still land inside maxWaitMs, so take one last look as late as it
  // can complete rather than sleeping through the remainder blind.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['processing', 'completed']);
  const w = await worker(fake);
  const out = await w.runUntil(
    env.client.workflow.execute(longIntervalDurableWriteWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
      args: ['remember'],
    }),
  );

  assert.equal(out, 'completed');
  assert.equal(fake.count('writeStatus'), 2);
});

test('bad durable-write options are rejected before anything is enqueued', async () => {
  // A rejected option must not leave a queued write nobody waits on. NaN/Infinity
  // are absent because JSON cannot carry them: Temporal delivers null and the
  // default takes over. A zero interval hot-polls against Temporal's ~1ms floor.
  for (const bad of [
    { maxWaitMs: 0 },
    { pollIntervalMs: -1 },
    { pollIntervalMs: 0 },
    { pollIntervalMs: 0.5 },
    { maxPollIntervalMs: 0, pollIntervalMs: 5 },
  ]) {
    const fake = new FakeXmemoryInstance();
    const w = await worker(fake);
    await assert.rejects(
      () =>
        w.runUntil(
          env.client.workflow.execute(badOptionsDurableWriteWorkflow, {
            taskQueue: TASK_QUEUE,
            workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
            args: [bad],
          }),
        ),
      (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
    );
    assert.equal(fake.count('writeAsync'), 0, `enqueued despite ${JSON.stringify(bad)}`);
  }

  // The status budget is a constructor option, resolved in the same place: an
  // unusable one must also be caught before the non-idempotent enqueue, not on
  // the first poll after it.
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake);
  await assert.rejects(
    () =>
      w.runUntil(
        env.client.workflow.execute(badStatusTimeoutDurableWriteWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
          args: [0],
        }),
      ),
    (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
  );
  assert.equal(fake.count('writeAsync'), 0, 'enqueued despite an unusable writeStatusTimeout');
});

test('the whole wait is actually waited out', async () => {
  // A cadence longer than the wait must not collapse it: the final observation
  // belongs *at* the deadline, so sleeping zero and giving up immediately turns a
  // 10s wait into a 0.05s one.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['processing']); // never terminal
  const w = await worker(fake);
  const workflowId = `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`;
  await assert.rejects(
    () =>
      w.runUntil(
        env.client.workflow.execute(longIntervalDurableWriteWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId,
          args: ['remember'],
        }),
      ),
    (e: unknown) => hasFailureType(e, 'XmemoryWriteTimeout'),
  );

  const history = await env.client.workflow.getHandle(workflowId).fetchHistory();
  const times = (history.events ?? []).map((ev) => Number(ev.eventTime?.seconds ?? 0)).filter((s) => s > 0);
  const elapsed = Math.max(...times) - Math.min(...times);
  assert.ok(elapsed >= 9 && elapsed <= 11, `a 10s wait took ${elapsed}s`);
});

test('an equal retry hint still prevents an early poll', async () => {
  // "Retry after 2s" means exactly that, even when our own cadence is also 2s:
  // comparing the hint against the cadence rather than against zero let an equal
  // hint through and polled an endpoint that had just asked us to wait.
  const fake = new FakeXmemoryInstance();
  fake.failStatusAlways(apiError({ status: 429, code: 'RATE_LIMITED', retryAfter: 2 }));
  const w = await worker(fake);
  await assert.rejects(
    () =>
      w.runUntil(
        env.client.workflow.execute(briefWaitDurableWriteWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
          args: ['remember'],
        }),
      ),
    (e: unknown) => hasFailureType(e, 'XmemoryWriteTimeout'),
  );
  assert.equal(fake.count('writeStatus'), 1, 'polled again inside the hinted window');
});

test('an unusable poll policy is refused before the write is enqueued', async () => {
  // Temporal compiles the poll policy when it schedules the first poll — after the
  // enqueue — so an unusable one leaves a queued write nobody observes. The last
  // three are what `compileRetryPolicy` alone misses; only the service refuses them.
  const cases: { workflow: unknown; arg: number; what: string }[] = [
    { workflow: badPollPolicyDurableWriteWorkflow, arg: 0, what: 'maximumAttempts 0' },
    { workflow: serviceRejectedPollPolicyWorkflow, arg: 0.5, what: 'backoffCoefficient 0.5' },
    { workflow: overflowPollPolicyWorkflow, arg: 2_147_483_648, what: 'maximumAttempts past int32' },
    { workflow: negativeIntervalPollPolicyWorkflow, arg: -1, what: 'a negative initialInterval' },
  ];
  // These need no argument: the unusable value is baked into the fixture.
  const noArgCases: { workflow: unknown; what: string }[] = [
    { workflow: nonStringErrorTypePollPolicyWorkflow, what: 'a non-string nonRetryableErrorTypes entry' },
    { workflow: subNanosecondIntervalPollPolicyWorkflow, what: 'an interval below one nanosecond' },
    { workflow: derivedMaxIntervalPollPolicyWorkflow, what: 'an initialInterval whose derived maximum overflows' },
  ];
  for (const { workflow, arg, what } of cases) {
    const fake = new FakeXmemoryInstance();
    const w = await worker(fake);
    await assert.rejects(
      () =>
        w.runUntil(
          env.client.workflow.execute(workflow as never, {
            taskQueue: TASK_QUEUE,
            workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
            args: [arg] as never,
          }),
        ),
      (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
      `${what} was not rejected`,
    );
    assert.equal(fake.count('writeAsync'), 0, `enqueued despite ${what}`);
  }
  for (const { workflow, what } of noArgCases) {
    const fake = new FakeXmemoryInstance();
    const w = await worker(fake);
    await assert.rejects(
      () =>
        w.runUntil(
          env.client.workflow.execute(workflow as never, {
            taskQueue: TASK_QUEUE,
            workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
            args: [] as never,
          }),
        ),
      (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
      `${what} was not rejected`,
    );
    assert.equal(fake.count('writeAsync'), 0, `enqueued despite ${what}`);
  }
});

test('an unusable policy on a plain call fails the workflow, not every Workflow Task', async () => {
  // `read` has no enqueue to orphan, but an unusable policy still raises a raw
  // ValueError while the Activity command is built. Temporal reads that as a
  // Workflow *Task* failure: the workflow neither fails nor progresses, it retries
  // the same task forever. A typed non-retryable failure ends it instead.
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake);
  await assert.rejects(
    () =>
      w.runUntil(
        env.client.workflow.execute(badReadPolicyWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
          args: [0],
        }),
      ),
    (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
  );
  assert.equal(fake.count('read'), 0, 'reached the backend despite an unusable policy');
});

test('a poll stopped by nonRetryableErrorTypes is not polled again', async () => {
  // Temporal reports that verdict on the ActivityFailure's retryState; the
  // ApplicationFailure's own nonRetryable stays false. Reading only the latter, the
  // loop started another Activity and ended in a timeout verdict of our own
  // invention, hiding the failure the caller asked us to treat as terminal.
  const fake = new FakeXmemoryInstance();
  fake.failStatusAlways(apiError({ status: 500, code: 'INTERNAL_ERROR' }));
  const w = await worker(fake);
  await assert.rejects(
    () =>
      w.runUntil(
        env.client.workflow.execute(nonRetryableStatusDurableWriteWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
          args: [],
        }),
      ),
    (e: unknown) => !hasFailureType(e, 'XmemoryWriteTimeout'),
    'the server error must reach the caller, not a timeout verdict',
  );
  assert.equal(fake.count('writeStatus'), 1, 'polled again after a terminal verdict');
});

test('a non-string text or query is refused before anything is scheduled', async () => {
  // The client's write is overloaded, so an array in the text slot is applied as
  // structured mutations. And `summary()` reads `.length`, so a null query threw a
  // raw TypeError and left the workflow retrying its Workflow Task forever.
  const deleteMutation = [{ object_mutation: { object_type: 'Customer', delete: { key: { id: 1 } } } }];
  const cases: { workflow: unknown; arg: unknown; what: string }[] = [
    { workflow: nonStringTextWorkflow, arg: deleteMutation, what: 'a mutation array as text' },
    { workflow: nonStringTextWorkflow, arg: 42, what: 'a number as text' },
    // Explicitly null, which a default parameter does not cover: coercing it to ''
    // wrote an empty memory instead of reporting the caller's mistake.
    { workflow: nonStringTextWorkflow, arg: null, what: 'a null text' },
    { workflow: nonStringWriteIdWorkflow, arg: null, what: 'a null write id' },
    { workflow: nonStringQueryWorkflow, arg: null, what: 'a null query' },
    { workflow: nonStringQueryWorkflow, arg: { a: 1 }, what: 'an object as query' },
  ];
  for (const { workflow, arg, what } of cases) {
    const fake = new FakeXmemoryInstance();
    const w = await worker(fake);
    await assert.rejects(
      () =>
        w.runUntil(
          env.client.workflow.execute(workflow as never, {
            taskQueue: TASK_QUEUE,
            workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
            args: [arg] as never,
          }),
        ),
      (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
      `${what} was not rejected`,
    );
    assert.equal(fake.calls.length, 0, `${what} reached the backend`);
  }
});

test('an inherited retry-type list is not promoted into a default policy', async () => {
  // The snapshot read the caller's object before copying it, so an inherited
  // `nonRetryableErrorTypes` was copied in and then looked like theirs. A poll that
  // should have been retried became terminal, aborting a write already enqueued.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['completed']);
  fake.failStatusTimes(1, apiError({ status: 500, code: 'INTERNAL_ERROR' }));
  const w = await worker(fake);
  const out = await w.runUntil(
    env.client.workflow.execute(pollutedPolicyDurableWriteWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
      args: [],
    }),
  );
  assert.equal(out, 'completed', 'the retryable server error was treated as terminal');
});

test('a poll policy mutated after the enqueue does not reach Temporal', async () => {
  // The policy is validated before the enqueue, but the caller keeps its reference
  // and its code runs between our awaits. Mutating it left a queued write with a
  // policy Temporal refuses, so no poll was ever scheduled and every Workflow Task
  // failed. A private copy at the boundary is what stops that.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['completed']);
  const w = await worker(fake);
  const out = await w.runUntil(
    env.client.workflow.execute(mutatedPollPolicyWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
      args: [],
    }),
  );
  assert.equal(out, 'completed');
  assert.ok(fake.count('writeStatus') >= 1, 'no status poll was scheduled');
});

test('a non-boolean summary flag is refused, not read as truthy', async () => {
  // Summaries are persisted to workflow history, so "false" being truthy would put
  // memory text in the clear. Refused rather than quietly treated as off: a caller
  // who wrote "true" would otherwise get silence instead of what they asked for.
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake);
  const workflowId = `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`;
  await assert.rejects(
    () => w.runUntil(env.client.workflow.execute(stringSummaryFlagWorkflow, { taskQueue: TASK_QUEUE, workflowId, args: [] })),
    (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
  );
  const history = await env.client.workflow.getHandle(workflowId).fetchHistory();
  assert.ok(
    !JSON.stringify(history.events ?? []).includes('SECRET memory text'),
    'the memory text reached workflow history',
  );
  assert.equal(fake.count('write'), 0);
});

test('options that are not an options object are refused before the enqueue', async () => {
  // A string or an array was *boxed* into an object with no recognisable fields, so
  // every option silently fell back to its default — on the one call that promises
  // to validate its options before it enqueues anything.
  for (const bad of ['nope', [1, 2], 42]) {
    const fake = new FakeXmemoryInstance();
    const w = await worker(fake);
    await assert.rejects(
      () =>
        w.runUntil(
          env.client.workflow.execute(badOptionsContainerWorkflow, {
            taskQueue: TASK_QUEUE,
            workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
            args: [bad],
          }),
        ),
      (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
      `${JSON.stringify(bad)} was accepted as options`,
    );
    assert.equal(fake.count('writeAsync'), 0, `enqueued with ${JSON.stringify(bad)} as options`);
  }
});

test('a durable write with no text enqueues nothing', async () => {
  // A default value on `text` made it optional in the emitted declaration, and
  // `writeDurable()` then queued an empty deep write. The declaration gate asserts
  // the type side; this is the runtime half.
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake);
  await assert.rejects(
    () =>
      w.runUntil(
        env.client.workflow.execute(noTextDurableWriteWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
          args: [],
        }),
      ),
    (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
  );
  assert.equal(fake.count('writeAsync'), 0, 'enqueued a write with no text');
});

test('an options object cannot replace the text argument', async () => {
  // The payload was built as `{ text, ...options }`, so an options object carrying
  // its own `text` silently won over what the caller passed.
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake);
  await w.runUntil(
    env.client.workflow.execute(optionsOverrideTextWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
      args: [],
    }),
  );
  assert.equal(fake.calls[0]?.textOrQuery, 'INTENDED', 'the options object replaced the caller"s text');
});

test('a write policy that does not say how many attempts is refused', async () => {
  // `maximumAttempt` (singular) is ignored by Temporal, which then reads the policy
  // as unlimited retries — silently discarding the at-most-once default on the one
  // call that is not idempotent.
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake);
  await assert.rejects(
    () =>
      w.runUntil(
        env.client.workflow.execute(typoWritePolicyWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
          args: [],
        }),
      ),
    (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
  );
  assert.equal(fake.count('write'), 0, 'wrote under a policy that had lost its attempt limit');

  // The same rule with no typo to catch it: every field is valid, and the policy
  // still says nothing about attempts, which Temporal reads as unlimited.
  const plain = new FakeXmemoryInstance();
  const w2 = await worker(plain);
  await assert.rejects(
    () =>
      w2.runUntil(
        env.client.workflow.execute(unboundedWritePolicyWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
          args: [],
        }),
      ),
    (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
  );
  assert.equal(plain.count('write'), 0, 'wrote under an unbounded write policy');
});

test('a retry policy that is not an object is refused', async () => {
  // An array, string, number or boolean has none of a policy's fields, so every
  // check passes and Temporal compiles it with maximumAttempts unset — unlimited.
  // On writeRetryPolicy that silently replaces this package's at-most-once default
  // and lets a non-idempotent text write repeat.
  for (const value of [[], 'foo', 42, true]) {
    const fake = new FakeXmemoryInstance();
    const w = await worker(fake);
    await assert.rejects(
      () =>
        w.runUntil(
          env.client.workflow.execute(malformedPolicyContainerWorkflow, {
            taskQueue: TASK_QUEUE,
            workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
            args: [value],
          }),
        ),
      (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
      `${JSON.stringify(value)} was not rejected`,
    );
    assert.equal(fake.count('write'), 0, `wrote despite ${JSON.stringify(value)}`);
  }
});

test('options arriving as null are treated as omitted', async () => {
  // A default parameter only applies to `undefined`, and workflow arguments are
  // JSON, where an omitted object is usually `null`. Reading a field off it threw a
  // raw TypeError inside workflow code, which Temporal retries as a Workflow Task
  // forever rather than failing the workflow.
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake);
  const writeId = await w.runUntil(
    env.client.workflow.execute(nullOptionsWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
      args: [],
    }),
  );
  assert.equal(writeId, 'w1');
  assert.equal(fake.count('write'), 1);
});

test('a retry-type list that is not a list is refused', async () => {
  // A number or object is not iterable, so validating its members threw a raw
  // TypeError inside workflow code — a Workflow Task failure, retried forever. A
  // string is iterable and passed silently as one bogus type per character.
  for (const value of [1, { a: 1 }, 'XmemoryAuthFailed']) {
    const fake = new FakeXmemoryInstance();
    const w = await worker(fake);
    await assert.rejects(
      () =>
        w.runUntil(
          env.client.workflow.execute(nonArrayErrorTypesWorkflow, {
            taskQueue: TASK_QUEUE,
            workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
            args: [value],
          }),
        ),
      (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
      `${JSON.stringify(value)} was not rejected`,
    );
    assert.equal(fake.count('read'), 0, `reached the backend with ${JSON.stringify(value)}`);
  }
});

test('a durable poll never outlives a tighter total timeout', async () => {
  // The loop's own bound used to replace `totalTimeout` rather than compete with
  // it, so a 5s total still scheduled a 30s status poll. Asserted on what the loop
  // schedules, which is what Temporal enforces.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['processing']); // never terminal, so the wait runs its course
  const w = await worker(fake);
  const workflowId = `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`;
  await assert.rejects(
    () =>
      w.runUntil(
        env.client.workflow.execute(totalBoundedDurableWriteWorkflow, {
          taskQueue: TASK_QUEUE,
          workflowId,
          args: [],
        }),
      ),
    (e: unknown) => hasFailureType(e, 'XmemoryWriteTimeout'),
  );
  const history = await env.client.workflow.getHandle(workflowId).fetchHistory();
  const bounds = (history.events ?? [])
    .map((event) => event.activityTaskScheduledEventAttributes)
    .filter((scheduled) => scheduled?.activityType?.name === 'xmemory_write_status')
    .map((scheduled) => Number(scheduled?.scheduleToCloseTimeout?.seconds ?? 0));
  assert.ok(bounds.length > 0, 'no status poll was scheduled');
  for (const seconds of bounds) {
    assert.ok(seconds > 0 && seconds <= 5, `a poll was scheduled with a ${seconds}s bound, past the 5s total`);
  }
});

test('a total timeout is scheduled as the activity\'s schedule-to-close', async () => {
  // startToClose bounds one attempt; without this a preserved Retry-After can park
  // a call in retries for hours. Asserted on what the loop schedules, since a call
  // that merely succeeds says nothing about the bound being applied.
  const fake = new FakeXmemoryInstance();
  const w = await worker(fake);
  const workflowId = `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`;
  const out = await w.runUntil(
    env.client.workflow.execute(boundedReadWorkflow, { taskQueue: TASK_QUEUE, workflowId, args: [] }),
  );
  assert.equal(out, 'the answer');
  const history = await env.client.workflow.getHandle(workflowId).fetchHistory();
  const bounds = (history.events ?? [])
    .map((event) => event.activityTaskScheduledEventAttributes)
    .filter((scheduled) => scheduled?.activityType?.name === 'xmemory_read')
    .map((scheduled) => Number(scheduled?.scheduleToCloseTimeout?.seconds ?? 0));
  assert.deepEqual(bounds, [5], 'the read was not scheduled under the 5s total');
});

test('valid policies the SDK compiles are not refused', async () => {
  // The pre-enqueue check must reject only what the service would: `Infinity`
  // attempts is Temporal's own spelling of unlimited, and a month-long interval is
  // a protobuf Duration the service holds, not a `setTimeout` this package sets.
  for (const workflow of [longIntervalPollPolicyWorkflow, unlimitedPollPolicyWorkflow]) {
    const fake = new FakeXmemoryInstance();
    const w = await worker(fake);
    const out = await w.runUntil(
      env.client.workflow.execute(workflow, {
        taskQueue: TASK_QUEUE,
        workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
        args: [],
      }),
    );
    assert.equal(out, 'completed');
  }
});

test('the final look is a single attempt', async () => {
  // The last observation happens *at* the deadline, so nothing but its own retry
  // policy bounds it: letting it retry turned a 10s wait into 35s. Asserted on what
  // the loop schedules, since whether a retry chain materialises is timing-dependent.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['processing', 'completed']);
  const w = await worker(fake);
  const workflowId = `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`;
  const out = await w.runUntil(
    env.client.workflow.execute(longIntervalDurableWriteWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId,
      args: ['remember'],
    }),
  );
  assert.equal(out, 'completed');

  const history = await env.client.workflow.getHandle(workflowId).fetchHistory();
  const polls = (history.events ?? [])
    .map((event) => event.activityTaskScheduledEventAttributes)
    .filter((scheduled) => scheduled?.activityType?.name === 'xmemory_write_status')
    .map((scheduled) => scheduled?.retryPolicy?.maximumAttempts);
  assert.ok(polls.length >= 2, `expected an ordinary poll and a final one, got ${polls.length}`);
  assert.equal(polls[polls.length - 1], 1, 'the final look must not retry');
  assert.notEqual(polls[0], 1, 'ordinary polls keep the configured policy');
});

test('unusable durations are rejected on every call, not just writeDurable', async () => {
  // `msToNumber` passes Infinity straight through and throws a raw TypeError on a
  // malformed string. Either one reaches Temporal and fails the Workflow Task over
  // and over. The upper bound matters too: setTimeout turns anything above 2**31-1
  // into 1ms, and the service refuses oversized durations when it builds the
  // command — which for a durable write is after the enqueue.
  const cases: { workflow: unknown; arg: unknown; what: string }[] = [
    { workflow: infiniteReadTimeoutWorkflow, arg: undefined, what: 'infinite read timeout' },
    { workflow: badReadTimeoutWorkflow, arg: 'garbage', what: 'malformed read timeout' },
    { workflow: badReadTimeoutWorkflow, arg: 2_147_483_648, what: 'oversized read timeout' },
    // Truncated to zero by Temporal, after which the activity falls back to the
    // default ten-year schedule-to-close and the client is handed that as a budget.
    { workflow: badReadTimeoutWorkflow, arg: 0.5, what: 'sub-millisecond read timeout' },
    { workflow: badStatusTimeoutPollWorkflow, arg: 'garbage', what: 'malformed status timeout on writeStatus' },
    { workflow: badDurationDurableWriteWorkflow, arg: 'garbage', what: 'malformed status timeout on writeDurable' },
    { workflow: oversizedStatusTimeoutWorkflow, arg: 2_147_483_648, what: 'oversized durable status timeout' },
  ];
  for (const { workflow, arg, what } of cases) {
    const fake = new FakeXmemoryInstance();
    const w = await worker(fake);
    await assert.rejects(
      () =>
        w.runUntil(
          env.client.workflow.execute(workflow as never, {
            taskQueue: TASK_QUEUE,
            workflowId: `wf-${Date.now()}-${Math.round(Math.random() * 1e6)}`,
            args: (arg === undefined ? [] : [arg]) as never,
          }),
        ),
      (e: unknown) => hasFailureType(e, 'XmemoryBadOptions'),
      `${what} was not rejected as a bad option`,
    );
    assert.equal(fake.count('writeAsync'), 0, `${what} enqueued a write`);
    assert.equal(fake.count('read'), 0, `${what} reached the backend`);
  }
});
