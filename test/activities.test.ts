import assert from 'node:assert/strict';
import { test } from 'node:test';
import { MockActivityEnvironment } from '@temporalio/testing';
import { ApplicationFailure } from '@temporalio/common';
import {
  ACTIVITY_READ,
  ACTIVITY_WRITE,
  ACTIVITY_WRITE_START,
  ACTIVITY_WRITE_STATUS,
  createActivities,
  InstanceHolder,
} from '../src/activities';
import type { ReadOutput, WriteOutput, WriteStatusOutput } from '../src/dto';
import * as errors from '../src/errors';
import { activityBudgetMs } from '../src/deadline';
import { xmemoryForWorkflow } from '../src/workflow';
import { clientTimeoutMs, MAX_DURATION_MS } from '../src/defaults';
import { FakeXmemoryInstance, apiError } from './fakes';

/**
 * A MockActivityEnvironment with a live deadline. The mock anchors
 * `scheduledTimestampMs` at 1, which reads as long expired.
 */
function liveEnv(overrides: Record<string, unknown> = {}): MockActivityEnvironment {
  return new MockActivityEnvironment({ scheduledTimestampMs: Date.now(), ...overrides } as never);
}

function build(fake: FakeXmemoryInstance) {
  const holder = new InstanceHolder();
  holder.bind(fake);
  return createActivities(holder, { instanceId: 'inst-1' });
}

test('read projects the result', async () => {
  const fake = new FakeXmemoryInstance('Alice likes tea');
  const acts = build(fake);
  const env = liveEnv();
  const out = (await env.run(acts[ACTIVITY_READ], { query: "what does Alice like?" })) as ReadOutput;
  assert.equal(out.readerResult, 'Alice likes tea');
  assert.equal(fake.count('read'), 1);
});

test('write returns the write id', async () => {
  const fake = new FakeXmemoryInstance();
  const acts = build(fake);
  const env = liveEnv();
  const out = (await env.run(acts[ACTIVITY_WRITE], { text: 'Alice likes tea' })) as WriteOutput;
  assert.equal(out.writeId, 'w1');
});

test('write status projects the enum value', async () => {
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['processing']);
  const acts = build(fake);
  const env = liveEnv();
  const out = (await env.run(acts[ACTIVITY_WRITE_STATUS], { writeId: 'w1' })) as WriteStatusOutput;
  assert.equal(out.writeStatus, 'processing');
});

test('write status keeps the server error detail out of its result', async () => {
  // Activity results are persisted to cleartext history, and the server's
  // detail is not promised user-safe, so it must not ride along in the DTO.
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['failed'], 'boom at internal-db.local:5432');
  const acts = build(fake);
  const env = liveEnv();
  const out = (await env.run(acts[ACTIVITY_WRITE_STATUS], { writeId: 'w1' })) as WriteStatusOutput;

  assert.equal(out.writeStatus, 'failed');
  assert.doesNotMatch(JSON.stringify(out), /internal-db\.local|errorDetail/);
});

test('client error becomes an ApplicationFailure', async () => {
  const fake = new FakeXmemoryInstance();
  fake.failWriteTimes(1, apiError({ status: 401, code: 'UNAUTHORIZED' }));
  const acts = build(fake);
  const env = liveEnv();
  await assert.rejects(
    () => env.run(acts[ACTIVITY_WRITE], { text: 'x' }),
    (err: unknown) => {
      assert.ok(err instanceof ApplicationFailure);
      assert.equal(err.type, errors.TYPE_AUTH_FAILED);
      assert.equal(err.nonRetryable, true);
      return true;
    },
  );
});

test('unbound activity raises a clear, non-retryable, typed error', async () => {
  const holder = new InstanceHolder();
  const acts = createActivities(holder, { instanceId: 'inst-1' });
  const env = liveEnv();
  await assert.rejects(
    () => env.run(acts[ACTIVITY_READ], { query: 'q' }),
    (err: unknown) => {
      assert.ok(err instanceof ApplicationFailure);
      assert.match(String(err.message), /not bound/);
      assert.equal(err.type, errors.TYPE_NOT_BOUND);
      assert.equal(err.nonRetryable, true);
      return true;
    },
  );
});

test('client timeout tracks the activity deadline', async () => {
  // The invariant this redesign exists for: the client budget derives from the
  // deadline Temporal assigned this attempt, so a workflow that lowers its
  // startToClose lowers the client timeout with it. Previously the client read a
  // separate worker-side number, and a short workflow budget silently inverted
  // the order (Temporal abandoning the attempt while the request ran on).
  const fake = new FakeXmemoryInstance('ok');
  const acts = build(fake);
  for (const budgetMs of [3_000, 45_000, 120_000]) {
    // Realistic anchor: the mock defaults scheduledTimestampMs to 1, which reads
    // as an expired deadline and would quietly make this a constant-floor test.
    const env = liveEnv({
      startToCloseTimeoutMs: budgetMs,
      scheduleToCloseTimeoutMs: budgetMs,
      scheduledTimestampMs: Date.now(),
    });
    await env.run(acts[ACTIVITY_READ], { query: 'q' });
    const used = fake.calls[fake.calls.length - 1].options?.timeoutMs as number;
    // Below the budget, but *tracking* it. `used < budgetMs` alone passes even
    // when the derivation collapses to a constant floor for every budget.
    assert.ok(used < budgetMs, `client must give up first for a ${budgetMs}ms budget (got ${used})`);
    assert.ok(used >= budgetMs * 0.5, `client budget ${used}ms does not track a ${budgetMs}ms deadline`);
  }
});

test('a client budget never exceeds what a timer can hold', async () => {
  // Timeouts are Temporal's to validate, so a long one reaches the activity as is.
  // The client arms a `setTimeout` with its budget, and Node fires anything past
  // 2**31-1 after 1ms — so an uncapped 30-day deadline aborted every call at once.
  const fake = new FakeXmemoryInstance('ok');
  const acts = build(fake);
  const thirtyDaysMs = 30 * 24 * 60 * 60 * 1000;
  // Schedule-to-close unset (0), or the mock's 1s default would be the binding bound.
  const env = liveEnv({ startToCloseTimeoutMs: thirtyDaysMs, scheduleToCloseTimeoutMs: 0 });
  const out = (await env.run(acts[ACTIVITY_READ], { query: 'q' })) as ReadOutput;
  assert.equal(out.readerResult, 'ok');
  const used = fake.calls[fake.calls.length - 1].options?.timeoutMs as number;
  assert.ok(used <= MAX_DURATION_MS, `client budget ${used}ms is past a timer's limit`);
  assert.ok(used < thirtyDaysMs, 'the client must still give up before Temporal');
});

test('scheduleToClose alone is a valid deadline', async () => {
  // The only shape that reaches the second half of the `||`.
  const fake = new FakeXmemoryInstance('ok');
  const acts = build(fake);
  const env = liveEnv({
    startToCloseTimeoutMs: 0,
    scheduleToCloseTimeoutMs: 45_000,
    scheduledTimestampMs: Date.now(),
  });
  await env.run(acts[ACTIVITY_READ], { query: 'q' });
  const used = fake.calls[fake.calls.length - 1].options?.timeoutMs as number;
  assert.ok(used < 45_000, 'the client must still give up first');
  assert.ok(used >= 45_000 * 0.5, `client budget ${used}ms does not track the scheduleToClose deadline`);
});

test('no deadline fails with a typed non-retryable failure', async () => {
  // Temporal requires one of the two close timeouts, so this is unreachable in
  // practice; assert the *typed* failure rather than merely "something threw",
  // because toApplicationFailure would turn a re-mapped one into a retryable
  // XmemoryUnavailable and retry a misconfiguration that can never come good.
  const fake = new FakeXmemoryInstance('ok');
  const acts = build(fake);
  const env = liveEnv({ startToCloseTimeoutMs: 0, scheduleToCloseTimeoutMs: 0 });
  await assert.rejects(
    () => env.run(acts[ACTIVITY_READ], { query: 'q' }),
    (err: unknown) => {
      assert.ok(err instanceof ApplicationFailure);
      assert.equal(err.type, errors.TYPE_NO_DEADLINE);
      assert.equal(err.nonRetryable, true);
      return true;
    },
  );
  assert.equal(fake.calls.length, 0);
});

test('structured mutations skip extraction', async () => {
  // A structured write carries its own primary keys, so the client gets the
  // mutations verbatim and no extractionLogic: the server applies it without
  // running the extractor, which is what makes it deterministic to retry.
  const fake = new FakeXmemoryInstance();
  const acts = build(fake);
  const env = liveEnv();
  const mutations = [
    {
      object_mutation: {
        object_type: 'Customer',
        update: { key: { customerId: 'c-1' }, values: { tier: 'gold' } },
      },
    },
  ];
  await env.run(acts[ACTIVITY_WRITE], { text: '', structuredMutations: mutations } as never);
  const call = fake.calls[fake.calls.length - 1];
  // Forwarded to the client unchanged.
  assert.deepEqual(call.options?.structuredMutations, mutations);
  assert.equal(call.options?.extractionLogic, undefined);
});

test('text write still sends extractionLogic', async () => {
  const fake = new FakeXmemoryInstance();
  const acts = build(fake);
  const env = liveEnv();
  await env.run(acts[ACTIVITY_WRITE], { text: 'Alice likes tea' });
  const call = fake.calls[fake.calls.length - 1];
  assert.equal(call.options?.extractionLogic, 'fast');
  assert.equal(call.options?.structuredMutations, undefined);
});

test('a stalled response body cannot outlive the client budget', async () => {
  // The client clears its abort timer once fetch resolves at response headers,
  // so `timeoutMs` alone leaves the body read unbounded. Without the total
  // bound this call would hang past the Activity deadline and Temporal would
  // time out the attempt instead of the client failing first.
  const fake = new FakeXmemoryInstance('ok');
  fake.stallReads();
  const acts = build(fake);
  const env = liveEnv({ startToCloseTimeoutMs: 300, scheduleToCloseTimeoutMs: 300 });
  const started = Date.now();
  await assert.rejects(() => env.run(acts[ACTIVITY_READ], { query: 'q' }));
  assert.ok(Date.now() - started < 3_000, 'must give up on its own, not hang');
});

/** Within a tolerance that a slow CI tick cannot exceed but a real defect does. */
function near(actual: number | null, expected: number, toleranceMs = 250): boolean {
  return actual !== null && Math.abs(actual - expected) <= toleranceMs;
}

test('the budget is what is left of the deadline Temporal enforces', () => {
  const now = Date.now();
  const budget = (startToCloseTimeoutMs: number, scheduleToCloseTimeoutMs: number, scheduledMsAgo = 0) =>
    activityBudgetMs({ startToCloseTimeoutMs, scheduleToCloseTimeoutMs, scheduledTimestampMs: now - scheduledMsAgo });

  // Whichever bound expires first, not whichever is nominally per-attempt: a short
  // scheduleToClose ends this attempt long before a longer startToClose would, and
  // handing the client the larger figure lets a write commit after Temporal gave up.
  assert.equal(budget(30_000, 60_000), 30_000);
  // Bounds counted from a timestamp move with the clock between the two `Date.now()`
  // readings, so they are asserted as ranges. A millisecond of drift is not a defect;
  // taking the wrong bound is, and the gaps here are seconds wide.
  assert.ok(near(budget(30_000, 1_000), 1_000), `expected the 1s bound, got ${budget(30_000, 1_000)}`);
  // Only one set: that is what Temporal enforces.
  assert.ok(near(budget(0, 60_000), 60_000), `expected 60s, got ${budget(0, 60_000)}`);
  assert.equal(budget(45_000, 0), 45_000);
  // Neither: the activity has no deadline to derive a client timeout from.
  assert.equal(budget(0, 0), null);

  // scheduleToClose covers every attempt, so time already spent comes off it. The
  // service's own timestamp shows that much; without it a 10s deadline nine
  // seconds in still handed the client nine seconds.
  const remaining = budget(0, 10_000, 9_000);
  assert.ok(remaining !== null && remaining > 500 && remaining <= 1_000, `expected about 1s left, got ${remaining}`);
  // Spent: reported as spent, so the caller can refuse rather than send a doomed request.
  const expired = budget(0, 10_000, 11_000);
  assert.ok(expired !== null && expired <= 0, `expected an expired budget, got ${expired}`);
  // A stamp in the future (a worker clock behind the service, or a time-skipping
  // test server) must not lengthen the budget.
  assert.equal(budget(0, 10_000, -60_000), 10_000);
});

test('elapsed time inside the attempt is charged once, not twice', () => {
  // Auto-capture runs partway through an Activity and passes what it has measured.
  // scheduleToClose is counted from the service's timestamp and already contains
  // that time, so applying the caller's figure to it as well charged the same
  // milliseconds twice — and skipped capture with budget to spare.
  const now = Date.now();
  const info = { startToCloseTimeoutMs: 0, scheduleToCloseTimeoutMs: 10_000, scheduledTimestampMs: now - 3_000 };
  const withoutElapsed = activityBudgetMs(info);
  const withElapsed = activityBudgetMs(info, 3_000);
  // Equal within a tick: each call reads the clock itself. Charging twice would put
  // them three seconds apart.
  assert.ok(
    withoutElapsed !== null && near(withElapsed, withoutElapsed),
    `schedule-to-close was charged the elapsed figure again: ${withElapsed} vs ${withoutElapsed}`,
  );
  assert.ok(near(withElapsed, 7_000), `expected about 7s left, got ${withElapsed}`);

  // start-to-close has no timestamp behind it, so there the caller's figure is the
  // only account of the time spent.
  assert.equal(activityBudgetMs({ ...info, startToCloseTimeoutMs: 8_000 }, 3_000), 5_000);
});

test('an activity past its deadline fails instead of sending a doomed request', async () => {
  const fake = new FakeXmemoryInstance();
  const acts = build(fake);
  const env = new MockActivityEnvironment({
    scheduleToCloseTimeoutMs: 10_000,
    startToCloseTimeoutMs: 0,
    scheduledTimestampMs: Date.now() - 60_000,
  } as never);
  await assert.rejects(
    () => env.run(acts[ACTIVITY_READ], { query: 'q' }),
    (err: unknown) => {
      assert.equal((err as ApplicationFailure).type, 'XmemoryDeadlineExpired');
      return true;
    },
  );
  assert.equal(fake.count('read'), 0, 'no request may be sent past the deadline');
});

test('poll retry options that Temporal could not use are refused', () => {
  // The policy is built from these in the constructor, which is plain arithmetic —
  // no workflow needed to exercise it. The one thing that does need a workflow is
  // that the rejection lands before the enqueue, which workflow.test.ts covers.
  for (const [retry, what] of [
    [{ attempts: 0 }, 'zero attempts'],
    [{ attempts: 2.5 }, 'a fractional attempt count'],
    [{ intervalMs: 0 }, 'a zero interval'],
    [{ intervalMs: 1_000, maxIntervalMs: 500 }, 'a ceiling below the interval'],
  ] as const) {
    assert.throws(
      () => xmemoryForWorkflow({ writeStatusRetry: retry as never }),
      (err: unknown) => {
        assert.equal((err as ApplicationFailure).type, 'XmemoryBadOptions', what);
        assert.equal((err as ApplicationFailure).nonRetryable, true);
        return true;
      },
      `${what} was accepted`,
    );
  }
  // Unlimited is Temporal's own spelling, and the defaults stand on their own.
  assert.doesNotThrow(() => xmemoryForWorkflow({ writeStatusRetry: { attempts: Number.POSITIVE_INFINITY } }));
  assert.doesNotThrow(() => xmemoryForWorkflow({}));
});

test('the client deadline stays strictly under Temporal for every input', () => {
  // The margin exists so the client fails first with an attributable error. A
  // budget at or under the margin, and a nonsensical margin, must not invert it.
  for (const [activityMs, marginMs] of [
    [50, 5_000],
    [5_000, 0],
    [30_000, -1_000],
    [120_000, 5_000],
    [200, Number.NaN],
  ] as const) {
    const used = clientTimeoutMs(activityMs, marginMs);
    assert.ok(used > 0 && used < activityMs, `activity=${activityMs} margin=${marginMs} gave ${used}`);
  }
  assert.throws(() => clientTimeoutMs(0), RangeError);
});

test('logServerErrorDetail only logs on a real true', async () => {
  // Config comes from files and env vars as much as from code, and the string
  // "false" is both a plausible way to write "off" and truthy — which turned the
  // server's detail on and put memory text in the worker log.
  const logged: Record<string, unknown>[] = [];
  const logger = {
    trace: () => {},
    debug: () => {},
    info: () => {},
    warn: (_message: string, attrs?: Record<string, unknown>) => void logged.push(attrs ?? {}),
    error: () => {},
    log: () => {},
  };
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['failed'], 'SECRET memory text at internal-db.local');
  const holder = new InstanceHolder();
  holder.bind(fake);
  const acts = createActivities(holder, { instanceId: 'i', logServerErrorDetail: 'false' as never });
  const env = new MockActivityEnvironment({ scheduledTimestampMs: Date.now() } as never, { logger } as never);
  await env.run(acts[ACTIVITY_WRITE_STATUS], { writeId: 'w1' });

  const dumped = JSON.stringify(logged);
  assert.ok(!dumped.includes('SECRET'), `the detail was logged under the string "false": ${dumped}`);
  assert.ok(dumped.includes('errorDetailLength'), `the withheld-detail note is missing: ${dumped}`);

  // And a real `true` still logs it, so the check above is not passing by accident.
  logged.length = 0;
  const onFake = new FakeXmemoryInstance();
  onFake.statusSequence(['failed'], 'SECRET memory text at internal-db.local');
  const onHolder = new InstanceHolder();
  onHolder.bind(onFake);
  const onActs = createActivities(onHolder, { instanceId: 'i', logServerErrorDetail: true });
  const onEnv = new MockActivityEnvironment({ scheduledTimestampMs: Date.now() } as never, { logger } as never);
  await onEnv.run(onActs[ACTIVITY_WRITE_STATUS], { writeId: 'w1' });
  assert.ok(JSON.stringify(logged).includes('SECRET'), 'an explicit true must still log the detail');
});

test('an empty write id is not a usable one', async () => {
  // On the enqueue, where nothing else can catch it: an empty string passed every
  // type check, and the durable loop was then left polling for a write it could not
  // name. (On a status poll the correlation check would reject it first, so this is
  // the path that isolates the emptiness rule.)
  const fake = new FakeXmemoryInstance();
  fake.enqueueWithoutWriteId();
  const acts = build(fake);
  await assert.rejects(
    () => liveEnv().run(acts[ACTIVITY_WRITE_START], { text: 'remember' }),
    (err: unknown) => {
      assert.equal((err as ApplicationFailure).type, 'XmemoryUnavailable');
      return true;
    },
  );
});

test('a status for another write is refused, before its detail is logged', async () => {
  // Nothing tied the answer to the question, so another write's outcome — a
  // `completed` that never happened — would be read as this one's, and its detail
  // logged under the id we asked about.
  const logged: Record<string, unknown>[] = [];
  const logger = {
    trace: () => {},
    debug: () => {},
    info: () => {},
    warn: (_m: string, attrs?: Record<string, unknown>) => void logged.push(attrs ?? {}),
    error: () => {},
    log: () => {},
  };
  const fake = new FakeXmemoryInstance();
  fake.statusSequence(['failed'], 'SECRET detail for another write');
  fake.answerWithWriteId('some-other-write');
  const holder = new InstanceHolder();
  holder.bind(fake);
  const acts = createActivities(holder, { instanceId: 'i', logServerErrorDetail: true });
  const env = new MockActivityEnvironment({ scheduledTimestampMs: Date.now() } as never, { logger } as never);
  await assert.rejects(() => env.run(acts[ACTIVITY_WRITE_STATUS], { writeId: 'w1' }));
  assert.ok(!JSON.stringify(logged).includes('SECRET'), `another write's detail was logged: ${JSON.stringify(logged)}`);
});

test('a malformed status response is a transport failure, not a status', async () => {
  // Without its required fields a response would read as an unknown status, which
  // the durable loop polls straight through. Refused instead, and mapped as a
  // transport failure: a malformed response may well be transient.
  const fake = new FakeXmemoryInstance();
  fake.returnMalformedStatus(); // a response with no fields at all
  const acts = build(fake);
  await assert.rejects(
    () => liveEnv().run(acts[ACTIVITY_WRITE_STATUS], { writeId: 'w1' }),
    (err: unknown) => {
      assert.equal((err as ApplicationFailure).type, 'XmemoryUnavailable');
      return true;
    },
  );
});

test('an empty structuredMutations list is a bad option on both write paths', async () => {
  // The client answers `[]` with a plain Error, which the mapper reads as a
  // retryable transport failure — so Temporal would retry what cannot succeed,
  // including on the enqueue, the one call that is not idempotent.
  for (const activity of [ACTIVITY_WRITE, ACTIVITY_WRITE_START] as const) {
    const fake = new FakeXmemoryInstance();
    const acts = build(fake);
    await assert.rejects(
      () => liveEnv().run(acts[activity], { text: '', structuredMutations: [] } as never),
      (err: unknown) => {
        const f = err as ApplicationFailure;
        assert.equal(f.type, 'XmemoryBadOptions', activity);
        assert.equal(f.nonRetryable, true);
        return true;
      },
    );
    assert.equal(fake.calls.length, 0, `a request was sent for an unusable list on ${activity}`);
  }
});

test('a text that is not a string never reaches the client, which would read it as mutations', async () => {
  // `write` and `writeAsync` are overloaded on their first argument, so an array in
  // the text slot is applied as structured mutations — a delete, in this probe —
  // instead of being written as memory. Activity inputs are JSON: the type
  // annotation proves nothing at this boundary.
  const deleteMutation = [{ object_mutation: { object_type: 'Customer', delete: { key: { customerId: 'c-1' } } } }];
  for (const bad of [deleteMutation, 42, null, { a: 1 }]) {
    for (const activity of [ACTIVITY_WRITE, ACTIVITY_WRITE_START] as const) {
      const fake = new FakeXmemoryInstance();
      const acts = build(fake);
      await assert.rejects(
        () => liveEnv().run(acts[activity], { text: bad } as never),
        (err: unknown) => {
          const f = err as ApplicationFailure;
          assert.equal(f.type, 'XmemoryBadOptions', `${JSON.stringify(bad)} on ${activity}`);
          assert.equal(f.nonRetryable, true);
          return true;
        },
      );
      assert.equal(fake.calls.length, 0, `${JSON.stringify(bad)} reached the client on ${activity}`);
    }
  }
});

test('structuredMutations that is not a list never reaches the client', async () => {
  // A string has a `length`, so it would pass the empty check and go to the client's
  // *text* overload: the mutation value written as memory, the caller's text dropped.
  for (const bad of ['ATTACKER TEXT', 42, { object_mutation: {} }, true]) {
    for (const activity of [ACTIVITY_WRITE, ACTIVITY_WRITE_START] as const) {
      const fake = new FakeXmemoryInstance();
      const acts = build(fake);
      await assert.rejects(
        () => liveEnv().run(acts[activity], { text: 'INTENDED TEXT', structuredMutations: bad } as never),
        (err: unknown) => {
          const f = err as ApplicationFailure;
          assert.equal(f.type, 'XmemoryBadOptions', `${JSON.stringify(bad)} on ${activity}`);
          assert.equal(f.nonRetryable, true);
          return true;
        },
      );
      assert.equal(fake.calls.length, 0, `${JSON.stringify(bad)} reached the client on ${activity}`);
    }
  }
});
