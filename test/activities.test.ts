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
import { clientTimeoutMs } from '../src/defaults';
import { FakeXmemoryInstance, apiError } from './fakes';
import { InstanceHandle, XmemoryClient } from 'xmemory';

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
  // Compared as JSON: the mutations are forwarded as a null-prototype copy, so a
  // missing field cannot be answered by Object.prototype on its way to the client.
  // What matters is that the content reaches the wire unchanged.
  assert.deepEqual(JSON.parse(JSON.stringify(call.options?.structuredMutations)), mutations);
  assert.equal(Object.getPrototypeOf(call.options?.structuredMutations as object[]).constructor, Array);
  assert.equal(Object.getPrototypeOf((call.options?.structuredMutations as object[])[0]), null);
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

test('an inherited field inside a nested scope does not widen a read', async () => {
  // Normalizing only the outer input leaves nested objects with their own
  // prototypes, and the scope's fields decide what the read may reach: an inherited
  // `relationsScope` widened a scoped read to all_relations, and the extra memory
  // it returned would be persisted to workflow history.
  const proto = Object.prototype as Record<string, unknown>;
  try {
    proto.relationsScope = 'all_relations';
    const fake = new FakeXmemoryInstance();
    const acts = build(fake);
    await liveEnv().run(acts[ACTIVITY_READ], {
      query: 'q',
      scope: { objects: [{ type: 'Person', key: { name: 'Ada' } }] },
    } as never);
    // Read the property, not a JSON dump: `JSON.stringify` skips inherited fields
    // by definition, so a dump cannot see the widening at all — the client reads
    // `scope.relationsScope` directly, which is where it happens.
    const scope = fake.calls[0]?.options?.scope as { relationsScope?: string } | undefined;
    assert.equal(scope?.relationsScope, undefined, `the read was widened to ${scope?.relationsScope}`);
  } finally {
    delete proto.relationsScope;
  }
});

test('inherited option fields do not reach the client', async () => {
  // The objects handed to the client are read by it, so an omitted `readMode` or
  // `diffEngine` was answered by Object.prototype: a default read became raw-tables
  // and a plain text write picked up a diff engine it was never given.
  const proto = Object.prototype as Record<string, unknown>;
  try {
    proto.readMode = 'raw-tables';
    proto.diffEngine = true;
    proto.scope = { objects: [] };
    const fake = new FakeXmemoryInstance();
    const acts = build(fake);
    await liveEnv().run(acts[ACTIVITY_READ], { query: 'q' });
    await liveEnv().run(acts[ACTIVITY_WRITE], { text: 'plain text' });
    const [read, write] = fake.calls;
    assert.equal((read.options as { readMode?: string }).readMode, undefined, 'the read mode was inherited');
    assert.equal((read.options as { scope?: unknown }).scope, undefined, 'a scope was inherited');
    assert.equal((write.options as { diffEngine?: boolean }).diffEngine, undefined, 'the diff engine was inherited');
  } finally {
    delete proto.readMode;
    delete proto.diffEngine;
    delete proto.scope;
  }
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

test('the real client does not hand us fabricated sub-answers', async () => {
  // Through the *real* client, not a fake: the client normalizes the wire shape,
  // and normalizing with `result.field ?? default` used to read an inherited value
  // and write it back as an own property — where no check here could tell it apart
  // from something the server sent. Fixed upstream in xmemory 3.8.1; this is the
  // regression that says so.
  const proto = Object.prototype as Record<string, unknown>;
  try {
    proto.reader_results = [{ sub_query: 'fabricated', reader_result: 'LEAKED MEMORY', error: null }];
    proto.trace_id = 'fabricated-trace';
    proto.reader_result = 'FABRICATED ANSWER';
    // The whole client, not just the handle: an `items` array supplied by the
    // prototype made an empty `200` body come back as a genuine result, which is a
    // layer above the one `InstanceHandle` alone exercises.
    proto.items = [{ reader_result: 'FABRICATED', reader_results: [], trace_id: null, console_url: null }];
    const client = new XmemoryClient({ apiKey: 'k', url: 'https://api.example.com' });
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () =>
      new Response('{}', { status: 200, headers: { 'content-type': 'application/json' } })) as never;
    try {
      await assert.rejects(() => client.instance('inst-1').read('q'), /Expected one item/);
    } finally {
      globalThis.fetch = originalFetch;
      delete proto.items;
    }

    // A transport that answers with exactly what a lean server sends.
    const handle = new InstanceHandle('inst-1', (async () => ({ reader_result: 'the real answer' })) as never);
    const holder = new InstanceHolder();
    holder.bind(handle as never);
    const acts = createActivities(holder, { instanceId: 'inst-1' });
    const out = (await liveEnv().run(acts[ACTIVITY_READ], { query: 'q' })) as ReadOutput;
    assert.equal(out.readerResult, 'the real answer');
    assert.deepEqual(out.subAnswers, [], `fabricated sub-answers reached the workflow: ${JSON.stringify(out.subAnswers)}`);
    assert.equal(out.traceId, null);
  } finally {
    delete proto.reader_results;
    delete proto.trace_id;
    delete proto.reader_result;
  }
});

test('an inherited error_detail is never logged', async () => {
  // The detail was read straight off the response, before any own-property
  // projection — so a polluted prototype put text into the worker log that no
  // server sent, on the one setting that is documented as opt-in.
  const proto = Object.prototype as Record<string, unknown>;
  const logged: Record<string, unknown>[] = [];
  const logger = {
    trace: () => {},
    debug: () => {},
    info: () => {},
    warn: (_m: string, attrs?: Record<string, unknown>) => void logged.push(attrs ?? {}),
    error: () => {},
    log: () => {},
  };
  try {
    proto.error_detail = 'SECRET inherited detail';
    const fake = new FakeXmemoryInstance();
    fake.statusSequence(['failed']);
    fake.omitErrorDetail(); // the field is absent, so the prototype would answer it
    const holder = new InstanceHolder();
    holder.bind(fake);
    const acts = createActivities(holder, { instanceId: 'i', logServerErrorDetail: true });
    const env = new MockActivityEnvironment({ scheduledTimestampMs: Date.now() } as never, { logger } as never);
    await env.run(acts[ACTIVITY_WRITE_STATUS], { writeId: 'w1' });
    assert.ok(!JSON.stringify(logged).includes('SECRET'), `an inherited detail was logged: ${JSON.stringify(logged)}`);
  } finally {
    delete proto.error_detail;
  }
});

test('a mutation nested past the copy limit is a bad option, not a stack overflow', async () => {
  // The copy is recursive, and an unbounded one turns a deep payload into a
  // RangeError from the call stack — which reaches Temporal untyped, and therefore
  // retryable, for input that can never work.
  let deep: Record<string, unknown> = {};
  for (let i = 0; i < 200; i++) deep = { nested: deep };
  const fake = new FakeXmemoryInstance();
  const acts = build(fake);
  await assert.rejects(
    () => liveEnv().run(acts[ACTIVITY_WRITE], { text: '', structuredMutations: [deep] } as never),
    (err: unknown) => {
      const f = err as ApplicationFailure;
      assert.equal(f.type, 'XmemoryBadOptions');
      assert.equal(f.nonRetryable, true);
      return true;
    },
  );
  assert.equal(fake.calls.length, 0);
});

test('a malformed response cannot report a write as completed', async () => {
  // Responses are JSON, so a missing field is answered by Object.prototype: against
  // an empty response the projection reported `completed`, with an id and timestamp
  // no server ever sent. The durable loop would have called that a finished write.
  const proto = Object.prototype as Record<string, unknown>;
  try {
    proto.write_status = 'completed';
    proto.write_id = 'w-fabricated';
    const fake = new FakeXmemoryInstance();
    fake.returnMalformedStatus(); // a response with no fields of its own
    const acts = build(fake);
    await assert.rejects(
      () => liveEnv().run(acts[ACTIVITY_WRITE_STATUS], { writeId: 'w1' }),
      (err: unknown) => {
        // Mapped as a transport failure: a malformed response may well be transient.
        assert.equal((err as ApplicationFailure).type, 'XmemoryUnavailable');
        return true;
      },
    );
  } finally {
    delete proto.write_status;
    delete proto.write_id;
  }
});

test('inherited fields on an activity input are not read', async () => {
  // Activity payloads are JSON objects backed by Object.prototype, so a polluted
  // prototype supplies fields the caller never sent. An inherited
  // `structuredMutations` made a plain text write discard its text and apply a
  // delete instead.
  const proto = Object.prototype as Record<string, unknown>;
  try {
    proto.structuredMutations = [{ object_mutation: { object_type: 'Person', delete: { key: { name: 'victim' } } } }];
    const fake = new FakeXmemoryInstance();
    const acts = build(fake);
    await liveEnv().run(acts[ACTIVITY_WRITE], { text: 'harmless memory' });
    const call = fake.calls[0];
    assert.equal(call.method, 'write');
    assert.equal(call.textOrQuery, 'harmless memory', 'the inherited mutations replaced the text');
    assert.equal(
      JSON.stringify(call.options ?? {}).includes('delete'),
      false,
      `a delete mutation reached the client: ${JSON.stringify(call.options)}`,
    );
  } finally {
    delete proto.structuredMutations;
  }
});

test('a null activity payload is a typed failure, not a retryable TypeError', async () => {
  // The activity names are public, so a workflow can schedule them directly rather
  // than through this package's helpers. Dereferencing a null payload raised a raw
  // TypeError, which the mapper can only read as retryable — so Temporal repeated a
  // call that cannot succeed.
  for (const activity of [ACTIVITY_READ, ACTIVITY_WRITE, ACTIVITY_WRITE_START, ACTIVITY_WRITE_STATUS] as const) {
    const fake = new FakeXmemoryInstance();
    const acts = build(fake);
    await assert.rejects(
      () => liveEnv().run(acts[activity], null as never),
      (err: unknown) => {
        const f = err as ApplicationFailure;
        assert.equal(f.type, 'XmemoryBadOptions', `${activity} on a null payload`);
        assert.equal(f.nonRetryable, true);
        return true;
      },
    );
    assert.equal(fake.calls.length, 0, `${activity} reached the client with a null payload`);
  }
});

test('a non-string writeId never reaches the client', async () => {
  const fake = new FakeXmemoryInstance();
  const acts = build(fake);
  await assert.rejects(
    () => liveEnv().run(acts[ACTIVITY_WRITE_STATUS], { writeId: null } as never),
    (err: unknown) => {
      assert.equal((err as ApplicationFailure).type, 'XmemoryBadOptions');
      return true;
    },
  );
  assert.equal(fake.count('writeStatus'), 0);
});

test('a non-string text never reaches the client, which would read it as mutations', async () => {
  // `write` is overloaded on its first argument, so an array in the text slot is
  // applied as structured mutations — a delete, in one probe — instead of being
  // written as memory. Activity inputs are JSON: the type annotation proves nothing.
  const deleteMutation = [{ object_mutation: { object_type: 'Customer', delete: { key: { id: 1 } } } }];
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
  // And the read side, where a non-string query would reach the backend as-is.
  const fake = new FakeXmemoryInstance();
  const acts = build(fake);
  await assert.rejects(
    () => liveEnv().run(acts[ACTIVITY_READ], { query: 42 } as never),
    (err: unknown) => {
      assert.equal((err as ApplicationFailure).type, 'XmemoryBadOptions');
      return true;
    },
  );
  assert.equal(fake.count('read'), 0);
});

test('structuredMutations that is not a list never reaches the client', async () => {
  // A string has a `length`, so it passed the empty check and went to the client's
  // *text* overload: the mutation value was written as the memory and the caller's
  // text was discarded. Payloads are JSON, so any caller can produce this.
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
