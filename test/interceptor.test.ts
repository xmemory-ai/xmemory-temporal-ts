/**
 * Auto-capture interceptor: projection, sampling, and fail-open behavior.
 *
 * The interceptor is an activity interceptor, so we drive it through a real
 * user activity registered on the worker alongside the plugin.
 */

import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { MockActivityEnvironment, TestWorkflowEnvironment } from '@temporalio/testing';
import { Worker } from '@temporalio/worker';
import { captureBudgetMs, createAutoCaptureInterceptor, type AutoCaptureConfig } from '../src/interceptor';
import { XmemoryPlugin } from '../src/plugin';
import { InstanceHolder } from '../src/activities';
import { FakeXmemoryInstance, apiError } from './fakes';
import { userActivity } from './user-activity';
import { writeWorkflow } from './workflows';

const WORKFLOWS_PATH = require.resolve('./user-workflow');
const TASK_QUEUE = 'xmemory-interceptor-test';

let env: TestWorkflowEnvironment;

before(async () => {
  env = await TestWorkflowEnvironment.createTimeSkipping();
});
after(async () => {
  await env?.teardown();
});

async function run(fake: FakeXmemoryInstance, autoCapture: AutoCaptureConfig): Promise<void> {
  const plugin = new XmemoryPlugin({ instanceId: 'inst-1' }, { instance: fake, autoCapture });
  const worker = await Worker.create({
    connection: env.nativeConnection,
    taskQueue: TASK_QUEUE,
    workflowsPath: WORKFLOWS_PATH,
    activities: { user_activity: userActivity },
    plugins: [plugin],
  });
  await worker.runUntil(
    env.client.workflow.execute('userWorkflow', {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(performance.now())}`,
      args: ['the user said hello'],
    }),
  );
}

// Capture goes through the ENQUEUE path (writeAsync), not a full synchronous
// write, so the fake records it as "writeAsync".
test('projection captures the activity result', async () => {
  const fake = new FakeXmemoryInstance();
  await run(fake, { project: (name, result) => `[${name}] ${result}` });
  const writes = fake.calls.filter((c) => c.method === 'writeAsync');
  assert.equal(writes.length, 1);
  assert.match(writes[0].textOrQuery, /handled: the user said hello/);
});

test('projection returning undefined skips capture', async () => {
  const fake = new FakeXmemoryInstance();
  await run(fake, { project: () => undefined });
  assert.equal(fake.count('writeAsync'), 0);
});

test('sampleRate 0 skips capture', async () => {
  const fake = new FakeXmemoryInstance();
  await run(fake, { project: () => 'remember', sampleRate: 0 });
  assert.equal(fake.count('writeAsync'), 0);
});

test('capture failure does not fail the wrapped activity', async () => {
  const fake = new FakeXmemoryInstance();
  fake.failWriteTimes(10, apiError({ status: 500 }));
  // The workflow (and its user activity) must still complete.
  await run(fake, { project: () => 'remember' });
  assert.ok(fake.count('writeAsync') >= 1);
});

test('capture is skipped when the activity deadline leaves no room', async () => {
  // Capture runs inside the wrapped activity, so it spends that activity's
  // budget. On a deadline this tight the enqueue must be dropped rather than
  // pushing the activity over it: a timeout there would fail, and retry, an
  // activity that had already produced its result.
  const fake = new FakeXmemoryInstance();
  const plugin = new XmemoryPlugin(
    { instanceId: 'inst-1' },
    { instance: fake, autoCapture: { project: (name, result) => `[${name}] ${result}` } },
  );
  const worker = await Worker.create({
    connection: env.nativeConnection,
    taskQueue: TASK_QUEUE,
    workflowsPath: WORKFLOWS_PATH,
    activities: { user_activity: userActivity },
    plugins: [plugin],
  });
  const out = await worker.runUntil(
    env.client.workflow.execute('shortDeadlineUserWorkflow', {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(performance.now())}`,
      args: ['hello'],
    }),
  );

  assert.equal(out, 'handled: hello'); // the wrapped activity is untouched
  assert.equal(fake.count('writeAsync'), 0); // capture was dropped, not attempted
});

test('capture budget never outlives the activity deadline', () => {
  // The arithmetic behind the skip above, without the wall-clock.
  // Fresh activity, plenty of room: the configured ceiling applies.
  assert.equal(captureBudgetMs(29_500, 5_000, 5_000), 5_000);
  // Nearly spent: the remainder wins over the ceiling.
  assert.equal(captureBudgetMs(8_000, 5_000, 5_000), 3_000);
  // Nothing left once the margin is honored: skip rather than overrun.
  assert.equal(captureBudgetMs(3_000, 5_000, 5_000), null);

  // A nonsensical margin must not widen the budget past the remainder. Zero
  // reserves no completion gap; a negative one is arithmetic that would hand
  // capture more time than the Activity has left.
  for (const margin of [0, -5_000, Number.NaN]) {
    const budget = captureBudgetMs(1_000, 5_000, margin);
    assert.ok(budget !== null && budget < 1_000, `margin ${margin} overbudgets capture: ${budget}`);
  }
  assert.equal(captureBudgetMs(1_000_000, 5_000, -5_000), 5_000, 'the ceiling still binds');
  assert.equal(captureBudgetMs(10_000, -1, 1_000), null, 'so does a nonsensical ceiling');
});

test('own xmemory write activity is not captured', async () => {
  // The recursion guard, exercised for real. `writeWorkflow` calls mem.write(),
  // which dispatches the `xmemory_write` ACTIVITY — that passes through the
  // auto-capture interceptor. The guard (activity name starts with "xmemory_")
  // must skip it, or capture would re-capture xmemory's own writes. With a
  // projection that fires on everything, the capture path must NOT fire.
  // (Delete the guard and writeAsync goes to 1, failing this test.)
  const fake = new FakeXmemoryInstance();
  const plugin = new XmemoryPlugin(
    { instanceId: 'inst-1' },
    { instance: fake, autoCapture: { project: (name) => `[${name}]` } },
  );
  const worker = await Worker.create({
    connection: env.nativeConnection,
    taskQueue: TASK_QUEUE,
    workflowsPath: require.resolve('./workflows'),
    plugins: [plugin],
  });
  await worker.runUntil(
    env.client.workflow.execute(writeWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(performance.now())}`,
      args: ['remember me'],
    }),
  );
  assert.equal(fake.count('write'), 1); // the user's write happened
  assert.equal(fake.count('writeAsync'), 0); // its result was NOT captured (guard worked)
});

test('auto-capture is registered as the outermost interceptor', () => {
  // Capture charges itself against what is left of the Activity's deadline, and
  // measures that from its own stamp — so it has to wrap the whole attempt, or the
  // time another interceptor spends is invisible to it. The Worker composes index 0
  // outermost, so outermost means first.
  const marker = () => ({});
  const plugin = new XmemoryPlugin(
    { instanceId: 'inst-1' },
    { instance: new FakeXmemoryInstance(), autoCapture: { project: () => 'remember' } },
  );
  const configured = plugin.configureWorker({
    taskQueue: 'tq',
    interceptors: { activityInbound: [marker] },
  } as never);

  const registered = configured.interceptors?.activityInbound ?? [];
  assert.equal(registered[registered.length - 1], marker, "a user's interceptor must keep its place");
  assert.notEqual(registered[0], marker, 'auto-capture must be registered first (outermost)');
});

test('a capture does not pick up inherited write options', async () => {
  // Driven directly rather than through a Worker: polluting Object.prototype around
  // `Worker.create` breaks the bundler's own schema traversal, which says nothing
  // about this package. What matters is the options object handed to the client.
  const proto = Object.prototype as Record<string, unknown>;
  try {
    proto.scope = { objects: [{ type: 'Person', key: { name: 'victim' } }] };
    proto.diffEngine = true;
    const fake = new FakeXmemoryInstance();
    const holder = new InstanceHolder();
    holder.bind(fake);
    const interceptor = createAutoCaptureInterceptor(holder, { instanceId: 'inst-1' }, { project: () => 'remember' });
    const env = new MockActivityEnvironment({
      activityType: 'user_activity',
      startToCloseTimeoutMs: 30_000,
      // Explicit: the mock defaults this to one second, which correctly leaves a
      // capture no budget at all and would make this test pass without capturing.
      scheduleToCloseTimeoutMs: 60_000,
      scheduledTimestampMs: Date.now(),
    } as never);
    const execute = interceptor.execute?.bind(interceptor);
    assert.ok(execute, 'the interceptor has no execute hook');
    await env.run((async () => execute({} as never, (async () => 'handled') as never)) as never);
    const options = fake.calls.find((c) => c.method === 'writeAsync')?.options as
      | { scope?: unknown; diffEngine?: unknown }
      | undefined;
    assert.ok(options, 'nothing was captured');
    assert.equal(options.scope, undefined, 'an inherited scope rode along on the capture');
    assert.equal(options.diffEngine, undefined, 'an inherited diffEngine rode along on the capture');
  } finally {
    delete proto.scope;
    delete proto.diffEngine;
  }
});

test('a projector defined as a class method still captures', async () => {
  // `project` is often a method on a class instance's *prototype*, and sanitizing
  // the block with a spread dropped it — leaving an interceptor with nothing to
  // call, so capture silently stopped happening.
  class Projector {
    project(name: string, result: unknown): string {
      return `[${name}] ${String(result)}`;
    }
  }
  const fake = new FakeXmemoryInstance();
  const plugin = new XmemoryPlugin({ instanceId: 'inst-1' }, { instance: fake, autoCapture: new Projector() as never });
  const worker = await Worker.create({
    connection: env.nativeConnection,
    taskQueue: TASK_QUEUE,
    workflowsPath: WORKFLOWS_PATH,
    activities: { user_activity: userActivity },
    plugins: [plugin],
  });
  await worker.runUntil(
    env.client.workflow.execute('userWorkflow', {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(performance.now())}`,
      args: ['hello'],
    }),
  );
  assert.equal(fake.count('writeAsync'), 1, 'the class projector was dropped and nothing was captured');
  assert.match(String(fake.calls[0]?.textOrQuery), /\[user_activity\] handled: hello/);
});

test('a projector that returns something other than text captures nothing', async () => {
  // `writeAsync` is overloaded on its first argument, so an array returned by the
  // projector would be applied as structured mutations — a delete, in one probe —
  // instead of being remembered. `project` is the caller's code, and its return
  // type is erased at runtime.
  const fake = new FakeXmemoryInstance();
  const plugin = new XmemoryPlugin(
    { instanceId: 'inst-1' },
    {
      instance: fake,
      autoCapture: {
        project: () =>
          [{ object_mutation: { object_type: 'Customer', delete: { key: { id: 1 } } } }] as never,
      },
    },
  );
  const worker = await Worker.create({
    connection: env.nativeConnection,
    taskQueue: TASK_QUEUE,
    workflowsPath: WORKFLOWS_PATH,
    activities: { user_activity: userActivity },
    plugins: [plugin],
  });
  const out = await worker.runUntil(
    env.client.workflow.execute('userWorkflow', {
      taskQueue: TASK_QUEUE,
      workflowId: `wf-${Date.now()}-${Math.round(performance.now())}`,
      args: ['hello'],
    }),
  );
  assert.equal(out, 'handled: hello'); // the wrapped activity is untouched
  assert.equal(fake.count('writeAsync'), 0, 'a non-string projection reached the client');
});

test('an activity name collision is refused, not silently replaced', () => {
  // Merging over a user's identically-named Activity would change what their
  // workflows execute, and would do it invisibly.
  const plugin = new XmemoryPlugin({ instanceId: 'inst-1' }, { instance: new FakeXmemoryInstance() });
  assert.throws(
    () =>
      plugin.configureWorker({
        taskQueue: 'tq',
        activities: { xmemory_write: async () => 'mine' },
      } as never),
    /already registers xmemory_write/,
  );
});

test('a nonsensical sample rate is rejected', () => {
  // Silently sampling everything (or nothing) hides a misconfiguration that
  // changes how much is written.
  for (const rate of [-0.1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    assert.throws(
      () =>
        new XmemoryPlugin(
          { instanceId: 'inst-1' },
          { instance: new FakeXmemoryInstance(), autoCapture: { project: () => 'x', sampleRate: rate } },
        ).configureWorker({ taskQueue: 'tq' } as never),
      /sampleRate/,
      `accepted ${rate}`,
    );
  }
});

test('an activity named like an Object member is not a collision', () => {
  // `in` walks the prototype chain, so `constructor` and `toString` looked taken.
  const plugin = new XmemoryPlugin({ instanceId: 'inst-1' }, { instance: new FakeXmemoryInstance() });
  const configured = plugin.configureWorker({
    taskQueue: 'tq',
    activities: { constructor: async () => 'c', toString: async () => 's' },
  } as never);
  const registered = Object.keys(configured.activities ?? {});
  assert.ok(registered.includes('xmemory_write'), 'our activities must still be registered');
  assert.ok(registered.includes('constructor'), "the user's activity must keep its place");
});
