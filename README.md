# @xmemory/temporal

Durable agent memory for [Temporal](https://temporal.io) — add
[xmemory](https://xmemory.ai) reads and writes to your workflows as replay-safe
Temporal Activities, with one plugin line on your Worker.

> An agent's memory is exactly the state you don't want to lose when a worker
> crashes mid-turn. Putting xmemory behind Temporal makes a memory write a
> durable step: it survives process death, redeploys, and rolling upgrades, and
> Temporal — not your code — owns its retries and timeouts.

A Python port with the same API ships as
[`xmemory-temporal`](https://github.com/xmemory-ai/xmemory-temporal).

### Memory is untrusted data, both ways

What goes in is user-controlled text. What comes back is that text plus whatever
the extraction engine made of it. Neither is a safe source of instructions: a read
result in a prompt is the indirect prompt-injection path, and the same string in a
shell or a query is the ordinary injection path. Quote it, bound it, and keep it
out of anything that decides what to do next.

## What you get

- **Memory as Activities.** `read`, `write`, `writeAsyncStart` + `writeStatus` run as
  Activities (all I/O stays out of workflow code, so workflows replay
  deterministically).
- **A durable deep write.** `writeDurable(text)` enqueues a write and polls it to
  completion from the workflow, so a multi-minute extraction survives worker
  restarts — the poll state lives in workflow history, not a worker process.
- **A near-zero-diff migration.** The workflow-side handle mirrors the plain
  xmemory client's methods, so agent code that already calls `inst.read(...)` /
  `inst.write(...)` keeps working — it just dispatches to an Activity. Two
  deliberate differences: the enqueue is `writeAsyncStart` (the client calls it
  `writeAsync`), and results are projected into this package's own DTOs so a
  client field rename cannot break replay of a completed workflow.
- **Temporal-owned retries and timeouts.** xmemory errors map to typed
  `ApplicationFailure`s with retryable/non-retryable verdicts, so you can tune
  `RetryPolicy` against stable error-type strings.
- **Opt-in auto-capture** of activity results into memory, via an Activity
  interceptor that never touches the replay path.

## Install

```bash
npm install @xmemory/temporal
```

Requires Node.js 22.12+ and `@temporalio/*` 1.20.

Two things to know if you type-check with `skipLibCheck: false`:

- Add `@types/ms` to your own devDependencies. `@temporalio/common` references
  `ms`, which ships no types.
- Use `@types/node` 22 or 24. Temporal's own declarations do not compile against
  25 or newer (`EventEmitter<[never]>` fails its own constraint, still true in
  `@temporalio/worker` 1.22.0), so those need `skipLibCheck: true` until the SDK
  catches up. The runtime is unaffected — only the types.

## Quickstart

Register the plugin on your **Worker**:

```ts
import { NativeConnection, Worker } from '@temporalio/worker';
import { XmemoryPlugin } from '@xmemory/temporal';

const plugin = new XmemoryPlugin({ instanceId: '<your-instance-id>' }); // reads XMEM_API_KEY
const connection = await NativeConnection.connect({ address: 'localhost:7233' });
const worker = await Worker.create({
  connection,
  taskQueue: 'my-agent',
  workflowsPath: require.resolve('./workflows'),
  plugins: [plugin],
});
```

Then call memory from inside a workflow:

```ts
// workflows.ts
import { xmemoryForWorkflow } from '@xmemory/temporal/workflow';

export async function myWorkflow(userName: string, userMessage: string): Promise<unknown> {
  const mem = xmemoryForWorkflow();
  // Name whom the fact is about — a memory store has no ambient "current user".
  await mem.writeDurable(`${userName}: ${userMessage}`);
  return (await mem.read(`what do we know about ${userName}?`)).readerResult;
}
```

One worker, one instance. The activities bind to whatever instance the plugin
configured, so **every worker polling a task queue must share that configuration**.
Per-tenant isolation means a task queue per tenant, not a per-workflow option; the
example above uses one shared instance and names the user in the text rather than
isolating them.

Which queue is a trust decision: derive the tenant from an authenticated identity,
never from a caller- or model-supplied value. A workflow argument naming a task
queue is a request to read someone else's memory.

Workflow code imports from **`@xmemory/temporal/workflow`**, not the package
root. That subpath is a leaf: it reaches no Activity code and no xmemory client,
so Temporal's workflow bundler accepts it. The root entry loads the plugin and
the client, which the bundler rejects by design, so it belongs in worker setup
only. A CI step bundles a workflow against the built package to keep it that
way.

> **Use one plugin instance per Worker.** The bound client lives in a per-plugin
> holder, so reusing one plugin object across two Workers is last-bind-wins.

Runnable end-to-end scripts live in [`examples/`](./examples): create an instance
with a schema, run a worker, and drive a support-agent workflow.

## Timeouts

**The workflow owns every activity budget.** `xmemoryForWorkflow()` sets each
call's `startToCloseTimeout`, and the activity derives its xmemory client timeout
from the deadline Temporal assigned it, always a margin below, so the client gives
up first and you get an attributable xmemory error instead of an opaque Temporal
activity timeout. An activity scheduled with neither close timeout fails fast with
`XmemoryNoDeadline` rather than picking a budget of its own.

The margin covers the whole attempt, including work Temporal does before the
activity function runs. Keep it larger than your Client-level activity interceptors.
The ordering holds above timer resolution: a deadline of a few milliseconds is too
short to fit a call and a margin, and which side fires first is then a coin toss.

```ts
const mem = xmemoryForWorkflow({
  readTimeout: '60s',      // a deep read on a large instance
  writeTimeout: '5m',
});
```

The client timeout is derived, not configured separately, so lowering a workflow's
budget lowers the client's with it. `DEFAULT_TIMEOUTS` supplies the defaults;
`{ clientMarginMs }` tunes the gap.

Each of those bounds one **attempt**. Nothing bounds the retry sequence unless you
say so: a server asking for an hour before the next try is honoured as asked, so a
rate-limited read can sit in retries far longer than its own timeout suggests. Set
`totalTimeout` when a call has a deadline of its own — it becomes the activity's
`scheduleToCloseTimeout`, covering every attempt.

```ts
const mem = xmemoryForWorkflow({ readTimeout: '30s', totalTimeout: '2m' });
```

**A timed-out write is indeterminate.** Nothing distinguishes "never arrived" from
"arrived, response lost", and the request is abandoned rather than cancelled (the
client exposes no `AbortSignal`). So write Activities default to
`maximumAttempts: 1` and surface the failure. Treat a timed-out write as *may or may
not have happened*, and reconcile with a read if it matters.

## Durable writes

`writeDurable(text)` enqueues a deep write and polls it to completion from the
workflow, so the wait is a Temporal timer in server-side history rather than a
blocked activity slot. Redeploy the worker fleet mid-write and nothing is lost:
the poll loop resumes on the new worker and completes.

```ts
const status = await mem.writeDurable(text, { maxWaitMs: 15 * 60_000 });
```

Each poll adds history events — an activity, a timer, and the workflow tasks driving
them — and how many is a detail of your SDK and server versions, not something this
package can predict for you. Backoff slows the growth until the interval reaches
`maxPollIntervalMs`, after which history grows linearly with the wait. Keep
`pollIntervalMs` at seconds rather than milliseconds, since that history is shared
with the rest of your workflow; the loop warns once Temporal itself suggests
continuing as new.

Two different kinds of pacing sit here, so the names are worth separating.
`pollIntervalMs` and `maxPollIntervalMs` set how long the loop waits *between*
polls. `writeStatusRetry` sets how one poll retries when it fails — attempts and
backoff for the activity itself:

```ts
const mem = xmemoryForWorkflow({
  writeStatusRetry: { attempts: 4, intervalMs: 2_000, maxIntervalMs: 8_000 },
});
```

Those are plain numbers rather than a Temporal `RetryPolicy`, and this package
builds the policy from them. Temporal compiles a policy when it schedules the
activity, which for the first poll is *after* the write is enqueued — so a policy it
refuses would leave a queued write nobody is watching. There is no policy to refuse
if the package builds it.

This helper cannot call `continueAsNew` for you — it runs inside *your* workflow,
and restarting that would discard your state. For multi-hour waits, run
`writeDurable` in a child workflow.

`maxWaitMs` bounds the *waiting*: every poll is scheduled to finish inside it. The
one exception is the last observation, which happens **at** the deadline so a write
that lands late is still seen rather than reported as a timeout — so a call can
return up to one status poll after `maxWaitMs`. When the server has asked for a
retry delay longer than the wait has left, that final poll is skipped instead:
arriving before the server said it would answer is worse than not looking.

For the fire-and-forget pattern (kick off several writes, keep working, join
before the turn ends), `writeAsyncStart()` and `writeStatus()` are public too.

## Credentials never reach workflow history

The config holds the **name** of the environment variable that supplies the API
key (`XMEM_API_KEY` by default), never the key itself — so nothing secret is ever
serialized into activity arguments, which Temporal persists in the clear. Pass
the key in-process instead with `new XmemoryPlugin(config, { apiKey })` if you
prefer.

**Your memory text and queries, however, *are* in history.** Queries, written text,
and `readerResult` are activity payloads, persisted in the clear and visible in the
Web UI. The error mapping keeps raw transport strings and the server's failure
detail out of failures (set `logServerErrorDetail: true` to log the reason
worker-side), but the payloads themselves remain.

`includeContentInSummary: false` (the default) only affects the one-line activity
*summary*. For sensitive memory text, install a Temporal **Payload Codec**; this
plugin does not impose one, since a codec applies to every payload in the
namespace, not just xmemory's.

## Replay safety and idempotency

Two things keep memory operations correct under retries and replay:

- **Replay never re-issues an operation.** All I/O is in Activities; workflow
  code only schedules Activities and sleeps. Temporal replays workflow code but
  never re-runs a completed Activity, so a replay never repeats a memory read or
  write. The suite proves this with a forced-replay (`maxCachedWorkflows: 0`)
  side-effects test.
- **Writes default to at-most-once.** Primary-key dedup looks like it would make
  retries safe, but PK extraction is non-deterministic: a model normalizes the same
  value differently across runs (`Dr. Robert Kim` vs `Robert Kim`), and a
  disagreement forks a new row. So a lost-response retry can duplicate. Write
  Activities default to `maximumAttempts: 1` and surface the failure to your
  workflow. Reads and status polls are idempotent and retry generously.

**A structured write is retryable when the mutation names what it addresses.** Pass
explicit mutations instead of free text and nothing is extracted, so re-applying is
deterministic — for an `update` or `delete` keyed by primary key. A `create` is not:
the server assigns the key, so a retry after a lost response inserts a second row.
Opt into `writeRetryPolicy` for the keyed shapes only:

```ts
const mem = xmemoryForWorkflow({ writeRetryPolicy: { maximumAttempts: 3 } });
await mem.write('', {
  structuredMutations: [
    {
      object_mutation: {
        object_type: 'Customer',
        update: { key: { customerId: 'c-1' }, values: { tier: 'gold' } },
      },
    },
  ],
});
```

For text writes, opt into retries only when your primary keys are literal
identifiers appearing verbatim in the text, such as a `customerId` you supply. That
is a convention you keep, not something the API enforces.

[`examples/setup-memory.ts`](./examples/setup-memory.ts) shows creating an
instance with a schema.

## Error handling

xmemory errors become `ApplicationFailure`s with stable `type` strings you can
match in a `RetryPolicy` (`nonRetryableErrorTypes: [...]`). The mapping is
derived from the server's error codes:

| xmemory condition | `type` | Retryable? |
|---|---|---|
| transport error / timeout / HTTP ≥ 500 / 408 | `XmemoryServerError` / `XmemoryUnavailable` | yes |
| `RATE_LIMITED` (429) | `XmemoryRateLimited` | yes — honors `Retry-After` |
| `QUOTA_EXCEEDED` + `daily_quota_exceeded` | `XmemoryDailyQuotaExceeded` | yes (long backoff) |
| `QUOTA_EXCEEDED` + `monthly_quota_exceeded` | `XmemoryMonthlyQuotaExceeded` | no |
| `QUOTA_EXCEEDED` (kind unknown) | `XmemoryQuotaExceeded` | no |
| `UNAUTHORIZED` / `FORBIDDEN` | `XmemoryAuthFailed` | no |
| `NOT_FOUND` | `XmemoryNotFound` | no |
| validation / conflict / schema-evolution rejections | `XmemoryBadRequest` / `XmemorySchemaRejected` | no |
| activities registered without the plugin | `XmemoryNotBound` | no |
| an activity scheduled with neither close timeout | `XmemoryNoDeadline` | no |
| durable-write options that cannot be honored | `XmemoryBadOptions` | no — the same arguments fail identically |
| an unrecognized code | `XmemoryUnknown` | follows the HTTP status |

Plus three raised by the durable write loop (`writeDurable`), from a polled
`writeStatus`, all non-retryable:

| durable-write outcome | `type` |
|---|---|
| the queued write reported `failed` | `XmemoryWriteFailed` |
| the queued write id was `not_found` | `XmemoryWriteNotFound` |
| polling exceeded `maxWait` | `XmemoryWriteTimeout` |

An unrecognized error code never raises and keeps its own type — a stricter client
that crashed on a newer server's code would break during rolling deploys. Whether it
is retried comes from the HTTP status rather than the code: 5xx, 408, 429 and a
missing status are retried, while a 4xx is terminal however it is labelled. Retrying
a 401 until the attempts run out helps nobody, and on a write it repeats a call that
cannot succeed.

> **Note.** 402 means `QUOTA_EXCEEDED` only. `TRIAL_ENDED` was removed from the
> xmemory contract when trials were retired end-to-end; do not rely on it.

## Auto-capture (opt-in)

```ts
const plugin = new XmemoryPlugin(
  { instanceId: '<your-instance-id>' },
  {
    autoCapture: {
      project: (activityName, result) => summarize(result), // return undefined to skip
      sampleRate: 0.25,
    },
  },
);
```

Off by default. It runs as an **Activity** interceptor, outside the replay path;
`project` decides what to remember, sampling bounds fan-out, and a capture failure
never fails the wrapped activity. Capture is an enqueue (`writeAsync`) clamped to
what the activity has left of its deadline, and skipped when nothing is left, so it
cannot push the activity past its `startToClose`.

Your `project` function is the exception: it is synchronous code, and JavaScript
cannot interrupt it. One that blocks longer than the activity has left will push it
past the deadline however carefully the enqueue is budgeted. Keep projections
cheap.

> **Auto-capture is at-least-once.** If a worker dies after the capture enqueue but
> before the Activity's completion is recorded, the Activity runs again and captures
> again — and since primary keys are extracted, a duplicate can fork an entity.
> Capture facts a duplicate would not corrupt.

> **Naming caveat.** Auto-capture skips any activity whose name starts with
> `xmemory_` (to avoid capturing its own writes). If you name one of *your* own
> activities `xmemory_...`, it will be silently skipped. It also never captures
> Queries.

## Testing

```bash
npm ci
npm run lint    # tsc over src + test + examples (strict)
npm test        # tsc, then node:test over test/*.test.ts
```

The suite runs with no live backend (a fake instance is injected). See
[`TESTING.md`](./TESTING.md) for the full strategy.

## Legal

- Privacy policy: <https://xmemory.ai/privacy-policy.html>
- Terms: <https://xmemory.ai/terms-and-conditions.html>

**MIT licensed** — see [`LICENSE`](./LICENSE). The grant covers this integration's
code only. The xmemory service and its technology remain proprietary to xmemory
Inc.; using it requires valid credentials and is governed by the Terms above. The
scope and trademark notices live in [`NOTICE`](./NOTICE), kept separate so the
package classifies cleanly as MIT.
