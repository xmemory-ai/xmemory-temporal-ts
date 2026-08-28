# Testing

The suite answers one question with evidence: **does a memory operation ever run
twice — or fail to run — under retries and replay?**

Two principles shape it:

- **No live backend by default.** Every test injects a fake xmemory instance, so
  the suite is deterministic, fast, and runnable offline (including in CI with no
  secrets). The fake is a call ledger used as a complementary cross-check in the
  replay-safety test.
- **Real Temporal, skipped time.** Integration tests run an actual Temporal
  worker against `TestWorkflowEnvironment.createTimeSkipping()`, so `sleep` and
  durable poll loops that model a 15-minute write complete in milliseconds while
  still exercising the real scheduling, activity, and replay machinery.

The suite uses the built-in Node test runner (`node:test`) with `tsx`, plus the
TypeScript compiler as a type-level gate — no extra test framework.

## Test layers

### 1. Unit

Each module verified in isolation, no worker involved.

- **`activities.test.ts`** — activities in isolation via `MockActivityEnvironment`:
  vendor result → our DTO projection, the write-status enum value, a client error
  → typed `ApplicationFailure`, and the unbound-activity failure (asserting its
  `type` and `nonRetryable`, not just the message).
- **`errors.test.ts`** — table-driven over the `XmemoryAPIError` →
  `ApplicationFailure` mapping: retryability verdicts, `Retry-After` handling,
  `MAX_RETRIES_EXCEEDED`, and the transport-leak guard (an internal host/port must
  not survive into the failure message, `details`, or a chained cause).
- **`names.test.ts`** — pins the activity-name and durable-write outcome-type
  string literals, which are a public contract for `RetryPolicy`.

### 2. Integration (real worker, time-skipped)

- **`workflow.test.ts`** — read/write round-trips through a real worker and the
  plugin; the **at-most-once write default** and the **opt-in retry** path; and
  the full durable-write matrix: polls to completion, terminal `failed`,
  `not_found` (terminal on the first result, since `writeAsync` is transactional),
  unknown-status (keeps polling), and `max_wait` timeout — all in milliseconds.
- **`interceptor.test.ts`** — auto-capture: projection, sampling, fail-open (a
  capture error never fails the wrapped activity), and the recursion guard
  (xmemory's own write activity is never re-captured).

### 3. Replay safety

`workflow.test.ts` runs the worker with `maxCachedWorkflows: 0`, which evicts the
workflow after every task and forces a full replay from history. The **primary**
assertion is at the **history level** — N logical operations produce exactly N
`ActivityTaskScheduled` events. Counting scheduled events is retry-independent:
each intended call is one scheduled event, regardless of retries or replays. A
complementary **ledger-level** cross-check confirms the fake saw each
logical write exactly once. A deliberate **sensitivity control** (a double-write
workflow) proves the harness reports *two* when there are two — so the
"exactly one" assertions can actually fail.

> **Replay across builds, and the assumption behind it.** The suite records and
> replays within one build, which catches nondeterminism inside a version but not
> between them. `writeDurable` polls from the *caller's* workflow, so its command
> sequence is part of their history: changing the loop is a breaking change for an
> execution already in flight, and it did change during review. This is safe today
> only because nothing has been released, so no history from an earlier build
> exists anywhere. From the first release onward, any change to the poll loop needs
> `patched(...)`, the previous branch kept, and a checked-in history from the older
> build to replay against.

## The injected fake

`test/fakes.ts` provides a `FakeXmemoryInstance` implementing the same narrow
surface the plugin depends on and recording every call. It is scriptable
(`failWriteTimes(n, err)`, `statusSequence([...])`) and provides the replay test's
complementary ledger cross-check (the authoritative assertion there is the
`ActivityTaskScheduled` event count). Because the instance is *injected* through the
plugin's options, no test needs module mocking. `test/user-activity.ts`,
`test/user-workflow.ts`, and `test/workflows.ts` are the workflow/activity
fixtures the worker registers.

## Type checking is part of the gate

`npm run lint` is `tsc --noEmit -p tsconfig.test.json`, which type-checks `src`,
`test`, **and `examples`** under `strict`. A type error anywhere — including a
broken example — fails before a single test runs. `npm run build` additionally
compiles the shippable `dist/`.

## Running the tests

```bash
npm ci

npm run lint    # tsc over src + test + examples (strict)
npm test        # tsc, then node:test over test/*.test.ts
npm run build   # emit dist/ (also runs on prepublish)

# The packaging gate: bundles a workflow against the BUILT package, by package
# name, so it goes through the `exports` map a consumer resolves.
npx tsx scripts/check-workflow-bundle.ts
```

### Manual end-to-end run

The automated suite is fully offline (no live-marked test in this package), so a
real round-trip is driven by hand through the scripts in
[`examples/`](https://github.com/xmemory-ai/xmemory-temporal-ts/tree/main/examples).
They import `../src` directly, so they run from a checkout of this repository and
are deliberately not part of the published package. Three terminals:

```bash
# Credentials. The client defaults to https://api.xmemory.ai, so set
# XMEM_API_URL when your key belongs to some other environment.
export XMEM_API_KEY=xmem_...

# Terminal 1: a local Temporal dev server (UI on http://localhost:8233)
temporal server start-dev

# Terminal 2: create an instance with a name-keyed schema, then run the worker
export XMEM_INSTANCE_ID="$(npx tsx examples/setup-memory.ts)"
npx tsx examples/worker.ts
#   worker running on task queue xmemory-example

# Terminal 3: drive one workflow
npx tsx examples/run-workflow.ts
```

The workflow does a `writeDurable` and then reads the fact back, so a successful
run exercises the plugin, the client, and the durable poll loop against a real
backend. Inspect the run in the Temporal UI to check that activity summaries
render legibly.

**The durability demo.** The whole value proposition is that a durable write
survives worker death, so also **kill the worker mid-`writeDurable` and restart
it**: the poll loop must resume from history and complete rather than restarting
the write.

## Continuous integration

GitHub Actions runs `npm ci`, `npm run lint` (tsc over src + test + examples),
`npm test`, `npm run build`, and the packaging gate
(`scripts/check-workflow-bundle.ts`, which packs the tarball and bundles a
workflow against it) as a matrix over **Node.js 22 / 24 / 26** — the 22 leg pinned
to 22.12, the `engines` floor, behind a stable check name — adding a step
that **pre-warms the Temporal test-server binary** — `node:test` runs test files
in parallel, and on a cold runner they would otherwise race to download the same
time-skipping server binary and flake; warming it once, single-process, removes
the race. Each leg reports as its own check (`Node.js 22` and so on), gated by
branch protection before any npm release.

## What CI runs

* `npm run lint` — `tsc` over `src`, `test`, and `examples` under `strict`.
* `npm test` — the `node:test` suite, on Node 22, 24, and 26.
* `npm run build` — the published `dist`.
* `npm run check:bundle` — bundles a workflow that imports the **packed tarball**
  from a foreign package root, so neither a missing `files` entry nor a dropped
  subpath export can pass. Self-reference inside our own package would otherwise
  resolve the import to the working tree and prove nothing.
* `npm run check:declarations` — installs the packed tarball into a scratch package
  with production dependencies only, then type-checks a consumer against it under
  `node16`, `nodenext`, and `bundler` with `skipLibCheck: false`. This package emits
  CommonJS, so a vendor type reaching our `.d.ts` from a module built ESM-only breaks
  those consumers with TS1541 while our own build stays green.

  The fixture installs exactly what a consumer gets — this package and
  `@types/node` — because a type package added here to quieten an error makes the
  gate green for a consumer who still fails. Errors are then split by where they are
  reported: inside our own published declarations they fail the gate; inside another
  package's they are printed as a note. A consumer meets those with or without us,
  and there is one live example, below.

  **A strict consumer needs `@types/ms` of their own.** `@temporalio/common`
  references `ms`, which ships no types, so `skipLibCheck: false` reports TS7016
  inside `@temporalio/common/lib/time.d.ts`. That is Temporal's gap: this package
  does not depend on `ms`, and declaring `@types/ms` as a runtime dependency to
  paper over it would push an upstream workaround onto everyone who installs us.
  Consumers who compile strictly add `@types/ms` to their own devDependencies.

  **The runtime and its types are different questions.** The runtime is supported and
  CI tests on it. Type-checking against `@types/node` 25 or newer is not: Temporal's
  own declarations do not compile there (`EventEmitter<[never]>` fails its own
  constraint on 25, and 26 the same way), still true in `@temporalio/worker` 1.22.0,
  so no upgrade fixes it. Measured: `@types/node` 24 passes, 25 and 26 fail inside
  `@temporalio/worker/lib/worker.d.ts`. Those consumers need `skipLibCheck: true`
  until the SDK catches up.
  The gate checks `@types/node` 22 and 24 — the supported majors — in all three
  module modes. It does not check 25 or 26, where `skipLibCheck: true` would be
  forced and the gate would stop proving anything.
