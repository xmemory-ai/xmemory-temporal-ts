---
id: xmemory
title: xmemory integration
sidebar_label: xmemory
description: Add durable, schema-grounded agent memory to your Temporal Workflows in TypeScript with the xmemory plugin.
tags:
  - typescript-sdk
  - integrations
  - agents
keywords:
  - temporal
  - typescript sdk
  - xmemory
  - agent memory
  - durable memory
  - plugin
---

> Add durable, schema-grounded agent memory to your Temporal Workflows in TypeScript with the [xmemory](https://xmemory.ai) plugin.

[xmemory](https://xmemory.ai) is a memory store for agents — it holds durable, schema-grounded knowledge your Workflows can recall across runs, users, and services. The **xmemory Temporal plugin** turns every memory read and write into a replay-safe Temporal Activity, added to your Worker with a single line. A memory write becomes a durable step that survives worker crashes, redeploys, and rolling upgrades, with Temporal — not your code — owning its retries and timeouts.

> ⓘ **Preview.** The plugin is under review for Temporal's AI Partner Ecosystem and is not yet on npm. Install it from the [source repository](https://github.com/xmemory-ai/xmemory-temporal-ts); the npm release (`@xmemory/temporal`) follows once the review completes.

## Prerequisites

- Familiarity with the [Temporal TypeScript SDK](https://docs.temporal.io/develop/typescript)
- A local Temporal dev server (`temporal server start-dev`) or a Temporal Cloud namespace
- An [xmemory](https://xmemory.ai) API key and instance

## Configure your Worker to use xmemory

1. Install the plugin (from source until the npm release):

   ```bash
   npm install github:xmemory-ai/xmemory-temporal-ts @temporalio/worker @temporalio/client
   ```

2. Provide your xmemory API key through the environment:

   ```bash
   export XMEM_API_KEY="your-xmemory-api-key"
   ```

3. Register the plugin on your **Worker**:

   ```ts {4,9}
   import { NativeConnection, Worker } from '@temporalio/worker';
   import { XmemoryPlugin } from '@xmemory/temporal';

   const plugin = new XmemoryPlugin({ instanceId: '<your-instance-id>' }); // reads XMEM_API_KEY
   const connection = await NativeConnection.connect({ address: 'localhost:7233' });
   const worker = await Worker.create({
     connection,
     taskQueue: 'xmemory-support',
     workflowsPath: require.resolve('./workflows'),
     plugins: [plugin],
   });
   await worker.run();
   ```

   > 💡 Use one plugin instance per Worker. The TypeScript plugin is a `WorkerPlugin`; the Client does not carry it.

## Develop a Workflow that uses memory

Inside a Workflow, call `xmemoryForWorkflow()` to get a handle whose methods dispatch to Activities. The call sites are identical to the plain xmemory client — only where the handle comes from changes, which keeps migration near-zero-diff.

```ts {5,8,11}
// workflows.ts
import { xmemoryForWorkflow } from '@xmemory/temporal';

export async function customerSupportWorkflow(customerId: string, interactionId: string, message: string): Promise<unknown> {
  const mem = xmemoryForWorkflow();

  // Recall what the agent already knows about this customer.
  const context = await mem.read(`What should support know about ${customerId}?`);

  const response = `Interaction ${interactionId} recorded for ${customerId}.`;

  // A durable write: enqueue + poll to completion, surviving worker restarts.
  await mem.writeDurable(
    `interaction_id: ${interactionId}. customer_id: ${customerId}. ` +
      `message: ${message}. response: ${response}. status: completed.`,
  );
  return context.readerResult;
}
```

All memory I/O runs in Activities, so the Workflow stays deterministic and replay-safe: Temporal replays your Workflow code but never re-runs a completed Activity, so a replay never repeats a memory read or write. Importing `xmemoryForWorkflow` from the package root is safe — the package is side-effect-free, so the Workflow bundler tree-shakes the plugin and client out of the sandbox.

## Durable writes

A deep xmemory extraction can take minutes. `writeDurable` enqueues the write and then polls its status **from the Workflow**, so the wait is a Temporal timer in server-side history rather than a blocked Activity slot. Redeploy your Worker fleet mid-write and nothing is lost — the poll loop resumes on the new worker and completes.

```ts
const status = await mem.writeDurable(text, { maxWaitMs: 15 * 60_000 });
```

For the fire-and-forget pattern — kick off several writes, keep working, join before the turn ends — `writeAsyncStart()` and `writeStatus()` are public too.

## Retries and idempotency

Writes default to **at-most-once** (`maximumAttempts: 1`). xmemory assigns primary keys with a model, and that assignment is non-deterministic — a re-extraction can normalize the same value differently (`Dr. Robert Kim` vs `Robert Kim`) and fork the record — so a lost-response retry could duplicate. A failed write surfaces to your Workflow, which decides to retry, compensate, or fail. Reads and status-polls are idempotent and retry freely.

Opt into write retries only when your primary keys are literal identifiers present verbatim in the text (for example a `customerId` / `interactionId` you supply), which re-extract deterministically:

```ts
const mem = xmemoryForWorkflow({ writeRetryPolicy: { maximumAttempts: 3 } });
```

## Handle errors with typed failures

xmemory errors become `ApplicationFailure`s with stable `type` strings you can match in a `RetryPolicy` (`nonRetryableErrorTypes: [...]`):

| xmemory condition | `type` | Retryable? |
| --- | --- | --- |
| transport error / timeout / HTTP ≥ 500 / 408 | `XmemoryServerError` / `XmemoryUnavailable` | yes |
| `RATE_LIMITED` (429) | `XmemoryRateLimited` | yes — honors `Retry-After` |
| daily quota exceeded | `XmemoryDailyQuotaExceeded` | yes (long backoff) |
| monthly quota exceeded | `XmemoryMonthlyQuotaExceeded` | no |
| `UNAUTHORIZED` / `FORBIDDEN` | `XmemoryAuthFailed` | no |
| `NOT_FOUND` | `XmemoryNotFound` | no |
| validation / schema-evolution rejections | `XmemoryBadRequest` / `XmemorySchemaRejected` | no |
| an unrecognized code | `XmemoryUnknown` | yes (never fatal) |

The durable-write loop adds `XmemoryWriteFailed`, `XmemoryWriteNotFound`, and `XmemoryWriteTimeout`, all non-retryable.

## Keep credentials and sensitive text out of history

The config carries the **name** of the environment variable holding your API key, never the key itself, so nothing secret is serialized into Activity arguments (which Temporal persists in the clear). Your memory text and queries, however, are Activity inputs and are stored in cleartext Workflow history — install a Temporal [Payload Codec](https://docs.temporal.io/develop/typescript/converters-and-encryption) if that content is sensitive.

## Capture Activity results into memory (optional)

Auto-capture is off by default. Enable it to record the results of your own Activities into memory through an Activity interceptor that never touches the replay path:

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

## Learn more

- Source repositories: [xmemory-temporal-ts](https://github.com/xmemory-ai/xmemory-temporal-ts) (TypeScript) and [xmemory-temporal](https://github.com/xmemory-ai/xmemory-temporal) (Python)
- [xmemory documentation](https://xmemory.ai)
