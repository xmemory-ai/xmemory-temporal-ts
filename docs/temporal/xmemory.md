# xmemory integration

> Add durable, schema-grounded agent memory to your Temporal Workflows in TypeScript with the xmemory plugin.

Temporal's integration with [xmemory](https://xmemory.ai) lets you read and write agent memory directly from your Workflow code while Temporal handles Durable Execution. xmemory is a memory store for agents: it holds durable, schema-grounded knowledge your Workflows can recall across runs, users, and services.

Like all API calls, xmemory reads and writes are non-deterministic. In a [Temporal Application](/glossary#temporal-application), that means you cannot call xmemory directly from a [Workflow](/glossary#workflow); it must run as an [Activity](/glossary#activity). The xmemory plugin handles this automatically: the workflow-side handle you call (`read`, `write`, `writeDurable`) dispatches to Activities behind the scenes. This preserves the plain xmemory client's developer experience while Temporal handles Durable Execution for you — a memory write becomes a durable step that survives worker crashes, redeploys, and rolling upgrades.

The code in this guide is based on the [examples in the xmemory-temporal-ts repository](https://github.com/xmemory-ai/xmemory-temporal-ts/tree/main/examples).

> **Preview**
>
>    The plugin is under review for Temporal's AI Partner Ecosystem and is not yet on npm. Install it from the [source repository](https://github.com/xmemory-ai/xmemory-temporal-ts); the npm release (`@xmemory/temporal`) follows once the review completes.

## Prerequisites

- This guide assumes you are already familiar with xmemory. If you aren't, refer to the [xmemory documentation](https://xmemory.ai) for more details.
- If you are new to Temporal, we also recommend you read the [Understanding Temporal](/evaluate/understanding-temporal) document or take the [Temporal 101](https://learn.temporal.io/courses/temporal_101/) course to understand the basics of Temporal.
- Ensure you have set up your local development environment by following the [Set up your local with the TypeScript SDK](/develop/typescript/set-up-your-local-typescript) guide. When you are done, leave the Temporal Development Server running if you want to test your code locally.
- An [xmemory](https://xmemory.ai) API key and instance.

## Configure Workers to use xmemory

Workers are the compute layer of a Temporal Application. They are responsible for executing the code that defines your [Workflows](/glossary#workflow) and [Activities](/glossary#activity). Before you can execute a Workflow that uses xmemory, you need to create a Worker and configure it to use the xmemory plugin.

Follow the steps below to configure your Worker.

1. Install the `@xmemory/temporal` package (from source until the npm release).

   ```bash
   npm install github:xmemory-ai/xmemory-temporal-ts @temporalio/worker @temporalio/client
   ```

2. Provide your xmemory API key through the environment. The plugin reads it by name, so it is never serialized into Workflow history.

   ```bash
   export XMEM_API_KEY="your-xmemory-api-key"
   ```

3. Create a `worker.ts` file and register the xmemory plugin on your Worker.

   ```ts {4,9}
   import { NativeConnection, Worker } from '@temporalio/worker';
   import { XmemoryPlugin } from '@xmemory/temporal';

   const plugin = new XmemoryPlugin({ instanceId: '<your-instance-id>' }); // reads XMEM_API_KEY
   const connection = await NativeConnection.connect({ address: 'localhost:7233' });
   const worker = await Worker.create({
     connection,
     taskQueue: 'xmemory-support',
     plugins: [plugin],
     workflowsPath: require.resolve('./workflows'),
   });
   await worker.run();
   ```

   In the Worker options, you are specifying that the Worker polls the `xmemory-support` Task Queue. Make sure that you configure your Client application to use the same Task Queue and Namespace.

4. Run the Worker. This Worker will now poll the Temporal Service for work on the `xmemory-support` Task Queue until you stop it.

   ```bash
   nodemon worker.ts
   ```

> **💡 Tip:**
>
>    Use one plugin instance per Worker. The TypeScript plugin is a `WorkerPlugin`; the Client does not carry it.

## Develop a durable memory Workflow

If you weren't using Temporal, you would read and write memory with the xmemory client directly:

```ts
const inst = new XmemoryClient({ apiKey }).instance(instanceId);
const context = await inst.read(`What should support know about ${customerId}?`);
await inst.write('... interaction summary ...');
```

To add Durable Execution, implement the same logic as a Temporal Workflow. Call `xmemoryForWorkflow()` to get a handle with the same method names. The call sites are close to identical, so migration is near-zero-diff — the differences are that results are this plugin's own flattened DTOs rather than the client's raw shapes, and that the durable helpers (`writeDurable`, `writeAsyncStart`) exist only here.

```ts {5,7,11}
// workflows.ts
// The workflow-safe subpath: it reaches no Activity code and no xmemory client,
// so Temporal's workflow bundler accepts it. The package root loads the plugin
// and the client, and belongs in worker setup only.
import { xmemoryForWorkflow } from '@xmemory/temporal/workflow';

export async function customerSupportWorkflow(customerId: string, interactionId: string, message: string): Promise<unknown> {
  const mem = xmemoryForWorkflow();

  const context = await mem.read(`What should support know about ${customerId}?`);

  const response = `Interaction ${interactionId} recorded for ${customerId}.`;

  await mem.writeDurable(
    `interaction_id: ${interactionId}. customer_id: ${customerId}. ` +
      `message: ${message}. response: ${response}. status: completed.`,
  );
  return context.readerResult;
}
```

All memory I/O runs in Activities, so the Workflow stays deterministic and replay-safe: Temporal replays your Workflow code but never re-runs a completed Activity, so a replay never repeats a memory read or write. Workflow code imports `xmemoryForWorkflow` from `@xmemory/temporal/workflow`, a leaf entry point that reaches no Activity code and no client; the package root loads both and belongs in Worker setup only.

> **Memory is untrusted data in both directions.** What goes in is user-controlled; what comes back is that text plus whatever the extraction engine made of it. Reading it into a prompt is the indirect prompt-injection path, so treat a read result as data to quote and bound, never as instructions.
>
> **One Worker, one instance.** The Activities are generic and bind to whichever instance the plugin configured, so every Worker polling a Task Queue must share that configuration. Per-tenant isolation means a Task Queue (and Worker) per tenant — not a per-Workflow option, and the tenant must come from an authenticated identity your service establishes rather than from a caller- or model-supplied value. The example below writes and reads a single shared instance, naming the customer in the text rather than isolating them; do not use it as-is for multi-user data that must be kept apart.

## Write durably

A deep xmemory extraction can take minutes. `writeDurable` enqueues the write and then polls its status from the Workflow, so the wait is a Temporal timer in server-side history rather than a blocked Activity slot. Redeploy your Worker fleet mid-write and nothing is lost — the poll loop resumes on the new worker and completes.

```ts
const status = await mem.writeDurable(text, { maxWaitMs: 15 * 60_000 });
```

For the fire-and-forget pattern — kick off several writes, keep working, join before the turn ends — `writeAsyncStart()` and `writeStatus()` are public too.

## Retries and idempotency

Writes default to at-most-once (`maximumAttempts: 1`). xmemory assigns primary keys with a model, and that assignment is non-deterministic — a re-extraction can normalize the same value differently (`Dr. Robert Kim` vs `Robert Kim`) and fork the record — so a lost-response retry could duplicate. A failed write surfaces to your Workflow, which decides to retry, compensate, or fail. Reads and status-polls are idempotent and retry freely.

Opt into write retries only when your primary keys are literal identifiers present verbatim in the text, such as a `customerId` or `interactionId` you supply, which re-extract deterministically:

```ts
const mem = xmemoryForWorkflow({ writeRetryPolicy: { maximumAttempts: 3 } });
```

## Handle errors with typed failures

xmemory errors become `ApplicationFailure`s with stable `type` strings you can match in a `RetryPolicy` with `nonRetryableErrorTypes`:

| xmemory condition | `type` | Retryable? |
| --- | --- | --- |
| transport error / timeout / HTTP ≥ 500 / 408 | `XmemoryServerError` / `XmemoryUnavailable` | yes |
| `RATE_LIMITED` (429) | `XmemoryRateLimited` | yes — honors `Retry-After` |
| daily quota exceeded | `XmemoryDailyQuotaExceeded` | yes (long backoff) |
| monthly quota exceeded | `XmemoryMonthlyQuotaExceeded` | no |
| `UNAUTHORIZED` / `FORBIDDEN` | `XmemoryAuthFailed` | no |
| `NOT_FOUND` | `XmemoryNotFound` | no |
| validation / schema-evolution rejections | `XmemoryBadRequest` / `XmemorySchemaRejected` | no |
| an unrecognized code | `XmemoryUnknown` | follows the HTTP status |

The durable-write loop adds `XmemoryWriteFailed`, `XmemoryWriteNotFound`, and `XmemoryWriteTimeout`, all non-retryable.

## Keep credentials and sensitive text out of history

The config carries the name of the environment variable holding your API key, never the key itself, so nothing secret is serialized into Activity arguments, which Temporal persists in the clear. Your memory text and queries, however, are Activity inputs and are stored in cleartext Workflow history — install a Temporal [Payload Codec](/develop/typescript/converters-and-encryption) if that content is sensitive.

## Capture Activity results into memory

Auto-capture is off by default. Enable it to record the results of your own Activities into memory through an Activity interceptor that never touches the replay path.

```ts {4-7}
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

See the full example in the [xmemory-temporal-ts repository](https://github.com/xmemory-ai/xmemory-temporal-ts/tree/main/examples).
