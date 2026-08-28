/**
 * A minimal Temporal worker wired to xmemory.
 *
 * First create an instance with a matching schema (once):
 *   export XMEM_INSTANCE_ID="$(XMEM_API_KEY=xmem_... npx tsx examples/setup-memory.ts)"
 *
 *   temporal server start-dev
 *   XMEM_API_KEY=xmem_... npx tsx examples/worker.ts
 */
import { NativeConnection, Worker } from '@temporalio/worker';
// Running in-repo, so we import from source. In your own project this is:
//   import { XmemoryPlugin } from '@xmemory/temporal';
import { XmemoryPlugin } from '../src/plugin';
import { TASK_QUEUE } from './agent-workflow';

async function main(): Promise<void> {
  const plugin = new XmemoryPlugin({
    instanceId: process.env.XMEM_INSTANCE_ID!,
    url: process.env.XMEM_API_URL,
  });

  const connection = await NativeConnection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  });

  const worker = await Worker.create({
    connection,
    taskQueue: TASK_QUEUE,
    workflowsPath: require.resolve('./agent-workflow'),
    plugins: [plugin],
  });

  console.log(`worker running on task queue ${TASK_QUEUE} — Ctrl-C to stop`);
  await worker.run();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
