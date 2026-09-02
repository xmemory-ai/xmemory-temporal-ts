/** Kick off the example workflow against a running worker. */
import { Client, Connection } from '@temporalio/client';
import { supportAgentWorkflow, TASK_QUEUE } from './agent-workflow';

async function main(): Promise<void> {
  const connection = await Connection.connect({
    address: process.env.TEMPORAL_ADDRESS ?? 'localhost:7233',
  });
  const client = new Client({ connection });
  const result = await client.workflow.execute(supportAgentWorkflow, {
    taskQueue: TASK_QUEUE,
    workflowId: `support-${Date.now()}`,
    args: ['Alex', 'I prefer email over phone calls.'],
  });
  console.log('agent recalled:', result);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
