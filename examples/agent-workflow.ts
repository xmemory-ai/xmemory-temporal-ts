/**
 * A tiny agent-shaped workflow that remembers and recalls through xmemory.
 *
 * The migration story: it reads and writes memory with the same calls a
 * non-Temporal agent would make against the xmemory `InstanceHandle` — only the
 * handle differs.
 */
// In your own project:  import { xmemoryForWorkflow } from '@xmemory/temporal/workflow';
// That subpath is a leaf: it reaches no Activity code and no xmemory client, so
// the workflow bundler accepts it. Running in-repo we import from source.
import { xmemoryForWorkflow } from '../src/workflow';

export const TASK_QUEUE = 'xmemory-example';

export async function supportAgentWorkflow(userName: string, userMessage: string): Promise<string> {
  const mem = xmemoryForWorkflow();

  // Durably remember the fact, attributed to the user by name. A memory store
  // has no ambient "current user" or session — the text itself must say whom the
  // fact is about. Survives worker restarts.
  await mem.writeDurable(`${userName} says: ${userMessage}`);

  // Recall by that same name — "this user" would mean nothing to xmemory.
  const recalled = await mem.read(`What do we know about ${userName}?`);
  const answer = recalled.readerResult;
  if (answer == null) return '(nothing remembered yet)';
  return typeof answer === 'string' ? answer : JSON.stringify(answer);
}
