/** A user workflow that calls a plain user activity (not xmemory). */
import { proxyActivities } from '@temporalio/workflow';

const { user_activity } = proxyActivities<{ user_activity(payload: string): Promise<string> }>({
  startToCloseTimeout: '30s',
});

export async function userWorkflow(payload: string): Promise<string> {
  return user_activity(payload);
}

/** The same activity on a deadline too tight to fit a capture enqueue. */
const short = proxyActivities<{ user_activity(payload: string): Promise<string> }>({
  startToCloseTimeout: '1s',
});

export async function shortDeadlineUserWorkflow(payload: string): Promise<string> {
  return short.user_activity(payload);
}
