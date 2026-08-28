/** A plain user activity the auto-capture interceptor observes. */
export async function userActivity(payload: string): Promise<string> {
  return `handled: ${payload}`;
}
