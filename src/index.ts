/**
 * Temporal plugin for xmemory — durable agent memory as Temporal Activities.
 *
 * Add {@link XmemoryPlugin} to your Temporal Worker. Workflow code imports its
 * side of the API from `@xmemory/temporal/workflow`, a leaf that pulls in no
 * Activity or client code, so Temporal's workflow bundler accepts it.
 */

export { XmemoryPlugin, PLUGIN_NAME } from './plugin';
export type { XmemoryPluginOptions } from './plugin';
export type { XmemoryConfig } from './config';
export { DEFAULT_TIMEOUTS, DEFAULT_CLIENT_MARGIN_MS } from './defaults';
export type { XmemoryTimeouts } from './defaults';
// Workflow-side values live at `@xmemory/temporal/workflow`, never here: this
// entry reaches @temporalio/activity and the xmemory client, both of which the
// workflow bundler rejects. Types are erased, so they are safe to re-export.
export type { WorkflowXmemory, WorkflowXmemoryOptions, WriteDurableOptions, WriteStatusRetry } from './workflow';
export type { AutoCaptureConfig } from './interceptor';
// The `type` strings themselves, so a consumer can match on them in a
// RetryPolicy without hard-coding literals. The export map has no deep-import
// escape hatch, so anything a caller needs has to leave through here.
export {
  TYPE_AUTH_FAILED,
  TYPE_BAD_OPTIONS,
  TYPE_BAD_REQUEST,
  TYPE_DAILY_QUOTA_EXCEEDED,
  TYPE_DEADLINE_EXPIRED,
  TYPE_MONTHLY_QUOTA_EXCEEDED,
  TYPE_NOT_BOUND,
  TYPE_NOT_FOUND,
  TYPE_NO_DEADLINE,
  TYPE_QUOTA_EXCEEDED,
  TYPE_RATE_LIMITED,
  TYPE_SCHEMA_REJECTED,
  TYPE_SERVER_ERROR,
  TYPE_UNAVAILABLE,
  TYPE_UNKNOWN,
  TYPE_WRITE_FAILED,
  TYPE_WRITE_NOT_FOUND,
  TYPE_WRITE_TIMEOUT,
} from './errors';
