/**
 * The xmemory activities: the only place this package does I/O.
 *
 * The client is injected through a holder, so tests substitute a fake with no
 * patching. Each call's client timeout is derived from the deadline Temporal
 * assigned the activity.
 */

import { Context } from '@temporalio/activity';
import { ApplicationFailure } from '@temporalio/common';
import type { InstanceHandle } from 'xmemory';
import type { XmemoryConfig } from './config';
import { activityBudgetMs, withDeadline } from './deadline';
import { clientTimeoutMs } from './defaults';
import {
  projectRead,
  projectWrite,
  projectWriteStart,
  projectWriteStatus,
  type ReadInput,
  type ReadOutput,
  type WriteInput,
  type WriteMutation,
  type WriteOutput,
  type WriteStartOutput,
  type WriteStatusInput,
  type WriteStatusOutput,
} from './dto';
import {
  TYPE_BAD_OPTIONS,
  TYPE_DEADLINE_EXPIRED,
  TYPE_NOT_BOUND,
  TYPE_NO_DEADLINE,
  toApplicationFailure,
} from './errors';
import { ACTIVITY_READ, ACTIVITY_WRITE, ACTIVITY_WRITE_START, ACTIVITY_WRITE_STATUS } from './names';

export { ACTIVITY_READ, ACTIVITY_WRITE, ACTIVITY_WRITE_START, ACTIVITY_WRITE_STATUS };

/** The client methods this package calls. Injectable, so tests need no backend. */
export type XmemoryInstance = Pick<InstanceHandle, 'read' | 'write' | 'writeAsync' | 'writeStatus'>;

/** Holds the injected client so activity functions can close over it. */
export class InstanceHolder {
  private instance: XmemoryInstance | undefined;
  bind(instance: XmemoryInstance): void {
    this.instance = instance;
  }
  get(): XmemoryInstance {
    if (!this.instance) {
      // The plugin's runWorker hook never bound a client. Retrying cannot fix it.
      throw ApplicationFailure.create({
        message:
          'xmemory activities are not bound to a client — register XmemoryPlugin on the Worker ' +
          'rather than registering the activity functions directly.',
        type: TYPE_NOT_BOUND,
        nonRetryable: true,
      });
    }
    return this.instance;
  }
}

export interface XmemoryActivities {
  [ACTIVITY_READ]: (input: ReadInput) => Promise<ReadOutput>;
  [ACTIVITY_WRITE]: (input: WriteInput) => Promise<WriteOutput>;
  [ACTIVITY_WRITE_START]: (input: WriteInput) => Promise<WriteStartOutput>;
  [ACTIVITY_WRITE_STATUS]: (input: WriteStatusInput) => Promise<WriteStatusOutput>;
}

/**
 * What to hand the client: the text, or the mutations when there are any.
 *
 * Checked at runtime, because activity inputs arrive as JSON and nothing enforces
 * their TypeScript types. The client's `write` and `writeAsync` are overloaded on
 * the first argument and treat anything but a string as structured mutations, so a
 * `text` that arrived as an array would skip extraction and apply as an update or
 * delete, and a `structuredMutations` string would be written as memory.
 *
 * An empty list is refused too: the client answers `[]` with a plain Error, which
 * the mapper reads as a retryable transport failure. Called outside the try, or
 * `toApplicationFailure` would remap the verdict.
 */
function requireWritePayload(input: WriteInput): string | readonly WriteMutation[] {
  const mutations: unknown = input.structuredMutations ?? undefined;
  if (mutations !== undefined) {
    if (!Array.isArray(mutations)) {
      throw badOptions(`xmemory structuredMutations must be a list of mutations, got ${kindOf(mutations)}`);
    }
    if (mutations.length === 0) {
      throw badOptions('xmemory write was given an empty structuredMutations list; omit it to write text instead');
    }
    return mutations;
  }
  const text: unknown = input.text;
  if (typeof text !== 'string') {
    throw badOptions(`xmemory write text must be a string, got ${kindOf(text)}`);
  }
  return text;
}

// The kind only, never the value: failure messages are persisted to history.
function kindOf(value: unknown): string {
  return value === null ? 'null' : Array.isArray(value) ? 'an array' : typeof value;
}

function badOptions(message: string): ApplicationFailure {
  return ApplicationFailure.create({ message, type: TYPE_BAD_OPTIONS, nonRetryable: true });
}

export function createActivities(holder: InstanceHolder, config: XmemoryConfig): XmemoryActivities {
  const defaultLogic = config.defaultExtractionLogic ?? 'fast';

  /**
   * Client budget for the running activity, from its own deadline. Derived rather
   * than kept as a second worker-side copy, so "the client gives up first" holds
   * even when a workflow lowers its timeout.
   */
  const budgetMs = (): number => {
    const info = Context.current().info;
    const deadlineMs = activityBudgetMs(info);
    if (deadlineMs === null) {
      throw ApplicationFailure.create({
        message:
          `activity ${info.activityType} was scheduled without a deadline: ` +
          'set startToCloseTimeout or scheduleToCloseTimeout on it.',
        type: TYPE_NO_DEADLINE,
        nonRetryable: true,
      });
    }
    if (deadlineMs <= 0) {
      // Temporal has given up on this attempt; a token budget would send a request
      // nobody reads, and a write the server could still accept.
      throw ApplicationFailure.create({
        message: `activity ${info.activityType} is past its deadline`,
        type: TYPE_DEADLINE_EXPIRED,
      });
    }
    return clientTimeoutMs(deadlineMs, config.clientMarginMs);
  };

  const writeOptions = (input: WriteInput, timeoutMs: number) => ({
    extractionLogic: (input.extractionLogic ?? defaultLogic) as 'fast' | 'deep',
    ...(input.diffEngine !== undefined ? { diffEngine: input.diffEngine } : {}),
    timeoutMs,
  });

  return {
    async [ACTIVITY_READ](input: ReadInput): Promise<ReadOutput> {
      // Outside the try: an unbound-client or missing-deadline failure must keep
      // its non-retryable type rather than being re-mapped by toApplicationFailure.
      const instance = holder.get();
      const timeoutMs = budgetMs();
      try {
        const result = await withDeadline(
          instance.read(input.query, {
            ...(input.readMode !== undefined ? { readMode: input.readMode } : {}),
            ...(input.scope !== undefined ? { scope: input.scope } : {}),
            timeoutMs,
          }),
          timeoutMs,
        );
        return projectRead(result);
      } catch (err) {
        throw toApplicationFailure(err);
      }
    },

    async [ACTIVITY_WRITE](input: WriteInput): Promise<WriteOutput> {
      const instance = holder.get();
      const ms = budgetMs();
      const payload = requireWritePayload(input);
      try {
        // A structured write carries its own keys, so the server applies it without
        // running the extractor; text and extractionLogic are moot.
        const result = await withDeadline(
          typeof payload === 'string'
            ? instance.write(payload, writeOptions(input, ms))
            : instance.write(payload, { timeoutMs: ms }),
          ms,
        );
        return projectWrite(result);
      } catch (err) {
        throw toApplicationFailure(err);
      }
    },

    async [ACTIVITY_WRITE_START](input: WriteInput): Promise<WriteStartOutput> {
      const instance = holder.get();
      const ms = budgetMs();
      const payload = requireWritePayload(input);
      try {
        return projectWriteStart(
          await withDeadline(
            typeof payload === 'string'
              ? instance.writeAsync(payload, writeOptions(input, ms))
              : instance.writeAsync(payload, { timeoutMs: ms }),
            ms,
          ),
        );
      } catch (err) {
        throw toApplicationFailure(err);
      }
    },

    async [ACTIVITY_WRITE_STATUS](input: WriteStatusInput): Promise<WriteStatusOutput> {
      const instance = holder.get();
      const timeoutMs = budgetMs();
      const writeId = input.writeId;
      try {
        const result = await withDeadline(instance.writeStatus(writeId, { timeoutMs }), timeoutMs);
        const projected = projectWriteStatus(result);
        if (projected.writeId !== writeId) {
          // Before logging: another write's detail would otherwise be logged under
          // the id we asked about, and its outcome read as ours.
          throw new Error('xmemory returned a status for a different write');
        }
        const errorDetail = result.error_detail;
        if (errorDetail) {
          // Never the return value: history keeps Activity results in the clear.
          // Not verbatim in the log either unless asked for — the detail is not
          // promised user-safe, and logs travel.
          Context.current().log.warn(
            'xmemory write failed',
            // `=== true`, not truthiness: this config can come from a file or an
            // env var, and the string "false" would otherwise turn the detail on.
            config.logServerErrorDetail === true
              ? { writeId, errorDetail }
              : {
                  writeId,
                  errorDetailLength: errorDetail.length,
                  note: 'detail withheld from logs and history; set logServerErrorDetail to include it',
                },
          );
        }
        return projected;
      } catch (err) {
        throw toApplicationFailure(err);
      }
    },
  };
}
