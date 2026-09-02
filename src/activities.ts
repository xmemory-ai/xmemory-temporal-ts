/**
 * The xmemory activities: the only place this package does I/O.
 *
 * The client is injected through a holder, so tests substitute a fake with no
 * patching. Each call's client timeout is derived from the deadline Temporal
 * assigned the activity.
 */

import { Context } from '@temporalio/activity';
import type { InstanceHandle } from 'xmemory';
import type { XmemoryConfig } from './config';
import { activityBudgetMs, withDeadline } from './deadline';
import { applicationFailure, clientTimeoutMs, ownDeep, ownOnly, TooDeepError } from './defaults';
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
      throw applicationFailure({
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
 * The input as a null-prototype copy, or a non-retryable failure if it is not an
 * object. The Activity names are public, so a workflow can schedule them directly:
 * a `null` payload would otherwise raise a raw, retryable TypeError, and an
 * inherited `structuredMutations` would turn a text write into a delete.
 */
function requireInput<T extends object>(input: T, activity: string): T {
  if (typeof input !== 'object' || input === null) {
    throw applicationFailure({
      message: `xmemory ${activity} needs an input object, got ${input === null ? 'null' : typeof input}`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
  // Own properties only: an inherited `structuredMutations` made a text write
  // discard its text and apply a delete instead.
  return ownOnly(input);
}

/**
 * The text to send, or a non-retryable failure if it is not a string.
 *
 * `write` is overloaded on its first argument, so an array here is applied as
 * mutations rather than written as memory. Activity inputs are JSON, so the type
 * annotation proves nothing at this boundary.
 */
function requireText(value: unknown, field: string): string {
  if (typeof value !== 'string') {
    throw applicationFailure({
      message: `xmemory ${field} must be a string, got ${value === null ? 'null' : typeof value}`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
  return value;
}

/**
 * The mutations to send, or `undefined` to write `text` instead.
 *
 * An empty list is refused before any request: the client answers `[]` with a plain
 * Error, which the mapper reads as a retryable transport failure. Called outside the
 * try, or `toApplicationFailure` would remap the verdict.
 */
function requireUsableMutations(input: WriteInput): readonly WriteMutation[] | undefined {
  const mutations = input.structuredMutations;
  // The container first: a string has a `length` too, and reached `write(string)`
  // — the text overload — sending the mutation value as the memory.
  if (mutations !== undefined && mutations !== null && !Array.isArray(mutations)) {
    throw applicationFailure({
      message: `xmemory write was given a ${typeof mutations} as structuredMutations; it must be a list of mutations`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
  if (mutations !== undefined && mutations !== null && mutations.length === 0) {
    throw applicationFailure({
      message: 'xmemory write was given an empty structuredMutations list; omit it to write text instead',
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
  // Deep, for the same reason as the read scope: these are forwarded verbatim, and
  // an inherited field inside one — `allow_bulk_delete`, say — widens what it does.
  return mutations === undefined || mutations === null ? undefined : copyForwarded(mutations, 'structuredMutations');
}

/** A forwarded value, copied own-deep, with an over-nested one reported as an option error. */
function copyForwarded<T>(value: T, field: string): T {
  try {
    return ownDeep(value);
  } catch (err) {
    if (!(err instanceof TooDeepError)) throw err;
    throw applicationFailure({
      message: `xmemory ${field} nests too deeply to send: ${err.message}`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
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
      throw applicationFailure({
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
      throw applicationFailure({
        message: `activity ${info.activityType} is past its deadline`,
        type: TYPE_DEADLINE_EXPIRED,
      });
    }
    return clientTimeoutMs(deadlineMs, config.clientMarginMs);
  };

  // Null-prototype, like every object handed to the client: it reads these fields,
  // so an omitted `diffEngine` or `readMode` was answered by `Object.prototype` — a
  // default read became raw-tables, and a plain text write picked up a diff engine.
  const writeOptions = (input: WriteInput, timeoutMs: number) =>
    ownOnly({
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
      const safe = requireInput(input, ACTIVITY_READ);
      const query = requireText(safe.query, 'read query');
      // Outside the try, like the mutation copy: its verdict is a bad option, and
      // inside it `toApplicationFailure` would turn that into a retryable transport
      // error and Temporal would repeat input that cannot work.
      const scope = safe.scope === undefined ? undefined : copyForwarded(safe.scope, 'read scope');
      try {
        const result = await withDeadline(
          // Null-prototype, like every object handed to the client: it reads these
          // fields, so an omitted `readMode` or `scope` was answered by
          // `Object.prototype` and a default read came back as raw-tables.
          instance.read(
            query,
            ownOnly({
              ...(safe.readMode !== undefined ? { readMode: safe.readMode } : {}),
              ...(scope !== undefined ? { scope } : {}),
              timeoutMs,
            }),
          ),
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

      const safe = requireInput(input, ACTIVITY_WRITE);
      const mutations = requireUsableMutations(safe);
      const text = mutations === undefined ? requireText(safe.text, 'write text') : '';
      try {
        // A structured write carries its own keys, so the server applies it without
        // running the extractor; text and extractionLogic are moot.
        const result = await withDeadline(
          mutations !== undefined
            ? instance.write(mutations, ownOnly({ timeoutMs: ms }))
            : instance.write(text, writeOptions(safe, ms)),
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
      const safe = requireInput(input, ACTIVITY_WRITE_START);
      const mutations = requireUsableMutations(safe);
      const text = mutations === undefined ? requireText(safe.text, 'write text') : '';
      try {
        return projectWriteStart(
          await withDeadline(
            mutations !== undefined
              ? instance.writeAsync(mutations, ownOnly({ timeoutMs: ms }))
              : instance.writeAsync(text, writeOptions(safe, ms)),
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
      const writeId = requireText(requireInput(input, ACTIVITY_WRITE_STATUS).writeId, 'writeId');
      try {
        const result = await withDeadline(instance.writeStatus(writeId, ownOnly({ timeoutMs })), timeoutMs);
        const projected = projectWriteStatus(result);
        if (projected.writeId !== writeId) {
          // Before logging: another write's detail would otherwise be logged under
          // the id we asked about, and its outcome read as ours.
          throw new Error('xmemory returned a status for a different write');
        }
        // Own property, like every response field: an inherited one would be logged
        // as though the server had sent it.
        const errorDetail = Object.hasOwn(result, 'error_detail') ? result.error_detail : null;
        if (typeof errorDetail === 'string' && errorDetail !== '') {
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
