/**
 * The workflow-facing xmemory surface.
 *
 * `WorkflowXmemory` mirrors the plain client's method *names*, so existing agent
 * call sites keep working and just dispatch to an activity. Two differences: the
 * enqueue is `writeAsyncStart`, and results are this package's own DTOs rather than
 * the client's raw shapes. Replay-safe by construction: only `proxyActivities` and
 * `sleep`, no I/O and no wall-clock.
 */

import { ApplicationFailure, isCancellation, log, proxyActivities, sleep, workflowInfo } from '@temporalio/workflow';
import {
  ActivityFailure,
  ApplicationFailure as CommonApplicationFailure,
  compileRetryPolicy,
  msToNumber,
  RetryState,
} from '@temporalio/common';
import type { Duration, RetryPolicy } from '@temporalio/common';
import {
  ACTIVITY_READ,
  ACTIVITY_WRITE,
  ACTIVITY_WRITE_START,
  ACTIVITY_WRITE_STATUS,
  TYPE_WRITE_FAILED,
  TYPE_WRITE_NOT_FOUND,
  TYPE_WRITE_TIMEOUT,
  TYPE_BAD_OPTIONS,
} from './names';
// The workflow owns every activity budget: what is set here is what Temporal
// enforces AND what each activity derives its client timeout from, so the two
// can never disagree. `DEFAULT_TIMEOUTS` is only the source of the numbers.
import { applicationFailure, DEFAULT_TIMEOUTS, MAX_DURATION_MS, ownOnly } from './defaults';
import type {
  ReadMode,
  ReadInput,
  WriteMutation,
  ReadScope,
  ReadOutput,
  WriteInput,
  WriteOutput,
  WriteStartOutput,
  WriteStatusInput,
  WriteStatusOutput,
} from './dto';

// Terminal `WriteQueueStatus` values (see xmemory's WriteQueueStatus).
const STATUS_COMPLETED = 'completed';
const STATUS_FAILED = 'failed';
const STATUS_NOT_FOUND = 'not_found';
// Non-terminal states we keep polling through. Listed explicitly so a new,
// unseen server-side state is treated as unknown and the loop fails loudly
// rather than silently deciding it is terminal.
const STATUS_IN_PROGRESS = new Set(['queued', 'processing', 'extracting', 'extracted', 'applying']);

const DEFAULT_READ_RETRY: RetryPolicy = {
  initialInterval: '1s',
  backoffCoefficient: 2,
  maximumInterval: '30s',
  maximumAttempts: 10,
};
// At-most-once: xmemory assigns primary keys with a model, so a re-extraction can
// normalize the same value differently and fork the record. A failed write is
// surfaced to the workflow rather than retried. See the README's idempotency
// section for when opting in is safe.
const DEFAULT_WRITE_RETRY: RetryPolicy = { maximumAttempts: 1 };
const DEFAULT_POLL_RETRY: RetryPolicy = {
  initialInterval: '1s',
  backoffCoefficient: 2,
  maximumInterval: '20s',
  maximumAttempts: 10,
};
// Shape the activity proxy is typed against (names -> IO types).
interface ActivitySignatures {
  [ACTIVITY_READ]: (input: ReadInput) => Promise<ReadOutput>;
  [ACTIVITY_WRITE]: (input: WriteInput) => Promise<WriteOutput>;
  [ACTIVITY_WRITE_START]: (input: WriteInput) => Promise<WriteStartOutput>;
  [ACTIVITY_WRITE_STATUS]: (input: WriteStatusInput) => Promise<WriteStatusOutput>;
}

/**
 * A duration in ms, or a non-retryable option failure.
 *
 * `msToNumber` throws a raw TypeError on a malformed string and passes `Infinity`
 * straight through; either reaches Temporal and fails the Workflow Task over and
 * over, with a durable write already enqueued.
 */
function durationMs(value: Duration, label: string): number {
  let ms: number;
  try {
    ms = msToNumber(value);
  } catch (err) {
    throw applicationFailure({
      message: `${label} is not a valid duration: ${String(err)}`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
  // `< 1`, not `<= 0`: Temporal truncates a sub-millisecond duration to zero, and a
  // zeroed startToClose falls back to the default schedule-to-close — ten years.
  if (!Number.isFinite(ms) || ms < 1 || ms > MAX_DURATION_MS) {
    throw applicationFailure({
      message: `${label} must be between 1 and ${MAX_DURATION_MS}ms, got ${ms}`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
  return ms;
}

// Temporal carries an attempt count as a signed int32; 2**31 arrives negative.
const MAX_ATTEMPTS = 2_147_483_647;
// Retry intervals are held by the service, not by `setTimeout`, so
// `MAX_DURATION_MS` does not apply. The range that does: one nanosecond, below
// which a positive interval encodes as no delay, up to the int64 nanosecond count
// Temporal keeps them in (~292 years), past which it wraps negative.
const MIN_INTERVAL_MS = 1e-6;
const MAX_INTERVAL_MS = 9_223_372_036_854;
// With `maximumInterval` unset Temporal derives one at 100x the initial. An
// interval that fits alone can overflow through that: three years derives past int64.
const DEFAULT_MAX_INTERVAL_FACTOR = 100;

/**
 * Refuse a retry policy Temporal will not schedule, before the Activity starts.
 *
 * About failure *mode*, not about repeating the SDK's rules: an unusable policy
 * raises a raw `ValueError` while the Activity command is built, which Temporal
 * treats as a Workflow *Task* failure — the workflow neither fails nor progresses.
 * On `writeDurable` the enqueue happens first, leaving a queued write nobody polls.
 *
 * `compileRetryPolicy` is the SDK's contract, not the service's: it accepts a
 * sub-1 coefficient, an attempt count past int32, and a negative interval, all of
 * which the service refuses — after the enqueue.
 */
// Everything `RetryPolicy` carries. Checked because Temporal ignores what it does
// not recognise: `maximumAttempt` (singular) compiles to an unset `maximumAttempts`,
// which means *unlimited* — a typo that silently opts a caller into retrying.
const POLICY_FIELDS = new Set([
  'initialInterval',
  'backoffCoefficient',
  'maximumInterval',
  'maximumAttempts',
  'nonRetryableErrorTypes',
]);

function assertSchedulablePolicy(policy: RetryPolicy, label: string): void {
  const problems: string[] = [];
  // The container before its fields: an array or string has none of them, so every
  // check passes and the compiled policy has `maximumAttempts` unset — unlimited,
  // which on a write replaces the at-most-once default.
  if (typeof policy !== 'object' || policy === null || Array.isArray(policy)) {
    throw applicationFailure({
      message: `${label} must be a RetryPolicy object, got ${policy === null ? 'null' : typeof policy}`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
  const unknown = Object.keys(policy).filter((key) => !POLICY_FIELDS.has(key));
  if (unknown.length > 0) {
    problems.push(`unknown field(s) ${unknown.join(', ')}; Temporal ignores those, so a typo changes how it retries`);
  }
  const { backoffCoefficient: coefficient, maximumAttempts: attempts } = policy;
  if (coefficient !== undefined && (!Number.isFinite(coefficient) || coefficient < 1)) {
    problems.push(`backoffCoefficient must be a finite number >= 1, got ${coefficient}`);
  }
  // `Infinity` is Temporal's own spelling of unlimited and compiles away to unset.
  if (attempts !== undefined && attempts !== Number.POSITIVE_INFINITY) {
    if (!Number.isInteger(attempts) || attempts < 0 || attempts > MAX_ATTEMPTS) {
      problems.push(`maximumAttempts must be an integer between 0 and ${MAX_ATTEMPTS}, or Infinity, got ${attempts}`);
    }
  }
  for (const [field, value] of [
    ['initialInterval', policy.initialInterval],
    ['maximumInterval', policy.maximumInterval],
  ] as const) {
    if (value === undefined) continue;
    let ms: number;
    try {
      ms = msToNumber(value);
    } catch {
      problems.push(`${field} is not a valid duration: ${String(value)}`);
      continue;
    }
    // A negative interval compiles and the service refuses it; zero and the
    // interval ordering the SDK catches below. A positive value under a nanosecond
    // encodes to no delay at all, so the polls it paces run flat out.
    if (!Number.isFinite(ms) || ms < 0 || ms > MAX_INTERVAL_MS || (ms > 0 && ms < MIN_INTERVAL_MS)) {
      problems.push(`${field} must be 0, or between ${MIN_INTERVAL_MS} and ${MAX_INTERVAL_MS}ms, got ${ms}`);
    }
  }
  // Checked on the value Temporal will actually use: with `maximumInterval` unset
  // the derived one is what overflows, and it is never seen in the policy object.
  if (policy.maximumInterval === undefined && policy.initialInterval !== undefined) {
    let initialMs: number | undefined;
    try {
      initialMs = msToNumber(policy.initialInterval);
    } catch {
      // Already reported above.
    }
    if (initialMs !== undefined && Number.isFinite(initialMs)) {
      const derivedMs = initialMs * DEFAULT_MAX_INTERVAL_FACTOR;
      if (derivedMs > MAX_INTERVAL_MS) {
        problems.push(
          `initialInterval ${initialMs}ms leaves Temporal to derive a maximumInterval of ${derivedMs}ms ` +
            `(${DEFAULT_MAX_INTERVAL_FACTOR}x), past the ${MAX_INTERVAL_MS}ms it can hold. ` +
            'Set maximumInterval explicitly, or lower initialInterval.',
        );
      }
    }
  }
  // The container before its members: a number is not iterable, and a string is —
  // silently passing as one bogus type per character.
  const types = policy.nonRetryableErrorTypes;
  if (types !== undefined && !Array.isArray(types)) {
    problems.push(`nonRetryableErrorTypes must be an array of strings, got ${typeof types}`);
  } else {
    for (const value of types ?? []) {
      // These are encoded as protobuf strings. A non-string compiles, then throws
      // ERR_INVALID_ARG_TYPE when the Activity command is built — after the enqueue.
      // Reachable from JavaScript callers and from anything typed `any`.
      if (typeof value !== 'string') {
        problems.push(`nonRetryableErrorTypes must all be strings, got ${typeof value}`);
        break;
      }
    }
  }
  if (problems.length === 0) {
    // Whatever the SDK itself refuses, on top of the checks above.
    try {
      compileRetryPolicy(policy);
    } catch (err) {
      problems.push(String(err));
    }
  }
  if (problems.length > 0) {
    throw applicationFailure({
      message: `${label} is unusable: ${problems.join('; ')}`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
}

/**
 * The caller's options as a null-prototype copy, or a non-retryable failure.
 *
 * `null` means omitted, which is what JSON produces. Anything else that is not a
 * plain object is a mistake: `ownOnly` would *box* a string or array into an object
 * with no recognisable fields, and every option would fall back to its default.
 */
function requireOptions<T extends object>(options: T | null | undefined, label: string): T {
  if (options === null || options === undefined) return ownOnly({} as T);
  if (typeof options !== 'object' || Array.isArray(options)) {
    throw applicationFailure({
      message: `${label} options must be an object, got ${Array.isArray(options) ? 'an array' : typeof options}`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
  return ownOnly({ ...options });
}

/**
 * The value, or a non-retryable failure if it is not a string.
 *
 * Workflow arguments are JSON and the annotation is erased. Without this, an array
 * in the text slot is applied as structured mutations (`write` is overloaded), and
 * anything without a `.length` throws a raw TypeError while the summary is built.
 */
function requireText(value: unknown, label: string): string {
  if (typeof value !== 'string') {
    throw applicationFailure({
      message: `${label} must be a string, got ${value === null ? 'null' : typeof value}`,
      type: TYPE_BAD_OPTIONS,
      nonRetryable: true,
    });
  }
  return value;
}

/**
 * A private copy of a retry policy. The caller keeps its own reference and its code
 * runs between our awaits, so a policy validated before the enqueue could be
 * mutated before the first poll was scheduled. Left alone when it is not an object,
 * so validation reports that instead.
 */
function snapshotPolicy(policy: RetryPolicy): RetryPolicy {
  if (typeof policy !== 'object' || policy === null || Array.isArray(policy)) return policy;
  // Copy first, then read from the copy: reading the original would promote an
  // inherited `nonRetryableErrorTypes` into the snapshot as though it were theirs.
  const copy = ownOnly({ ...policy });
  const types = copy.nonRetryableErrorTypes;
  if (Array.isArray(types)) copy.nonRetryableErrorTypes = [...types];
  return copy;
}

export interface WorkflowXmemoryOptions {
  readTimeout?: Duration;
  writeTimeout?: Duration;
  writeStartTimeout?: Duration;
  writeStatusTimeout?: Duration;
  readRetryPolicy?: RetryPolicy;
  writeRetryPolicy?: RetryPolicy;
  pollRetryPolicy?: RetryPolicy;
  /**
   * Total bound on a call including its retries, as `scheduleToCloseTimeout`.
   *
   * The per-call timeouts above bound one *attempt*. Nothing bounds the sequence
   * unless this is set, and the server's own `Retry-After` is honoured as given —
   * so a rate-limited read with an hour-long hint can sit in retries for hours.
   * Unset by default, which is Temporal's behaviour; set it when a call has a
   * deadline of its own.
   */
  totalTimeout?: Duration;
  includeContentInSummary?: boolean;
}

export interface WriteDurableOptions {
  extractionLogic?: 'fast' | 'deep';
  diffEngine?: boolean;
  structuredMutations?: readonly WriteMutation[];
  pollIntervalMs?: number;
  maxPollIntervalMs?: number;
  maxWaitMs?: number;
}

export class WorkflowXmemory {
  private readonly opts: Required<
    Pick<WorkflowXmemoryOptions, 'readTimeout' | 'writeTimeout' | 'writeStartTimeout' | 'writeStatusTimeout'>
  > & {
    readRetry: RetryPolicy;
    writeRetry: RetryPolicy;
    pollRetry: RetryPolicy;
    totalTimeout: Duration | undefined;
    includeContent: boolean;
  };

  constructor(options: WorkflowXmemoryOptions = {}) {
    // `?? {}` as well as the default: a default only applies to `undefined`, and
    // workflow arguments arrive as JSON, where an omitted object is often `null`.
    // Reading a field off that throws a raw TypeError inside workflow code, which
    // Temporal retries as a Workflow Task — forever.
    options = requireOptions(options, 'xmemoryForWorkflow');
    const summarize = options.includeContentInSummary;
    if (summarize !== undefined && typeof summarize !== 'boolean') {
      throw applicationFailure({
        message: `includeContentInSummary must be a boolean, got ${typeof summarize}`,
        type: TYPE_BAD_OPTIONS,
        nonRetryable: true,
      });
    }
    this.opts = {
      readTimeout: options.readTimeout ?? DEFAULT_TIMEOUTS.readMs,
      writeTimeout: options.writeTimeout ?? DEFAULT_TIMEOUTS.writeMs,
      writeStartTimeout: options.writeStartTimeout ?? DEFAULT_TIMEOUTS.writeStartMs,
      writeStatusTimeout: options.writeStatusTimeout ?? DEFAULT_TIMEOUTS.writeStatusMs,
      readRetry: snapshotPolicy(options.readRetryPolicy ?? DEFAULT_READ_RETRY),
      writeRetry: snapshotPolicy(options.writeRetryPolicy ?? DEFAULT_WRITE_RETRY),
      pollRetry: snapshotPolicy(options.pollRetryPolicy ?? DEFAULT_POLL_RETRY),
      totalTimeout: options.totalTimeout,
      // `=== true`, not truthiness: options can be built from config, and the string
      // "false" would otherwise put memory text into the Activity summary, which is
      // persisted to workflow history.
      includeContent: options.includeContentInSummary === true,
    };
  }

  async read(query: string, options: { readMode?: ReadMode; scope?: ReadScope } = {}): Promise<ReadOutput> {
    query = requireText(query, 'read: query');
    options = requireOptions(options, 'read');
    durationMs(this.opts.readTimeout, 'read: timeout');
    assertSchedulablePolicy(this.opts.readRetry, 'read: readRetryPolicy');
    const acts = proxyActivities<ActivitySignatures>({
      startToCloseTimeout: this.opts.readTimeout,
      ...this.totalBound(),
      retry: this.opts.readRetry,
      summary: this.summary('read', query),
    });
    // Named fields, not a spread, and the positional value last: an options object
    // carrying its own `query` would otherwise replace what the caller passed.
    return acts[ACTIVITY_READ]({ readMode: options.readMode, scope: options.scope, query });
  }

  /**
   * Write memory, from free `text` or explicit `structuredMutations`.
   *
   * Structured mutations skip extraction, so an update or delete addressed by a
   * primary key is safe to retry. A create without one is not: the server assigns
   * the key, so a lost response plus a retry inserts the record twice.
   */
  async write(
    text: string,
    options: { extractionLogic?: 'fast' | 'deep'; diffEngine?: boolean; structuredMutations?: readonly WriteMutation[] } = {},
  ): Promise<WriteOutput> {
    text = requireText(text, 'write: text');
    options = requireOptions(options, 'write');
    durationMs(this.opts.writeTimeout, 'write: timeout');
    this.assertWritePolicy('write');
    const acts = proxyActivities<ActivitySignatures>({
      startToCloseTimeout: this.opts.writeTimeout,
      ...this.totalBound(),
      retry: this.opts.writeRetry,
      summary: this.summary('write', text, options.extractionLogic),
    });
    return acts[ACTIVITY_WRITE]({
      extractionLogic: options.extractionLogic,
      diffEngine: options.diffEngine,
      structuredMutations: options.structuredMutations,
      text,
    });
  }

  /**
   * Enqueue a write and return its id, without waiting for it.
   *
   * Nothing is forced here: an omitted `extractionLogic` resolves worker-side to
   * `XmemoryConfig.defaultExtractionLogic`. Only `writeDurable` asks for `deep`.
   */
  async writeAsyncStart(
    text: string,
    options: { extractionLogic?: 'fast' | 'deep'; diffEngine?: boolean; structuredMutations?: readonly WriteMutation[] } = {},
  ): Promise<WriteStartOutput> {
    text = requireText(text, 'writeAsyncStart: text');
    options = requireOptions(options, 'writeAsyncStart');
    durationMs(this.opts.writeStartTimeout, 'writeAsyncStart: timeout');
    this.assertWritePolicy('writeAsyncStart');
    const logic = options.extractionLogic;
    const acts = proxyActivities<ActivitySignatures>({
      startToCloseTimeout: this.opts.writeStartTimeout,
      ...this.totalBound(),
      retry: this.opts.writeRetry,
      summary: this.summary('write_start', text, logic),
    });
    return acts[ACTIVITY_WRITE_START]({
      text,
      extractionLogic: logic,
      diffEngine: options.diffEngine,
      structuredMutations: options.structuredMutations,
    });
  }

  /** Poll a queued write once. The caller owns the overall wait. */
  async writeStatus(writeId: string): Promise<WriteStatusOutput> {
    writeId = requireText(writeId, 'writeStatus: writeId');
    durationMs(this.opts.writeStatusTimeout, 'writeStatus: timeout');
    assertSchedulablePolicy(this.opts.pollRetry, 'writeStatus: pollRetryPolicy');
    return this.pollStatus(writeId, undefined);
  }

  /**
   * `writeStatus` with an optional deadline, for `writeDurable`.
   *
   * `bound` caps the whole poll, retries included. Without it a rate-limited poll
   * can back off far past the caller's `maxWaitMs` and surface as
   * XmemoryRateLimited instead of XmemoryWriteTimeout.
   *
   * `singleAttempt` is for the last look taken *at* the deadline, which nothing
   * else bounds. Letting it retry turned a 10s wait into 35s.
   */
  private async pollStatus(
    writeId: string,
    bound: Duration | undefined,
    singleAttempt = false,
  ): Promise<WriteStatusOutput> {
    const acts = proxyActivities<ActivitySignatures>({
      startToCloseTimeout: this.opts.writeStatusTimeout,
      ...this.pollBound(bound),
      retry: singleAttempt ? { ...this.opts.pollRetry, maximumAttempts: 1 } : this.opts.pollRetry,
      summary: `xmemory write_status: ${writeId}`,
    });
    return acts[ACTIVITY_WRITE_STATUS]({ writeId });
  }

  /**
   * Enqueue a write and poll it to completion, durably. The poll loop lives in
   * workflow history, so a slow extraction survives worker restarts — the whole
   * reason to put Temporal in front of xmemory. The enqueue is the only
   * non-idempotent step; the extraction is observed through idempotent polls.
   *
   * `text` is required, with no default: a default made it optional in the emitted
   * declaration, and `writeDurable()` then enqueued an empty deep write. A caller
   * sending only `structuredMutations` passes `''` and means it.
   */
  async writeDurable(text: string, options: WriteDurableOptions = {}): Promise<WriteStatusOutput> {
    text = requireText(text, 'writeDurable: text');
    options = requireOptions(options, 'writeDurable');
    // Everything below is validated before the enqueue: a rejected option must not
    // leave a queued write behind that nobody is waiting on.
    let delayMs = options.pollIntervalMs ?? 2_000;
    const capMs = options.maxPollIntervalMs ?? 30_000;
    const maxWaitMs = options.maxWaitMs ?? 15 * 60_000;
    const statusBudgetMs = durationMs(this.opts.writeStatusTimeout, 'writeDurable: writeStatusTimeout');
    for (const [name, value] of [
      ['maxWaitMs', maxWaitMs],
      ['pollIntervalMs', delayMs],
      ['maxPollIntervalMs', capMs],
    ] as const) {
      // Below a millisecond Temporal truncates to zero, which hot-polls against its
      // timer floor; the upper bound is what setTimeout and the service can represent.
      if (!Number.isFinite(value) || value < 1 || value > MAX_DURATION_MS) {
        throw applicationFailure({
          message: `writeDurable: ${name} must be between 1 and ${MAX_DURATION_MS}, got ${value}`,
          type: TYPE_BAD_OPTIONS,
          nonRetryable: true,
        });
      }
    }
    if (capMs < delayMs) {
      throw applicationFailure({
        message: `writeDurable: maxPollIntervalMs (${capMs}) must not be below pollIntervalMs (${delayMs})`,
        type: TYPE_BAD_OPTIONS,
        nonRetryable: true,
      });
    }

    assertSchedulablePolicy(this.opts.pollRetry, 'writeDurable: pollRetryPolicy');

    const start = await this.writeAsyncStart(text, {
      extractionLogic: options.extractionLogic ?? 'deep',
      structuredMutations: options.structuredMutations,
      diffEngine: options.diffEngine,
    });
    // `Date.now()` is workflow time inside a Temporal workflow, so this is
    // replay-safe.
    const deadline = Date.now() + maxWaitMs;

    let warnedHistory = false;
    let lastStatus: string | undefined;
    const maxWaitElapsed = (): ApplicationFailure =>
      applicationFailure({
        message: `xmemory write ${start.writeId} did not complete within ${maxWaitMs}ms`,
        type: TYPE_WRITE_TIMEOUT,
        nonRetryable: true,
        details: [{ writeId: start.writeId, lastStatus: lastStatus ?? 'unknown' }],
      });

    let final = false;
    for (;;) {
      let leftMs = deadline - Date.now();
      if (leftMs <= 0 && !final) throw maxWaitElapsed();
      // Ordinary polls are bounded by the wait. The last observation happens *at*
      // the deadline and gets one ordinary budget: the grace on top of the wait.
      const boundMs = final ? statusBudgetMs : Math.min(leftMs, statusBudgetMs);
      let backoffMs = delayMs;
      let hintMs: number | undefined;
      try {
        const status = await this.pollStatus(start.writeId, boundMs, final);
        lastStatus = status.writeStatus;
        const terminal = interpretStatus(status);
        if (terminal) return terminal;
      } catch (err) {
        // Cancellation must reach the caller as cancellation, not as a timeout
        // verdict we invented.
        if (isCancellation(err)) throw err;
        if (!(err instanceof ActivityFailure)) throw err;
        const cause = err.cause;
        if (cause instanceof CommonApplicationFailure && cause.nonRetryable) throw err;
        // A `nonRetryableErrorTypes` match is reported here, not on the cause,
        // whose own `nonRetryable` stays false. Polling on would hide the failure
        // behind a timeout verdict of our own.
        if (err.retryState === RetryState.NON_RETRYABLE_FAILURE) throw err;
        // A poll ending is not the wait ending: Temporal stops one when its retries
        // are spent. Honor the server's pacing, which arrives in `details`.
        const detail = (cause instanceof CommonApplicationFailure ? cause.details?.[0] : undefined) as
          | { retryAfterSeconds?: number }
          | undefined;
        if (typeof detail?.retryAfterSeconds === 'number') {
          hintMs = detail.retryAfterSeconds * 1_000;
          backoffMs = Math.max(backoffMs, hintMs);
        }
      }
      // Polling grows history, and this helper cannot call continueAsNew from
      // inside the caller's workflow. Surface Temporal's own signal so they can
      // move the durable write into a child workflow. Checked every iteration, so
      // a loop that is about to stop still reports it.
      if (!warnedHistory && workflowInfo().continueAsNewSuggested) {
        warnedHistory = true;
        log.warn('xmemory writeDurable has polled into a history Temporal suggests continuing-as-new', {
          writeId: start.writeId,
          hint: 'run it in a child workflow, or raise maxPollIntervalMs',
        });
      }
      if (final) throw maxWaitElapsed();
      // Decided on the clock, not the budget allocated before the poll: a fast
      // reply must leave room for the next one.
      leftMs = deadline - Date.now();
      if (backoffMs < leftMs) {
        await sleep(backoffMs);
        delayMs = Math.min(delayMs * 1.5, capMs);
        continue;
      }
      if (hintMs !== undefined && hintMs > leftMs) {
        // The server will not answer before the deadline, so a last look would only
        // arrive early.
        await sleep(Math.max(0, leftMs));
        throw maxWaitElapsed();
      }
      // Our own cadence does not fit. Wait the rest of the wait out, then take one
      // last look: the write may still land inside it.
      final = true;
      await sleep(Math.max(0, leftMs));
    }
  }

  /**
   * Validate the write policy, including that it says what it does about retrying.
   *
   * Writes are at-most-once by default: xmemory assigns primary keys with a model,
   * so a re-extraction can fork a record. Temporal reads an unset `maximumAttempts`
   * as unlimited, so a policy omitting it discards that default silently. Opting in
   * is allowed, it just has to be said.
   */
  private assertWritePolicy(call: string): void {
    assertSchedulablePolicy(this.opts.writeRetry, `${call}: writeRetryPolicy`);
    if (this.opts.writeRetry.maximumAttempts === undefined) {
      throw applicationFailure({
        message:
          `${call}: writeRetryPolicy must set maximumAttempts. Temporal reads an unset value as unlimited ` +
          'retries, and a retried write can fork a record; pass 1 to keep writes at-most-once.',
        type: TYPE_BAD_OPTIONS,
        nonRetryable: true,
      });
    }
  }

  /** The configured total bound, as proxy options. Empty when none is set. */
  private totalBound(): { scheduleToCloseTimeout?: Duration } {
    if (this.opts.totalTimeout === undefined) return {};
    durationMs(this.opts.totalTimeout, 'totalTimeout');
    return { scheduleToCloseTimeout: this.opts.totalTimeout };
  }

  /** The tighter of the loop's own bound and the caller's `totalTimeout`. */
  private pollBound(bound: Duration | undefined): { scheduleToCloseTimeout?: Duration } {
    if (bound === undefined) return this.totalBound();
    if (this.opts.totalTimeout === undefined) return { scheduleToCloseTimeout: bound };
    const totalMs = durationMs(this.opts.totalTimeout, 'totalTimeout');
    return { scheduleToCloseTimeout: Math.min(msToNumber(bound), totalMs) };
  }

  private summary(op: string, content: string, logic?: string): string {
    const label = logic ? `xmemory ${op} (${logic})` : `xmemory ${op}`;
    return this.opts.includeContent ? `${label}: ${content.slice(0, 60)}` : `${label}: ${content.length} chars`;
  }
}

function interpretStatus(status: WriteStatusOutput): WriteStatusOutput | null {
  const value = status.writeStatus;
  if (value === STATUS_COMPLETED) return status;
  if (value === STATUS_FAILED) {
    // The server's detail never leaves the worker: Temporal persists both Activity
    // results and failure details to cleartext history.
    throw applicationFailure({
      message: `xmemory write ${status.writeId} failed`,
      type: TYPE_WRITE_FAILED,
      nonRetryable: true,
      details: [{ writeId: status.writeId, writeStatus: status.writeStatus }],
    });
  }
  if (value === STATUS_NOT_FOUND) {
    // `writeAsync` is transactional, so a returned id is always queryable. A
    // not_found here means the write is genuinely gone.
    throw applicationFailure({
      message: `xmemory write ${status.writeId} not found`,
      type: TYPE_WRITE_NOT_FOUND,
      nonRetryable: true,
      details: [{ writeId: status.writeId }],
    });
  }
  // In-progress or unrecognized: keep polling. The enum has grown before, and a new
  // state must not fail in-flight writes.
  if (!STATUS_IN_PROGRESS.has(value)) {
    log.warn('xmemory returned an unrecognized write status; continuing to poll', {
      writeId: status.writeId,
      writeStatus: value,
    });
  }
  return null;
}

/**
 * A {@link WorkflowXmemory} handle. Carries only configuration (no per-call
 * mutable state), so making a fresh one per call is correct and cheap.
 */
export function xmemoryForWorkflow(options: WorkflowXmemoryOptions = {}): WorkflowXmemory {
  return new WorkflowXmemory(options ?? {});
}

// Re-exported here so workflow code can branch on a failure's `type` without
// importing the package root, which pulls in the worker and client modules.
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
} from './names';

// Everything this entry point's own signatures name, so workflow code needs no
// second import. Mutations, read modes and scopes are the client's own types; the
// `*Output` shapes are ours, because they are what Temporal persists to history.
export type {
  ObjectMutationBody,
  ReadMode,
  ReadOutput,
  ReadScope,
  RelationEndpoint,
  RelationMutationBody,
  RelationsScope,
  ScopeObject,
  SubAnswer,
  WriteMutation,
  WriteOutput,
  WriteStartOutput,
  WriteStatusOutput,
} from './dto';
