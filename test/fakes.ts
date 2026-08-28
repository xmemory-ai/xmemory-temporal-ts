/**
 * A recording, scriptable stand-in for the xmemory `InstanceHandle`.
 *
 * Because the plugin injects the instance, the suite runs with no backend and
 * no network. This fake is the side-effect ledger the replay test asserts
 * against, so it stays dead simple: every call is recorded.
 */

import { XmemoryAPIError } from 'xmemory';
import type { XmemoryInstance } from '../src/activities';

export interface CallRecord {
  method: string;
  textOrQuery: string;
  /** Options the activity passed through, e.g. the derived `timeoutMs`. */
  options?: Record<string, unknown>;
}

export class FakeXmemoryInstance implements XmemoryInstance {
  readonly calls: CallRecord[] = [];
  private writeCounter = 0;
  private failWrites = 0;
  private failError: unknown;
  private statusValues: string[] = ['completed'];
  private statusIndex = 0;
  private statusErrorDetail: string | null = null;
  private stallRead = false;
  private malformedStatus = false;
  private statusWriteId: string | undefined;
  private withoutErrorDetail = false;
  private emptyEnqueueId = false;
  private failStatusError: unknown;
  private failStatusCount: number | undefined;

  constructor(private readonly readAnswer: unknown = 'the answer') {}

  /** Every `writeStatus` rejects, as a rate-limited backend would. */
  failStatusAlways(error: unknown): void {
    this.failStatusError = error;
  }

  /** The first `n` polls reject, then the scripted sequence resumes. */
  failStatusTimes(n: number, error: unknown): void {
    this.failStatusCount = n;
    this.failStatusError = error;
  }

  failWriteTimes(n: number, error: unknown): void {
    this.failWrites = n;
    this.failError = error;
  }

  /** Answer an enqueue with an empty write id, as a malformed response would. */
  enqueueWithoutWriteId(): void {
    this.emptyEnqueueId = true;
  }

  /** Answer without an `error_detail` field at all, as a lean server would. */
  omitErrorDetail(): void {
    this.withoutErrorDetail = true;
  }

  /** Answer every status poll with a different write's id. */
  answerWithWriteId(writeId: string): void {
    this.statusWriteId = writeId;
  }

  /** Answer with a response carrying none of the documented fields. */
  returnMalformedStatus(): void {
    this.malformedStatus = true;
  }

  /** Never resolve a read, as a stalled response body would not. */
  stallReads(): void {
    this.stallRead = true;
  }

  statusSequence(values: string[], errorDetail: string | null = null): void {
    this.statusValues = values;
    this.statusIndex = 0;
    this.statusErrorDetail = errorDetail;
  }

  count(method: string): number {
    return this.calls.filter((c) => c.method === method).length;
  }

  // --- InstanceHandle surface (only the methods we call) -------------------

  async read(query: string, options?: Record<string, unknown>): Promise<never> {
    this.calls.push({ method: 'read', textOrQuery: query, options });
    // A stalled body: the client's own timeout has already been cleared by the
    // time this would be reading it, so nothing here ever settles.
    if (this.stallRead) return new Promise<never>(() => {});
    return { trace_id: 'trace-read', reader_result: this.readAnswer, reader_results: [] } as never;
  }

  async write(input: unknown, options?: Record<string, unknown>): Promise<never> {
    const isText = typeof input === 'string';
    this.calls.push({
      method: 'write',
      textOrQuery: isText ? input : '',
      options: isText ? options : { ...options, structuredMutations: input },
    });
    this.maybeFail();
    this.writeCounter += 1;
    return { write_id: `w${this.writeCounter}`, trace_id: 'trace-write', changes: null } as never;
  }

  async writeAsync(input: unknown, options?: Record<string, unknown>): Promise<never> {
    const isText = typeof input === 'string';
    this.calls.push({
      method: 'writeAsync',
      textOrQuery: isText ? input : '',
      options: isText ? options : { ...options, structuredMutations: input },
    });
    this.maybeFail();
    this.writeCounter += 1;
    return { write_id: this.emptyEnqueueId ? '' : `w${this.writeCounter}` } as never;
  }

  async writeStatus(writeId: string, options?: Record<string, unknown>): Promise<never> {
    this.calls.push({ method: 'writeStatus', textOrQuery: writeId, options });
    if (this.failStatusError !== undefined) {
      if (this.failStatusCount === undefined) throw this.failStatusError;
      if (this.failStatusCount > 0) {
        this.failStatusCount -= 1;
        throw this.failStatusError;
      }
    }
    // A response with no fields of its own — a proxy error page, a truncated body.
    if (this.malformedStatus) return {} as never;
    const value = this.statusValues[Math.min(this.statusIndex, this.statusValues.length - 1)];
    this.statusIndex += 1;
    const errorDetail = value === 'failed' ? this.statusErrorDetail : null;
    return {
      write_id: this.statusWriteId ?? writeId,
      write_status: value,
      // Omitted entirely when asked: an own `null` would shadow anything inherited,
      // so a response that simply lacks the field is the case worth covering.
      ...(this.withoutErrorDetail ? {} : { error_detail: errorDetail }),
      completed_at: null,
    } as never;
  }

  private maybeFail(): void {
    if (this.failWrites > 0) {
      this.failWrites -= 1;
      throw this.failError;
    }
  }
}

export function apiError(opts: {
  status?: number;
  code?: string;
  details?: Record<string, unknown> | null;
  retryAfter?: number;
}): XmemoryAPIError {
  return new XmemoryAPIError('boom', opts.status, opts.code, opts.details, opts.retryAfter);
}
