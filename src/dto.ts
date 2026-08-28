/**
 * Activity input and output types.
 *
 * The `*Input` / `*Output` shapes are ours: activity payloads are persisted
 * verbatim into workflow history, so they are a compatibility contract for every
 * workflow that has ever run, and owning them lets a result field be added or
 * renamed upstream without breaking replay. What we send through untransformed —
 * mutations, read modes, scopes — is the client's own type.
 */

// The client's own types, used directly: a mutation is sent to the server
// untransformed, and a result is what the client hands back.
import type {
  AsyncWriteResult,
  ReadMode,
  ReadResult,
  ReadScope,
  WriteMutation,
  WriteResult,
  WriteStatusResult,
} from 'xmemory';

// Re-exported so a caller writing a mutation or a scope needs one import, not two.
export type {
  ObjectMutationBody,
  ReadMode,
  ReadScope,
  RelationEndpoint,
  RelationMutationBody,
  RelationsScope,
  ScopeObject,
  WriteMutation,
} from 'xmemory';

export interface ReadInput {
  query: string;
  readMode?: ReadMode;
  scope?: ReadScope;
  // NOTE: no `readId`. The npm `xmemory` client's ReadOptions has no such field
  // (only `traceId`), so exposing it here would silently drop it and bloat
  // history. Add it back only alongside a client field it maps to.
}

export interface SubAnswer {
  subQuery: string;
  readerResult: unknown;
  error: string | null;
}

export interface ReadOutput {
  readerResult: unknown;
  subAnswers: SubAnswer[];
  traceId: string | null;
}

/** Either free `text` for the extractor, or explicit `structuredMutations`. */
export interface WriteInput {
  text: string;
  extractionLogic?: string;
  diffEngine?: boolean;
  structuredMutations?: readonly WriteMutation[];
}

export interface WriteOutput {
  writeId: string;
  traceId: string | null;
  changes: unknown;
}

export interface WriteStartOutput {
  writeId: string;
}

export interface WriteStatusInput {
  writeId: string;
}

export interface WriteStatusOutput {
  writeId: string;
  writeStatus: string;
  // Deliberately no `errorDetail`. The server's detail is not promised
  // user-safe, and an Activity's return value is persisted to cleartext
  // workflow history; the write_status Activity logs it worker-side instead.
  completedAt: string | null;
  // What the write applied. `null` until the client surfaces it on write_status;
  // kept for symmetry with WriteOutput.changes (an upstream client follow-up).
  changes: unknown;
}

// --- Projections from the client's result shapes ---------------------------
//
// Every field is read as an *own* property and type-checked. A response is JSON, so
// a missing field is answered by `Object.prototype`: against a malformed `{}` these
// once reported a write as `completed`, with an id no server sent.

function ownField(result: unknown, field: string): unknown {
  return typeof result === 'object' && result !== null && Object.hasOwn(result, field)
    ? (result as Record<string, unknown>)[field]
    : undefined;
}

function requiredString(result: unknown, field: string): string {
  const value = ownField(result, field);
  // Non-empty: an empty id passes every type check and names no write.
  if (typeof value !== 'string' || value === '') {
    throw new Error(`xmemory response has no usable ${field}`);
  }
  return value;
}

function optionalString(result: unknown, field: string): string | null {
  const value = ownField(result, field);
  return typeof value === 'string' ? value : null;
}

/** Present as an own property, whatever its type. `reader_result` may legitimately be null. */
function requiredField(result: unknown, field: string): unknown {
  if (typeof result !== 'object' || result === null || !Object.hasOwn(result, field)) {
    throw new Error(`xmemory response has no ${field}`);
  }
  return (result as Record<string, unknown>)[field];
}

export function projectRead(result: ReadResult): ReadOutput {
  // Required, not defaulted: turning a missing answer into `undefined` hands the
  // workflow a confident empty one. The client normalizes `reader_results` to an
  // array, so a non-array here did not come from a client that did.
  const readerResult = requiredField(result, 'reader_result');
  const subAnswers = requiredField(result, 'reader_results');
  if (!Array.isArray(subAnswers)) {
    throw new Error('xmemory response has a malformed reader_results');
  }
  return {
    readerResult,
    subAnswers: subAnswers.map((r) => ({
      subQuery: requiredString(r, 'sub_query'),
      readerResult: requiredField(r, 'reader_result'),
      error: optionalString(r, 'error'),
    })),
    traceId: optionalString(result, 'trace_id'),
  };
}

export function projectWrite(result: WriteResult): WriteOutput {
  return {
    writeId: requiredString(result, 'write_id'),
    traceId: optionalString(result, 'trace_id'),
    changes: ownField(result, 'changes') ?? null,
  };
}

export function projectWriteStart(result: AsyncWriteResult): WriteStartOutput {
  return { writeId: requiredString(result, 'write_id') };
}

export function projectWriteStatus(result: WriteStatusResult): WriteStatusOutput {
  return {
    writeId: requiredString(result, 'write_id'),
    // Required: the durable loop decides a write is done by this value.
    writeStatus: requiredString(result, 'write_status'),
    completedAt: optionalString(result, 'completed_at'),
    // Not surfaced by the client yet; populates itself if a later release adds it.
    // `null`, never absent: an omitted field vanishes in JSON.
    changes: ownField(result, 'changes') ?? ownField(result, 'result') ?? null,
  };
}
