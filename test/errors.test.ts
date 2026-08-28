import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ApplicationFailure, defaultFailureConverter, defaultPayloadConverter } from '@temporalio/common';
import * as errors from '../src/errors';
import { DEFAULT_ENDPOINT, resolveApiKey, resolveEndpoint, resolveUrl } from '../src/config';
import { XmemoryPlugin } from '../src/plugin';
import { XmemoryClient } from 'xmemory';
import { MockActivityEnvironment } from '@temporalio/testing';
import { toApplicationFailure } from '../src/errors';
import { apiError, FakeXmemoryInstance } from './fakes';

interface Case {
  label: string;
  args: Parameters<typeof apiError>[0];
  type: string;
  nonRetryable: boolean;
}

const CASES: Case[] = [
  { label: '500', args: { status: 500 }, type: errors.TYPE_SERVER_ERROR, nonRetryable: false },
  { label: '408', args: { status: 408 }, type: errors.TYPE_SERVER_ERROR, nonRetryable: false },
  { label: 'transport', args: { status: undefined }, type: errors.TYPE_UNAVAILABLE, nonRetryable: false },
  { label: 'internal', args: { status: 500, code: 'INTERNAL_ERROR' }, type: errors.TYPE_SERVER_ERROR, nonRetryable: false },
  { label: 'rate', args: { status: 429, code: 'RATE_LIMITED' }, type: errors.TYPE_RATE_LIMITED, nonRetryable: false },
  {
    label: 'quota-daily',
    args: { status: 402, code: 'QUOTA_EXCEEDED', details: { kind: 'daily_quota_exceeded' } },
    type: errors.TYPE_DAILY_QUOTA_EXCEEDED,
    nonRetryable: false,
  },
  {
    label: 'quota-monthly',
    args: { status: 402, code: 'QUOTA_EXCEEDED', details: { kind: 'monthly_quota_exceeded' } },
    type: errors.TYPE_MONTHLY_QUOTA_EXCEEDED,
    nonRetryable: true,
  },
  { label: 'quota-no-kind', args: { status: 402, code: 'QUOTA_EXCEEDED' }, type: errors.TYPE_QUOTA_EXCEEDED, nonRetryable: true },
  { label: 'unauthorized', args: { status: 401, code: 'UNAUTHORIZED' }, type: errors.TYPE_AUTH_FAILED, nonRetryable: true },
  { label: 'not-found', args: { status: 404, code: 'NOT_FOUND' }, type: errors.TYPE_NOT_FOUND, nonRetryable: true },
  { label: 'validation', args: { status: 422, code: 'VALIDATION_ERROR' }, type: errors.TYPE_BAD_REQUEST, nonRetryable: true },
  {
    label: 'schema',
    args: { status: 409, code: 'destructive_confirmation_required' },
    type: errors.TYPE_SCHEMA_REJECTED,
    nonRetryable: true,
  },
  // An unfamiliar code keeps its own type, but retryability comes from the status:
  // 4xx is terminal whatever the code says, 5xx and no-status are not.
  { label: 'unknown-4xx', args: { status: 418, code: 'SOMETHING_NEW' }, type: errors.TYPE_UNKNOWN, nonRetryable: true },
  { label: 'unknown-5xx', args: { status: 503, code: 'SOMETHING_NEW' }, type: errors.TYPE_UNKNOWN, nonRetryable: false },
  { label: 'unknown-no-status', args: { code: 'SOMETHING_NEW' }, type: errors.TYPE_UNKNOWN, nonRetryable: false },
];

for (const c of CASES) {
  test(`error mapping: ${c.label}`, () => {
    const f = toApplicationFailure(apiError(c.args));
    assert.equal(f.type, c.type, c.label);
    assert.equal(f.nonRetryable ?? false, c.nonRetryable, c.label);
  });
}

test('an unfamiliar code does not throw, and is retried while the status allows', () => {
  // The point of the unknown branch: a newer server must not break this client.
  const f = toApplicationFailure(apiError({ status: 503, code: 'BRAND_NEW' }));
  assert.equal(f.type, errors.TYPE_UNKNOWN);
  assert.equal(f.nonRetryable ?? false, false);
});

test('retryAfter becomes nextRetryDelay', () => {
  // Milliseconds, as a number: see the pacing test below for why not a `<n>s` string.
  const f = toApplicationFailure(apiError({ status: 429, code: 'RATE_LIMITED', retryAfter: 7 }));
  assert.equal(f.nextRetryDelay, 7_000);
});

test('retry_after_seconds from details is honored', () => {
  const f = toApplicationFailure(
    apiError({ status: 402, code: 'QUOTA_EXCEEDED', details: { kind: 'daily_quota_exceeded', retry_after_seconds: 30 } }),
  );
  assert.equal(f.type, errors.TYPE_DAILY_QUOTA_EXCEEDED);
  assert.equal(f.nextRetryDelay, 30_000);
});

test('non-retryable never carries a delay', () => {
  const f = toApplicationFailure(
    apiError({ status: 402, code: 'QUOTA_EXCEEDED', details: { kind: 'monthly_quota_exceeded', retry_after_seconds: 30 } }),
  );
  assert.equal(f.nonRetryable, true);
  assert.equal(f.nextRetryDelay ?? null, null);
});

test('non-transport errors map to a retryable failure', () => {
  const f = toApplicationFailure(new Error('socket hang up'));
  assert.ok(f instanceof ApplicationFailure);
  assert.equal(f.type, errors.TYPE_UNAVAILABLE);
  assert.equal(f.nonRetryable ?? false, false);
});

test('type string literals are pinned', () => {
  // Public RetryPolicy contract — a rename must fail here, not silently pass.
  assert.equal(errors.TYPE_UNAVAILABLE, 'XmemoryUnavailable');
  assert.equal(errors.TYPE_SERVER_ERROR, 'XmemoryServerError');
  assert.equal(errors.TYPE_RATE_LIMITED, 'XmemoryRateLimited');
  assert.equal(errors.TYPE_DAILY_QUOTA_EXCEEDED, 'XmemoryDailyQuotaExceeded');
  assert.equal(errors.TYPE_MONTHLY_QUOTA_EXCEEDED, 'XmemoryMonthlyQuotaExceeded');
  assert.equal(errors.TYPE_QUOTA_EXCEEDED, 'XmemoryQuotaExceeded');
  assert.equal(errors.TYPE_AUTH_FAILED, 'XmemoryAuthFailed');
  assert.equal(errors.TYPE_NOT_FOUND, 'XmemoryNotFound');
  assert.equal(errors.TYPE_BAD_REQUEST, 'XmemoryBadRequest');
  assert.equal(errors.TYPE_SCHEMA_REJECTED, 'XmemorySchemaRejected');
  assert.equal(errors.TYPE_WRITE_FAILED, 'XmemoryWriteFailed');
  assert.equal(errors.TYPE_WRITE_NOT_FOUND, 'XmemoryWriteNotFound');
  assert.equal(errors.TYPE_WRITE_TIMEOUT, 'XmemoryWriteTimeout');
  assert.equal(errors.TYPE_NOT_BOUND, 'XmemoryNotBound');
  assert.equal(errors.TYPE_NO_DEADLINE, 'XmemoryNoDeadline');
  assert.equal(errors.TYPE_BAD_OPTIONS, 'XmemoryBadOptions');
  assert.equal(errors.TYPE_UNKNOWN, 'XmemoryUnknown');
});

test('MAX_RETRIES_EXCEEDED is non-retryable', () => {
  const f = toApplicationFailure(apiError({ status: 500, code: 'MAX_RETRIES_EXCEEDED' }));
  assert.equal(f.type, errors.TYPE_WRITE_FAILED);
  assert.equal(f.nonRetryable, true);
});

test('transport string is not leaked into history', () => {
  // The raw transport message (internal hostnames/ports) must not reach the
  // failure message Temporal persists to cleartext history.
  const { XmemoryAPIError } = require('xmemory');
  const leaky = new XmemoryAPIError(
    "Connection error: HTTPSConnectionPool(host='internal-db.local', port=5432)",
    undefined,
    undefined,
  );
  const f = toApplicationFailure(leaky);
  assert.equal(f.type, errors.TYPE_UNAVAILABLE);
  assert.doesNotMatch(String(f.message), /internal-db\.local/);
  assert.doesNotMatch(String(f.message), /5432/);
  // Parity with the Python serialized-chain check: the leak must not survive in
  // the details payload or a chained cause either (both persist to history).
  assert.doesNotMatch(JSON.stringify(f.details ?? []), /internal-db\.local|5432/);
  assert.equal(f.cause, undefined);
});

test('a fetch network error is retryable, not a bad request', () => {
  // undici rejects DNS failures / refused connections with a TypeError carrying
  // `cause`. Treating that as a client-side bug would make a transient blip a
  // permanent failure, overriding the read retry policy.
  const networkErr = new TypeError('fetch failed', { cause: new Error('ECONNREFUSED') });
  const f = toApplicationFailure(networkErr);
  assert.equal(f.type, errors.TYPE_UNAVAILABLE);
  assert.equal(f.nonRetryable, false);
});

test('an inherited cause does not make a programming error retryable', () => {
  // `cause` is how a fetch network failure is told apart from a bug in this code.
  // Reading it through the prototype chain turned a deterministic TypeError into a
  // transient blip, and Temporal repeated it.
  const proto = Object.prototype as Record<string, unknown>;
  try {
    proto.cause = 'inherited';
    const f = toApplicationFailure(new TypeError('Cannot read properties of undefined'));
    assert.equal(f.type, errors.TYPE_BAD_REQUEST);
    assert.equal(f.nonRetryable, true);
    // A real network failure carries its own cause and stays retryable.
    const network = toApplicationFailure(new TypeError('fetch failed', { cause: new Error('ECONNREFUSED') }));
    assert.equal(network.type, errors.TYPE_UNAVAILABLE);
  } finally {
    delete proto.cause;
  }
});

test('a programming TypeError stays non-retryable', () => {
  // No `cause`, so it is our bug, not the network: retrying replays it.
  const f = toApplicationFailure(new TypeError("Cannot read properties of undefined"));
  assert.equal(f.type, errors.TYPE_BAD_REQUEST);
  assert.equal(f.nonRetryable, true);
});

test('a server retry hint paces the next attempt, uncapped', () => {
  // Milliseconds as a number, not a `<n>s` string: the string form goes through a
  // duration parser that rejects exponent notation. No cap either — the activity's
  // own policy bounds the retrying, so a long hint is the server's to give.
  const hints: [number, number][] = [
    [90, 90_000],
    [86_400, 86_400_000], // a day
    [2_147_484, 2_147_484_000], // just past the timer cap this used to be judged by
    [2_592_000, 2_592_000_000], // 30 days
  ];
  for (const [seconds, expectedMs] of hints) {
    const f = toApplicationFailure(apiError({ status: 429, code: 'RATE_LIMITED', retryAfter: seconds }));
    assert.equal(f.nextRetryDelay, expectedMs, `hint of ${seconds}s was not preserved`);
    assert.deepEqual((f.details?.[0] as { retryAfterSeconds: number }).retryAfterSeconds, seconds);
  }
});

test('a retry hint Temporal cannot carry is dropped, not passed on', () => {
  // `Infinity` throws in conversion; past MAX_SAFE_INTEGER milliseconds the int64
  // conversion saturates and would report a delay the server never asked for; below
  // a millisecond there is nothing to pace with. Dropping leaves Temporal's backoff.
  // 315_576_000_000s is 10,000 years: a safe integer in milliseconds that converts
  // cleanly, but the next-attempt *timestamp* it implies is past protobuf's
  // Timestamp ceiling, and a real server rejects the failure carrying it.
  for (const hint of [Number.POSITIVE_INFINITY, 1e30, 1e-7, 315_576_000_000]) {
    const f = toApplicationFailure(apiError({ status: 429, code: 'RATE_LIMITED', retryAfter: hint }));
    assert.equal(f.nextRetryDelay, undefined, `hint ${hint} reached Temporal`);
    assert.equal(f.type, errors.TYPE_RATE_LIMITED, 'the verdict itself is unaffected');
  }
});

test('every hint the mapper emits actually converts', () => {
  // The bound is asserted against the real converter rather than restated, so a
  // value that survives the check but breaks serialization fails here.
  for (const hint of [90, 2_592_000, Number.POSITIVE_INFINITY, 1e30, 1e-7, 0.5]) {
    const f = toApplicationFailure(apiError({ status: 429, code: 'RATE_LIMITED', retryAfter: hint }));
    if (f.nextRetryDelay === undefined) continue;
    assert.doesNotThrow(
      () => defaultFailureConverter.errorToFailure(f, defaultPayloadConverter),
      `hint ${hint} was emitted as ${String(f.nextRetryDelay)} but does not convert`,
    );
  }
});

test('an unrecognized code does not make a terminal status retryable', () => {
  // Staying retryable on an unfamiliar code is what keeps a newer server from
  // breaking this client mid-deploy — but a 401 or 404 is terminal whatever the
  // code says, and retrying it just burns the activity's attempts.
  for (const [status, retryable] of [
    [400, false],
    [401, false],
    [403, false],
    [404, false],
    [422, false],
    [500, true],
  ] as const) {
    const f = toApplicationFailure(apiError({ status, code: 'FUTURE_CODE' }));
    assert.equal(f.type, errors.TYPE_UNKNOWN, `HTTP ${status} lost the unknown-code signal`);
    assert.equal(f.nonRetryable, !retryable, `HTTP ${status} retryable=${!f.nonRetryable}`);
  }
  // No status at all: the benefit of the doubt still applies.
  const noStatus = toApplicationFailure(apiError({ code: 'FUTURE_CODE' }));
  assert.equal(noStatus.nonRetryable ?? false, false);
});

test('an API error from the other module build is still classified correctly', () => {
  // xmemory ships a CommonJS and an ESM build. A handle built from the ESM half
  // throws a different class object than this module imported, so `instanceof`
  // alone read an auth failure as a retryable transport error and Temporal retried
  // a 401. Verified against a real dual load; pinned here by name.
  class ForeignApiError extends Error {
    constructor(
      readonly status: number,
      readonly code: string,
      readonly details: Record<string, unknown> | null = null,
      readonly retryAfter?: number,
    ) {
      super('boom');
      this.name = 'XmemoryAPIError';
    }
  }
  const f = toApplicationFailure(new ForeignApiError(401, 'UNAUTHORIZED'));
  assert.equal(f.type, errors.TYPE_AUTH_FAILED);
  assert.equal(f.nonRetryable, true);
  // An ordinary Error is still a transport failure, not an API verdict.
  assert.equal(toApplicationFailure(new Error('boom')).type, errors.TYPE_UNAVAILABLE);
});

test('a configured url must be https, or loopback for local development', () => {
  // The API key travels as a bearer token, so the endpoint decides who receives
  // it: an empty string must not fall through to the production default, and
  // plaintext must not carry the credential off-box.
  assert.equal(resolveUrl({ instanceId: 'i' }), undefined);
  assert.equal(resolveUrl({ instanceId: 'i', url: 'https://api.example.com' }), 'https://api.example.com');
  assert.equal(resolveUrl({ instanceId: 'i', url: 'http://localhost:8080' }), 'http://localhost:8080');
  assert.equal(resolveUrl({ instanceId: 'i', url: 'http://127.0.0.1:8080' }), 'http://127.0.0.1:8080');

  for (const bad of ['', '   ', 'not-a-url', 'http://api.example.com']) {
    assert.throws(() => resolveUrl({ instanceId: 'i', url: bad }), /xmemory url/, `accepted ${JSON.stringify(bad)}`);
  }
});

test('the endpoint from the environment is validated too', () => {
  // Validating only an explicit `url` left the client free to fall back to
  // XMEM_API_URL, which then carried the bearer token to whatever it named.
  const saved = process.env.XMEM_API_URL;
  try {
    process.env.XMEM_API_URL = 'http://attacker.invalid';
    assert.throws(() => resolveUrl({ instanceId: 'i' }), /XMEM_API_URL must use https/);

    process.env.XMEM_API_URL = 'https://api.example.com';
    assert.equal(resolveUrl({ instanceId: 'i' }), 'https://api.example.com');

    // An explicit url still wins, and is still checked.
    assert.equal(resolveUrl({ instanceId: 'i', url: 'https://other.example' }), 'https://other.example');

    delete process.env.XMEM_API_URL;
    assert.equal(resolveUrl({ instanceId: 'i' }), undefined);
  } finally {
    if (saved === undefined) delete process.env.XMEM_API_URL;
    else process.env.XMEM_API_URL = saved;
  }
});

test('inherited configuration cannot redirect the credential', () => {
  // `process.env` is an ordinary object whose prototype is Object.prototype, and a
  // plain lookup sees the chain. Prototype pollution anywhere in the Worker could
  // therefore supply an endpoint this config never set, and the API key would be
  // sent there. Own properties only.
  const savedUrl = process.env.XMEM_API_URL;
  try {
    delete process.env.XMEM_API_URL;
    (Object.prototype as Record<string, unknown>).XMEM_API_URL = 'https://attacker.invalid';
    assert.equal(resolveUrl({ instanceId: 'i' }), undefined, 'an inherited endpoint was accepted');
    // And the client is handed that decision explicitly. Left to itself it reads
    // the same polluted variable and only then falls back, so `undefined` here is
    // exactly what reopened the hole.
    assert.equal(resolveEndpoint({ instanceId: 'i' }), DEFAULT_ENDPOINT);
    const client = new XmemoryClient({ apiKey: 'k', url: resolveEndpoint({ instanceId: 'i' }) });
    const baseUrl = (client as unknown as { _baseUrl: string })._baseUrl;
    assert.equal(baseUrl, DEFAULT_ENDPOINT, `the client still resolved ${baseUrl}`);
  } finally {
    delete (Object.prototype as Record<string, unknown>).XMEM_API_URL;
    if (savedUrl !== undefined) process.env.XMEM_API_URL = savedUrl;
  }

  // The same for a config object whose `url` comes from its prototype.
  const inherited = Object.create({ url: 'https://attacker.invalid' }) as { instanceId: string };
  inherited.instanceId = 'i';
  assert.equal(resolveUrl(inherited), undefined, 'an inherited config.url was accepted');

  // And an env-var *name* that names something on the prototype resolves a
  // function, which the client would send as a bearer token.
  for (const name of ['toString', 'constructor', '__proto__']) {
    assert.throws(
      () => resolveApiKey({ instanceId: 'i', apiKeyEnv: name }),
      /unset or empty|non-empty string/,
      `apiKeyEnv ${name} resolved something`,
    );
  }

  // An explicit null is a mistake, not an omission: defaulting it read the ambient
  // key and sent it to whatever endpoint this config names.
  const savedKey = process.env.XMEM_API_KEY;
  try {
    process.env.XMEM_API_KEY = 'AMBIENT_PRODUCTION_KEY';
    assert.throws(
      () => resolveApiKey({ instanceId: 'i', apiKeyEnv: null as never }),
      /non-empty string/,
      'apiKeyEnv null fell through to the default',
    );
  } finally {
    if (savedKey === undefined) delete process.env.XMEM_API_KEY;
    else process.env.XMEM_API_KEY = savedKey;
  }
});

test('inherited configuration cannot switch on detail logging', async () => {
  // A plain `{ ...config }` copy still answers a *missing* field from
  // Object.prototype, so pollution reached the opt-in log toggle. Asserted through
  // the plugin's own activities, not a hand-built snapshot: the point is what the
  // plugin does with the config it was given.
  const proto = Object.prototype as Record<string, unknown>;
  const logged: Record<string, unknown>[] = [];
  const logger = {
    trace: () => {},
    debug: () => {},
    info: () => {},
    warn: (_m: string, attrs?: Record<string, unknown>) => void logged.push(attrs ?? {}),
    error: () => {},
    log: () => {},
  };
  try {
    proto.logServerErrorDetail = true;
    const fake = new FakeXmemoryInstance();
    fake.statusSequence(['failed'], 'SECRET memory text');
    const plugin = new XmemoryPlugin({ instanceId: 'inst-1' }, { instance: fake });
    const configured = plugin.configureWorker({ taskQueue: 'tq' } as never);
    const activities = configured.activities as Record<string, (input: unknown) => Promise<unknown>>;
    await plugin.runWorker({} as never, async () => {
      const env = new MockActivityEnvironment({ scheduledTimestampMs: Date.now() } as never, { logger } as never);
      await env.run(activities.xmemory_write_status as never, { writeId: 'w1' } as never);
    });
    const dumped = JSON.stringify(logged);
    assert.ok(!dumped.includes('SECRET'), `an inherited flag switched on detail logging: ${dumped}`);
    assert.ok(dumped.includes('errorDetailLength'), `the withheld-detail note is missing: ${dumped}`);
  } finally {
    delete proto.logServerErrorDetail;
  }
});

test('a class getter is read with the instance as its receiver', () => {
  // A getter defined on a class prototype has to run against the instance. Reading
  // it off the prototype gave `undefined` — so a `sampleRate` of 0, meaning capture
  // nothing, was dropped and sampling defaulted to capturing everything.
  class Config {
    readonly rate: number = 0;
    project(): string {
      return 'x';
    }
    get sampleRate(): number {
      return this.rate;
    }
  }
  // A rate of 0 is valid and means "never"; an out-of-range one is refused. If the
  // getter were read off the prototype it would return undefined, and neither the
  // acceptance below nor the rejection after it would say anything.
  assert.doesNotThrow(
    () => new XmemoryPlugin({ instanceId: 'i' }, { instance: new FakeXmemoryInstance(), autoCapture: new Config() as never }),
  );
  // The getter reads `this.rate`, so it only returns 5 when it runs against the
  // instance. Read off the prototype it yields undefined, the validation is skipped,
  // and nothing throws — which is exactly the defect.
  class OutOfRange extends Config {
    override readonly rate = 5;
  }
  assert.throws(
    () => new XmemoryPlugin({ instanceId: 'i' }, { autoCapture: new OutOfRange() as never }),
    /sampleRate/,
    'the class getter was not read at all',
  );
});

test('an inherited projector cannot switch on capture', () => {
  // Preserving a class's `project` must not also accept one from Object.prototype:
  // an otherwise empty block would then install capture the caller never asked for.
  const proto = Object.prototype as Record<string, unknown>;
  try {
    proto.project = () => 'captured';
    assert.throws(
      () => new XmemoryPlugin({ instanceId: 'i' }, { autoCapture: {} as never }),
      /autoCapture.project/,
      'an inherited projector enabled capture',
    );
  } finally {
    delete proto.project;
  }
});

test('auto-capture keeps a class projector and refuses an unusable one', () => {
  // `project` is often a method on a class instance's prototype, which a spread
  // drops — leaving an interceptor with nothing to call. And `{ ...null }` is an
  // empty object, so `autoCapture: null` installed capture that could never work.
  class Projector {
    project(_name: string, result: unknown): string {
      return `remembered ${String(result)}`;
    }
  }
  // That it survives all the way to a capture is asserted in the interceptor
  // suite, where one actually runs; here it only has to be accepted.
  assert.doesNotThrow(
    () =>
      new XmemoryPlugin(
        { instanceId: 'inst-1' },
        { instance: new FakeXmemoryInstance(), autoCapture: new Projector() as never },
      ),
  );

  // null is an omission; anything else that cannot capture is refused at setup.
  assert.doesNotThrow(() => new XmemoryPlugin({ instanceId: 'i' }, { autoCapture: null as never }));
  for (const bad of ['nope', [], {}, { project: 'not a function' }]) {
    assert.throws(
      () => new XmemoryPlugin({ instanceId: 'i' }, { autoCapture: bad as never }),
      /autoCapture/,
      `accepted ${JSON.stringify(bad)}`,
    );
  }
});

test('an inherited sample rate does not decide how much is captured', () => {
  // `autoCapture` is nested, and its fields are read one by one at capture time.
  const proto = Object.prototype as Record<string, unknown>;
  try {
    proto.sampleRate = 5; // out of range: construction would reject a real one
    assert.doesNotThrow(
      () =>
        new XmemoryPlugin(
          { instanceId: 'inst-1' },
          { instance: new FakeXmemoryInstance(), autoCapture: { project: () => 'x' } },
        ),
      'an inherited sampleRate reached validation',
    );
  } finally {
    delete proto.sampleRate;
  }
});

test('an unusable header hint does not mask a usable structured one', () => {
  // Preferring the header and validating afterwards discarded a perfectly good
  // structured hint whenever the header was unusable.
  const f = toApplicationFailure(
    apiError({ status: 429, code: 'RATE_LIMITED', details: { retry_after_seconds: 30 }, retryAfter: Infinity }),
  );
  assert.equal(f.nextRetryDelay, 30_000, 'the structured hint was lost');
});

test('a malformed server code is not carried into logs or history', () => {
  // The code lands in the failure details, which Temporal persists in the clear.
  // The client bounds neither its shape nor its length, so whatever the server
  // sends would be persisted verbatim — the same reason error_detail is withheld.
  const nasty = 'SECRET memory text\nAuthorization: Bearer abc123';
  // The warning is captured too: it is the other place the code outlives the call.
  const warnings: string[] = [];
  const originalWarn = console.warn;
  console.warn = (...args: unknown[]) => void warnings.push(args.map(String).join(' '));
  let f;
  try {
    f = toApplicationFailure(apiError({ status: 503, code: nasty }));
  } finally {
    console.warn = originalWarn;
  }
  const logged = warnings.join('\n');
  assert.ok(!logged.includes('SECRET'), `the code was logged verbatim: ${logged}`);
  assert.ok(!logged.includes('Bearer'), `the code was logged verbatim: ${logged}`);
  const details = JSON.stringify(f.details?.[0]);
  assert.ok(!details.includes('SECRET'), `the code reached the failure details: ${details}`);
  assert.ok(!details.includes('Bearer'), `the code reached the failure details: ${details}`);
  // Identifier-shaped is not the same as safe: a leaked key or a line of memory
  // text can look exactly like a code, so only codes this module branches on are
  // echoed at all.
  for (const shaped of ['xmem_sk_AbCd1234567890', 'PRIVATE_MEMORY_TEXT', 'A'.repeat(500)]) {
    const carried = JSON.stringify(toApplicationFailure(apiError({ status: 503, code: shaped })).details?.[0]);
    assert.ok(!carried.includes(shaped.slice(0, 12)), `${shaped.slice(0, 12)} survived into details`);
    assert.match(carried, /<unrecognized>/, 'no marker was left in place of the code');
  }
  // A normal code still travels, and the verdict is unaffected either way.
  const normal = toApplicationFailure(apiError({ status: 500, code: 'INTERNAL_ERROR' }));
  assert.match(JSON.stringify(normal.details?.[0]), /INTERNAL_ERROR/);
  assert.equal(normal.type, errors.TYPE_SERVER_ERROR);
  assert.equal(f.type, errors.TYPE_UNKNOWN);
});

test('an unusable apiKey override never falls back to the ambient key', () => {
  // The client reads a falsy key as absent and takes XMEM_API_KEY instead — so
  // passing a null through would send the ambient production credential to whatever
  // URL this config names.
  const saved = process.env.XMEM_API_KEY;
  try {
    process.env.XMEM_API_KEY = 'AMBIENT_PRODUCTION_KEY';
    for (const bad of [null, '', 0, {}]) {
      assert.throws(
        () => resolveApiKey({ instanceId: 'i' }, bad as never),
        /unusable/,
        `accepted ${JSON.stringify(bad)}`,
      );
    }
    // A real key still wins over the environment.
    assert.equal(resolveApiKey({ instanceId: 'i' }, 'explicit'), 'explicit');
  } finally {
    if (saved === undefined) delete process.env.XMEM_API_KEY;
    else process.env.XMEM_API_KEY = saved;
  }
});

test('an endpoint may carry a path prefix or a query string', () => {
  // Neither is this package's business: the client composes request URLs from the
  // base, keeping a gateway's path prefix and joining its query. Rejecting them was
  // a workaround for a client that concatenated instead, fixed in xmemory 3.8.3.
  for (const url of [
    'https://api.example.com',
    'https://gw.example.com/xmemory',
    'https://api.example.com?tenant=acme',
    'https://api.example.com#frag',
  ]) {
    assert.equal(resolveUrl({ instanceId: 'i', url }), url, `rejected ${url}`);
  }
});

test('a configured endpoint is returned normalized', () => {
  // `new URL()` ignores surrounding whitespace, but the client concatenates this
  // string with the request path, so the spaces would survive into the request.
  assert.equal(resolveUrl({ instanceId: 'i', url: '  https://api.example.com  ' }), 'https://api.example.com');
  // A path prefix is the caller's business and is left intact.
  assert.equal(resolveUrl({ instanceId: 'i', url: 'https://api.example.com/api' }), 'https://api.example.com/api');
});

test('an endpoint may not use an unsupported scheme or embed credentials', () => {
  // Loopback relaxes https, not the scheme list. Credentials in the URL are refused
  // outright: Node rejects them when it builds the request, so accepting them here
  // would only defer the failure to the first call.
  for (const bad of ['ftp://localhost/x', 'file:///tmp/x', 'https://user:pw@api.example.com']) {
    assert.throws(() => resolveUrl({ instanceId: 'i', url: bad }), /xmemory url/, `accepted ${bad}`);
  }
});

test('a plugin instance does not print its configuration', () => {
  // A plugin ends up in logs and error dumps. With the config as a public
  // property, JSON.stringify printed the configured endpoint straight out.
  const plugin = new XmemoryPlugin({ instanceId: 'inst-1', url: 'https://secret-host.example' });
  const dumped = JSON.stringify(plugin);
  assert.ok(!dumped.includes('secret-host'), `configuration leaked into ${dumped}`);
  assert.ok(!dumped.includes('inst-1'), `configuration leaked into ${dumped}`);
  // Nor the holder, nor an injected instance: a TypeScript `private` field is an
  // ordinary enumerable own property at runtime, so anything held that way is
  // printed too — including whatever credentials an injected client carries.
  const withInstance = new XmemoryPlugin(
    { instanceId: 'inst-2' },
    { instance: Object.assign(new FakeXmemoryInstance(), { apiKey: 'xmem_SECRET' }) },
  );
  const dumpedWithInstance = JSON.stringify(withInstance);
  assert.ok(!dumpedWithInstance.includes('xmem_SECRET'), `instance leaked into ${dumpedWithInstance}`);
  assert.equal(dumpedWithInstance, '{"name":"xmemory"}', `unexpected serialization: ${dumpedWithInstance}`);
});

test('a rejected endpoint reveals neither the value nor the host', () => {
  // The value may itself be a secret, and the host can name internal
  // infrastructure; `source` already says which setting to fix.
  try {
    resolveUrl({ instanceId: 'i', url: 'http://vault.internal.example:8200' });
    assert.fail('expected a rejection');
  } catch (err) {
    assert.ok(!String(err).includes('vault.internal'), `the error named the host: ${String(err)}`);
    assert.match(String(err), /must use https/);
  }
  try {
    resolveUrl({ instanceId: 'i', url: 'not a url with secret=abc' });
    assert.fail('expected a rejection');
  } catch (err) {
    assert.ok(!String(err).includes('secret=abc'), `the error echoed the value: ${String(err)}`);
  }
});
