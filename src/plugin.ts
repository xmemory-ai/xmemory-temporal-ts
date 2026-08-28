/**
 * `XmemoryPlugin` — the single line a Temporal user adds to their Worker.
 *
 * `configureWorker` injects the activities (and the auto-capture interceptor,
 * if enabled); `runWorker` opens one xmemory client for the Worker's lifetime.
 * Installs no data converter: one would rewrite every payload on the Worker,
 * not just xmemory's.
 *
 * Use one plugin instance per Worker. The bound client lives in a per-plugin
 * holder, so reusing one object across Workers is last-bind-wins.
 */

import { XmemoryClient } from 'xmemory';
import type { Worker, WorkerOptions, WorkerPlugin } from '@temporalio/worker';
import { createActivities, InstanceHolder, type XmemoryInstance } from './activities';
import { resolveApiKey, resolveEndpoint, type XmemoryConfig } from './config';
import { DEFAULT_TIMEOUTS, clientTimeoutMs, ownOnly } from './defaults';
import { type AutoCaptureConfig, createAutoCaptureInterceptor } from './interceptor';

export const PLUGIN_NAME = 'xmemory';

/**
 * A configuration field, from the object or its own class. Stops at
 * `Object.prototype`, where nothing legitimate lives — but a config expressed as a
 * class keeps its methods and getters on a prototype, which a spread would drop.
 */
function configuredField(source: object, field: string): unknown {
  for (let o: object | null = source; o !== null && o !== Object.prototype; o = Object.getPrototypeOf(o) as object) {
    // Found on `o`, read from `source`: a getter must run with the instance as its
    // receiver, or it reads the prototype's absent fields.
    if (Object.hasOwn(o, field)) return (source as Record<string, unknown>)[field];
  }
  return undefined;
}

/**
 * The auto-capture block, sanitized, or nothing when it was not supplied.
 *
 * Its fields are read one by one at capture time, so an inherited `sampleRate`
 * would decide how much is captured. Each supported field is copied explicitly:
 * a spread drops a class's `project` method and `sampleRate` getter alike, and
 * `{ ...null }` is an empty object that would install capture doing nothing.
 */
function normalizeAutoCapture(autoCapture: AutoCaptureConfig | undefined): { autoCapture?: AutoCaptureConfig } {
  if (autoCapture === undefined || autoCapture === null) return {};
  if (typeof autoCapture !== 'object' || Array.isArray(autoCapture)) {
    throw new TypeError(`xmemory autoCapture must be an object, got ${typeof autoCapture}`);
  }
  const project = configuredField(autoCapture, 'project');
  if (typeof project !== 'function') {
    throw new TypeError('xmemory autoCapture.project must be a function; it decides what is worth remembering');
  }
  const snapshot = ownOnly({
    project: (project as AutoCaptureConfig['project']).bind(autoCapture),
  }) as AutoCaptureConfig & Record<string, unknown>;
  for (const field of ['sampleRate', 'extractionLogic', 'captureTimeoutMs'] as const) {
    const value = configuredField(autoCapture, field);
    if (value !== undefined) snapshot[field] = value as never;
  }
  return { autoCapture: snapshot };
}

export interface XmemoryPluginOptions {
  /** Provide the API key in-process instead of via the env var. */
  apiKey?: string;
  /**
   * Inject a pre-built instance handle.
   *
   * @internal Test seam: it is how this package's suite runs with no backend.
   */
  instance?: XmemoryInstance;
  /** Enable auto-capture of activity results into memory. Off by default. */
  autoCapture?: AutoCaptureConfig;
}

export class XmemoryPlugin implements WorkerPlugin {
  readonly name = PLUGIN_NAME;
  // ECMAScript private fields throughout: TypeScript `private` is an ordinary
  // enumerable property at runtime, so JSON.stringify(plugin) printed the API key,
  // the endpoint, and anything an injected instance carried.
  readonly #holder = new InstanceHolder();

  readonly #apiKey: string | undefined;
  readonly #options: Omit<XmemoryPluginOptions, 'apiKey'>;

  readonly #config: XmemoryConfig;

  constructor(config: XmemoryConfig, options: XmemoryPluginOptions = {}) {
    // Null-prototype, so a field this config never set reads as absent: a plain
    // spread still answers a missing one from `Object.prototype`.
    this.#config = ownOnly({ ...config });
    if (typeof this.#config.instanceId !== 'string' || this.#config.instanceId === '') {
      throw new TypeError('xmemory instanceId must be a non-empty string');
    }
    const safeOptions = ownOnly({ ...options });
    const { apiKey, ...rest } = safeOptions;
    this.#apiKey = apiKey;
    this.#options = ownOnly({ ...rest, ...normalizeAutoCapture(rest.autoCapture) });
    // At setup, not on the first Activity, like a missing API key.
    const rate = this.#options.autoCapture?.sampleRate;
    if (rate !== undefined && (!Number.isFinite(rate) || rate < 0 || rate > 1)) {
      throw new RangeError(`xmemory autoCapture.sampleRate must be a fraction between 0 and 1, got ${rate}`);
    }
    // Config comes from files and env vars, where the string "false" is a
    // plausible way to mean off — and a truthy one.
    // Read from the sanitized copy, not the caller's object: validating the
    // original still saw whatever the prototype supplied.
    const detail = this.#config.logServerErrorDetail;
    if (detail !== undefined && typeof detail !== 'boolean') {
      throw new TypeError(`xmemory logServerErrorDetail must be a boolean, got ${typeof detail}`);
    }
  }

  /** The in-process key, if one was supplied. Never serialized with the plugin. */
  private apiKey(): string | undefined {
    return this.#apiKey;
  }

  configureWorker(options: WorkerOptions): WorkerOptions {
    const ourActivities = createActivities(this.#holder, this.#config);
    // Refused, not merged over: replacing an identically-named Activity would
    // change what the caller's workflows execute. `hasOwn`, not `in`, so an
    // Activity named `constructor` is not a false collision.
    const taken = Object.keys(options.activities ?? {}).filter((name) => Object.hasOwn(ourActivities, name));
    if (taken.length > 0) {
      throw new Error(
        `xmemory plugin: the Worker already registers ${taken.join(', ')}. ` +
          'Those names belong to this plugin; rename the conflicting activities.',
      );
    }
    const activities = { ...(options.activities ?? {}), ...ourActivities };

    // Only auto-capture registers an interceptor, and only when enabled.
    if (!this.#options.autoCapture) return { ...options, activities };
    const autoCapture = this.#options.autoCapture;
    const holder = this.#holder;
    const config = this.#config;
    const interceptors = {
      ...options.interceptors,
      activityInbound: [
        () => createAutoCaptureInterceptor(holder, config, autoCapture),
        ...(options.interceptors?.activityInbound ?? []),
      ],
    };

    return { ...options, activities, interceptors };
  }

  async runWorker(worker: Worker, next: (w: Worker) => Promise<void>): Promise<void> {
    this.#holder.bind(this.#options.instance ?? this.buildInstance());
    await next(worker);
  }

  private buildInstance(): XmemoryInstance {
    const client = new XmemoryClient({
      apiKey: resolveApiKey(this.#config, this.apiKey()),
      // Always explicit — see `resolveEndpoint`.
      url: resolveEndpoint(this.#config),
      // A fallback only: every activity overrides this per call.
      timeoutMs: clientTimeoutMs(DEFAULT_TIMEOUTS.readMs, this.#config.clientMarginMs),
    });
    return client.instance(this.#config.instanceId);
  }
}
