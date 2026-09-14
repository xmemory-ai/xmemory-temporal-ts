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
import { DEFAULT_TIMEOUTS, clientTimeoutMs } from './defaults';
import { type AutoCaptureConfig, createAutoCaptureInterceptor } from './interceptor';

export const PLUGIN_NAME = 'xmemory';

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
    this.#config = config;
    const { apiKey, ...rest } = options;
    this.#apiKey = apiKey;
    this.#options = rest;
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
