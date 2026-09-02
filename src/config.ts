/**
 * Configuration for the xmemory Temporal plugin.
 *
 * Nothing here carries secret material: {@link XmemoryConfig} holds the *name* of
 * the env var supplying the API key, never the key. Workflow history is stored in
 * the clear, so this stays safe to log and to persist.
 */

export const DEFAULT_API_KEY_ENV = 'XMEM_API_KEY';

/**
 * An *own* property, or `undefined`. `process.env` is an ordinary object, so a
 * plain lookup can return an endpoint or key that was never set.
 */
function own(source: object, key: string): unknown {
  return Object.hasOwn(source, key) ? (source as Record<string, unknown>)[key] : undefined;
}
// The client falls back to this when no url is passed, so it is an endpoint source
// this plugin must validate too.
const URL_ENV = 'XMEM_API_URL';
const LOOPBACK_HOSTS = new Set(['localhost', '127.0.0.1', '[::1]']);

// Re-exported from the workflow-safe leaf, so callers have one import site.
export { DEFAULT_CLIENT_MARGIN_MS, DEFAULT_TIMEOUTS, clientTimeoutMs, type XmemoryTimeouts } from './defaults';

export interface XmemoryConfig {
  instanceId: string;
  url?: string;
  /** Name of the env var holding the key — never the key itself. */
  apiKeyEnv?: string;
  /**
   * How far below its Temporal deadline each call's client timeout sits. The
   * budgets themselves belong to the workflow (`xmemoryForWorkflow`).
   */
  clientMarginMs?: number;
  defaultExtractionLogic?: 'fast' | 'deep';
  /**
   * Log the server's `error_detail` verbatim when a write fails.
   *
   * Off by default: the detail can echo memory text or internal endpoints. Off, the
   * log names the failed write and the detail's size.
   */
  logServerErrorDetail?: boolean;
}

/**
 * Read the API key from the environment.
 *
 * Throws at worker start rather than on the first activity, so a misconfigured
 * worker fails visibly.
 */
export function resolveApiKey(config: XmemoryConfig, override?: string): string {
  // Passing a falsy key on is worse than failing: the client reads it as absent and
  // falls back to XMEM_API_KEY, sending the ambient credential to this config's URL.
  if (override !== undefined) {
    if (typeof override !== 'string' || override === '') {
      throw new Error(
        `xmemory apiKey was supplied but unusable (${override === '' ? 'empty' : typeof override}); ` +
          'omit it to read the environment',
      );
    }
    return override;
  }
  // Defaulted only when genuinely absent: `??` would treat an own `null` as one.
  const configured = own(config, 'apiKeyEnv');
  const varName = configured === undefined ? DEFAULT_API_KEY_ENV : configured;
  if (typeof varName !== 'string' || varName === '') {
    throw new Error(
      `xmemory apiKeyEnv must be a non-empty string, got ${varName === null ? 'null' : typeof varName}`,
    );
  }
  const key = own(process.env, varName);
  if (typeof key !== 'string' || key === '') {
    throw new Error(
      `xmemory API key not found: environment variable ${JSON.stringify(varName)} is unset or empty. ` +
        `Set it on the worker process, or pass { apiKey } to XmemoryPlugin.`,
    );
  }
  return key;
}

/**
 * Where the client points when nothing is configured. Mirrors its unexported
 * `DEFAULT_BASE_URL`: omitting `url` would let the client read
 * `process.env.XMEM_API_URL` itself, reopening what `resolveUrl` closes.
 */
export const DEFAULT_ENDPOINT = 'https://api.xmemory.ai';

/** The endpoint to hand the client: always a value, never left to its own fallback. */
export function resolveEndpoint(config: XmemoryConfig): string {
  return resolveUrl(config) ?? DEFAULT_ENDPOINT;
}

/** Validate the effective endpoint, or `undefined` when none was supplied. */
export function resolveUrl(config: XmemoryConfig): string | undefined {
  const configured = own(config, 'url');
  if (configured !== undefined) return validateEndpoint(configured, 'xmemory url');
  // With `url` unset the client falls back to this variable, which would then reach
  // the wire unchecked. Resolving it here makes this the only path to an endpoint.
  const fromEnv = own(process.env, URL_ENV);
  if (fromEnv === undefined) return undefined;
  return validateEndpoint(fromEnv, `$${URL_ENV}`);
}

/**
 * Return `candidate` if the API key may safely be sent to it.
 *
 * The key is a bearer token, so plaintext is refused off-box and credentials in the
 * URL are refused outright. What the endpoint otherwise looks like — path, query,
 * port — is the client's business, not this plugin's.
 */
export function validateEndpoint(candidate: unknown, source: string): string {
  if (typeof candidate !== 'string') {
    throw new Error(`${source} must be a string, got ${candidate === null ? 'null' : typeof candidate}`);
  }
  // The trimmed value is what is returned: `new URL()` ignores surrounding
  // whitespace, but the client concatenates this string with the request path.
  const endpoint = candidate.trim();
  if (endpoint === '') {
    throw new Error(`${source} was supplied but empty; unset it to use the default endpoint`);
  }
  let parsed: URL;
  try {
    parsed = new URL(endpoint);
  } catch {
    // Not echoed: it may hold a secret.
    throw new Error(`${source} is not a valid URL`);
  }
  if (parsed.username !== '' || parsed.password !== '') {
    // Node rejects these at request time anyway, and the config stops being safe to log.
    throw new Error(`${source} must not embed credentials; the API key is passed separately`);
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    throw new Error(`${source} must use https (got scheme ${JSON.stringify(parsed.protocol)})`);
  }
  if (parsed.protocol === 'http:' && !LOOPBACK_HOSTS.has(parsed.hostname)) {
    // The host is left out: it can name internal infrastructure.
    throw new Error(
      `${source} must use https; the API key is sent as a bearer token. ` +
        'Plaintext http is accepted only for loopback hosts.',
    );
  }
  return endpoint;
}
