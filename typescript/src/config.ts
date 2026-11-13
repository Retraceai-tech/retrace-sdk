export interface Config {
  apiKey: string;
  baseUrl: string;
  wsUrl: string;
  projectId: string | undefined;
  enabled: boolean;
  sampleRate: number;
  /** Optional seed for deterministic sampling. When set, the same trace name always produces the same sample decision. */
  sampleSeed: string | undefined;
  /** Transport mode. "auto" (default) tries WebSocket then falls back to HTTP; "http" is
   *  request/response only (recommended for short-lived scripts and serverless — it never
   *  holds an open socket and always surfaces upload errors); "ws" forces WebSocket. */
  transport: "auto" | "ws" | "http";
  /** How long flush() / the exit-time flush wait for the server to confirm every event (WebSocket
   *  acks) before falling back to HTTP or reporting an incomplete capture, in ms. Default 5000.
   *  Env: RETRACE_FLUSH_TIMEOUT (seconds, as in the Python SDK). */
  flushTimeoutMs: number;
  /** Replay safety. When a deterministic replay hits a cassette MISS (no recorded entry for a
   *  call), the SDK can either throw (fail-closed) or fall through to a REAL provider call (live cost
   *  + non-determinism, driven by a server-supplied cassette). Default TRUE (fail-closed): the SDK
   *  throws on a miss. Set strictReplay:false (or RETRACE_STRICT_REPLAY=false) to opt back into the
   *  legacy warn + fall-through behavior. Env: RETRACE_STRICT_REPLAY. */
  strictReplay: boolean;
  /** C2: honor SERVER-INITIATED resume/replay commands — dashboard-driven cascade replay that
   *  re-executes YOUR registered (resumable) function in THIS process with server-supplied input and
   *  cassette. Default FALSE: the SDK ignores those frames unless you explicitly opt in, because
   *  acting on them means trusting the server (and the transport) to invoke code in your environment
   *  with attacker-influenceable arguments. Recording, and server-side forking, are unaffected.
   *  Env: RETRACE_ALLOW_REMOTE_REPLAY. */
  allowRemoteReplay: boolean;
  /** Called with a STRUCTURED signal when the server signals credits_exhausted | rate_limited |
   *  halt | error. Branch on `signal.code`; use `signal.retryable`/`signal.fatal` to decide
   *  behavior. Defaults to a throttled console warning so signals are never silently dropped. */
  onError?: (signal: import("./errors.js").RetraceServerSignal) => void;
  /** Redaction hook: called with (eventType, data) for every event before it leaves the process
   *  (trace_started / span_started / span_ended / trace_ended). Return the (possibly edited) data to
   *  send, or null to drop the event. A throw drops the event (raw data is never sent). */
  beforeSend?: (eventType: string, data: Record<string, unknown>) => Record<string, unknown> | null;
  /** Enforcement (circuit breakers). LOCAL ceilings are enforced offline with zero network — they
   *  always apply; `undefined` = unset. Precedence: explicit configure() arg > env var > unset.
   *  `serverEnforcement=true` additionally consults the server /check endpoint for centrally-managed
   *  policies (best-effort from the SDK; authoritative at ingest). */
  maxStepsPerRun?: number;
  maxTokensPerRun?: number;
  maxUsdPerRun?: number;
  serverEnforcement: boolean;
  /** On a server `hold` verdict, poll up to this many seconds for a human decision before applying
   *  the fail-closed timeout verdict. 0 = trip immediately. (The auto path is synchronous, so the
   *  poll runs in the background and a denial/timeout trips the NEXT span.) */
  enforcementHoldWaitSeconds: number;
  /** Environment indicator sent with every trace as `metadata.environment`. Set to "sandbox" to
   *  record into the auto-expiring sandbox data-namespace instead of production (the server also
   *  accepts an `X-Retrace-Environment` header). Env: RETRACE_ENVIRONMENT. */
  environment: string | undefined;
  /** Anonymous diagnostic telemetry (SDK version, runtime, event categories; never content or keys).
   *  On by default; `false` turns it off. Env: RETRACE_TELEMETRY=0|false|off|no, or DO_NOT_TRACK=1. */
  telemetry: boolean;
}

// Runtime-safe env: edge runtimes (Cloudflare Workers, Deno/Supabase Edge) may not define a global
// `process`, so reading process.env.* directly would throw at module-evaluation time. Fall back to
// an empty map there; explicit configure({ apiKey, ... }) still works on every runtime.
const env: Record<string, string | undefined> =
  typeof process !== "undefined" && process.env ? process.env : {};

/** RETRACE_FLUSH_TIMEOUT is in seconds (parity with the Python SDK); invalid values keep the default. */
function flushTimeoutFromEnv(raw: string | undefined): number {
  if (!raw) return 5000;
  const seconds = Number(raw);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.round(seconds * 1000);
  console.warn(`[retrace] ignoring RETRACE_FLUSH_TIMEOUT=${JSON.stringify(raw)} (not a number of seconds)`);
  return 5000;
}

/** Telemetry is on unless RETRACE_TELEMETRY is 0/false/off/no or DO_NOT_TRACK is set (consoledonottrack.com). */
export function telemetryFromEnv(e: Record<string, string | undefined>): boolean {
  if (["0", "false", "off", "no"].includes((e.RETRACE_TELEMETRY || "").trim().toLowerCase())) return false;
  return !["1", "true", "yes"].includes((e.DO_NOT_TRACK || "").trim().toLowerCase());
}

const config: Config = {
  apiKey: env.RETRACE_API_KEY || "",
  baseUrl: env.RETRACE_BASE_URL || "https://api.retraceai.tech",
  wsUrl: "",
  projectId: env.RETRACE_PROJECT_ID || undefined,
  enabled: !["false", "0"].includes((env.RETRACE_ENABLED || "true").toLowerCase()),
  sampleRate: parseFloat(env.RETRACE_SAMPLE_RATE || "1"),
  sampleSeed: env.RETRACE_SAMPLE_SEED || undefined,
  transport: (["auto", "ws", "http"].includes(env.RETRACE_TRANSPORT || "") ? env.RETRACE_TRANSPORT : "auto") as "auto" | "ws" | "http",
  flushTimeoutMs: flushTimeoutFromEnv(env.RETRACE_FLUSH_TIMEOUT),
  strictReplay: !["false", "0", "no"].includes((env.RETRACE_STRICT_REPLAY || "").toLowerCase()),
  allowRemoteReplay: ["true", "1", "yes"].includes((env.RETRACE_ALLOW_REMOTE_REPLAY || "").toLowerCase()),
  maxStepsPerRun: env.RETRACE_MAX_STEPS_PER_RUN ? parseInt(env.RETRACE_MAX_STEPS_PER_RUN, 10) : undefined,
  maxTokensPerRun: env.RETRACE_MAX_TOKENS_PER_RUN ? parseInt(env.RETRACE_MAX_TOKENS_PER_RUN, 10) : undefined,
  maxUsdPerRun: env.RETRACE_MAX_USD_PER_RUN ? parseFloat(env.RETRACE_MAX_USD_PER_RUN) : undefined,
  serverEnforcement: ["true", "1", "yes"].includes((env.RETRACE_SERVER_ENFORCEMENT || "").toLowerCase()),
  enforcementHoldWaitSeconds: env.RETRACE_ENFORCEMENT_HOLD_WAIT_SECONDS ? parseInt(env.RETRACE_ENFORCEMENT_HOLD_WAIT_SECONDS, 10) : 0,
  environment: env.RETRACE_ENVIRONMENT || undefined,
  telemetry: telemetryFromEnv(env),
};
config.wsUrl = config.baseUrl.replace("https://", "wss://").replace("http://", "ws://");

function isLoopbackHost(host: string): boolean {
  const h = host.toLowerCase().replace(/^\[|\]$/g, "");
  return h === "localhost" || h === "127.0.0.1" || h === "::1" || h.endsWith(".localhost");
}

/**
 * C3: refuse cleartext transports for non-loopback hosts. Over http:// → ws:// the one-time auth
 * frame sends the API key in plaintext AND every server command frame (resume/replay/halt) is
 * injectable on the wire — which is the transport half of the C2 command-injection chain. https://
 * (and http://localhost for local dev) are allowed; anything else hard-errors before any bytes,
 * including the API key, leave the process.
 */
function assertSecureBaseUrl(baseUrl: string): void {
  let u: URL;
  try {
    u = new URL(baseUrl);
  } catch {
    throw new Error(`Retrace: invalid base URL ${JSON.stringify(baseUrl)} (set RETRACE_BASE_URL or configure({ baseUrl }) to an https:// URL).`);
  }
  if (u.protocol === "https:") return;
  if (u.protocol === "http:" && isLoopbackHost(u.hostname)) return; // local dev only
  throw new Error(
    `Retrace: refusing insecure base URL ${JSON.stringify(baseUrl)}. Use https:// (http:// is allowed only for localhost). ` +
      "Over cleartext the API key is transmitted in plaintext and server replay/resume command frames can be injected on the wire.",
  );
}

/** The WebSocket URL carries the key in its auth frame: wss:// only (ws:// for localhost). An explicit
 *  `wsUrl` used to skip the base-URL check. */
export function assertSecureWsUrl(wsUrl: string): void {
  if (!wsUrl) return;
  let u: URL;
  try {
    u = new URL(wsUrl);
  } catch {
    throw new Error(`Retrace: invalid WebSocket URL ${JSON.stringify(wsUrl)}.`);
  }
  if (u.protocol === "wss:" || (u.protocol === "ws:" && isLoopbackHost(u.hostname))) return;
  throw new Error(`Retrace: refusing insecure WebSocket URL ${JSON.stringify(wsUrl)}. Use wss:// (ws:// is allowed only for localhost): the API key is sent in the first frame.`);
}

export function configure(opts: Partial<Config>): Config {
  if (opts.apiKey && (!opts.apiKey.startsWith("rt_") || opts.apiKey.startsWith("rt_live_") || opts.apiKey.startsWith("rt_test_"))) {
    throw new Error("Invalid Retrace API key. Keys must start with 'rt_'. Get yours at https://dash.retraceai.tech/workspace/api-keys");
  }
  Object.assign(config, opts);
  if (opts.baseUrl && !opts.wsUrl) {
    config.wsUrl = config.baseUrl.replace("https://", "wss://").replace("http://", "ws://");
  }
  assertSecureBaseUrl(config.baseUrl);
  assertSecureWsUrl(config.wsUrl);
  // Eagerly install provider interceptors so clients constructed AFTER configure() are patched.
  // @google/genai binds generateContent as an own instance property, and our accessor only wraps
  // instances built after install — so install must precede client construction. Fire-and-forget;
  // the dynamic import resolves before the first awaited LLM call in any real async flow.
  if (config.enabled) {
    void import("./interceptors/install.js").then((m) => m.ensureInterceptorsInstalled()).catch(() => {});
  }
  void import("./telemetry.js").then((m) => m.track("sdk_init", { has_api_key: !!config.apiKey, enabled: config.enabled })).catch(() => {});
  return config;
}

export function requireApiKey(): string {
  if (!config.apiKey) {
    throw new Error("Retrace API key required. Call configure({ apiKey: 'rt_...' }) or set RETRACE_API_KEY. Get yours at https://dash.retraceai.tech/workspace/api-keys");
  }
  // Validate the transport is encrypted before the key is ever used on the wire (covers env-only
  // config that never called configure()).
  assertSecureBaseUrl(config.baseUrl);
  assertSecureWsUrl(config.wsUrl);
  return config.apiKey;
}

export function getConfig(): Config {
  return config;
}
