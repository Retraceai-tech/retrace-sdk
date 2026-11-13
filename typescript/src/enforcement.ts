/**
 * Client-side enforcement gate (circuit breakers) — the TypeScript twin of the Python
 * `EnforcementGate` (same names, same semantics).
 *
 * LOCAL ceilings (max steps / tokens / USD per run) are enforced entirely offline — zero network, so
 * the breaker trips even when the API is unreachable, and it throws SYNCHRONOUSLY before the next
 * call. When `serverEnforcement` is enabled the gate ALSO consults the server `/enforcement/check`
 * endpoint; because auto-instrumentation routes spans synchronously, that call is best-effort and
 * fire-and-forget — a server block trips the NEXT span (one-span lag) rather than the current one,
 * and a transport failure is logged, never swallowed (server policy is authoritative at ingest).
 */
import type { Config } from "./config.js";
import { RetraceEnforcementError } from "./errors.js";

// Synchronous, dependency-free hash → 32 hex chars (cyrb53 variant, two seeds for a wider digest).
// Used ONLY for LOCAL loop/debounce dedup keys within a single run — never a security or
// cross-language boundary — so a fast non-cryptographic hash is sufficient. This keeps the gate
// synchronous (Web Crypto's subtle.digest is async) AND runtime-agnostic (no `node:crypto` import,
// which would break module evaluation on edge runtimes).
function shortHash(input: string): string {
  const pass = (seed: number): string => {
    let h1 = 0xdeadbeef ^ seed;
    let h2 = 0x41c6ce57 ^ seed;
    for (let i = 0; i < input.length; i++) {
      const ch = input.charCodeAt(i);
      h1 = Math.imul(h1 ^ ch, 2654435761);
      h2 = Math.imul(h2 ^ ch, 1597334677);
    }
    h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
    h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
    const n = 4294967296 * (2097151 & h2) + (h1 >>> 0);
    return n.toString(16).padStart(16, "0");
  };
  return (pass(0) + pass(0x9e3779b9)).slice(0, 32);
}

/** Stable short hash of tool arguments so raw args never leave the process for a loop/debounce check. */
export function hashToolArgs(args: unknown): string {
  let serialized: string;
  try {
    serialized = typeof args === "string" ? args : JSON.stringify(args, Object.keys(args as object ?? {}).sort());
  } catch {
    serialized = String(args);
  }
  return shortHash(serialized);
}

export class EnforcementGate {
  private steps = 0;
  private tokens = 0;
  private usd = 0;
  /** A server block resolved asynchronously; trips on the next span (best-effort, one-span lag). */
  private pendingBlock: RetraceEnforcementError | null = null;

  constructor(private config: Config, private runId: string) {}

  /** No-op fast path unless a local ceiling is set or server enforcement is enabled. */
  get active(): boolean {
    const c = this.config;
    return c.maxStepsPerRun !== undefined || c.maxTokensPerRun !== undefined || c.maxUsdPerRun !== undefined || c.serverEnforcement;
  }

  /**
   * Accumulate this span's usage and enforce. Throws RetraceEnforcementError on a tripped local
   * ceiling (synchronous) or a previously-resolved server block. Safe to call on every span.
   */
  recordAndCheck(opts: { tokens?: number; usd?: number; traceId?: string; spanId?: string; toolName?: string; toolArgsHash?: string }): void {
    if (!this.active) return;
    // A server block resolved after the previous span trips here, before this call proceeds.
    if (this.pendingBlock) {
      const err = this.pendingBlock;
      this.pendingBlock = null;
      throw err;
    }
    this.steps += 1;
    this.tokens += Math.max(0, opts.tokens ?? 0);
    this.usd += Math.max(0, opts.usd ?? 0);

    const c = this.config;
    if (c.maxStepsPerRun !== undefined && this.steps > c.maxStepsPerRun) {
      throw new RetraceEnforcementError(`Local step ceiling reached: ${this.steps} > ${c.maxStepsPerRun} per run.`);
    }
    if (c.maxTokensPerRun !== undefined && this.tokens > c.maxTokensPerRun) {
      throw new RetraceEnforcementError(`Local token ceiling reached: ${this.tokens} > ${c.maxTokensPerRun} per run.`);
    }
    if (c.maxUsdPerRun !== undefined && this.usd > c.maxUsdPerRun) {
      throw new RetraceEnforcementError(`Local USD ceiling reached: $${this.usd.toFixed(4)} > $${c.maxUsdPerRun} per run.`);
    }

    if (c.serverEnforcement) void this.serverCheck(opts);
  }

  /** Best-effort server consult. Records a pending block on a block/hold verdict; never throws. */
  private async serverCheck(opts: { tokens?: number; usd?: number; traceId?: string; spanId?: string; toolName?: string; toolArgsHash?: string }): Promise<void> {
    const c = this.config;
    if (!c.apiKey) return;
    const body: Record<string, unknown> = { run_id: this.runId, proposed_tokens: opts.tokens ?? 0, proposed_usd: opts.usd ?? 0 };
    if (c.projectId) body.project_id = c.projectId;
    if (opts.traceId) body.trace_id = opts.traceId;
    // The server's ingest backstop evaluates the same recorded span; with its id the step counts once.
    if (opts.spanId) body.span_id = opts.spanId;
    if (opts.toolName) body.tool_name = opts.toolName;
    if (opts.toolArgsHash) body.tool_args_hash = opts.toolArgsHash;
    try {
      const resp = await fetch(`${c.baseUrl}/api/v1/enforcement/check`, {
        method: "POST",
        headers: { "x-retrace-key": c.apiKey, "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(5000),
        redirect: "error", // never forward the key to a redirect target
      });
      if (resp.status !== 200 && resp.status !== 202) {
        console.warn(`[retrace] server enforcement check returned ${resp.status} (local limits still apply)`);
        return;
      }
      const data = (await resp.json()) as { verdict?: string; reason?: string; policy_id?: string; hold_id?: string };
      if (data.verdict === "block") {
        this.pendingBlock = new RetraceEnforcementError(data.reason || "Blocked by server policy.", "block", data.policy_id);
      } else if (data.verdict === "hold") {
        await this.handleHold(data);
      }
    } catch (e) {
      console.warn(`[retrace] server enforcement check unreachable (local limits still apply): ${(e as Error).message}`);
    }
  }

  /** A held action: poll for a human decision up to the configured wait, then trip on denial/timeout
   *  (sets pendingBlock, which stops the next span). With wait=0 the hold trips immediately. */
  private async handleHold(data: { reason?: string; policy_id?: string; hold_id?: string }): Promise<void> {
    const c = this.config;
    const err = new RetraceEnforcementError(data.reason || "Held by server policy.", "hold", data.policy_id);
    if (!data.hold_id || c.enforcementHoldWaitSeconds <= 0) {
      this.pendingBlock = err;
      return;
    }
    const deadline = Date.now() + c.enforcementHoldWaitSeconds * 1000;
    const interval = Math.min(2000, Math.max(500, (c.enforcementHoldWaitSeconds * 1000) / 10));
    while (Date.now() < deadline) {
      const status = await this.pollHold(data.hold_id);
      if (status === "approved") return;                 // released — no pending block
      if (status === "denied" || status === "expired") { this.pendingBlock = err; return; }
      await new Promise((r) => setTimeout(r, interval));
    }
    this.pendingBlock = err;                              // timed out → fail-closed
  }

  /** Read a hold's status; returns "pending" on any transport error so the loop keeps waiting. */
  private async pollHold(holdId: string): Promise<string> {
    try {
      const resp = await fetch(`${this.config.baseUrl}/api/v1/enforcement/holds/${holdId}`, {
        headers: { "x-retrace-key": this.config.apiKey },
        signal: AbortSignal.timeout(5000),
        redirect: "error",
      });
      if (resp.status === 200) return ((await resp.json()) as { status?: string }).status || "pending";
    } catch {
      // transient — keep waiting
    }
    return "pending";
  }
}
