import { getConfig, requireApiKey } from "./config.js";
import { SpanBuilder, SpanData, SpanType, TraceBuilder, TraceStatus } from "./trace.js";
import { createTransport, registerProcessExitFlush, Transport } from "./transport.js";
import { shouldSample, describeError } from "./utils.js";
import { installGeminiInterceptor } from "./interceptors/gemini.js";
import { installOpenAIInterceptor } from "./interceptors/openai.js";
import { installAnthropicInterceptor } from "./interceptors/anthropic.js";
import { ensureInterceptorsInstalled, interceptorsSettled } from "./interceptors/install.js";
import { dispatchInterceptedSpan, runWithActiveRecorder, setActiveRecorderFallback, currentFallbackSink, OpenSpanSink } from "./interceptors/_dispatch.js";
import { withTraceContext, enterTraceContext, exitTraceContext } from "./traceparent.js";
import { EnforcementGate, hashToolArgs } from "./enforcement.js";
import { currentAgent } from "./agents.js";

// Shared transport — stays open across multiple traces for resume/replay listening
let sharedTransport: Transport | null = null;
// Count of imperative (non-HOF) recorders currently between start() and end(). The bare imperative
// record()/TraceRecorder path is NOT context-isolated (see record() doc + traceparent.ts), so two
// overlapping imperative traces can cross-attribute spans/traceparent. We can't make it isolated
// without an async scope, but we MUST NOT let the corruption be silent — overlap is loudly warned,
// pointing at trace() (the concurrency-safe API). HOF-managed starts pass {managed:true} and are
// excluded (they ARE isolated via withTraceContext/runWithActiveRecorder).
let activeImperativeRecorders = 0;
function getSharedTransport(): Transport {
  if (!sharedTransport) {
    sharedTransport = createTransport(getConfig().transport);
    // Hand the transport the user's callback (or undefined). The transport owns the policy:
    // callback-safety (a throwing onError can't kill the WS loop) and the throttled default-warn
    // when no callback is registered — see WSTransport.surfaceSignal. Set onError in configure()
    // before the first trace.
    sharedTransport.onError = getConfig().onError;
    // Flush pending data before the process exits. Covers beforeExit (graceful) + SIGTERM/SIGINT
    // (with signal-ownership semantics), using an HTTP one-shot for buffered events since a WS
    // handshake can't reliably complete during teardown. See registerProcessExitFlush.
    // Only register the network exit hook when actually configured — an imported-but-unconfigured
    // SDK installs no signal handlers and makes no outbound call on exit.
    const cfg = getConfig();
    if (typeof process !== "undefined" && cfg.enabled && cfg.apiKey) {
      registerProcessExitFlush(sharedTransport);
    }
  }
  return sharedTransport;
}

/** Drain the shared transport's in-flight data to the network (awaited on graceful shutdown). */
export async function flushSharedTransport(): Promise<void> {
  await sharedTransport?.flush();
}

/** Exit-path drain — uses the transport's HTTP one-shot for buffered events when available. */
export async function drainSharedTransportOnExit(budgetMs?: number): Promise<void> {
  if (sharedTransport?.flushOnExit) await sharedTransport.flushOnExit(budgetMs);
  else await sharedTransport?.flush();
}

export interface RecordOptions {
  name?: string;
  input?: unknown;
  metadata?: Record<string, unknown>;
  sessionId?: string;
  /** When set, spans emitted before this span ID is encountered are suppressed (pre-fork filtering). */
  forkPointSpanId?: string;
  /** 0-based ordinal of the fork-point span among the original ordered spans. Suppression is
   *  positional: spans with counter <= index are suppressed, emission starts at index+1. */
  forkPointIndex?: number;
}

export class TraceRecorder {
  private builder: TraceBuilder;
  private transport: Transport;
  private interceptorsInstalled = false;
  private countedImperative = false;
  private prevFallback: ((span: SpanData) => void) | null = null;
  private prevFallbackSink: OpenSpanSink | null = null;
  private forkPointSpanId: string | undefined;
  private forkPointIndex: number | undefined;
  private forkPointReached = false;
  private spanCounter = 0;
  // Tool results already recorded for this trace (keyed by provider tool-call id): each model call
  // re-sends the conversation's earlier tool results, and only the first sighting is a step. Per trace —
  // a process-wide set made two concurrent runs that shared an id lose one of them.
  private seenToolResults = new Set<string>();
  // Tool calls the model requested (recorded by the interceptors, no result yet) waiting for the app's
  // own span that runs them: startSpan continues the matching one instead of adding a second tool_call.
  private requestedToolCalls: SpanData[] = [];
  // Spans actually emitted (span_started sent), reported on trace_ended as span_count so the server
  // can tell a complete capture from one that lost spans in transit.
  private emittedSpans = 0;
  output: unknown = undefined;
  private enforcement: EnforcementGate;

  constructor(opts?: RecordOptions) {
    requireApiKey();
    this.builder = new TraceBuilder();
    this.transport = getSharedTransport();
    this.forkPointSpanId = opts?.forkPointSpanId;
    this.forkPointIndex = opts?.forkPointIndex;
    // Suppress pre-fork spans only when BOTH a fork point and its positional index are known;
    // otherwise (normal recording, or a fork command without an index) emit everything.
    this.forkPointReached = !opts?.forkPointSpanId || opts?.forkPointIndex === undefined;
    const cfg = getConfig();
    // Per-run circuit breaker (no-op unless a local ceiling or server enforcement is configured).
    this.enforcement = new EnforcementGate(cfg, this.builder.id);
    if (cfg.projectId) this.builder.setProjectId(cfg.projectId);
    if (opts?.metadata) this.builder.setMetadata(opts.metadata);
    if (opts?.sessionId) this.builder.setSessionId(opts.sessionId);
    if (opts?.name || opts?.input) {
      this.builder.start(opts.name, opts.input);
    }
  }

  get traceId() { return this.builder.id; }

  /**
   * Export this recorder's completed spans as a golden cassette (Phase 4b). The snapshot is the
   * CI regression baseline — `retrace ci replay` diffs a fresh run against it. Capture AFTER the
   * traced function returns so all spans are present.
   */
  toCassette(): { version: 1; name: string; trace_id: string; recorded_at: string; status: string; steps: { index: number; span_type: string; name: string; model?: string | null; input?: unknown; output?: unknown; error?: string | null }[] } {
    const data = this.builder.toDict();
    const steps = (data.spans ?? []).map((s, index) => ({
      index,
      span_type: s.span_type,
      name: s.name,
      model: s.model ?? null,
      input: s.input,
      output: s.output,
      error: s.error ?? null,
    }));
    return {
      version: 1,
      name: data.name ?? "trace",
      trace_id: data.id,
      recorded_at: new Date().toISOString(),
      status: data.status,
      steps,
    };
  }

  start(name?: string, input?: unknown, opts?: { managed?: boolean }): this {
    // Never-silent guard for the imperative path: if another imperative trace is already active when
    // this one starts, overlapping imperative record() use can cross-attribute spans/traceparent.
    // Warn loudly and point at trace() — convert silent corruption into a loud signal. HOF-managed
    // starts are isolated (withTraceContext) and excluded.
    if (!opts?.managed) {
      if (activeImperativeRecorders > 0) {
        console.warn(
          "[retrace] CONCURRENT imperative record() detected: another imperative trace is still active. " +
          "The bare record()/TraceRecorder path is NOT concurrency-isolated — spans and traceparent from " +
          "overlapping imperative traces can be cross-attributed. Use trace() (the concurrency-safe API) " +
          "for concurrent/parallel workloads.",
        );
      }
      activeImperativeRecorders++;
      this.countedImperative = true;
    }
    this.builder.start(name, input);
    this.installInterceptors();
    // Imperative API: route intercepted spans to this recorder and propagate trace context for
    // outbound requests. Save the prior fallback so end() can RESTORE it (a nested trace() must not
    // wipe the ambient init() fallback). The trace()/HOF path additionally isolates via ALS.
    this.prevFallbackSink = currentFallbackSink();
    this.prevFallback = setActiveRecorderFallback((span) => this.addSpan(span), this);
    // ALS-isolated traceparent (per async execution), NOT a module global — so concurrent imperative
    // traces never leak context into one another's outbound requests. The trace()/HOF path also
    // wraps in withTraceContext; this covers the bare start()/end() imperative path.
    enterTraceContext(this.builder.id, this.builder.id);
    this.emit("trace_started", this.builder.toDict() as unknown as Record<string, unknown>);
    return this;
  }

  /** Every event leaves through here: the `beforeSend` redaction hook sees it first (fail-closed). */
  private emit(eventType: string, data: Record<string, unknown>): void {
    const hook = getConfig().beforeSend;
    let payload: Record<string, unknown> | null = data;
    if (hook) {
      try {
        payload = hook(eventType, data);
      } catch (e) {
        console.warn(`[retrace] beforeSend threw for ${eventType}; event dropped: ${e instanceof Error ? e.message : String(e)}`);
        return;
      }
      if (!payload) return;
    }
    this.transport.send(eventType, payload);
  }

  // ── OpenSpanSink: two-phase streaming spans (open at invocation, finalize once) ──
  private openSpans = new Map<string, (reason: "complete" | "partial") => void>();

  registerOpenSpan(spanId: string, finalize: (reason: "complete" | "partial") => void): void {
    this.openSpans.set(spanId, finalize);
  }

  unregisterOpenSpan(spanId: string): void {
    this.openSpans.delete(spanId);
  }

  /** Finalize any still-open streaming spans as partial (capture_complete:false). Called on end()
   *  so a stream the AFC layer abandoned mid-drain is still emitted into the trace, flagged
   *  not-byte-replayable, rather than lost or appearing only at process exit. */
  private finalizeOpenSpans(): void {
    if (this.openSpans.size === 0) return;
    for (const [, finalize] of this.openSpans) {
      try { finalize("partial"); } catch { /* best effort */ }
    }
    this.openSpans.clear();
  }

  /** End the trace. `opts.error`: why a failed run failed (the thrown value, or a message); it is
   *  stored with the trace (it was dropped: a run that threw showed only "failed"). */
  end(output?: unknown, status: TraceStatus = TraceStatus.COMPLETED, opts?: { terminatedEarly?: boolean; error?: unknown }) {
    if (this.countedImperative) {
      activeImperativeRecorders = Math.max(0, activeImperativeRecorders - 1);
      this.countedImperative = false;
    }
    if (output !== undefined) this.output = output;
    // Close any dangling streaming spans (capture_complete:false) BEFORE the terminal event, so
    // they land in this trace.
    this.finalizeOpenSpans();
    const data = this.builder.end(this.output, status);
    this.emit("trace_ended", {
      id: data.id,
      ended_at: data.ended_at!,
      output: data.output,
      status: data.status,
      total_tokens: data.total_tokens,
      total_cost: data.total_cost,
      // Measured locally by TraceBuilder.end(). Omitting it made every stored trace read 0 ms.
      total_duration_ms: data.total_duration_ms,
      span_count: this.emittedSpans,
      // Force-closed by exit-flush/signal: a synthesized terminal must NOT look clean to the
      // replay-guard. terminated_early ⇒ refuse byte-deterministic replay (same as no-terminal /
      // lossy / capture_complete:false). Only a naturally-drained run produces a clean terminal.
      ...(opts?.terminatedEarly ? { terminated_early: true } : {}),
      ...(describeError(opts?.error) ? { error: describeError(opts?.error) } : {}),
    });
    // Anonymous usage analytics (fire-and-forget; never throws): record success/failure + which
    // provider families ran. No PII, no user content.
    try {
      const dict = this.builder.toDict() as { spans?: Array<{ model?: string | null }>; metadata?: Record<string, unknown> };
      void import("./telemetry.js").then((m) => m.trackRecord(String(data.status), dict.spans ?? [], dict.metadata ?? {})).catch(() => {});
    } catch { /* never affect the user's program */ }
    // Restore the enclosing trace's fallback (e.g. the ambient init() recorder) instead of nulling.
    setActiveRecorderFallback(this.prevFallback, this.prevFallbackSink);
    this.prevFallback = null;
    this.prevFallbackSink = null;
    exitTraceContext();
    // Shared transport stays open for resume/replay listening
  }

  addSpan(span: SpanData) {
    const callId = (span.metadata as Record<string, unknown> | undefined)?.tool_call_id;
    if (span.span_type === SpanType.TOOL_RESULT && typeof callId === "string") {
      if (this.seenToolResults.has(callId)) return;
      if (this.seenToolResults.size > 10_000) this.seenToolResults.clear();
      this.seenToolResults.add(callId);
    }
    this.spanCounter++;
    // Fork-point filtering: during cascade replay suppress the pre-fork spans (the server already
    // has them / they replay from the cassette) and emit only from the fork point onward. The fork
    // point is the (forkPointIndex)-th span (0-based), i.e. the (index+1)-th counted here, so
    // suppress while spanCounter <= index and emit once spanCounter > index. (Previously this
    // compared spanCounter >= 1, which is always true after the increment ⇒ zero suppression.)
    if (!this.forkPointReached) {
      if (this.forkPointIndex !== undefined && this.spanCounter > this.forkPointIndex) {
        this.forkPointReached = true;
      } else {
        return; // Suppress pre-fork span
      }
    }
    span.trace_id = this.builder.id;
    const actx = currentAgent();
    if (actx) {
      if (!span.agent_id) span.agent_id = actx.agentId;
      if (!span.agent_role) span.agent_role = actx.agentRole;
      if (!span.parent_agent_id) span.parent_agent_id = actx.parentAgentId;
    }
    if (span.ended_at && span.duration_ms === undefined && span.started_at) {
      const d = Date.parse(span.ended_at) - Date.parse(span.started_at);
      if (Number.isFinite(d) && d >= 0) span.duration_ms = d;
    }
    this.builder.addSpan(span);
    this.emit("span_started", span as unknown as Record<string, unknown>);
    this.emittedSpans++;
    if (span.span_type === SpanType.TOOL_CALL && typeof callId === "string" && (span.output === undefined || span.output === null)) {
      this.requestedToolCalls.push(span);
      if (this.requestedToolCalls.length > 100) this.requestedToolCalls.shift();
    }
    if (span.ended_at) {
      this.emit("span_ended", {
        id: span.id,
        trace_id: span.trace_id,
        ended_at: span.ended_at,
        output: span.output,
        output_tokens: span.output_tokens,
        cost: span.cost,
        duration_ms: span.duration_ms,
        error: span.error,
      });
    }

    // Circuit breaker: record this span's usage and enforce BEFORE the next call. Throws
    // RetraceEnforcementError on a tripped local ceiling (recording above is already done, so the
    // trace keeps the offending span; only the run is stopped).
    if (this.enforcement.active) {
      const isTool = span.span_type === SpanType.TOOL_CALL;
      this.enforcement.recordAndCheck({
        tokens: (span.input_tokens ?? 0) + (span.output_tokens ?? 0),
        usd: span.cost ?? 0,
        traceId: this.builder.id,
        spanId: span.id,
        toolName: isTool ? span.name : undefined,
        toolArgsHash: isTool ? hashToolArgs(span.input) : undefined,
      });
    }
  }

  startSpan(name: string, spanType: SpanType = SpanType.LLM_CALL, input?: unknown, model?: string, parentId?: string): SpanBuilder {
    const sb = new SpanBuilder(name, spanType).start();
    sb.setTraceId(this.builder.id);
    if (spanType === SpanType.TOOL_CALL) {
      // The app's span for a call the model requested: one step, not two. The requested call (already
      // sent, with the provider's tool_call_id) gets this run's result and timing when it ends.
      const key = canonicalJson(input);
      const at = this.requestedToolCalls.findIndex((s) => s.name === name && canonicalJson(s.input) === key);
      if (at >= 0) {
        const [requested] = this.requestedToolCalls.splice(at, 1);
        if (input !== undefined) sb.setInput(input);
        return sb.adopt(requested);
      }
    }
    if (input !== undefined) sb.setInput(input);
    if (model) sb.setModel(model);
    if (parentId) sb.setParentId(parentId);
    const actx = currentAgent();
    if (actx) sb.setAgent({ id: actx.agentId, role: actx.agentRole, parentId: actx.parentAgentId });
    this.emit("span_started", sb.toData() as unknown as Record<string, unknown>);
    this.emittedSpans++;
    return sb;
  }

  endSpan(spanBuilder: SpanBuilder, output?: unknown, error?: string) {
    const span = spanBuilder.end(output, error);
    span.trace_id = this.builder.id;
    this.builder.addSpan(span);
    this.emit("span_ended", {
      id: span.id,
      trace_id: span.trace_id,
      ended_at: span.ended_at,
      output: span.output,
      output_tokens: span.output_tokens,
      cost: span.cost,
      // Measured locally by SpanBuilder.end(). Omitting it made every stored span read null.
      duration_ms: span.duration_ms,
      error: span.error,
    });
  }

  private installInterceptors() {
    if (this.interceptorsInstalled) return;
    // Install ONE stable dispatcher; the active recorder is resolved per async-context (see
    // interceptors/_dispatch.ts) so concurrent traces don't cross-route intercepted spans. Each call
    // also patches a provider's CommonJS build if the app has loaded it since the last trace.
    void installGeminiInterceptor(dispatchInterceptedSpan);
    void installOpenAIInterceptor(dispatchInterceptedSpan);
    void installAnthropicInterceptor(dispatchInterceptedSpan);
    this.interceptorsInstalled = true;
  }
}

/** Key-order-independent JSON for comparing tool arguments. */
function canonicalJson(v: unknown): string {
  if (v === null || typeof v !== "object") return JSON.stringify(v ?? null);
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(",")}]`;
  const o = v as Record<string, unknown>;
  return `{${Object.keys(o).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson(o[k])}`).join(",")}}`;
}

/**
 * Create a manual (imperative) recorder you drive with start()/end().
 *
 * CONCURRENCY: this bare imperative path is NOT context-isolated — start()/end() have no async
 * scope to bind, so span-routing and traceparent are tracked via process/async-context state that
 * concurrent imperative traces (e.g. several started under one Promise.all) can stomp. For
 * concurrent workloads use `trace()`, which wraps your function in AsyncLocalStorage (provably
 * isolated). Sequential start→end cycles are correct, and overlapping imperative use is detected
 * and LOUDLY WARNED (never silent corruption) — pointing you at `trace()`. (Python's `@record`
 * decorator wraps the function and IS isolated on both asyncio and threads; the TS equivalent is
 * `trace()`.)
 */
export function record(opts?: RecordOptions): TraceRecorder {
  const cfg = getConfig();
  if (!cfg.enabled || !shouldSample(cfg.sampleRate, cfg.sampleSeed, opts?.name)) {
    // Return a properly-typed no-op recorder that satisfies the TraceRecorder interface
    const noop: TraceRecorder = Object.create(TraceRecorder.prototype);
    Object.defineProperties(noop, {
      traceId: { get: () => "" },
      output: { value: undefined, writable: true },
    });
    noop.start = () => noop;
    noop.end = () => {};
    noop.addSpan = () => {};
    noop.startSpan = (name: string) => new SpanBuilder(name, "llm_call" as SpanType);
    noop.endSpan = () => {};
    return noop;
  }
  return new TraceRecorder(opts);
}

export function trace<T>(fn: (...args: unknown[]) => T, opts?: RecordOptions & { resumable?: boolean }): (...args: unknown[]) => T {
  const cfg = getConfig();
  // Register for cascade replay if resumable
  if (opts?.resumable) {
    import("./resume.js").then(({ registerResumable }) => {
      registerResumable(opts?.name || fn.name || "anonymous", fn as (...args: unknown[]) => unknown);
    });
  }
  return (...args: unknown[]): T => {
    if (!cfg.enabled || !shouldSample(cfg.sampleRate, cfg.sampleSeed, opts?.name || fn.name)) return fn(...args);
    // An async function's result is a promise anyway: if a provider patch is still landing (runtimes
    // that cannot require() ES modules), start the trace once it has, so its first call is recorded.
    if (!interceptorsSettled() && fn.constructor?.name === "AsyncFunction") {
      return ensureInterceptorsInstalled().then(() => runTraced(fn, args, opts)) as T;
    }
    return runTraced(fn, args, opts);
  };
}

function runTraced<T>(fn: (...args: unknown[]) => T, args: unknown[], opts?: RecordOptions): T {
  const recorder = new TraceRecorder({
    name: opts?.name || fn.name || "anonymous",
    input: opts?.input ?? args,
    metadata: opts?.metadata,
    // Was dropped here, so trace(fn, { sessionId }) traces never joined their session.
    sessionId: opts?.sessionId,
  });
  // Isolate this trace's intercepted-span routing AND traceparent context to its own async
  // context, so concurrent traces on a server never cross-route spans or leak context.
  const tid = recorder.traceId;
  const route = (span: SpanData) => recorder.addSpan(span);
  return runWithActiveRecorder(route, () => withTraceContext(tid, tid, () => {
    recorder.start(opts?.name || fn.name || "anonymous", opts?.input ?? args, { managed: true });
    try {
      const result = fn(...args);
      // Handle async functions
      if (result && typeof (result as { then?: unknown }).then === "function") {
        return (result as unknown as Promise<unknown>).then(
          (resolved) => { recorder.end(resolved, TraceStatus.COMPLETED); return resolved; },
          (err) => { recorder.end(undefined, TraceStatus.FAILED, { error: err }); throw err; }
        ) as unknown as T;
      }
      recorder.end(result, TraceStatus.COMPLETED);
      return result;
    } catch (err) {
      recorder.end(undefined, TraceStatus.FAILED, { error: err });
      throw err;
    }
  }), recorder);
}
