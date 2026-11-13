export { configure, getConfig } from "./config.js";
export { init, getActiveRecorder, shutdown } from "./init.js";
export type { InitOptions } from "./init.js";
export { record, trace, TraceRecorder } from "./recorder.js";
export { stream } from "./stream.js";
export { SpanBuilder, TraceBuilder } from "./trace.js";
export type { SpanData, TraceData } from "./trace.js";
export { SpanType, TraceStatus } from "./trace.js";
export { installGeminiInterceptor, uninstallGeminiInterceptor } from "./interceptors/gemini.js";
export { installOpenAIInterceptor, uninstallOpenAIInterceptor } from "./interceptors/openai.js";
export { installAnthropicInterceptor, uninstallAnthropicInterceptor } from "./interceptors/anthropic.js";
export { RetraceError, RetraceAuthError, RetraceCreditsExhaustedError, RetraceConnectionError, RetraceRateLimitError, RetraceEnforcementError } from "./errors.js";
export { registerResumable, handleResume } from "./resume.js";
export type { ResumeCommand } from "./resume.js";
export { isReplaying, consumeCassetteEntry, handleReplay } from "./replay.js";
export type { CassetteEntry, ReplayCommand } from "./replay.js";
export { setTraceContext, clearTraceContext, getTraceparent, injectTraceparent, parseTraceparent, withTraceContext } from "./traceparent.js";
export { markGolden } from "./golden.js";
export { writeGoldenCassette } from "./cassette.js";
export type { CassetteTolerance } from "./cassette.js";
export { withAgent, currentAgent } from "./agents.js";
export type { AgentContext, AgentSpec } from "./agents.js";
export { setTruncationLimits } from "./utils.js";
// Framework adapters (5B) — drop-in instrumentation for LangChain/LangGraph + Vercel AI SDK.
export { createLangChainHandler } from "./adapters/langchain.js";
export { retraceOnStepFinish, recordVercelStep } from "./adapters/vercel-ai.js";
export type { AISDKStep } from "./adapters/vercel-ai.js";

export { ready } from "./interceptors/install.js";

// Patch provider SDKs at import. NO top-level await (it would break require("retrace-sdk") and
// CommonJS bundles): where the runtime can require() an ES module the patch is synchronous, so the
// first LLM call after `init()` is recorded (pass 7: it was missed); elsewhere `ready()` waits for it.
// Prototype methods are patched, so clients constructed before or after this are all covered.
import { ensureInterceptorsInstalled } from "./interceptors/install.js";
void ensureInterceptorsInstalled();
// v0.5.0
// trigger
