import { SpanType, type SpanData } from "../trace.js";
import { genId, nowIso } from "../utils.js";
import { getActiveRecorder } from "../init.js";
import type { TraceRecorder } from "../recorder.js";

/**
 * Vercel AI SDK adapter for Retrace.
 *
 * The AI SDK talks to providers through its own `@ai-sdk/*` packages (not the raw OpenAI/Anthropic
 * SDKs), so the provider interceptors don't see those calls. This adapter records the LLM step plus
 * structured `tool_call` / `tool_result` spans from the AI SDK's per-step callback, aligned with the
 * detectors. Drop it into `generateText` / `streamText`:
 *
 * ```ts
 * import { init } from "retrace-sdk";
 * import { retraceOnStepFinish } from "retrace-sdk/adapters/vercel-ai";
 * init();
 * await generateText({ model, prompt, tools, onStepFinish: retraceOnStepFinish() });
 * ```
 *
 * Both step shapes are read: AI SDK 5+ (`usage.inputTokens`, `toolCalls[].input`,
 * `toolResults[].output`) and AI SDK 4 (`usage.promptTokens`, `args`, `result`). Only the v4 names were
 * read before, so with a current AI SDK every step recorded no tokens and no tool arguments or results.
 */
interface AISDKToolCall { toolName?: string; toolCallId?: string; input?: unknown; args?: unknown }
interface AISDKToolResult { toolName?: string; toolCallId?: string; output?: unknown; result?: unknown }
interface AISDKUsage {
  inputTokens?: number | { total?: number };
  outputTokens?: number | { total?: number };
  promptTokens?: number;
  completionTokens?: number;
  totalTokens?: number;
}
export interface AISDKStep {
  text?: string;
  toolCalls?: AISDKToolCall[];
  toolResults?: AISDKToolResult[];
  usage?: AISDKUsage;
  finishReason?: string | { unified?: string };
  model?: { modelId?: string };
  response?: { modelId?: string };
}

/** A token count from either SDK generation (a number, or `{ total }` in the newest usage shape). */
function tokens(...values: Array<number | { total?: number } | undefined>): number | undefined {
  for (const v of values) {
    if (typeof v === "number" && Number.isFinite(v)) return v;
    if (v && typeof v === "object" && typeof v.total === "number") return v.total;
  }
  return undefined;
}

/** Record one AI SDK step (LLM output + tool calls/results) into the trace. */
export function recordVercelStep(step: AISDKStep, recorder?: TraceRecorder): void {
  const rec = recorder ?? getActiveRecorder();
  if (!rec || !step || typeof step !== "object") return;

  const calls = step.toolCalls ?? [];
  const results = step.toolResults ?? [];
  const now = nowIso();
  const llmId = genId();
  const text = step.text ?? "";
  // A tool-calling step has no text: summarize the calls so the LLM span is not empty.
  const callSummary = calls.map((c) => `${c.toolName || "tool"}(${JSON.stringify(c.input ?? c.args ?? {})})`).join("; ");
  const output = text || (calls.length ? `[tool calls] ${callSummary}` : "");
  const llm: SpanData = {
    id: llmId, trace_id: "", parent_id: null,
    span_type: SpanType.LLM_CALL, name: "ai.generate",
    model: step.response?.modelId ?? step.model?.modelId,
    output,
    input_tokens: tokens(step.usage?.inputTokens, step.usage?.promptTokens),
    output_tokens: tokens(step.usage?.outputTokens, step.usage?.completionTokens),
    started_at: now, ended_at: now,
  };
  rec.addSpan(llm);

  for (const call of calls) {
    const sb = rec.startSpan(call.toolName || "tool", SpanType.TOOL_CALL, call.input ?? call.args);
    if (call.toolCallId) sb.setMetadata({ tool_call_id: call.toolCallId });
    rec.endSpan(sb, undefined);
  }
  for (const res of results) {
    const sb = rec.startSpan(res.toolName || "tool_result", SpanType.TOOL_RESULT, undefined);
    if (res.toolCallId) sb.setMetadata({ tool_call_id: res.toolCallId });
    rec.endSpan(sb, res.output ?? res.result);
  }
}

type StepCallback = (step: AISDKStep) => void;

/**
 * Returns an `onStepFinish` callback for `generateText` / `streamText`: `onStepFinish:
 * retraceOnStepFinish()`. Passing it without calling it (`onStepFinish: retraceOnStepFinish`) also
 * works: the AI SDK then calls it with the step, which is recorded (that used to record nothing).
 */
export function retraceOnStepFinish(recorder?: TraceRecorder): StepCallback;
export function retraceOnStepFinish(step: AISDKStep): void;
export function retraceOnStepFinish(arg?: TraceRecorder | AISDKStep): StepCallback | void {
  if (arg && typeof arg === "object" && !("addSpan" in arg) && ("text" in arg || "toolCalls" in arg || "usage" in arg || "finishReason" in arg)) {
    recordVercelStep(arg as AISDKStep);
    return;
  }
  const recorder = arg as TraceRecorder | undefined;
  return (step: AISDKStep) => recordVercelStep(step, recorder);
}
