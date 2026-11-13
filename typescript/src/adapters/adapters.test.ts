import { describe, it, expect, vi, beforeEach } from "vitest";

// Stub transport so recorders never open a socket.
vi.mock("../transport.js", () => ({ createTransport: () => ({ send: vi.fn(), close: vi.fn(), flush: vi.fn() }), registerProcessExitFlush: vi.fn(), onProcessExit: vi.fn() }));

import { configure } from "../config.js";
import { TraceRecorder, trace } from "../recorder.js";
import { getActiveRecorder } from "../init.js";
import { recordVercelStep, retraceOnStepFinish } from "./vercel-ai.js";
import { createLangChainHandler } from "./langchain.js";

describe("Vercel AI adapter", () => {
  beforeEach(() => configure({ apiKey: "rt_test", enabled: true }));

  it("records an llm span + tool_call + tool_result into the recorder", () => {
    const rec = new TraceRecorder({ name: "t" });
    rec.start("t");
    const addSpan = vi.spyOn(rec, "addSpan");
    const startSpan = vi.spyOn(rec, "startSpan");
    recordVercelStep(
      {
        text: "hi",
        usage: { promptTokens: 5, completionTokens: 3 },
        toolCalls: [{ toolName: "search", args: { q: "x" } }],
        toolResults: [{ toolName: "search", result: "ok" }],
      },
      rec,
    );
    expect(addSpan).toHaveBeenCalledTimes(1); // llm span (carries token usage)
    expect(startSpan).toHaveBeenCalledTimes(2); // tool_call + tool_result
    const llmSpan = addSpan.mock.calls[0][0];
    expect(llmSpan.span_type).toBe("llm_call");
    expect(llmSpan.input_tokens).toBe(5);
    expect(llmSpan.output_tokens).toBe(3);
  });

  it("no-ops without an active recorder", () => {
    expect(() => recordVercelStep({ text: "x" })).not.toThrow();
  });

  // The step shape of AI SDK 5+ (ai@7): it used to record no tokens, tool arguments or results.
  const v5Step = {
    text: "",
    usage: { inputTokens: 100, outputTokens: 50, totalTokens: 150 },
    toolCalls: [{ type: "tool-call", toolCallId: "c1", toolName: "get_weather", input: { city: "Paris" } }],
    toolResults: [{ type: "tool-result", toolCallId: "c1", toolName: "get_weather", input: { city: "Paris" }, output: { sky: "sunny" } }],
    finishReason: "tool-calls",
  };

  it("reads the AI SDK 5+ step shape (tokens, tool input/output) and summarizes a tool-only step", () => {
    const rec = new TraceRecorder({ name: "t" });
    rec.start("t");
    const addSpan = vi.spyOn(rec, "addSpan");
    const startSpan = vi.spyOn(rec, "startSpan");
    const endSpan = vi.spyOn(rec, "endSpan");
    recordVercelStep(v5Step, rec);
    const llm = addSpan.mock.calls[0][0];
    expect([llm.input_tokens, llm.output_tokens]).toEqual([100, 50]);
    expect(llm.output).toBe('[tool calls] get_weather({"city":"Paris"})');
    expect(startSpan.mock.calls.map((c) => [c[0], c[1], c[2]])).toEqual([["get_weather", "tool_call", { city: "Paris" }], ["get_weather", "tool_result", undefined]]);
    expect(endSpan.mock.calls[1][1]).toEqual({ sky: "sunny" });
  });

  it("records into the active trace() run, also when passed uncalled (onStepFinish: retraceOnStepFinish)", async () => {
    const seen: string[] = [];
    const run = trace(async () => {
      const rec = getActiveRecorder();
      expect(rec).not.toBeNull();
      const spy = vi.spyOn(rec!, "addSpan").mockImplementation((s) => { seen.push(s.name); });
      const onStepFinish: (step: typeof v5Step) => unknown = retraceOnStepFinish; // the audit's wiring
      onStepFinish(v5Step);
      retraceOnStepFinish()(v5Step);
      spy.mockRestore();
    }, { name: "vercel" });
    await run();
    expect(seen).toEqual(["ai.generate", "ai.generate"]);
    expect(getActiveRecorder()).toBeNull(); // nothing ambient outside the run
  });
});

describe("LangChain adapter", () => {
  it("throws a helpful error when @langchain/core is not installed", async () => {
    await expect(createLangChainHandler()).rejects.toThrow(/@langchain\/core/);
  });

  it("a handler created inside trace() records into that run, even when called outside it", async () => {
    vi.doMock("@langchain/core/callbacks/base", () => ({ BaseCallbackHandler: class {} }));
    try {
      const names: string[] = [];
      let handler: { handleToolStart: (t: { name: string }, i: string, id: string) => void; handleToolEnd: (o: unknown, id: string) => void } | undefined;
      let runRecorder: TraceRecorder | null = null;
      await trace(async () => {
        runRecorder = getActiveRecorder();
        vi.spyOn(runRecorder!, "startSpan").mockImplementation((name) => { names.push(name); return { } as never; });
        vi.spyOn(runRecorder!, "endSpan").mockImplementation(() => {});
        handler = (await createLangChainHandler()) as typeof handler;
      }, { name: "lc" })();
      expect(runRecorder).not.toBeNull();
      handler!.handleToolStart({ name: "get_weather" }, "{\"city\":\"Paris\"}", "run-1"); // outside the trace context
      handler!.handleToolEnd("21C", "run-1");
      expect(names).toEqual(["get_weather", "tool_result"]);
    } finally {
      vi.doUnmock("@langchain/core/callbacks/base");
    }
  });
});
