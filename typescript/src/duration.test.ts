// Regression tests for the P0 "duration stored as 0 ms / null" defect (TypeScript SDK).
// The recorder measured durations locally (SpanBuilder/TraceBuilder) but never put them on the
// terminal span_ended / trace_ended events, so the server persisted 0 / null.
import { describe, it, expect, vi, beforeAll } from "vitest";
import { configure } from "./config.js";
import { TraceRecorder } from "./recorder.js";
import { SpanType, TraceStatus } from "./trace.js";
import type { SpanData } from "./trace.js";

beforeAll(() => {
  vi.stubGlobal("fetch", vi.fn(async () => ({ ok: true, status: 200, text: async () => "" })));
  configure({ apiKey: "rt_testkey", transport: "http", baseUrl: "http://127.0.0.1:0" });
});

type Sent = [string, Record<string, unknown>];
function spySends(rec: TraceRecorder) {
  const t = (rec as unknown as { transport: { send: (e: string, d: Record<string, unknown>) => void } }).transport;
  const spy = vi.spyOn(t, "send");
  return { spy, sent: () => spy.mock.calls as unknown as Sent[] };
}

describe("terminal events carry measured durations", () => {
  it("span_ended has duration_ms and trace_ended has total_duration_ms", async () => {
    const rec = new TraceRecorder({ name: "dur" });
    const { spy, sent } = spySends(rec);
    rec.start("dur", { q: 1 });
    const sb = rec.startSpan("synthetic-delay", SpanType.TOOL_CALL, { expr: "2+2" });
    await new Promise((r) => setTimeout(r, 120));
    rec.endSpan(sb, { result: 4 });
    rec.end({ answer: 4 }, TraceStatus.COMPLETED);

    const spanEnded = sent().filter(([e]) => e === "span_ended").map(([, d]) => d);
    const traceEnded = sent().filter(([e]) => e === "trace_ended").map(([, d]) => d);
    expect(spanEnded).toHaveLength(1);
    expect(spanEnded[0].duration_ms as number).toBeGreaterThanOrEqual(110);
    expect(traceEnded).toHaveLength(1);
    expect(traceEnded[0].total_duration_ms as number).toBeGreaterThanOrEqual(110);
    spy.mockRestore();
  });

  it("a completed span passed to addSpan() gets a derived duration_ms", () => {
    const rec = new TraceRecorder({ name: "add" });
    const { spy, sent } = spySends(rec);
    rec.start("add", null);
    const start = new Date(Date.now() - 250).toISOString();
    const span = { id: "s1", trace_id: "", parent_id: null, span_type: "llm_call", name: "llm", started_at: start, ended_at: new Date().toISOString() } as SpanData;
    rec.addSpan(span);
    const ended = sent().find(([e]) => e === "span_ended")![1];
    expect(ended.duration_ms as number).toBeGreaterThanOrEqual(240);
    spy.mockRestore();
    rec.end(undefined, TraceStatus.COMPLETED);
  });
});
