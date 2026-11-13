// One span per tool call (pass 7: the app's own tool span + the SDK's record of the requested call were
// two tool_call spans), and tool results recorded once per trace, not once per process.
import { describe, it, expect, vi, beforeAll } from "vitest";
import { configure } from "./config.js";
import { TraceRecorder } from "./recorder.js";
import { SpanType, type SpanData } from "./trace.js";

beforeAll(() => {
  vi.stubGlobal("fetch", vi.fn(async () => ({ ok: true, status: 200, text: async () => "" })));
  configure({ apiKey: "rt_testkey", transport: "http", baseUrl: "http://127.0.0.1:0" });
});

type Sent = [string, Record<string, unknown>];
const spy = (rec: TraceRecorder) => vi.spyOn((rec as unknown as { transport: { send: (e: string, d: Record<string, unknown>) => void } }).transport, "send");
const span = (o: Partial<SpanData>): SpanData => ({ id: crypto.randomUUID(), trace_id: "", parent_id: null, span_type: SpanType.TOOL_CALL, name: "lookup_order", started_at: new Date().toISOString(), ...o } as SpanData);

describe("tool steps", () => {
  it("the app's span fills in the requested call instead of adding a second tool_call", () => {
    const rec = new TraceRecorder({ name: "tools" });
    const s = spy(rec);
    rec.start();
    const requested = span({ input: { order_id: "A-1" }, metadata: { tool_call_id: "call_1" }, ended_at: new Date().toISOString() });
    rec.addSpan(requested);
    const sb = rec.startSpan("lookup_order", SpanType.TOOL_CALL, { order_id: "A-1" });
    rec.endSpan(sb, { carrier: "DHL" });
    const calls = s.mock.calls as unknown as Sent[];
    expect(calls.filter(([e]) => e === "span_started")).toHaveLength(1);
    const ended = calls.filter(([e]) => e === "span_ended").map(([, d]) => d);
    expect(ended.at(-1)).toMatchObject({ id: requested.id, output: { carrier: "DHL" } });
    const other = rec.startSpan("lookup_order", SpanType.TOOL_CALL, { order_id: "B-2" });
    expect(other.id).not.toBe(requested.id);
    rec.end();
    s.mockRestore();
  });

  it("records a tool result once per trace, and in each trace that sees it", () => {
    const result = () => span({ span_type: SpanType.TOOL_RESULT, output: "{}", metadata: { tool_call_id: "call_same" } });
    const a = new TraceRecorder({ name: "a" });
    const s = spy(a);
    a.start();
    a.addSpan(result()); a.addSpan(result()); // the next model call re-sends the same history
    a.end();
    // A later run whose provider reused the call id: a process-wide dedupe dropped its result.
    const b = new TraceRecorder({ name: "b" });
    b.start();
    b.addSpan(result());
    b.end();
    const started = (s.mock.calls as unknown as Sent[]).filter(([e, d]) => e === "span_started" && d.span_type === SpanType.TOOL_RESULT).map(([, d]) => d.trace_id);
    expect(started).toEqual([a.traceId, b.traceId]);
    s.mockRestore();
  });
});
