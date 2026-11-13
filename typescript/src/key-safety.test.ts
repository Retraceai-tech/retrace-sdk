import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { configure, assertSecureWsUrl } from "./config.js";
import { openAIOutputText, anthropicOutputText } from "./interceptors/tool-spans.js";

// Pass 8 (TS): the key followed fetch redirects to any host, an explicit ws:// URL was accepted for a
// public host, a tool-calling LLM span stored an empty output, there was no redaction hook, and a
// full send queue evicted trace lifecycle frames first.

describe("API key safety", () => {
  it("refuses a cleartext WebSocket URL for a public host", () => {
    expect(() => assertSecureWsUrl("ws://evil.example")).toThrow(/insecure WebSocket URL/);
    expect(() => configure({ apiKey: "rt_testkeytestkey", baseUrl: "https://api.retraceai.tech", wsUrl: "ws://evil.example" })).toThrow(/insecure WebSocket URL/);
    expect(() => assertSecureWsUrl("wss://api.retraceai.tech")).not.toThrow();
    expect(() => assertSecureWsUrl("ws://localhost:3001")).not.toThrow();
    configure({ wsUrl: "wss://api.retraceai.tech" });
  });

  it("every fetch that carries the key refuses redirects", () => {
    for (const file of ["transport.ts", "golden.ts", "enforcement.ts", "resume.ts"]) {
      const src = readFileSync(fileURLToPath(new URL(`./${file}`, import.meta.url)), "utf8");
      const calls = [...src.matchAll(/fetch\(([\s\S]*?)\}\);/g)].map((m) => m[1]).filter((c) => /headers/.test(c));
      expect(calls.length, file).toBeGreaterThan(0);
      for (const c of calls) expect(c, `${file}: ${c.slice(0, 60)}`).toMatch(/redirect: "error"/);
    }
  });
});

describe("tool-calling LLM output", () => {
  it("summarizes the requested tools when the turn has no text", () => {
    expect(openAIOutputText({ content: null, tool_calls: [{ function: { name: "get_weather", arguments: "{\"city\":\"Paris\"}" } }] })).toBe('[tool calls] get_weather({"city":"Paris"})');
    expect(openAIOutputText({ content: "hi" })).toBe("hi");
    expect(anthropicOutputText([{ type: "tool_use", name: "lookup", input: { q: "x" } }])).toBe('[tool calls] lookup({"q":"x"})');
    expect(anthropicOutputText([{ type: "text", text: "a" }, { type: "tool_use", name: "n" }, { type: "text", text: "b" }])).toBe("a\nb");
  });
});

describe("beforeSend and the send queue", () => {
  const sent: Array<[string, Record<string, unknown>]> = [];
  beforeEach(() => {
    sent.length = 0;
    vi.resetModules();
    vi.doMock("./transport.js", async (orig) => ({
      ...(await orig() as object),
      createTransport: () => ({ send: (t: string, d: Record<string, unknown>) => sent.push([t, d]), close: vi.fn(), flush: vi.fn(), hasPendingData: () => false }),
    }));
  });
  afterEach(async () => {
    vi.doUnmock("./transport.js");
    (await import("./config.js")).configure({ beforeSend: undefined });
  });

  it("redacts and drops events before they leave the process, and fails closed", async () => {
    const { TraceRecorder } = await import("./recorder.js");
    const { configure } = await import("./config.js"); // the module instance the recorder sees
    configure({ apiKey: "rt_testkeytestkey", baseUrl: "https://api.retraceai.tech", enabled: true,
      beforeSend: (type, data) => (type === "trace_ended" ? null : { ...data, input: "[redacted]" }) });
    const rec = new TraceRecorder({ name: "t", input: { password: "hunter2" } });
    rec.start("t", { password: "hunter2" });
    rec.end("done");
    expect(sent.map(([t]) => t)).toEqual(["trace_started"]);
    expect(JSON.stringify(sent)).not.toContain("hunter2");

    sent.length = 0;
    configure({ beforeSend: () => { throw new Error("bug"); } });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const rec2 = new TraceRecorder({ name: "t2" });
    rec2.start("t2", "secret");
    expect(sent).toHaveLength(0);
    warn.mockRestore();
  });

  it("a full queue evicts span events before trace lifecycle events", async () => {
    vi.doUnmock("./transport.js");
    const { WSTransport } = await import("./transport.js");
    const { configure } = await import("./config.js");
    configure({ apiKey: "rt_testkeytestkey", baseUrl: "https://api.retraceai.tech", enabled: true });
    const t = new WSTransport() as unknown as { enqueue(i: object): void; queue: Array<{ eventType: string }> };
    t.enqueue({ eventType: "trace_started", traceId: "t1", data: "{}" });
    for (let i = 0; i < 998; i++) t.enqueue({ eventType: "span_started", traceId: "t1", spanId: `s${i}`, data: "{}" });
    t.enqueue({ eventType: "trace_ended", traceId: "t1", data: "{}" });
    t.enqueue({ eventType: "span_started", traceId: "t1", spanId: "late", data: "{}" }); // overflow
    expect(t.queue[0].eventType).toBe("trace_started");
    expect(t.queue.filter((q) => q.eventType === "trace_ended")).toHaveLength(1);
    (t as unknown as { close?: () => void }).close?.();
  });
});
