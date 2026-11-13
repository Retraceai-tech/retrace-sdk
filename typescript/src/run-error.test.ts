// A failed run stores why it failed (pass 7: a run that threw Error("boom") was stored as "failed"
// with no message).
import { describe, it, expect, vi, beforeAll } from "vitest";
import { configure } from "./config.js";
import { TraceRecorder, trace } from "./recorder.js";
import { TraceStatus } from "./trace.js";
import { describeError } from "./utils.js";

beforeAll(() => {
  vi.stubGlobal("fetch", vi.fn(async () => ({ ok: true, status: 200, text: async () => "" })));
  configure({ apiKey: "rt_testkey", transport: "http", baseUrl: "http://127.0.0.1:0" });
});

type Sent = [string, Record<string, unknown>];
function spyShared() {
  const t = (new TraceRecorder({ name: "probe" }) as unknown as { transport: { send: (e: string, d: Record<string, unknown>) => void } }).transport;
  const spy = vi.spyOn(t, "send");
  return { spy, ended: () => (spy.mock.calls as unknown as Sent[]).filter(([e]) => e === "trace_ended").map(([, d]) => d) };
}

describe("run error", () => {
  it("trace() records the thrown error (sync and async)", async () => {
    const { spy, ended } = spyShared();
    expect(() => trace(() => { throw new TypeError("bad input"); }, { name: "sync-fail" })()).toThrow("bad input");
    await expect(trace(async () => { throw new Error("boom"); }, { name: "async-fail" })()).rejects.toThrow("boom");
    await trace(async () => "ok", { name: "fine" })();
    const [a, b, c] = ended();
    expect(a).toMatchObject({ status: "failed", error: "TypeError: bad input" });
    expect(b).toMatchObject({ status: "failed", error: "Error: boom" });
    expect(c.error).toBeUndefined();
    spy.mockRestore();
  });

  it("an imperative end() takes the error, bounded", () => {
    const rec = new TraceRecorder({ name: "manual" });
    const t = (rec as unknown as { transport: { send: (e: string, d: Record<string, unknown>) => void } }).transport;
    const spy = vi.spyOn(t, "send");
    rec.start("manual");
    rec.end(undefined, TraceStatus.FAILED, { error: "x".repeat(5000) });
    const end = (spy.mock.calls as unknown as Sent[]).find(([e]) => e === "trace_ended")![1];
    expect((end.error as string).length).toBe(2000);
    spy.mockRestore();
  });

  it("describeError", () => {
    expect(describeError(undefined)).toBeUndefined();
    expect(describeError("plain")).toBe("plain");
    expect(describeError({ code: 1 })).toBe('{"code":1}');
    const e = new Error();
    expect(describeError(e)).toBe("Error");
  });
});
