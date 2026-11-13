import { describe, it, expect, beforeEach } from "vitest";
import { readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { configure } from "./config.js";
import { TraceRecorder } from "./recorder.js";
import { SpanType } from "./trace.js";
import { writeGoldenCassette } from "./cassette.js";

beforeEach(() => configure({ apiKey: "rt_t", baseUrl: "http://127.0.0.1:1", enabled: true }));

function recordedRun(): TraceRecorder {
  const rec = new TraceRecorder({ name: "agent", input: { q: 1 } });
  const sb = rec.startSpan("call-model", SpanType.LLM_CALL, { p: "hi" }, "gpt-4");
  rec.endSpan(sb, "the answer");
  return rec;
}

describe("golden cassette", () => {
  it("toCassette() captures ordered steps in the contract shape", () => {
    const cas = recordedRun().toCassette();
    expect(cas.version).toBe(1);
    expect(cas.name).toBe("agent");
    expect(cas.steps).toHaveLength(1);
    expect(cas.steps[0]).toMatchObject({ index: 0, span_type: "llm_call", name: "call-model", model: "gpt-4", output: "the answer" });
    expect(typeof cas.recorded_at).toBe("string");
  });

  it("writeGoldenCassette writes the cassette + tolerance to a file", () => {
    const path = join(tmpdir(), `retrace-cas-${Date.now()}.json`);
    try {
      writeGoldenCassette(path, { recorder: recordedRun(), tolerance: { default: "exact", steps: { "call-model": "semantic" } } });
      const cas = JSON.parse(readFileSync(path, "utf-8"));
      expect(cas.version).toBe(1);
      expect(cas.steps[0].name).toBe("call-model");
      expect(cas.tolerance.steps["call-model"]).toBe("semantic");
    } finally {
      rmSync(path, { force: true });
    }
  });

  it("throws when no recorder is active", () => {
    expect(() => writeGoldenCassette("/tmp/never.json")).toThrow("no active recorder");
  });
});
