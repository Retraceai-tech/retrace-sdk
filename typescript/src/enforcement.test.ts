import { describe, it, expect, vi, afterEach } from "vitest";
import { EnforcementGate, hashToolArgs } from "./enforcement.js";
import { RetraceEnforcementError } from "./errors.js";
import type { Config } from "./config.js";

function makeConfig(overrides: Partial<Config>): Config {
  return {
    apiKey: "rt_test",
    baseUrl: "http://localhost:3001",
    wsUrl: "",
    projectId: undefined,
    enabled: true,
    sampleRate: 1,
    sampleSeed: undefined,
    transport: "auto",
    strictReplay: false,
    allowRemoteReplay: false,
    serverEnforcement: false,
    enforcementHoldWaitSeconds: 0,
    ...overrides,
  };
}

function gate(overrides: Partial<Config> = {}): EnforcementGate {
  return new EnforcementGate(makeConfig(overrides), "run-test");
}

describe("EnforcementGate (client-side circuit breaker)", () => {
  it("is inactive (no-op) when no limits are configured", () => {
    const g = gate();
    expect(g.active).toBe(false);
    for (let i = 0; i < 1000; i++) g.recordAndCheck({ tokens: 10_000, usd: 100 });
  });

  it("trips the step ceiling after the limit", () => {
    const g = gate({ maxStepsPerRun: 3 });
    expect(g.active).toBe(true);
    g.recordAndCheck({}); // 1
    g.recordAndCheck({}); // 2
    g.recordAndCheck({}); // 3
    expect(() => g.recordAndCheck({})).toThrow(RetraceEnforcementError); // 4 > 3
  });

  it("accumulates tokens across calls", () => {
    const g = gate({ maxTokensPerRun: 100 });
    g.recordAndCheck({ tokens: 60 });
    expect(() => g.recordAndCheck({ tokens: 50 })).toThrow(/token ceiling/i); // 110 > 100
  });

  it("accumulates USD across calls", () => {
    const g = gate({ maxUsdPerRun: 1 });
    g.recordAndCheck({ usd: 0.5 });
    try {
      g.recordAndCheck({ usd: 0.7 }); // 1.2 > 1.0
      throw new Error("should have thrown");
    } catch (e) {
      expect(e).toBeInstanceOf(RetraceEnforcementError);
      expect((e as RetraceEnforcementError).verdict).toBe("block");
    }
  });

  it("enforces local limits with serverEnforcement disabled (no network)", () => {
    const g = gate({ maxStepsPerRun: 1 });
    g.recordAndCheck({});
    expect(() => g.recordAndCheck({})).toThrow(RetraceEnforcementError);
  });

  it("hashToolArgs is stable across key order and is short", () => {
    const a = hashToolArgs({ q: "weather", n: 3 });
    const b = hashToolArgs({ n: 3, q: "weather" });
    expect(a).toBe(b);
    expect(a).toHaveLength(32);
  });
});

describe("EnforcementGate server check", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("reports the span id, so the server counts the step once", async () => {
    const bodies: unknown[] = [];
    vi.stubGlobal("fetch", vi.fn(async (_url: string, init: { body: string }) => {
      bodies.push(JSON.parse(init.body));
      return new Response(JSON.stringify({ verdict: "allow" }), { status: 200 });
    }));
    const g = gate({ serverEnforcement: true });
    g.recordAndCheck({ tokens: 5, traceId: "t-1", spanId: "s-1" });
    await vi.waitFor(() => expect(bodies).toHaveLength(1));
    expect(bodies[0]).toEqual({ run_id: "run-test", proposed_tokens: 5, proposed_usd: 0, trace_id: "t-1", span_id: "s-1" });
  });
});
