import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { configure } from "./config.js";

// The production transport prefers globalThis.WebSocket and consumes the DOM addEventListener API, so
// the test double implements THAT interface (not the `ws` package's .on()) and is installed as the
// global WebSocket. It never transitions to OPEN — simulating an unreachable endpoint — so connect()
// stays pending and the close()/HTTP-fallback paths can be exercised with no real socket.
const { MockWebSocket, mockWsInstances } = vi.hoisted(() => {
  const instances: Array<Record<string, unknown>> = [];
  class MockWebSocket {
    static OPEN = 1;
    readyState = 0;            // CONNECTING — never fires "open"
    bufferedAmount = 0;
    private listeners: Record<string, Array<(ev?: unknown) => void>> = {};
    constructor(_url?: string) { instances.push(this as unknown as Record<string, unknown>); }
    addEventListener(event: string, cb: (ev?: unknown) => void) { (this.listeners[event] ??= []).push(cb); }
    send = vi.fn();
    close = vi.fn(function (this: MockWebSocket) {
      this.readyState = 3;     // CLOSED
      for (const cb of (this.listeners["close"] ?? [])) cb();
    });
  }
  return { MockWebSocket, mockWsInstances: instances };
});
// Older runtimes without a global WebSocket resolve the same double via the optional `ws` import.
vi.mock("ws", () => ({ default: MockWebSocket }));

beforeEach(() => {
  configure({ apiKey: "rt_test", baseUrl: "http://localhost:3001", enabled: true });
  // Force resolveWebSocketCtor onto the controllable double so no real undici socket is opened to
  // localhost during the unit test (Node ships a global WebSocket, which the transport prefers).
  vi.stubGlobal("WebSocket", MockWebSocket);
  mockWsInstances.length = 0;
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Auto transport fallback on early close()", () => {
  it("flushes to HTTP when close() is called before WS connects", async () => {
    const mockFetch = vi.fn().mockResolvedValue({ ok: true });
    vi.stubGlobal("fetch", mockFetch);

    const { createTransport } = await import("./transport.js");
    const transport = createTransport("auto");

    // Send trace events immediately (before WS could connect)
    transport.send("trace_started", { id: "t1", name: "fast-trace", status: "running", started_at: "2026-01-01T00:00:00Z", total_tokens: 0, total_cost: 0, total_duration_ms: 0 });
    transport.send("span_started", { id: "s1", trace_id: "t1", span_type: "llm_call", name: "call", started_at: "2026-01-01T00:00:00Z" });
    transport.send("span_ended", { id: "s1", ended_at: "2026-01-01T00:00:01Z", output: "response" });
    transport.send("trace_ended", { id: "t1", ended_at: "2026-01-01T00:00:01Z", status: "completed" });

    // Close immediately — WS has NOT connected yet
    transport.close();

    // Should have fallen back to HTTP and flushed the trace exactly once. Filter to the trace-upload
    // endpoint: configure() also fires an always-on anonymous telemetry ping (POST
    // /api/v1/internal/logs) whose deferred dynamic-import lands on this same mock, so asserting the
    // TOTAL fetch count would spuriously see 2. Scope to the endpoint under test.
    const traceCalls = mockFetch.mock.calls.filter(([u]) => String(u).endsWith("/api/v1/traces"));
    expect(traceCalls).toHaveLength(1);
    const [url, opts] = traceCalls[0];
    expect(url).toBe("http://localhost:3001/api/v1/traces");
    const body = JSON.parse(opts.body);
    expect(body.id).toBe("t1");
    expect(body.name).toBe("fast-trace");
    expect(body.spans).toHaveLength(1);
    expect(body.spans[0].output).toBe("response");
  });

  it("does NOT reconnect after intentional close()", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: true }));

    const { WSTransport } = await import("./transport.js");
    const ws = new WSTransport();
    ws.connect();

    // connect() resolves the WebSocket constructor via a dynamic import("ws") (async, for
    // edge-runtime compat), so the socket is created on a later microtask. Poll for it instead of a
    // fixed sleep: under a loaded parallel test run the first import can take longer than 50ms.
    await vi.waitFor(() => expect(mockWsInstances.length).toBeGreaterThan(0), { timeout: 5000 });

    // Should have created a WS instance
    expect(mockWsInstances.length).toBeGreaterThan(0);
    const instanceCount = mockWsInstances.length;

    // Close intentionally
    ws.close();

    // Trigger the "close" event that was registered — already fired by our mock's close()
    // Wait a tick for any setTimeout reconnect to fire
    await new Promise(r => setTimeout(r, 1100));

    // Should NOT have created new WS instances (no reconnect)
    expect(mockWsInstances.length).toBe(instanceCount);
  });
});
