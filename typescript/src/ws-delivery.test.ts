import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { configure } from "./config.js";

// Confirmed delivery over WSTransport (pass-5 P1). Writing a frame to the socket used to count as
// delivered; short scripts exited before the server had read their frames and lost the run. Every
// frame now carries a seq, the server acks it, and flush/exit wait for the acks.
const { AckSocket, sockets } = vi.hoisted(() => {
  const sockets: AckSocketT[] = [];
  type AckSocketT = InstanceType<typeof AckSocket>;
  class AckSocket {
    static OPEN = 1;
    readyState = 1;
    bufferedAmount = 0;
    sent: Array<Record<string, unknown>> = [];
    closed = false;
    /** Ack each ingest frame automatically (after `ackDelayMs`) — the server's processing time. */
    autoAck = true;
    ackDelayMs = 0;
    private listeners: Record<string, Array<(ev?: unknown) => void>> = {};
    constructor(_url?: string) { sockets.push(this); }
    addEventListener(event: string, cb: (ev?: unknown) => void) { (this.listeners[event] ??= []).push(cb); }
    emit(event: string, ev?: unknown) { for (const cb of this.listeners[event] ?? []) cb(ev); }
    send(raw: string) {
      const m = JSON.parse(raw) as { type: string; seq?: number; data?: { id?: string } };
      this.sent.push(m);
      if (m.type === "auth") { setTimeout(() => this.emit("message", { data: JSON.stringify({ type: "auth_ok" }) }), 0); return; }
      if (this.autoAck && typeof m.seq === "number") {
        setTimeout(() => this.emit("message", { data: JSON.stringify({ type: "ack", id: m.data?.id, seq: m.seq }) }), this.ackDelayMs);
      }
    }
    /** The server end drops the connection. */
    drop() { this.readyState = 3; this.emit("close"); }
    close() { this.closed = true; this.readyState = 3; this.emit("close"); }
  }
  return { AckSocket, sockets };
});
vi.mock("ws", () => ({ default: AckSocket }));

let fetchMock: ReturnType<typeof vi.fn>;
beforeEach(() => {
  configure({ apiKey: "rt_test", baseUrl: "http://localhost:3001", enabled: true, flushTimeoutMs: 2000 });
  vi.stubGlobal("WebSocket", AckSocket);
  fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200 });
  vi.stubGlobal("fetch", fetchMock);
  sockets.length = 0;
});
// Every transport a test opens is closed afterwards, so none reconnects into a later test.
const opened: Array<{ close(): void }> = [];
afterEach(() => {
  for (const t of opened.splice(0)) t.close();
  vi.unstubAllGlobals(); vi.restoreAllMocks(); configure({ flushTimeoutMs: 5000 });
});

const ingest = (s: InstanceType<typeof AckSocket>) => s.sent.filter((m) => m.type !== "auth");
const traceCalls = () => fetchMock.mock.calls.filter(([u]) => String(u).includes("/api/v1/traces"));

function sendRun(t: { send(e: string, d: Record<string, unknown>): void }, spans = 3, id = "t1") {
  t.send("trace_started", { id, name: "run", status: "running", started_at: "2026-10-01T10:00:00Z" });
  for (let i = 0; i < spans; i++) {
    const sid = `${id}-s${i}`;
    t.send("span_started", { id: sid, trace_id: id, name: "search", span_type: "tool_call", started_at: "2026-10-01T10:00:00Z" });
    t.send("span_ended", { id: sid, trace_id: id, ended_at: "2026-10-01T10:00:01Z", output: "x" });
  }
  t.send("trace_ended", { id, ended_at: "2026-10-01T10:00:02Z", status: "failed", output: "gave up", span_count: spans });
}

async function connected() {
  const { WSTransport } = await import("./transport.js");
  const t = new WSTransport();
  opened.push(t);
  t.connect();
  await vi.waitFor(() => expect(sockets.length).toBe(1), { timeout: 5000 });
  sockets[0].emit("open");
  await vi.waitFor(() => expect(t.isConnected).toBe(true));
  return { t, socket: sockets[0] };
}

describe("WSTransport confirmed delivery", () => {
  it("asks for acks, numbers frames, and flushOnExit waits for every ack before closing", async () => {
    const { t, socket } = await connected();
    socket.ackDelayMs = 150; // the server is still ingesting when the last frame is written
    sendRun(t);
    expect(t.hasPendingData()).toBe(true);
    await t.flushOnExit();
    expect(socket.sent[0]).toMatchObject({ type: "auth", acks: true });
    expect(ingest(socket).map((m) => m.seq)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    expect(t.hasPendingData()).toBe(false);
    expect(socket.closed).toBe(true);
    expect(traceCalls()).toHaveLength(0);
  });

  it("re-sends what a dropped socket never confirmed, in order, on the next connection", async () => {
    const { t, socket } = await connected();
    socket.autoAck = false;
    sendRun(t, 1); // 4 frames, none confirmed
    socket.drop();
    await vi.waitFor(() => expect(sockets.length).toBe(2), { timeout: 5000 });
    const second = sockets[1];
    second.emit("open");
    await t.flushOnExit();
    expect(ingest(second).map((m) => m.seq)).toEqual([1, 2, 3, 4]);
    expect(t.hasPendingData()).toBe(false);
    expect(traceCalls()).toHaveLength(0);
  });

  it("sends frames a dead socket never confirmed over HTTP at exit", async () => {
    configure({ flushTimeoutMs: 200 });
    const { t, socket } = await connected();
    socket.autoAck = false;
    sendRun(t, 2);
    socket.drop(); // gone, and no reconnect will succeed in time
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await t.flushOnExit();
    const posts = traceCalls().filter(([, o]) => o.method === "POST");
    expect(posts).toHaveLength(1);
    const body = JSON.parse(posts[0][1].body);
    expect(body).toMatchObject({ id: "t1", status: "failed", output: "gave up" });
    expect(body.spans).toHaveLength(2);
    expect(warn.mock.calls.flat().join(" ")).not.toContain("INCOMPLETE");
  });

  it("reports events still unconfirmed on a live socket instead of assuming they arrived", async () => {
    configure({ flushTimeoutMs: 200 });
    const { t, socket } = await connected();
    socket.autoAck = false;
    sendRun(t, 1);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await t.flushOnExit();
    expect(warn.mock.calls.flat().join(" ")).toContain("4 event(s) of 1 trace(s) were sent but not confirmed");
    expect(traceCalls()).toHaveLength(0); // still on the server's socket: no duplicate over HTTP
    expect(t.hasPendingData()).toBe(false); // nothing left for the exit hook to loop on
  });

  it("reports an undeliverable capture as INCOMPLETE", async () => {
    fetchMock.mockResolvedValue({ ok: false, status: 503 });
    const { WSTransport } = await import("./transport.js");
    const t = new WSTransport(); // never connects
    opened.push(t);
    sendRun(t, 1);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    configure({ flushTimeoutMs: 50 });
    await t.flushOnExit(200);
    expect(warn.mock.calls.flat().join(" ")).toMatch(/capture INCOMPLETE: 4 event\(s\) of 1 trace\(s\).*t1/);
  });

  it("matches acks without a seq (older servers) to the oldest frame with that id", async () => {
    const { t, socket } = await connected();
    socket.autoAck = false;
    t.send("span_started", { id: "s1", trace_id: "t1", name: "a", span_type: "tool_call", started_at: "2026-10-01T10:00:00Z" });
    t.send("span_ended", { id: "s1", trace_id: "t1", ended_at: "2026-10-01T10:00:01Z" });
    socket.emit("message", { data: JSON.stringify({ type: "ack", id: "s1" }) });
    socket.emit("message", { data: JSON.stringify({ type: "ack", id: "s1" }) });
    expect(t.hasPendingData()).toBe(false);
    t.close();
  });

  it("closes a socket that cannot be unref'd once idle with everything confirmed, and reconnects on demand", async () => {
    const { t, socket } = await connected(); // the double has no `_socket` (like the global WebSocket)
    sendRun(t, 1);
    await vi.waitFor(() => expect(socket.closed).toBe(true), { timeout: 3000 });
    t.send("trace_started", { id: "t2", name: "next", status: "running", started_at: "2026-10-01T10:00:00Z" });
    await vi.waitFor(() => expect(sockets.length).toBe(2), { timeout: 5000 });
    t.close();
  });
});

describe("TraceRecorder wire fields", () => {
  // A fresh module graph on the HTTP transport, so the uploaded body shows exactly what was sent.
  async function httpRecorder() {
    vi.resetModules();
    const cfg = await import("./config.js");
    cfg.configure({ apiKey: "rt_test", baseUrl: "http://localhost:3001", enabled: true, transport: "http" });
    return { ...(await import("./recorder.js")), ...(await import("./trace.js")) };
  }
  const uploaded = async () => {
    await vi.waitFor(() => expect(traceCalls().length).toBeGreaterThan(0));
    return JSON.parse(traceCalls()[0][1].body) as Record<string, unknown> & { spans: unknown[] };
  };

  it("trace(fn, { sessionId }) keeps the session (it was dropped before the recorder was built)", async () => {
    const { trace } = await httpRecorder();
    const run = trace(async (n: unknown) => (n as number) * 2, { name: "chat", sessionId: "sess-1" });
    expect(await run(2)).toBe(4);
    const body = await uploaded();
    expect(body).toMatchObject({ name: "chat", session_id: "sess-1", output: 4, span_count: 0 });
  });

  it("trace_ended reports span_count for startSpan() and addSpan() emissions", async () => {
    const { TraceRecorder, SpanType } = await httpRecorder();
    const rec = new TraceRecorder({ name: "run" });
    rec.start("run");
    rec.endSpan(rec.startSpan("search", SpanType.TOOL_CALL, { q: 1 }), "x");
    const now = new Date().toISOString();
    rec.addSpan({ id: crypto.randomUUID(), trace_id: rec.traceId, span_type: SpanType.TOOL_CALL, name: "lookup", started_at: now, ended_at: now } as never);
    rec.end("done");
    const body = await uploaded();
    expect(body.span_count).toBe(2);
    expect(body.spans).toHaveLength(2);
  });
});
