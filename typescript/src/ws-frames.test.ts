import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { configure } from "./config.js";

// Drives WSTransport's real message listener with server frames through a controllable WebSocket
// double (the transport prefers globalThis.WebSocket and uses the DOM addEventListener API).
const { FrameSocket, sockets } = vi.hoisted(() => {
  const sockets: Array<{ emit(event: string, ev?: unknown): void; send: ReturnType<typeof vi.fn> }> = [];
  class FrameSocket {
    static OPEN = 1;
    readyState = 1;
    bufferedAmount = 0;
    private listeners: Record<string, Array<(ev?: unknown) => void>> = {};
    constructor(_url?: string) { sockets.push(this as never); }
    addEventListener(event: string, cb: (ev?: unknown) => void) { (this.listeners[event] ??= []).push(cb); }
    emit(event: string, ev?: unknown) { for (const cb of this.listeners[event] ?? []) cb(ev); }
    send = vi.fn();
    close = vi.fn();
  }
  return { FrameSocket, sockets };
});
vi.mock("ws", () => ({ default: FrameSocket }));

beforeEach(() => {
  configure({ apiKey: "rt_test", baseUrl: "http://localhost:3001", enabled: true });
  vi.stubGlobal("WebSocket", FrameSocket);
  sockets.length = 0;
});
afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

async function connectedTransport() {
  const { WSTransport } = await import("./transport.js");
  const t = new WSTransport();
  t.connect();
  // The socket appears after a dynamic import("ws"); under a loaded parallel run that can exceed the 1 s default.
  await vi.waitFor(() => expect(sockets.length).toBe(1), { timeout: 5000 });
  const socket = sockets[0];
  socket.emit("open");
  socket.emit("message", { data: JSON.stringify({ type: "auth_ok" }) });
  return { t, socket };
}

describe("WSTransport server frames", () => {
  it("per-frame acks and the account's live feed are recognised, not reported as unknown", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { t, socket } = await connectedTransport();
    for (const type of ["ack", "trace_started", "span_started", "span_ended", "trace_ended"]) {
      socket.emit("message", { data: JSON.stringify({ type, id: "x", data: { trace_id: "t" } }) });
    }
    expect(warn).not.toHaveBeenCalled();
    t.close();
  });

  it("a genuinely unknown frame type is still surfaced (never silently swallowed)", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { t, socket } = await connectedTransport();
    socket.emit("message", { data: JSON.stringify({ type: "zzz_future_type" }) });
    expect(warn).toHaveBeenCalledOnce();
    expect(String(warn.mock.calls[0][0])).toContain("zzz_future_type");
    t.close();
  });
});
