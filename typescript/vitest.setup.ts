import { vi } from "vitest";

// Unit tests must never open a real network socket. Node ships a global WebSocket (undici) that the
// transport prefers; left unstubbed, any test that configure()s a loopback base URL and records a
// trace opens a real WS connection that fails asynchronously and surfaces as an *unhandled* error —
// failing the run even when every assertion passes. Install a no-op double that never transitions to
// OPEN, so the transport stays in its buffering / HTTP-fallback path with zero real I/O. Tests that
// assert socket behaviour (transport.test.ts) install their own tracked double in beforeEach, which
// takes precedence; tests that drive signals directly (server-signal.test.ts) never connect at all.
class NoopWebSocket {
  static readonly OPEN = 1;
  readyState = 0;
  bufferedAmount = 0;
  constructor(_url?: string) {}
  addEventListener(): void {}
  removeEventListener(): void {}
  send(): void {}
  close(): void {
    this.readyState = 3;
  }
}

vi.stubGlobal("WebSocket", NoopWebSocket);
