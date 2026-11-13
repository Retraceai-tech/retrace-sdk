import { getConfig } from "./config.js";
import { classifyServerSignal, type RetraceServerSignal } from "./errors.js";
import { SDK_VERSION } from "./version.js";

// Client identifier sent on every request so the backend can attribute SDK usage/version.
const CLIENT_ID = `typescript-sdk/${SDK_VERSION}`;

// ─── Runtime-agnostic WebSocket ──────────────────────────────────────────────
// On Node, prefer the OPTIONAL `ws` package (installed by default): its socket can be unref'd, so an
// open connection never keeps a finished script alive. Node's global WebSocket cannot be unref'd —
// with it, the default quickstart printed its last line and then never exited (pass 5). Everywhere
// else (Bun, Deno, browsers, edge), and on Node without `ws`, use the global Web `WebSocket`; a socket
// that can't be unref'd is closed once idle (see WSTransport.scheduleIdleClose). Both expose the
// standard `addEventListener`/`send`/`close`/`readyState`/`bufferedAmount` surface used here.
const WS_OPEN = 1; // CONNECTING=0, OPEN=1, CLOSING=2, CLOSED=3 (web standard; `ws` matches)

interface WSLikeEvent { data?: unknown }
interface WebSocketLike {
  readonly readyState: number;
  readonly bufferedAmount: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open" | "message" | "close" | "error", listener: (ev: WSLikeEvent) => void): void;
}
type WebSocketCtor = new (url: string) => WebSocketLike;

/** Plain Node (not Bun / Deno, which also define process.versions.node). */
function isPlainNode(): boolean {
  const p = (globalThis as { process?: { versions?: Record<string, string | undefined> } }).process;
  return typeof p?.versions?.node === "string" && !p.versions.bun && !("Deno" in globalThis);
}

let _wsCtor: Promise<WebSocketCtor | null> | null = null;
function resolveWebSocketCtor(): Promise<WebSocketCtor | null> {
  if (_wsCtor) return _wsCtor;
  const globalWS = (globalThis as { WebSocket?: WebSocketCtor }).WebSocket;
  const fallback = typeof globalWS === "function" ? globalWS : null;
  if (!isPlainNode()) {
    _wsCtor = Promise.resolve(fallback);
    return _wsCtor;
  }
  // The variable specifier + ignore comments stop bundlers from statically resolving an optional dep.
  const pkg = ["w", "s"].join("");
  _wsCtor = import(/* webpackIgnore: true */ /* @vite-ignore */ pkg)
    .then((m: unknown) => {
      const mod = m as { default?: WebSocketCtor } & WebSocketCtor;
      return ((mod.default ?? mod) as WebSocketCtor) || fallback;
    })
    .catch(() => fallback); // `ws` not installed → the global WebSocket (or none: HTTP fallback)
  return _wsCtor;
}

// Normalize a WS message payload to a string across runtimes: the global WebSocket delivers text
// frames as a string; `ws` delivers a Buffer; ArrayBuffer/typed-array are handled defensively.
function wsDataToString(data: unknown): string {
  if (typeof data === "string") return data;
  if (data instanceof ArrayBuffer) return new TextDecoder().decode(data);
  if (ArrayBuffer.isView(data)) return new TextDecoder().decode(data as ArrayBufferView);
  return String((data as { toString?: () => string })?.toString?.() ?? data);
}

export interface Transport {
  send(eventType: string, data: Record<string, unknown>): void;
  close(): void;
  /** Drain in-flight data to the network (awaited on graceful shutdown). */
  flush(): Promise<void>;
  /** Exit/signal path: drain via the most reliable channel for teardown (HTTP one-shot for
   *  buffered events), bounded by budgetMs. Falls back to flush() when not implemented. */
  flushOnExit?(budgetMs?: number): Promise<void>;
  /** True if there is anything worth flushing (buffered events or an unsent in-flight payload).
   *  Lets the exit path no-op — no fetch, no delay — for a zero-event run. */
  hasPendingData?(): boolean;
  /** Server-signal channel: a STRUCTURED signal (code/retryable/fatal), not a raw string — branch
   *  on `signal.code`, never string-match `signal.message`. */
  onError?: (signal: RetraceServerSignal) => void;
}

interface QueuedEvent {
  /** Frame sequence number; the server echoes it on the frame's `ack`. */
  seq: number;
  eventType: string;
  data: Record<string, unknown>;
  traceId?: string;
  spanId?: string;
}

/** How long a socket that cannot be unref'd may sit idle (everything confirmed) before it is closed. */
const IDLE_CLOSE_MS = 1000;
/** Bound on frames written but not yet confirmed (parity with the 1000-event send buffer). */
const UNACKED_CAP = 1000;

/**
 * Server frames an ingesting SDK receives but has no use for. `ack` confirms a frame was accepted; the
 * rest are the account's live feed (another connection's run), meant for dashboards. Servers from API
 * 1.28.22 on send neither to SDK sockets unless asked; older servers send both unconditionally.
 */
const INFORMATIONAL_SERVER_TYPES = new Set(["ack", "trace_started", "span_started", "span_ended", "trace_ended"]);

/** A server → SDK WebSocket frame. */
interface ServerFrame { type: string; error?: unknown; data?: Record<string, unknown> }

/**
 * WebSocket transport. Delivery is CONFIRMED, not assumed: every frame carries a sequence number,
 * the auth frame asks for acks, and a frame written to the socket stays unconfirmed until the server
 * acks it. flush()/flushOnExit() wait for the acks; a reconnect re-sends what the dead socket never
 * confirmed; at exit, frames still unconfirmed go over HTTP (dead socket) or are reported (live
 * socket). Writing to the socket used to count as delivered, and short scripts lost their spans when
 * the process exited before the server had read them.
 */
export class WSTransport implements Transport {
  private ws: WebSocketLike | null = null;
  private connecting = false;
  private connected = false;
  private closed = false;
  private backoff = 1000;
  private queue: QueuedEvent[] = [];
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private seq = 0;
  // Written to a socket, not yet acked (seq → frame, in send order).
  private unacked = new Map<number, { item: QueuedEvent; raw: string }>();
  // The current socket was unref'd (so it never keeps the process alive); otherwise idle-close it.
  private socketUnrefed = false;
  private idleTimer: ReturnType<typeof setTimeout> | null = null;
  private lastSendAt = 0;
  // F-DL2: integrity tracking for the bounded offline buffer.
  private lossyTraces = new Set<string>();        // traces that lost ≥1 event to a buffer drop
  private droppedOpenSpanIds = new Set<string>(); // open spans dropped → their later close is a no-op
  private droppedTotal = 0;
  private lastDropWarnMs = 0;
  // Throttle for the default (no-callback) server-signal warning, keyed per code, so a
  // rate_limited storm can't spew thousands of lines (same lesson as the drop-warn).
  private lastSignalWarnMs = new Map<string, number>();
  onError?: (signal: RetraceServerSignal) => void;
  /** Called once each time the socket authenticates (used by the auto transport to drain its buffer). */
  onConnected?: () => void;

  get isConnected() { return this.connected; }

  /** Whether a trace lost events to a buffer drop (so the API/replay can refuse byte-replay). */
  isTraceLossy(traceId: string) { return this.lossyTraces.has(traceId); }

  connect() {
    if (this.closed || this.ws || this.connecting) return;
    this.connecting = true;
    const cfg = getConfig();
    const url = `${cfg.wsUrl}/ws/v1/stream`;
    void resolveWebSocketCtor().then((Ctor) => {
      this.connecting = false;
      // No WebSocket implementation (edge without a global, or `ws` not installed), or we were
      // closed/connected while resolving → bail. In "auto" mode the fallback timer switches to HTTP.
      if (!Ctor || this.closed || this.ws) return;
      let socket: WebSocketLike;
      try {
        socket = new Ctor(url);
      } catch {
        return; // construction unsupported on this runtime → leave disconnected (HTTP fallback)
      }
      this.ws = socket;
      this.socketUnrefed = false;

      socket.addEventListener("open", () => {
        // Unref the underlying Node socket (the `ws` package) so a short-lived script can exit once
        // its work is done; graceful shutdown still drains via flush()/beforeExit. A socket without
        // `_socket` (a global WebSocket) is closed once idle instead (scheduleIdleClose).
        const raw = (socket as unknown as { _socket?: { unref?: () => void } })?._socket;
        if (typeof raw?.unref === "function") { raw.unref(); this.socketUnrefed = true; }
        socket.send(JSON.stringify({ type: "auth", api_key: getConfig().apiKey, resumable: getConfig().allowRemoteReplay, replay_generation: 1, acks: true }));
      });

      socket.addEventListener("message", (ev) => {
        try {
          this.handleServerFrame(socket, JSON.parse(wsDataToString(ev.data)));
        } catch (e) {
          // A malformed/unparseable frame must not take down the listener.
          this.throttledSignalWarn("parse", `[retrace] dropped an unparseable server frame: ${(e as Error)?.message ?? e}`);
        }
      });

      socket.addEventListener("close", () => {
        if (this.ws !== socket) return; // an idle-closed or superseded socket
        this.connected = false;
        this.ws = null;
        if (!this.closed) {
          this.reconnectTimer = setTimeout(() => this.reconnect(), this.backoff * (0.5 + Math.random() * 0.5));
          // Don't let the reconnect timer keep the event loop (and the process) alive.
          (this.reconnectTimer as unknown as { unref?: () => void })?.unref?.();
          this.backoff = Math.min(this.backoff * 2, 30000);
        }
      });

      socket.addEventListener("error", () => {
        socket.close();
      });
    });
  }

  /** Act on one parsed server frame (split from connect() so the socket wiring stays readable). */
  private handleServerFrame(socket: WebSocketLike, frame: unknown) {
    const candidate = frame as Partial<ServerFrame> | null;
    if (!candidate || typeof candidate.type !== "string") {
      this.warnUnknownType(candidate?.type);
      return;
    }
    const msg = candidate as ServerFrame;
    if (msg.type === "auth_ok") {
      this.connected = true;
      this.backoff = 1000;
      this.resendUnconfirmed();
      this.flushQueue();
      // Never let the listener break the socket loop — but don't swallow it either (throttled warn).
      try { this.onConnected?.(); } catch (err) {
        this.throttledSignalWarn("onConnected", `[retrace] onConnected listener threw: ${(err as Error)?.message ?? err}`);
      }
    } else if (msg.type === "ping") {
      socket.send(JSON.stringify({ type: "pong" }));
    } else if (msg.type === "ack") {
      this.confirm(frame as { id?: unknown; seq?: unknown });
    } else if (INFORMATIONAL_SERVER_TYPES.has(msg.type)) {
      // Known frames this SDK has no use for: per-frame delivery `ack`s and the account's live feed
      // (sent by servers that predate opt-in acks / live events). Not unknown, so no warning.
    } else if (msg.type === "error") {
      this.surfaceSignal(classifyServerSignal("error", String(msg.error ?? "")));
    } else if (msg.type === "resume" || msg.type === "replay") {
      // C2: server-initiated re-execution invokes the user's registered function in THIS process
      // with server-supplied input/args/cassette. Off by default — only act on these frames when
      // the developer explicitly opted in, so a compromised/misrouted server (or an injected
      // frame) can't drive code execution here.
      if (!getConfig().allowRemoteReplay) {
        this.throttledSignalWarn(
          `remote_blocked:${msg.type}`,
          `[retrace] ignoring server "${msg.type}" command — remote-controlled re-execution is disabled. Set allowRemoteReplay:true (or RETRACE_ALLOW_REMOTE_REPLAY=true) to opt in.`,
        );
        return;
      }
      if (msg.type === "resume") {
        import("./resume.js").then(({ parseResumeMessage, handleResume }) => {
          const cmd = parseResumeMessage(msg);
          if (cmd) handleResume(cmd);
        });
      } else {
        import("./replay.js").then(({ parseReplayMessage, handleReplay }) => {
          const cmd = parseReplayMessage(msg);
          if (cmd) handleReplay(cmd);
        });
      }
    } else if (msg.type === "halt") {
      const reason = (typeof msg.data?.reason === "string" && msg.data.reason) || "Guardrail triggered";
      const signal = classifyServerSignal("halt", reason);
      // halt CHANGES behavior, deliberately: flush what's already recorded, then STOP recording
      // (close the transport). This is a hard server directive, distinct from credits_exhausted
      // (which leaves recording alive so the user can swap keys / upgrade mid-run). Surfaced as a
      // fatal signal — never a silent dark-out.
      this.surfaceSignal(signal);
      void this.flush().finally(() => this.close());
    } else {
      // DEFAULT: an unhandled/unknown server message type must not be silently swallowed (the
      // F-P6 class). Throttled-warn so a future message type surfaces instead of vanishing.
      this.warnUnknownType(msg.type);
    }
  }

  private reconnect() {
    if (!this.closed && !this.connected && !this.ws) this.connect();
  }

  private flushQueue() {
    while (this.queue.length && this.connected) {
      this.transmit(this.queue.shift()!);
    }
  }

  /** Serialize + send a single event, stamping trace-level `lossy` on a trace_ended whose trace
   *  lost events to a buffer drop (so the server/replay can refuse byte-deterministic replay). The
   *  frame stays unconfirmed until the server acks its seq. */
  private transmit(item: QueuedEvent) {
    if (item.eventType === "trace_ended" && item.traceId && this.lossyTraces.has(item.traceId)) {
      item.data.lossy = true;
    }
    const raw = JSON.stringify({ type: item.eventType, data: item.data, seq: item.seq });
    this.unacked.set(item.seq, { item, raw });
    while (this.unacked.size > UNACKED_CAP) {
      const [oldest, evicted] = this.unacked.entries().next().value as [number, { item: QueuedEvent }];
      this.unacked.delete(oldest);
      if (evicted.item.traceId) this.lossyTraces.add(evicted.item.traceId);
    }
    this.write(raw);
    this.lastSendAt = Date.now();
    this.scheduleIdleClose();
  }

  /** Write to the current socket; a failed write leaves the frame unconfirmed (re-sent on reconnect). */
  private write(raw: string) {
    try { this.ws?.send(raw); } catch { /* socket died mid-write: its close handler reconnects */ }
  }

  /** After a reconnect: re-send, in order, every frame the previous socket never confirmed. The server
   *  ingests a re-sent frame idempotently (stored span ids / a started trace are not duplicated). */
  private resendUnconfirmed() {
    for (const { raw } of this.unacked.values()) this.write(raw);
  }

  /** A server ack: matched by the echoed seq; servers that do not echo it ack by id in ingest order. */
  private confirm(ack: { id?: unknown; seq?: unknown }) {
    if (typeof ack.seq === "number") {
      this.unacked.delete(ack.seq);
    } else {
      for (const [seq, { item }] of this.unacked) {
        if (item.data.id === ack.id) { this.unacked.delete(seq); break; }
      }
    }
    this.scheduleIdleClose();
  }

  /**
   * A socket that cannot be unref'd (Node's global WebSocket, Bun, Deno) keeps the process alive, so
   * close it once it has been idle with everything confirmed; the next send reconnects. Frames the
   * server never confirms are given up on (reported) once flushTimeoutMs has passed since the last
   * send. Sockets opted into remote replay stay open: they must be reachable for resume commands.
   */
  private scheduleIdleClose() {
    if (this.socketUnrefed || getConfig().allowRemoteReplay || this.closed) return;
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      if (!this.ws || this.queue.length) return;
      if (this.unacked.size && Date.now() - this.lastSendAt < getConfig().flushTimeoutMs) { this.scheduleIdleClose(); return; }
      this.reportUnconfirmed(new Map(), this.takeUnconfirmedCounts());
      this.dropSocket();
    }, IDLE_CLOSE_MS);
    (this.idleTimer as unknown as { unref?: () => void })?.unref?.();
  }

  /** Close the current socket on purpose (no reconnect until the next send). */
  private dropSocket() {
    const socket = this.ws;
    this.ws = null;
    this.connected = false;
    try { socket?.close(); } catch { /* already closing */ }
  }

  /** Frames written but never confirmed, counted per trace, and forgotten. */
  private takeUnconfirmedCounts(): Map<string, number> {
    const counts = new Map<string, number>();
    for (const { item } of this.unacked.values()) {
      if (item.traceId) counts.set(item.traceId, (counts.get(item.traceId) ?? 0) + 1);
    }
    this.unacked.clear();
    return counts;
  }

  /** Never exit silently on an incomplete capture. */
  private reportUnconfirmed(undelivered: Map<string, number>, pendingAck: Map<string, number>) {
    const sum = (m: Map<string, number>) => [...m.values()].reduce((a, b) => a + b, 0);
    if (undelivered.size) {
      console.warn(`[retrace] capture INCOMPLETE: ${sum(undelivered)} event(s) of ${undelivered.size} trace(s) could not be delivered before exit (traces: ${[...undelivered.keys()].sort().join(", ")}). Check connectivity to the Retrace API, or use RETRACE_TRANSPORT=http.`);
    }
    if (pendingAck.size) {
      console.warn(`[retrace] ${sum(pendingAck)} event(s) of ${pendingAck.size} trace(s) were sent but not confirmed within ${getConfig().flushTimeoutMs} ms (traces: ${[...pendingAck.keys()].sort().join(", ")}); the server may still store them. Raise RETRACE_FLUSH_TIMEOUT if they are missing.`);
    }
  }

  /** Enqueue with a bounded (1000) buffer. On overflow drop the OLDEST span event (a trace_started /
   *  trace_ended frame only when no span event is queued), mark its trace lossy, and if it was an open
   *  span (span_started) remember its id so the later close is a no-op. Dropping the oldest event of
   *  any kind evicted trace lifecycle frames during a span flood, leaving traces "running" (pass 8). */
  private enqueue(item: QueuedEvent) {
    if (this.queue.length >= 1000) {
      const spanIndex = this.queue.findIndex((q) => q.eventType.startsWith("span_"));
      const [dropped] = this.queue.splice(spanIndex >= 0 ? spanIndex : 0, 1);
      if (dropped) {
        if (dropped.traceId) this.lossyTraces.add(dropped.traceId);
        if (dropped.eventType === "span_started" && dropped.spanId) this.droppedOpenSpanIds.add(dropped.spanId);
        this.recordDrop();
      }
    }
    this.queue.push(item);
  }

  /** Throttled drop warning — at most once per ~5s burst, reporting the CUMULATIVE count (a
   *  single-tick burst that exits before the next window would otherwise under-report). A final
   *  summary is emitted on close(). */
  private recordDrop() {
    this.droppedTotal++;
    const now = Date.now();
    if (now - this.lastDropWarnMs > 5000) {
      console.warn(`[retrace] send buffer full (cap 1000): ${this.droppedTotal} event(s) dropped so far (oldest-first, API unreachable); affected traces are marked lossy and excluded from byte-deterministic replay`);
      this.lastDropWarnMs = now;
    }
  }

  /** Surface a structured server signal. If the user registered onError, invoke it WRAPPED — a
   *  throwing user callback must never kill the listener / WS loop (classic footgun). With no
   *  callback, fall back to a throttled console warning (per code) — never silent, never an
   *  unthrottled storm. */
  private surfaceSignal(signal: RetraceServerSignal) {
    if (this.onError) {
      try {
        this.onError(signal);
      } catch (e) {
        this.throttledSignalWarn(`onError:threw`, `[retrace] onError callback threw (recording continues): ${(e as Error)?.message ?? e}`);
      }
      return;
    }
    this.throttledSignalWarn(
      signal.code,
      `[retrace] server signal ${signal.code}${signal.fatal ? " (recording halted)" : ""}: ${signal.message}` +
        (signal.retryable ? " — retryable, back off and retry" : ""),
    );
  }

  private warnUnknownType(type: unknown) {
    this.throttledSignalWarn(`unknown:${String(type)}`, `[retrace] ignoring unknown server message type "${String(type)}" (SDK may be outdated)`);
  }

  /** At most one warn per ~5s per key, so a storm (rate_limited, unknown frames) can't flood. */
  private throttledSignalWarn(key: string, line: string) {
    const now = Date.now();
    const last = this.lastSignalWarnMs.get(key) ?? 0;
    if (now - last > 5000) {
      console.warn(line);
      this.lastSignalWarnMs.set(key, now);
    }
  }

  /** Whether anything is unsent or sent-but-unconfirmed (worth flushing on exit). */
  hasPendingData(): boolean {
    return this.queue.length > 0 || this.unacked.size > 0;
  }

  send(eventType: string, data: Record<string, unknown>) {
    // Unconfigured (no API key): never buffer and never reach the network. An imported-but-unused
    // SDK leaves zero footprint — no queued events, nothing for the exit path to send.
    if (!getConfig().apiKey) return;
    const traceId = (data.trace_id ?? data.id) as string | undefined;
    const spanId = eventType.startsWith("span") ? (data.id as string | undefined) : undefined;
    // Open-span-drop no-op: if this span's open was evicted, suppress its orphan close.
    if (eventType === "span_ended" && spanId && this.droppedOpenSpanIds.has(spanId)) {
      this.droppedOpenSpanIds.delete(spanId);
      return;
    }
    const item: QueuedEvent = { seq: ++this.seq, eventType, data, traceId, spanId };
    if (this.connected && this.ws?.readyState === WS_OPEN) {
      this.transmit(item);
    } else {
      this.enqueue(item);
      if (!this.ws && !this.closed) this.connect();
    }
  }

  close() {
    this.closed = true;
    if (this.droppedTotal > 0) {
      console.warn(`[retrace] shutdown: ${this.droppedTotal} event(s) were dropped this session due to buffer overflow (API unreachable); affected traces are lossy and excluded from byte-deterministic replay`);
    }
    this.stopTimers();
    this.dropSocket();
  }

  private stopTimers() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.idleTimer) {
      clearTimeout(this.idleTimer);
      this.idleTimer = null;
    }
  }

  /** Wait (bounded by flushTimeoutMs) until every event has been written AND confirmed by the server. */
  async flush(): Promise<void> {
    const deadline = Date.now() + getConfig().flushTimeoutMs;
    while (this.hasPendingData() && !this.closed && Date.now() < deadline) {
      if (!this.ws && this.queue.length) this.connect();
      await new Promise((r) => setTimeout(r, 20));
    }
  }

  /** Exit path: wait for the server to confirm what was sent (connecting first if needed), then send
   *  over HTTP what never reached a socket — or was written to a socket that died — and report the
   *  rest. Leaves nothing pending, so the exit hook cannot loop, and closes the socket. */
  async flushOnExit(budgetMs = 1500): Promise<void> {
    if (this.hasPendingData()) await this.flush();
    const connected = this.connected;
    const unsent = this.queue.splice(0);
    const deadSocketFrames = connected ? [] : [...this.unacked.values()].map(({ item }) => item);
    const pendingAck = connected ? this.takeUnconfirmedCounts() : new Map<string, number>();
    if (!connected) this.unacked.clear();
    const undelivered = await this.flushViaHttp([...deadSocketFrames, ...unsent], budgetMs);
    this.reportUnconfirmed(undelivered, pendingAck);
    // Exiting: no pending reconnect (a later send() still reconnects on demand).
    this.stopTimers();
    this.dropSocket();
  }

  /** POST the given events over bounded HTTP keepalive requests, grouped by trace. A trace whose
   *  trace_started already reached the server is finished with the terminal PATCH instead (a POST
   *  would 409). Incomplete / lossy traces are stamped lossy:true so the server/replay refuses
   *  byte-deterministic replay. Returns the events that could not be delivered, per trace. */
  private async flushViaHttp(events: QueuedEvent[], budgetMs: number): Promise<Map<string, number>> {
    const undelivered = new Map<string, number>();
    if (!events.length) return undelivered;
    const cfg = getConfig();
    const deadline = Date.now() + budgetMs;
    type Group = { trace?: Record<string, unknown>; started: boolean; ended: boolean; spans: Map<string, Record<string, unknown>>; frames: number };
    const byTrace = new Map<string, Group>();
    for (const item of [...events].sort((a, b) => a.seq - b.seq)) {
      const tid = item.traceId ?? "_";
      let g = byTrace.get(tid);
      if (!g) { g = { started: false, ended: false, spans: new Map(), frames: 0 }; byTrace.set(tid, g); }
      g.frames++;
      if (item.eventType === "trace_started") { g.trace = { ...(g.trace ?? {}), ...item.data }; g.started = true; }
      else if (item.eventType === "trace_ended") { g.trace = { ...(g.trace ?? {}), ...item.data }; g.ended = true; }
      else if (item.eventType === "span_started" && item.spanId) g.spans.set(item.spanId, { ...item.data });
      else if (item.eventType === "span_ended" && item.spanId) {
        if (g.spans.has(item.spanId)) Object.assign(g.spans.get(item.spanId)!, item.data);
        else g.spans.set(item.spanId, { ...item.data });
      }
    }
    const url = `${cfg.baseUrl}/api/v1/traces`;
    const headers = { "x-retrace-key": cfg.apiKey, "Content-Type": "application/json", "x-retrace-client": CLIENT_ID };
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), budgetMs);
    const terminalKeys = ["status", "output", "ended_at", "total_tokens", "total_cost", "total_duration_ms", "lossy", "terminated_early", "span_count", "error"];
    const patchTerminal = async (tid: string, g: Group): Promise<boolean> => {
      const patch: Record<string, unknown> = {};
      for (const k of terminalKeys) if (g.trace && k in g.trace) patch[k] = g.trace[k];
      if (this.lossyTraces.has(tid) || g.spans.size) patch.lossy = true; // span frames never reached the server
      const res = await fetch(`${url}/${tid}`, { method: "PATCH", headers, body: JSON.stringify(patch), keepalive: true, signal: ctrl.signal, redirect: "error" });
      return res.ok;
    };
    try {
      for (const [tid, g] of byTrace) {
        if (!g.trace || Date.now() >= deadline) { undelivered.set(tid, g.frames); continue; }
        try {
          if (!g.started && g.ended) {
            if (!(await patchTerminal(tid, g)) || g.spans.size) undelivered.set(tid, g.spans.size || g.frames);
            continue;
          }
          // A span whose span_started already went out over WS only has its end frame here; it can't
          // form a valid span and would 400 the whole trace — drop it and mark the trace lossy.
          const spans = [...g.spans.values()].filter((sp) => sp.started_at && sp.name != null && sp.span_type);
          const lossy = this.lossyTraces.has(tid) || !g.ended || spans.length !== g.spans.size;
          const res = await fetch(url, {
            method: "POST", headers, keepalive: true, signal: ctrl.signal, redirect: "error",
            body: JSON.stringify({ ...g.trace, ...(lossy ? { lossy: true } : {}), spans }),
          });
          if (res.status === 409 && g.ended) {
            // The trace reached the server after all (its confirmation was lost with the socket).
            await patchTerminal(tid, g);
            if (spans.length) undelivered.set(tid, spans.length);
          } else if (!res.ok) {
            undelivered.set(tid, g.frames);
          }
        } catch {
          undelivered.set(tid, g.frames); // process is exiting: report, never throw
        }
      }
    } finally {
      clearTimeout(timer);
    }
    return undelivered;
  }
}

export class HTTPTransport implements Transport {
  // Buffer per-trace so concurrent traces in one process (parallel requests / Promise.all) never
  // clobber each other. The previous single traceData/spans pair merged interleaved traces into one
  // corrupt POST (trace B's trace_started overwrote A; A's flush sent B's data + both sets of spans).
  // Keyed by trace id; spans route by their trace_id.
  private pending = new Map<string, { data: Record<string, unknown>; spans: Record<string, unknown>[] }>();
  // span_ended events carry no trace_id (parity with the WS protocol), so remember each span's trace
  // from its span_started to route the end event to the right per-trace buffer.
  private spanTrace = new Map<string, string>();
  // Bounded offline backlog (parity with WSTransport's queue + the Python HTTPTransport): a trace
  // that fails to upload after exhausting transient retries is RETAINED here and retried on a later
  // flush instead of being silently dropped. Bounded so a long outage can't grow memory unbounded.
  private backlog: string[] = [];
  private readonly backlogCap = 1000;
  private dropped = 0;
  // Uploads started by a trace_ended and still running. flush()/flushOnExit() await them: the exit
  // path used to return while the upload was in flight, and a SIGTERM before the WebSocket was up
  // (the auto transport falls back to HTTP) exited mid-request and lost the whole run.
  private inflight = new Set<Promise<void>>();

  private entry(traceId: string): { data: Record<string, unknown>; spans: Record<string, unknown>[] } {
    let e = this.pending.get(traceId);
    if (!e) { e = { data: {}, spans: [] }; this.pending.set(traceId, e); }
    return e;
  }

  send(eventType: string, data: Record<string, unknown>) {
    if (eventType === "trace_started") {
      const id = data.id as string;
      if (!id) return;
      const e = this.entry(id);
      e.data = { ...e.data, ...data };
    } else if (eventType === "span_started") {
      const tid = data.trace_id as string;
      if (!tid) return;
      this.spanTrace.set(data.id as string, tid);
      this.entry(tid).spans.push({ ...data, _event: eventType });
    } else if (eventType === "span_ended") {
      // span_ended has no trace_id — route by the span id captured at span_started.
      const tid = (data.trace_id as string) || this.spanTrace.get(data.id as string);
      if (!tid) return;
      this.entry(tid).spans.push({ ...data, _event: eventType });
    } else if (eventType === "trace_ended") {
      const id = data.id as string;
      if (!id) return;
      const e = this.pending.get(id);
      if (e) Object.assign(e.data, data);
      const upload = this.flush(id);
      this.inflight.add(upload);
      void upload.finally(() => this.inflight.delete(upload));
    }
  }

  /** Flush one trace (on its trace_ended) or, when no id is given, every pending trace (shutdown). */
  async flush(traceId?: string): Promise<void> {
    // Opportunistically retry anything stranded by a previous outage first.
    if (this.backlog.length) await this.drainBacklog();
    if (traceId !== undefined) { await this.flushOne(traceId); return; }
    for (const id of [...this.pending.keys()]) await this.flushOne(id);
    await Promise.all([...this.inflight]);
  }

  private async flushOne(traceId: string): Promise<void> {
    const e = this.pending.get(traceId);
    if (!e) return;
    // Remove first so a concurrent flush (e.g. trace_ended then shutdown drain) can't double-send.
    this.pending.delete(traceId);
    for (const ev of e.spans) this.spanTrace.delete(ev.id as string);
    const payload = JSON.stringify({ ...e.data, spans: this.buildSpans(e.spans) });
    // On a transient failure, RETAIN the trace in the bounded backlog instead of dropping it.
    if ((await this.upload(payload)) === "retry") this.enqueueBacklog(payload);
  }

  /** POST one complete trace payload. Returns "ok" | "reject" (non-retryable 4xx) | "retry"
   *  (429 / 5xx / network after retries exhausted — caller should retain the payload). */
  private async upload(payload: string): Promise<"ok" | "reject" | "retry"> {
    const cfg = getConfig();
    const url = `${cfg.baseUrl}/api/v1/traces`;
    // Retry up to 3 times with exponential backoff; awaited so shutdown can drain it.
    for (let n = 1; n <= 3; n++) {
      try {
        const res = await fetch(url, {
          method: "POST",
          headers: { "x-retrace-key": cfg.apiKey, "Content-Type": "application/json", "x-retrace-client": CLIENT_ID },
          body: payload,
          redirect: "error", // never forward the key to a redirect target
        });
        if (res.ok) return "ok";
        const txt = await res.text().catch(() => "");
        // 4xx (except 429) is a client/payload error that won't succeed on retry — surface
        // it loudly and stop, rather than silently dropping the trace.
        if (res.status < 500 && res.status !== 429) {
          console.error(`[retrace] trace upload rejected (HTTP ${res.status}): ${txt.slice(0, 300)}`);
          return "reject";
        }
        // 5xx / 429 — transient; retry.
        if (n === 3) console.error(`[retrace] trace upload failed after ${n} attempts (HTTP ${res.status}): ${txt.slice(0, 200)}`);
      } catch (err) {
        if (n === 3) console.error(`[retrace] trace upload network error after ${n} attempts: ${(err as Error)?.message ?? err}`);
      }
      if (n < 3) await new Promise((r) => setTimeout(r, 1000 * n));
    }
    return "retry";
  }

  /** Retain a failed trace for a later retry; bounded — drop + count the oldest on overflow. */
  private enqueueBacklog(payload: string) {
    this.backlog.push(payload);
    while (this.backlog.length > this.backlogCap) {
      this.backlog.shift();
      this.dropped++;
      console.error(`[retrace] offline buffer full (cap ${this.backlogCap}) — dropped oldest trace (total dropped: ${this.dropped})`);
    }
  }

  /** Retry previously-failed traces oldest-first; stop at the first still-failing one so a server
   *  that is still down isn't hammered. */
  private async drainBacklog(): Promise<void> {
    while (this.backlog.length) {
      const r = await this.upload(this.backlog[0]);
      if (r === "retry") break;       // still failing — keep the rest for next time
      this.backlog.shift();           // "ok" (delivered) or "reject" (unrecoverable) → remove
    }
  }

  private buildSpans(spans: Record<string, unknown>[]): Record<string, unknown>[] {
    const merged = new Map<string, Record<string, unknown>>();
    for (const ev of spans) {
      const { _event, ...rest } = ev;
      const id = rest.id as string;
      if (_event === "span_started") {
        merged.set(id, rest);
      } else if (_event === "span_ended" && merged.has(id)) {
        Object.assign(merged.get(id)!, rest);
      }
    }
    return [...merged.values()];
  }

  close() {
    void this.flush();
  }

  /** HTTP is already the one-shot channel — just drain. */
  async flushOnExit(): Promise<void> {
    await this.flush();
  }

  hasPendingData(): boolean {
    return this.pending.size > 0 || this.backlog.length > 0 || this.inflight.size > 0;
  }
}

export function createTransport(mode: "ws" | "http" | "auto" = "auto"): Transport {
  if (mode === "http") return new HTTPTransport();
  if (mode === "ws") return new WSTransport();

  // Auto: start with WS, fall back to HTTP if connection fails within timeout
  const ws = new WSTransport();
  const http = new HTTPTransport();
  let useWs = true;
  let decided = false;
  const buffer: Array<{ eventType: string; data: Record<string, unknown> }> = [];

  const fallbackTimer = setTimeout(() => {
    if (!decided && !ws.isConnected) {
      decided = true;
      useWs = false;
      ws.close();
      for (const item of buffer.splice(0)) http.send(item.eventType, item.data);
    }
  }, 5000);

  ws.connect();

  const originalSend = ws.send.bind(ws);
  const checkConnected = () => {
    if (!decided && ws.isConnected) {
      decided = true;
      clearTimeout(fallbackTimer);
      for (const item of buffer.splice(0)) originalSend(item.eventType, item.data);
    }
  };

  // Flush the pre-connect buffer as soon as the socket is up. Previously the buffer drained only on
  // the NEXT send(), so a run whose events were all emitted before connect (a short trace) sat in the
  // buffer until exit and its trace_ended was never delivered live.
  ws.onConnected = () => checkConnected();

  const autoTransport: Transport = {
    send(eventType: string, data: Record<string, unknown>) {
      checkConnected();
      if (decided) {
        if (useWs) originalSend(eventType, data);
        else http.send(eventType, data);
      } else {
        buffer.push({ eventType, data });
      }
    },
    close() {
      clearTimeout(fallbackTimer);
      if (!decided) {
        // WS not yet connected — force HTTP fallback to avoid data loss
        decided = true;
        useWs = false;
        ws.close();
        for (const item of buffer.splice(0)) http.send(item.eventType, item.data);
        http.close();
      } else if (useWs) {
        ws.close();
      } else {
        http.close();
      }
    },
    async flush() {
      if (!decided) {
        // Never connected over WS — force the HTTP fallback and drain the buffer so the
        // final trace isn't lost on shutdown.
        decided = true;
        useWs = false;
        clearTimeout(fallbackTimer);
        ws.close();
        for (const item of buffer.splice(0)) http.send(item.eventType, item.data);
        await http.flush();
      } else if (useWs) {
        await ws.flush();
      } else {
        await http.flush();
      }
    },
    async flushOnExit(budgetMs?: number) {
      if (!decided) {
        // Never connected over WS — force HTTP fallback and one-shot the buffer.
        decided = true;
        useWs = false;
        clearTimeout(fallbackTimer);
        ws.close();
        for (const item of buffer.splice(0)) http.send(item.eventType, item.data);
        await http.flushOnExit();
      } else if (useWs) {
        await ws.flushOnExit(budgetMs);
      } else {
        await http.flushOnExit();
      }
    },
    hasPendingData() {
      if (!decided) return buffer.length > 0;
      return useWs ? ws.hasPendingData() : http.hasPendingData();
    },
  };
  // Forward the user's onError to the inner WS transport so ITS surfaceSignal owns the policy
  // (callback-safety + throttled default-warn). Critically, when no user callback is set we leave
  // ws.onError undefined so the inner transport takes its DEFAULT-warn path — a forwarder that is
  // always-truthy would mask the default and re-create a silent dark-out.
  let _userOnError: ((signal: RetraceServerSignal) => void) | undefined;
  Object.defineProperty(autoTransport, "onError", {
    get() { return _userOnError; },
    set(v: ((signal: RetraceServerSignal) => void) | undefined) { _userOnError = v; ws.onError = v; },
    configurable: true,
  });
  return autoTransport;
}

export type ExitReason = "graceful" | "signal" | "uncaught";
const preExitHooks: Array<(reason: ExitReason) => void> = [];

/**
 * Register a synchronous hook run on the exit/signal path BEFORE the transport drains — e.g. the
 * ambient trace finishing itself (emitting trace_ended) so it's in the buffer for the drain.
 */
export function onProcessExit(fn: (reason: ExitReason) => void): void {
  preExitHooks.push(fn);
}

/**
 * Wire process-exit flushing for a shared transport. Node's exit hooks are weaker than Python's
 * atexit, so we cover three paths with different ownership semantics:
 *  - beforeExit: graceful (loop emptied naturally). Drain; do not exit (Node exits after).
 *  - SIGTERM/SIGINT, and WE are the sole listener: adding a listener suppresses Node's default
 *    terminate, so we now OWN the exit — flush THEN process.exit(), or the process hangs forever.
 *  - SIGTERM/SIGINT, but the USER already has handlers: best-effort flush only; their handler owns
 *    exit. We do not exit, and cannot guarantee our async flush completes (a synchronous
 *    process.exit() in their handler will cut it off).
 *
 * Irreducible residual (both SDKs): process.exit() mid-flush, os._exit(), and SIGKILL bypass these
 * hooks entirely and lose still-buffered events; a framework-owned SIGTERM (we stay a guest) drains
 * only what that framework's shutdown allows. Both SDKs DO flush on a sole-owner SIGTERM (TS via the
 * listenerCount gate here; Python via a SIG_DFL/main-thread gate) — that parity is real.
 */
export function registerProcessExitFlush(transport: Transport, budgetMs = 1500): void {
  // Edge runtimes (Cloudflare Workers, Vercel Edge, Deno/Supabase Edge) have no global `process`
  // and no exit lifecycle to hook — those runtimes drain via their own request lifecycle (call
  // flush()/await the transport, or use ctx.waitUntil). No-op there instead of throwing at startup.
  type ProcLike = {
    on(ev: string, cb: () => void): void;
    exit(code?: number): void;
    listenerCount(ev: string): number;
  };
  const proc = (globalThis as { process?: ProcLike }).process;
  if (!proc || typeof proc.on !== "function") return;

  const drain = async (reason: ExitReason) => {
    for (const h of preExitHooks) { try { h(reason); } catch { /* hook best-effort */ } }
    // No-op when there's nothing to send (zero-event run): no fetch, no delay. Open-span
    // finalization in the hooks above runs first, so a stream that opened but never closed still
    // produces events to flush; a truly empty run leaves zero footprint.
    if (transport.hasPendingData && !transport.hasPendingData()) return;
    if (transport.flushOnExit) await transport.flushOnExit(budgetMs);
    else await transport.flush();
  };

  let draining = false;
  proc.on("beforeExit", () => {
    if (draining) return;
    draining = true;
    void drain("graceful").finally(() => { draining = false; });
  });

  for (const sig of ["SIGTERM", "SIGINT"] as const) {
    const userOwnsExit = proc.listenerCount(sig) > 0; // captured BEFORE we register ours
    proc.on(sig, () => {
      drain("signal").finally(() => {
        // Sole listener: we suppressed the default terminate, so we must exit or the process hangs.
        // User has their own handler: they own exit — never call it for them.
        if (!userOwnsExit) proc.exit(sig === "SIGINT" ? 130 : 143);
      });
    });
  }
}
