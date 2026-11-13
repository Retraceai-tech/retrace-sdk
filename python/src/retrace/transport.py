from __future__ import annotations

import json
import logging
import queue
import select
import threading
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

import requests

from .config import get_config
from .errors import RetraceServerSignal
from .trace import Trace

if TYPE_CHECKING:
    import websocket

logger = logging.getLogger("retrace")

# Client identifier sent on every request so the backend can attribute SDK usage/version.
try:
    from importlib.metadata import version as _pkg_version
    _SDK_VERSION = _pkg_version("retrace-sdk")
except Exception:  # pragma: no cover - metadata may be unavailable in some envs
    _SDK_VERSION = "0.16.25"
_CLIENT_ID = f"python-sdk/{_SDK_VERSION}"

# Server frames an ingesting SDK receives but has no use for. `ack` confirms a frame was accepted;
# the rest are the account's live feed (another connection's run), meant for dashboards. Servers from
# API 1.28.22 on send neither to SDK sockets unless asked; older servers send both unconditionally.
_INFORMATIONAL_SERVER_TYPES = frozenset(
    {"ack", "auth_ok", "trace_started", "span_started", "span_ended", "trace_ended"},
)


# Longest the WebSocket listener defers a read to pending outbound frames (see WSTransport._listen).
_WRITE_PRIORITY_S = 0.05


def _socket_readable(ws: Any, timeout: float) -> bool:
    """Wait up to `timeout` for the socket under a websocket-client connection to have data, without
    touching its TLS state. Returns True when unknown (no `.sock`), leaving the caller to a timed recv."""
    sock = getattr(ws, "sock", None)
    if sock is None:
        return True
    pending = getattr(sock, "pending", None)
    if callable(pending) and pending() > 0:
        return True  # TLS bytes already decrypted into the SSL buffer: select() would not see them
    readable, _, _ = select.select([sock], [], [], timeout)
    return bool(readable)


class WSTransport:
    """WebSocket transport using websocket-client (sync) for span streaming.

    All network I/O (connect, auth handshake, send) runs on a dedicated background daemon
    thread. The public ``send`` only enqueues onto a bounded queue and never blocks the
    caller — so the first traced event no longer stalls the user's app while the socket
    connects, and a slow/unreachable API can't serialize the caller's threads.

    Delivery is CONFIRMED, not assumed: every frame carries a sequence number and the server acks
    each one it has ingested (the auth frame asks for acks). A frame written to the socket stays
    "unacked" until then: close() waits for the acks, a reconnect re-sends what the dead socket never
    confirmed, and anything still unconfirmed at exit goes over HTTP or is reported as a possibly
    incomplete capture. Writing a frame to the socket used to count as delivered, and the default
    quickstart lost its whole run when the process exited before the server had read the frames.
    """

    def __init__(self) -> None:
        self._ws: websocket.WebSocket | None = None
        self._lock = threading.RLock()
        # Set while the sender waits for `_lock`. Reads and writes share the lock (see _listen), and
        # the listener re-took it right after every read, so each outbound frame waited for one more
        # read: on a busy socket a run's frames went out at the pace of inbound reads (on a loaded CI
        # runner 150 frames needed > 10 s). Now the listener defers to a waiting or queued write.
        self._writer_waiting = threading.Event()
        self._connected = False
        self._backoff = 1.0
        self._listener_thread: threading.Thread | None = None
        self._closed = False
        # Set by a server `halt`: stop accepting new events while the background close drains.
        self._halted = False
        # Bounded outbound queue doubles as the offline buffer (cap 1000).
        self._send_queue: "queue.Queue[str]" = queue.Queue(maxsize=1000)
        # The frame the worker dequeued but has not delivered yet (held across reconnect attempts).
        # Tracked so the exit-time HTTP death-flush includes it — it is usually trace_started, and a
        # flush without it POSTs a trace with no started_at (HTTP 400) and loses the whole run.
        self._inflight: str | None = None
        self._worker_thread: threading.Thread | None = None
        self._worker_lock = threading.Lock()
        # Delivery confirmation: frame sequence numbers, and frames written but not yet acked
        # (seq -> serialized frame, in send order). Bounded like the send queue.
        self._seq = 0
        self._seq_lock = threading.Lock()
        self._unacked: "OrderedDict[int, str]" = OrderedDict()
        self._ack_lock = threading.Lock()
        self._unacked_cap = 1000
        # Set when a (re)connect must first re-send the frames the previous socket never confirmed.
        self._resend_unacked = False
        # Integrity tracking for the bounded offline buffer (F-DL2 parity with TS).
        self._lossy_traces: set[str] = set()
        self._dropped_open_spans: set[str] = set()
        self._dropped_total = 0
        self._last_drop_warn = 0.0
        # Throttle for the default (no-callback) server-signal warning, keyed per code, so a
        # rate_limited storm can't flood the logs (same lesson as the drop-warn).
        self._last_signal_warn: dict[str, float] = {}

    def _connect(self) -> None:
        """Open the socket + auth handshake. Runs ONLY on the worker thread."""
        import websocket

        cfg = get_config()
        url = f"{cfg.ws_url}/ws/v1/stream"
        try:
            # suppress_origin: websocket-client otherwise synthesizes `Origin: https://<api host>`,
            # which API servers without the same-origin allowance reject with close code 4003 —
            # silently dropping every WebSocket trace. SDKs are not browsers; send no Origin.
            self._ws = websocket.create_connection(url, timeout=5, suppress_origin=True, redirect_limit=0)
            # Auth (acks: confirm each ingested frame — see the class docstring)
            self._ws.send(json.dumps({
                "type": "auth",
                "api_key": cfg.api_key,
                "resumable": bool(cfg.allow_remote_replay),
                "replay_generation": 1,
                "acks": True,
            }))
            resp = json.loads(self._ws.recv())
            if resp.get("type") == "auth_ok":
                self._connected = True
                self._backoff = 1.0
                with self._ack_lock:
                    self._resend_unacked = bool(self._unacked)
                self._start_listener()
            else:
                self._ws.close()
                self._ws = None
        except Exception as e:
            logger.debug(f"WebSocket connection failed: {e}")
            self._ws = None
            self._connected = False

    def _ensure_worker(self) -> None:
        if self._worker_thread and self._worker_thread.is_alive():
            return
        with self._worker_lock:
            if self._worker_thread and self._worker_thread.is_alive():
                return
            self._worker_thread = threading.Thread(target=self._run_worker, daemon=True, name="retrace-ws-sender")
            self._worker_thread.start()

    def _write(self, frame: str) -> bool:
        """Write one frame to the current socket; on failure mark the connection down."""
        try:
            self._writer_waiting.set()
            try:
                with self._lock:
                    self._writer_waiting.clear()
                    if self._ws is None:
                        return False
                    self._ws.send(frame)
            finally:
                self._writer_waiting.clear()
            return True
        except Exception as e:
            logger.debug(f"WebSocket send failed: {e}")
            self._connected = False
            self._ws = None
            return False

    def _resend_unconfirmed(self) -> bool:
        """After a reconnect, re-send (in order) the frames the previous socket never confirmed. The
        server ingests a re-sent frame idempotently (stored span ids / a started trace are not
        duplicated). Returns False if the new socket failed too (they stay unacked)."""
        with self._ack_lock:
            if not self._resend_unacked:
                return True
            frames = list(self._unacked.values())
            self._resend_unacked = False
        for frame in frames:
            if not self._write(frame):
                with self._ack_lock:
                    self._resend_unacked = True
                return False
        return True

    def _run_worker(self) -> None:
        """Drain the send queue, connecting/reconnecting as needed — off the caller thread."""
        pending: str | None = None
        while not self._closed:
            try:
                if pending is None:
                    pending = self._send_queue.get(timeout=1.0)
                    self._inflight = pending
            except queue.Empty:
                # Idle: still re-send unconfirmed frames once a dropped socket is back.
                if self._connected and self._resend_unacked:
                    self._resend_unconfirmed()
                continue
            if not self._connected or self._ws is None:
                self._connect()
            if not self._connected or self._ws is None or not self._resend_unconfirmed():
                # Couldn't connect — hold `pending` and back off (it stays buffered).
                time.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, 30.0)
                continue
            self._remember_unacked(pending)
            if self._write(pending):
                pending = None
                self._inflight = None
                self._backoff = 1.0
            else:
                # Not written: it is still `pending` (re-sent after reconnect), not unconfirmed.
                self._forget_unacked(pending)

    def _remember_unacked(self, frame: str) -> None:
        seq = _frame_seq(frame)
        if seq is None:
            return
        with self._ack_lock:
            self._unacked[seq] = frame
            while len(self._unacked) > self._unacked_cap:
                _, evicted = self._unacked.popitem(last=False)
                self._mark_lossy(evicted)

    def _forget_unacked(self, frame: str) -> None:
        seq = _frame_seq(frame)
        if seq is not None:
            with self._ack_lock:
                self._unacked.pop(seq, None)

    def _confirm(self, ack: dict[str, Any]) -> None:
        """A server ack: drop the frame from the unconfirmed set. Matched by the echoed sequence
        number; servers that do not echo it ack in ingest order by id, so take the oldest match."""
        with self._ack_lock:
            seq = ack.get("seq")
            if isinstance(seq, int):
                self._unacked.pop(seq, None)
                return
            ack_id = ack.get("id")
            for s, frame in self._unacked.items():
                if _frame_data_id(frame) == ack_id:
                    del self._unacked[s]
                    return

    def _mark_lossy(self, frame: str) -> None:
        try:
            d = json.loads(frame).get("data") or {}
        except Exception:
            return
        tid = d.get("trace_id") or d.get("id")
        if tid:
            self._lossy_traces.add(tid)

    def _start_listener(self) -> None:
        """Start background thread to listen for server-initiated messages (resume, ping)."""
        if self._listener_thread and self._listener_thread.is_alive():
            return

        def _listen() -> None:
            import socket as _socket

            import websocket as _websocket

            # websocket-client re-raises socket timeouts as WebSocketTimeoutException, which is NOT a
            # TimeoutError subclass — without it here the listener died after the first idle second
            # and server ping/resume/signal frames were never read again.
            idle = (_websocket.WebSocketTimeoutException, _socket.timeout, TimeoutError)
            deferred_since: float | None = None
            while self._connected and not self._closed and self._ws:
                ws = self._ws
                try:
                    # Wait for data OUTSIDE the lock, then read under the lock the sender holds for
                    # every write. Python's SSL sockets are not safe for a read on one thread while
                    # another writes: over wss:// the overlap intermittently tore the connection down
                    # and lost every frame already sent on it (e.g. a run's first spans).
                    if not _socket_readable(ws, 1.0):
                        continue
                    # Outbound frames first (a waiting or queued write), for at most
                    # _WRITE_PRIORITY_S at a time, so acks and server pings are still read promptly.
                    if self._writer_waiting.is_set() or not self._send_queue.empty():
                        now = time.monotonic()
                        deferred_since = deferred_since or now
                        if now - deferred_since < _WRITE_PRIORITY_S:
                            time.sleep(0.001)
                            continue
                    deferred_since = None
                    with self._lock:
                        if ws is not self._ws:
                            continue  # reconnected meanwhile; poll the new socket
                        ws.settimeout(1.0)
                        msg = ws.recv()
                except idle:
                    continue  # idle poll — expected, not an error
                except Exception as e:
                    logger.debug(f"WebSocket recv ended: {e}")
                    break  # connection closed/broken — exit loop, reconnect logic handles it
                if not msg:
                    # websocket-client returns "" for a server close frame. Now that the listener
                    # outlives idle periods it actually sees it; treat it as closed, not as a bad frame.
                    logger.debug("WebSocket closed by server")
                    break
                # Dispatch is isolated from recv so a single bad frame can't kill the listener,
                # and is NOT silently swallowed (the F-P6 class) — unhandled types throttled-warn.
                try:
                    self._dispatch(json.loads(msg))
                except Exception as e:
                    self._throttled_signal_warn("dispatch", f"[retrace] dropped an unhandled server frame: {e}")

        self._listener_thread = threading.Thread(target=_listen, daemon=True, name="retrace-ws-listener")
        self._listener_thread.start()

    def _dispatch(self, parsed: dict[str, Any]) -> None:
        from .errors import classify_server_signal
        mtype = parsed.get("type")
        if mtype == "ping":
            with self._lock:
                if self._ws:
                    self._ws.send(json.dumps({"type": "pong"}))
        elif mtype == "ack":
            self._confirm(parsed)
        elif mtype in _INFORMATIONAL_SERVER_TYPES:
            # Known frames this SDK has no use for: per-frame delivery `ack`s and the account's live
            # feed (sent by servers that predate opt-in acks / live events). Not unknown, so no warning.
            return
        elif mtype == "resume":
            # C2: server-initiated re-execution runs the user's registered function in THIS process
            # with server-supplied input. Off by default — opt in via allow_remote_replay.
            if not get_config().allow_remote_replay:
                self._throttled_signal_warn(
                    "remote_blocked:resume",
                    "[retrace] ignoring server 'resume' command — remote-controlled re-execution is "
                    "disabled. Set allow_remote_replay=True (or RETRACE_ALLOW_REMOTE_REPLAY=true) to opt in.",
                )
                return
            from .resume import handle_resume, parse_resume_message
            cmd = parse_resume_message(parsed)
            if cmd:
                handle_resume(cmd)
        elif mtype == "replay":
            if not get_config().allow_remote_replay:
                self._throttled_signal_warn(
                    "remote_blocked:replay",
                    "[retrace] ignoring server 'replay' command — remote-controlled re-execution is "
                    "disabled. Set allow_remote_replay=True (or RETRACE_ALLOW_REMOTE_REPLAY=true) to opt in.",
                )
                return
            from .replay import handle_replay, parse_replay_message
            replay_cmd = parse_replay_message(parsed)
            if replay_cmd:
                handle_replay(replay_cmd)
        elif mtype == "error":
            # F-P6: server `error` frames were previously SWALLOWED. Surface them as a structured
            # signal (credits_exhausted / rate_limited / error) through on_error / throttled-warn.
            self._surface_signal(classify_server_signal("error", parsed.get("error", "")))
        elif mtype == "halt":
            reason = (parsed.get("data") or {}).get("reason", "Guardrail triggered")
            signal = classify_server_signal("halt", reason)
            # halt CHANGES behavior, deliberately: surface the fatal signal, flush what's already
            # recorded, then STOP recording (close). Distinct from credits_exhausted, which leaves
            # recording alive so the user can swap keys / upgrade mid-run.
            self._surface_signal(signal)
            # Stop accepting new events now; drain + close off this thread (close() waits for acks,
            # which only this listener thread can receive).
            self._halted = True
            threading.Thread(target=self.close, daemon=True, name="retrace-ws-halt-close").start()
        else:
            # DEFAULT: an unknown/unhandled message type must not vanish silently — the durable
            # F-P6 fix (the next new server type surfaces instead of being swallowed).
            self._throttled_signal_warn(
                f"unknown:{mtype}",
                f"[retrace] ignoring unknown server message type {mtype!r} (SDK may be outdated)",
            )

    def _surface_signal(self, signal: RetraceServerSignal) -> None:
        """Hand a structured signal to on_error (WRAPPED — a throwing user callback must never kill
        the listener thread), or throttled-warn by default. Never silent, never an unthrottled storm."""
        cb = get_config().on_error
        if cb is not None:
            try:
                cb(signal)
            except Exception as e:
                self._throttled_signal_warn(
                    "on_error:threw",
                    f"[retrace] on_error callback threw (recording continues): {e}",
                )
            return
        suffix = (
            " (recording halted)" if signal.fatal
            else (" — retryable, back off and retry" if signal.retryable else "")
        )
        self._throttled_signal_warn(signal.code, f"[retrace] server signal {signal.code}{suffix}: {signal.message}")

    def _throttled_signal_warn(self, key: str, line: str) -> None:
        now = time.time()
        if now - self._last_signal_warn.get(key, 0.0) > 5.0:
            logger.warning(line)
            self._last_signal_warn[key] = now

    def send(self, event_type: str, data: dict[str, Any]) -> None:
        """Non-blocking: enqueue the message for the background worker to deliver."""
        if self._closed or self._halted:
            return
        # Guard A: unconfigured (no API key) → never buffer, never reach the network. An
        # imported-but-unused SDK leaves zero footprint.
        if not get_config().api_key:
            return
        trace_id = data.get("trace_id") or data.get("id")
        span_id = data.get("id") if event_type.startswith("span") else None
        # Open-span-drop no-op: suppress the orphan close of a span whose open was evicted.
        if event_type == "span_ended" and span_id in self._dropped_open_spans:
            self._dropped_open_spans.discard(span_id)
            return
        # Stamp trace-level lossy on a trace_ended whose trace lost events to a buffer drop, so the
        # server/replay refuses byte-deterministic replay.
        if event_type == "trace_ended" and trace_id in self._lossy_traces:
            data = {**data, "lossy": True}
        with self._seq_lock:
            self._seq += 1
            seq = self._seq
        # default=str: a value the recorder could not make JSON-safe must not drop the frame
        # (a dropped trace_ended leaves the run "running").
        msg = json.dumps({"type": event_type, "data": data, "seq": seq}, default=str)
        self._ensure_worker()
        try:
            self._send_queue.put_nowait(msg)
        except queue.Full:
            self._drop_oldest()
            try:
                self._send_queue.put_nowait(msg)
            except queue.Full:
                pass

    def _drop_oldest(self) -> None:
        """Evict the oldest queued SPAN event (a trace_started / trace_ended frame only when nothing
        else is queued): mark its trace lossy, remember a dropped open span so its later close is a
        no-op, and warn (throttled, cumulative — a sustained outage drops continuously; per-event
        logging would be its own perf problem). Evicting the oldest frame of any kind dropped trace
        lifecycle frames during a span flood, and those traces stayed "running" with no spans."""
        dropped = self._take_evictable()
        if dropped is None:
            return
        try:
            m = json.loads(dropped)
            d = m.get("data", {})
            tid = d.get("trace_id") or d.get("id")
            if tid:
                self._lossy_traces.add(tid)
            if m.get("type") == "span_started" and d.get("id"):
                self._dropped_open_spans.add(d["id"])
        except Exception:
            pass
        self._dropped_total += 1
        now = time.time()
        if now - self._last_drop_warn > 5.0:
            logger.warning(
                "[retrace] send buffer full (cap 1000): %d event(s) dropped so far (oldest-first, "
                "API unreachable); affected traces are marked lossy and excluded from byte-deterministic replay",
                self._dropped_total,
            )
            self._last_drop_warn = now

    def _take_evictable(self) -> str | None:
        """Remove and return the oldest queued span frame, else the oldest frame, else None."""
        q = self._send_queue
        with q.mutex:
            if not q.queue:
                return None
            index = next((i for i, m in enumerate(q.queue) if m.startswith('{"type": "span_')), 0)
            frame: str = q.queue[index]
            del q.queue[index]
            q.not_full.notify()
            return frame

    def has_pending_data(self) -> bool:
        """True if anything is unsent or sent-but-unconfirmed (worth flushing on exit, Guard B)."""
        return self._inflight is not None or not self._send_queue.empty() or bool(self._unacked)

    def is_trace_lossy(self, trace_id: str) -> bool:
        return trace_id in self._lossy_traces

    def _report_dropped(self) -> None:
        if self._dropped_total > 0:
            logger.warning(
                "[retrace] shutdown: %d event(s) were dropped this session due to buffer overflow "
                "(API unreachable); affected traces are lossy and excluded from byte-deterministic replay",
                self._dropped_total,
            )

    def reset_for_child(self) -> None:
        """After os.fork: a child inherits the parent's buffered events + dead worker/socket refs.
        Clear them so a forked worker (gunicorn pre-fork) never re-emits the parent's buffer."""
        self._send_queue = queue.Queue(maxsize=1000)
        self._inflight = None
        self._unacked = OrderedDict()
        self._resend_unacked = False
        self._halted = False
        self._ws = None
        self._connected = False
        self._worker_thread = None
        self._listener_thread = None
        self._lossy_traces = set()
        self._dropped_open_spans = set()
        self._dropped_total = 0

    def _take_unsent(self) -> list[str]:
        """Frames never written to a socket (the worker's in-flight frame first), removed from the queue."""
        frames: list[str] = []
        inflight = self._inflight
        if inflight is not None:
            frames.append(inflight)
            self._inflight = None
        while True:
            try:
                frames.append(self._send_queue.get_nowait())
            except queue.Empty:
                break
        return frames

    def _flush_queue_via_http(self, budget_s: float = 2.0, extra: list[str] | None = None) -> dict[str, int]:
        """Death-flush: POST whatever was never delivered to the HTTP ingest endpoint, grouped per
        trace. Used when the WS worker couldn't deliver at exit (e.g. the API is unreachable on the WS
        port) — without this, every queued span is dropped. Mirrors the TS WSTransport.flushViaHttp.
        `extra` are frames written to a socket that died before confirming them. span_ended carries no
        trace_id on old SDK frames, so route it by the span id seen at span_started. Incomplete/lossy
        traces are stamped lossy so replay refuses byte-deterministic replay. Bounded; never raises
        (the process is exiting). Returns {trace_id: undelivered frame count} for the exit report."""
        cfg = get_config()
        if not cfg.api_key:
            return {}
        frames = list(extra or []) + self._take_unsent()
        if not frames:
            return {}
        by_trace: dict[str, dict[str, Any]] = {}
        span_trace: dict[str, str] = {}

        def entry(tid: str) -> dict[str, Any]:
            e = by_trace.get(tid)
            if e is None:
                e = {"trace": {}, "started": False, "ended": False, "spans": {}, "frames": 0}
                by_trace[tid] = e
            return e

        for raw in sorted(frames, key=lambda f: _frame_seq(f) or 0):
            try:
                m = json.loads(raw)
            except Exception:
                continue
            et = m.get("type")
            data: Any = m.get("data") or {}  # decoded JSON frame payload
            if et == "trace_started":
                tid = data.get("id")
                if tid:
                    entry(tid)["trace"].update(data)
                    entry(tid)["started"] = True
                    entry(tid)["frames"] += 1
            elif et == "trace_ended":
                tid = data.get("id")
                if tid:
                    e = entry(tid)
                    e["trace"].update(data)
                    e["ended"] = True
                    e["frames"] += 1
            elif et == "span_started":
                tid = data.get("trace_id")
                sid = data.get("id")
                if tid and sid:
                    span_trace[sid] = tid
                    entry(tid)["spans"][sid] = dict(data)
                    entry(tid)["frames"] += 1
            elif et == "span_ended":
                sid = data.get("id")
                tid = data.get("trace_id") or span_trace.get(sid)
                if tid and sid:
                    spans = entry(tid)["spans"]
                    if sid in spans:
                        spans[sid].update(data)
                    else:
                        spans[sid] = dict(data)
                    entry(tid)["frames"] += 1

        url = f"{cfg.base_url}/api/v1/traces"
        headers = {"x-retrace-key": cfg.api_key, "Content-Type": "application/json", "x-retrace-client": _CLIENT_ID}
        deadline = time.time() + budget_s
        undelivered: dict[str, int] = {}
        terminal_keys = (
            "status", "output", "ended_at", "total_tokens", "total_cost",
            "total_duration_ms", "lossy", "terminated_early", "span_count", "error",
        )

        def finish_with_patch(tid: str, e: dict[str, Any]) -> bool:
            # trace_started already reached the server — the trace exists, so finish it with the
            # terminal-state PATCH (same fields as the WS terminal frame).
            t = e["trace"]
            patch = {k: t[k] for k in terminal_keys if k in t}
            if tid in self._lossy_traces or e["spans"]:
                patch["lossy"] = True  # span frames that never reached the server are lost
            return _ok(requests.patch(f"{url}/{tid}", json=patch, headers=headers, timeout=1.5, allow_redirects=False))

        for tid, e in by_trace.items():
            if not e["trace"] or time.time() >= deadline:
                undelivered[tid] = e["frames"]
                continue
            try:
                if not e["started"] and e["ended"]:
                    if not finish_with_patch(tid, e) or e["spans"]:
                        undelivered[tid] = len(e["spans"]) or e["frames"]
                    continue
                # A span whose span_started already went out over WS only has its end frame here; it
                # can't form a valid span (no started_at/name/type) and would 400 the whole trace —
                # drop it and mark the trace lossy instead of losing everything.
                spans = [
                    sp for sp in e["spans"].values()
                    if sp.get("started_at") and sp.get("name") is not None and sp.get("span_type")
                ]
                body = {**e["trace"], "spans": spans}
                if (tid in self._lossy_traces) or not e["ended"] or len(spans) != len(e["spans"]):
                    body["lossy"] = True
                r = requests.post(url, json=body, headers=headers, timeout=1.5, allow_redirects=False)
                if getattr(r, "status_code", None) == 409 and e["ended"]:
                    # The trace reached the server after all (a confirmation lost with its socket):
                    # its terminal state still has to land; the spans in this body may be lost.
                    finish_with_patch(tid, e)
                    if spans:
                        undelivered[tid] = len(spans)
                elif not _ok(r):
                    undelivered[tid] = e["frames"]
            except Exception as ex:
                logger.debug(f"HTTP death-flush failed for trace {tid}: {ex}")
                undelivered[tid] = e["frames"]
        return undelivered

    def close(self) -> None:
        # Guard B: nothing pending → no worker spin-up, no drain wait, no exit penalty.
        if not self.has_pending_data():
            self._close_socket()
            self._report_dropped()
            return
        # Wait for the worker to connect (if it hasn't yet), write every queued frame AND for the
        # server to confirm them. A frame written to the socket is not delivered until acked: the
        # server reads frames one at a time, and closing right after the last write used to lose
        # whatever it had not read yet.
        self._ensure_worker()
        budget = float(getattr(get_config(), "flush_timeout", 5.0) or 5.0)
        deadline = time.time() + budget
        while time.time() < deadline and self.has_pending_data():
            time.sleep(0.02)
        # Stop the worker first so it cannot deliver a frame concurrently with the HTTP flush.
        self._closed = True
        with self._ack_lock:
            unconfirmed = list(self._unacked.values())
            self._unacked.clear()
        connected = self._connected
        unsent = self._inflight is not None or not self._send_queue.empty()
        undelivered: dict[str, int] = {}
        pending_ack: dict[str, int] = {}
        if unsent or (unconfirmed and not connected):
            # Never written, or written to a socket that died: send over HTTP. Frames written to a
            # live socket stay with the server, which ingests what it already received after we close.
            undelivered = self._flush_queue_via_http(extra=unconfirmed if not connected else None)
        if connected:
            for frame in unconfirmed:
                tid = _frame_trace_id(frame)
                if tid:
                    pending_ack[tid] = pending_ack.get(tid, 0) + 1
        self._close_socket()
        self._report_unconfirmed(undelivered, pending_ack, budget)
        self._report_dropped()

    def _close_socket(self) -> None:
        with self._lock:
            self._closed = True
            if self._ws:
                try:
                    self._ws.close()
                except Exception as e:
                    logger.debug(f"WebSocket close error: {e}")
                self._ws = None
                self._connected = False

    def _report_unconfirmed(
        self, undelivered: dict[str, int], pending_ack: dict[str, int], budget: float,
    ) -> None:
        """Never exit silently on an incomplete capture."""
        if undelivered:
            logger.warning(
                "[retrace] capture INCOMPLETE: %d event(s) of %d trace(s) could not be delivered before exit "
                "(traces: %s). Check connectivity to the Retrace API, or use RETRACE_TRANSPORT=http.",
                sum(undelivered.values()), len(undelivered), ", ".join(sorted(undelivered)),
            )
        if pending_ack:
            logger.warning(
                "[retrace] %d event(s) of %d trace(s) were sent but not confirmed within %.1fs (traces: %s); "
                "the server may still store them. Raise RETRACE_FLUSH_TIMEOUT if they are missing.",
                sum(pending_ack.values()), len(pending_ack), budget, ", ".join(sorted(pending_ack)),
            )


def _frame_meta(frame: str) -> dict[str, Any]:
    try:
        m = json.loads(frame)
        return m if isinstance(m, dict) else {}
    except Exception:
        return {}


def _frame_seq(frame: str) -> int | None:
    seq = _frame_meta(frame).get("seq")
    return seq if isinstance(seq, int) else None


def _frame_data_id(frame: str) -> str | None:
    return (_frame_meta(frame).get("data") or {}).get("id")


def _frame_trace_id(frame: str) -> str | None:
    m = _frame_meta(frame)
    d = m.get("data") or {}
    return d.get("trace_id") or (d.get("id") if str(m.get("type", "")).startswith("trace") else None)


def _ok(response: Any) -> bool:
    """A 2xx response. Unknown (mocked / no status) counts as delivered — nothing to report."""
    code = getattr(response, "status_code", None)
    return not isinstance(code, int) or 200 <= code < 300


class HTTPTransport:
    """HTTP transport: buffers each trace's events and uploads the whole trace when it ends.

    Uploads run on ONE background thread (``retrace-http-uploader``). They used to run in the thread
    that ended the trace, while holding the transport's lock: every other thread recording into the
    process waited for each upload in turn (≈2 s each against a distant API), so eight concurrent
    runs took 4–18 s and were stored with those inflated durations. The lock now only guards the
    buffers; ``end_trace`` returns at once; ``flush()`` / ``close()`` (atexit) wait for the queued
    uploads, bounded by ``flush_timeout``.

    ``background=False`` keeps the upload in the caller's thread (cascade replay must have delivered
    its trace before its thread exits).
    """

    def __init__(self, background: bool = True):
        # Buffer per-trace so concurrent traces in one process (threads / parallel work) never clobber
        # each other. Keyed by trace id; spans route by their trace_id.
        self._pending: dict[str, dict[str, Any]] = {}
        # span_ended events may carry no trace_id (parity with the WS protocol); remember each span's
        # trace from its span_started so the end event routes to the right per-trace buffer.
        self._span_trace: dict[str | None, str] = {}
        # Bounded in-memory offline backlog: a trace that fails to upload after exhausting transient
        # retries (429 / 5xx / network) is RETAINED and retried on a later upload instead of being
        # silently discarded. On overflow the oldest is dropped and counted (integrity signal).
        self._backlog: list[dict[str, Any]] = []
        self._backlog_cap = 1000
        self._dropped = 0
        # Guards the buffers above only — never held during network I/O.
        self._lock = threading.RLock()
        self._background = background
        self._uploads: queue.Queue[dict[str, Any]] = queue.Queue()
        self._worker: threading.Thread | None = None

    def _entry(self, trace_id: str) -> dict[str, Any]:
        e = self._pending.get(trace_id)
        if e is None:
            e = {"data": {}, "spans": []}
            self._pending[trace_id] = e
        return e

    def send_trace(self, trace: Trace) -> None:
        cfg = get_config()
        url = f"{cfg.base_url}/api/v1/traces"
        headers = {"x-retrace-key": cfg.api_key, "Content-Type": "application/json", "x-retrace-client": _CLIENT_ID}
        try:
            requests.post(url, json=trace.to_dict(), headers=headers, timeout=10, allow_redirects=False)
        except Exception as e:
            logger.debug(f"HTTP send_trace failed: {e}")

    def send(self, event_type: str, data: dict[str, Any]) -> None:
        # Guard A: unconfigured (no API key) → don't buffer.
        if not get_config().api_key:
            return
        payload: dict[str, Any] | None = None
        with self._lock:
            if event_type == "trace_started":
                tid = data.get("id")
                if tid:
                    self._entry(tid)["data"].update(data)
            elif event_type == "span_started":
                tid = data.get("trace_id")
                if tid:
                    self._span_trace[data.get("id")] = tid
                    self._entry(tid)["spans"].append({"_event": event_type, **data})
            elif event_type == "span_ended":
                # span_ended may have no trace_id — route by the span id captured at span_started.
                tid = data.get("trace_id") or self._span_trace.get(data.get("id"))
                if tid:
                    self._entry(tid)["spans"].append({"_event": event_type, **data})
            elif event_type == "trace_ended":
                tid = data.get("id")
                if not tid:
                    return
                e = self._pending.get(tid)
                if e is not None:
                    e["data"].update(data)
                payload = self._take_payload(tid)
        if payload is None:
            return
        if self._background:
            self._enqueue_upload(payload)
        else:
            self._deliver(payload, exit_mode=False)

    def has_pending_data(self) -> bool:
        return bool(self._pending) or len(self._backlog) > 0 or self._uploads.unfinished_tasks > 0

    def _take_payload(self, trace_id: str) -> dict[str, Any] | None:
        """Remove a trace's buffered events and merge them into one upload payload (lock held)."""
        e = self._pending.pop(trace_id, None)
        if not e:
            return None
        for ev in e["spans"]:
            self._span_trace.pop(ev.get("id"), None)
        # Merge span_started and span_ended events into complete spans.
        merged: dict[str, dict[str, Any]] = {}
        for ev in e["spans"]:
            ev = dict(ev)
            event_type = ev.pop("_event", None)
            span_id = ev.get("id", "")
            if event_type == "span_started":
                merged[span_id] = ev
            elif event_type == "span_ended" and span_id in merged:
                merged[span_id].update(ev)
        payload = dict(e["data"])
        payload["spans"] = list(merged.values())
        return payload

    def _enqueue_upload(self, payload: dict[str, Any]) -> None:
        self._uploads.put(payload)
        if self._worker is None or not self._worker.is_alive():
            with self._lock:
                if self._worker is None or not self._worker.is_alive():
                    self._worker = threading.Thread(
                        target=self._run_uploader, daemon=True, name="retrace-http-uploader",
                    )
                    self._worker.start()

    def _run_uploader(self) -> None:
        while True:
            payload = self._uploads.get()
            try:
                self._deliver(payload, exit_mode=False)
            except Exception as e:  # never let one bad payload stop the uploader
                logger.error("[retrace] trace upload failed unexpectedly: %s", e)
            finally:
                self._uploads.task_done()

    def _deliver(self, payload: dict[str, Any], exit_mode: bool) -> None:
        # Opportunistically retry anything stranded by a previous outage first (oldest first).
        self._drain_backlog(exit_mode)
        result = self._upload(payload, exit_mode)
        # On a transient failure in normal operation, RETAIN the trace (bounded offline backlog). On
        # exit there is no disk buffer to spill to (already logged as an error by _upload).
        if result == "retry" and not exit_mode:
            self._enqueue_backlog(payload)

    def _upload(self, payload: dict[str, Any], exit_mode: bool) -> str:
        """POST one complete trace payload. Returns: 'ok' | 'reject' (non-retryable 4xx) |
        'retry' (429 / 5xx / network after retries exhausted — caller should retain the payload)."""
        cfg = get_config()
        url = f"{cfg.base_url}/api/v1/traces"
        headers = {"x-retrace-key": cfg.api_key, "Content-Type": "application/json", "x-retrace-client": _CLIENT_ID}
        # Exit path must not hang the interpreter: a single ~1.5s attempt, no retries/backoff.
        timeout = 1.5 if exit_mode else 10
        attempts = 1 if exit_mode else 3
        # Encode once, up front. A value JSON cannot encode used to raise inside the retry loop, which
        # counted it as a network error: the trace went back to the head of the backlog and was
        # retried before every later upload, forever. default=str always produces a body.
        body = json.dumps(payload, default=str)
        for attempt in range(attempts):
            try:
                resp = requests.post(url, data=body, headers=headers, timeout=timeout, allow_redirects=False)
                if resp.status_code < 300:
                    return "ok"
                if resp.status_code < 500 and resp.status_code != 429:
                    # 4xx (client/payload error) won't succeed on retry — surface it loudly
                    # rather than silently dropping the trace.
                    logger.error(
                        "[retrace] trace upload rejected (HTTP %s): %s",
                        resp.status_code, (resp.text or "")[:300],
                    )
                    return "reject"
                # 5xx / 429 — transient
                if attempt == attempts - 1:
                    logger.error(
                        "[retrace] trace upload failed after %d attempts (HTTP %s): %s",
                        attempts, resp.status_code, (resp.text or "")[:200],
                    )
                else:
                    time.sleep(0.5 * (2 ** attempt))
                    continue
            except Exception as e:
                if attempt == attempts - 1:
                    logger.error("[retrace] trace upload network error after %d attempts: %s", attempts, e)
                else:
                    time.sleep(0.5 * (2 ** attempt))  # 0.5s, 1s backoff
                    continue
            break
        return "retry"

    def _enqueue_backlog(self, payload: dict[str, Any]) -> None:
        """Retain a failed trace for a later retry; bounded — drop + count the oldest on overflow."""
        with self._lock:
            self._backlog.append(payload)
            while len(self._backlog) > self._backlog_cap:
                self._backlog.pop(0)
                self._dropped += 1
                logger.error(
                    "[retrace] offline buffer full (cap %d) — dropped oldest trace (total dropped: %d)",
                    self._backlog_cap, self._dropped,
                )

    def _drain_backlog(self, exit_mode: bool) -> None:
        """Retry previously-failed traces oldest-first; stop at the first still-failing one so a
        server that is still down isn't hammered."""
        while True:
            with self._lock:
                if not self._backlog:
                    return
                head = self._backlog.pop(0)
            if self._upload(head, exit_mode) != "ok":
                with self._lock:
                    self._backlog.insert(0, head)
                return

    def _wait_for_uploads(self, timeout: float) -> bool:
        """Wait (bounded) until the background uploader has delivered everything queued."""
        deadline = time.monotonic() + max(0.0, timeout)
        while self._uploads.unfinished_tasks > 0:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.01)
        return True

    def flush(self, exit_mode: bool = False, trace_id: str | None = None) -> None:
        """Upload buffered traces (one, or all — including traces that never ended) and wait, bounded
        by ``flush_timeout``, for the queued background uploads."""
        with self._lock:
            ids = [trace_id] if trace_id is not None else list(self._pending.keys())
            payloads = [p for p in (self._take_payload(t) for t in ids) if p is not None]
        budget = float(getattr(get_config(), "flush_timeout", 5.0) or 5.0)
        if not self._wait_for_uploads(budget):
            logger.warning(
                "[retrace] %d trace upload(s) still in progress after %.1fs", self._uploads.unfinished_tasks, budget,
            )
        self._drain_backlog(exit_mode)
        for payload in payloads:
            self._deliver(payload, exit_mode)

    def close(self) -> None:
        self.flush(exit_mode=True)


def create_transport(mode: str = "auto") -> WSTransport | HTTPTransport:
    if mode == "http":
        return HTTPTransport()
    if mode == "ws":
        return WSTransport()
    # Auto: try WS
    try:
        import websocket  # noqa: F401
        return WSTransport()
    except ImportError:
        return HTTPTransport()
