"""Confirmed delivery over the WebSocket transport (pass-5 P1: the default quickstart lost its run).

The SDK used to treat "written to the socket" as delivered and close straight after the last write.
The server reads frames one at a time, so a short run that exited at once lost whatever had not been
read yet. Now every frame carries a sequence number, the server acks it once ingested, and close()
waits for those acks; frames a dead socket never confirmed are re-sent after a reconnect, or sent over
HTTP at exit, and anything still unconfirmed is reported instead of being assumed delivered.
"""
import json
import logging
import queue
import socket
import threading
import time

import pytest
import websocket

import retrace
import retrace.config as cfg
from retrace import SpanType, TraceRecorder
from retrace.transport import WSTransport


class FakeServer:
    """In-process stand-in for the API's WS endpoint: auth_ok, then acks each ingest frame after
    `ack_delay` (the server's processing time), unless `acks` is False.

    Like a real websocket-client connection it exposes `.sock`, readable only when a server frame is
    waiting, so the transport's listener waits in select() OUTSIDE its send lock. (Without it the
    listener sat in a blocking recv while holding the lock, and on Python 3.10 the sender thread was
    starved: a test artifact, not transport behaviour.)"""

    def __init__(self, ack_delay=0.0, acks=True, die_after=None):
        self.ack_delay = ack_delay
        self.acks = acks
        self.die_after = die_after  # raise on the N+1th ingest frame (connection lost)
        self.frames = []  # ingest frames received, in order
        self.auth = None
        self.closed = False
        self._inbox = queue.Queue()
        self._lock = threading.Lock()
        self.sock, self._wake = socket.socketpair()
        self.sock.setblocking(False)

    # websocket-client surface used by WSTransport
    def send(self, raw):
        m = json.loads(raw)
        if m["type"] == "auth":
            self.auth = m
            return
        with self._lock:
            if self.die_after is not None and len(self.frames) >= self.die_after:
                raise ConnectionResetError("connection lost")
            self.frames.append(m)
        if self.acks:
            ack = {"type": "ack", "id": m["data"]["id"], "seq": m["seq"]}
            threading.Timer(self.ack_delay, self._deliver, args=(json.dumps(ack),)).start()

    def _deliver(self, frame):
        self._inbox.put(frame)
        try:
            self._wake.send(b"x")  # the socket becomes readable
        except OSError:
            pass  # closed by close()

    def recv(self):
        if self.auth is not None and not getattr(self, "_auth_answered", False):
            self._auth_answered = True
            return json.dumps({"type": "auth_ok"})
        try:
            frame = self._inbox.get(timeout=0.05)
        except queue.Empty:
            raise websocket.WebSocketTimeoutException("idle")
        try:
            self.sock.recv(1)
        except BlockingIOError:
            pass
        return frame

    def settimeout(self, _t):
        pass

    def close(self):
        self.closed = True
        for s in (self.sock, self._wake):
            s.close()


@pytest.fixture
def servers(monkeypatch):
    """Queue of FakeServers handed out per connect; records HTTP fallback calls."""
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True, flush_timeout=3.0)
    pool = []
    http = {"post": [], "patch": []}

    def create_connection(url, **kw):
        if not pool:
            raise ConnectionRefusedError("no server")
        return pool.pop(0)

    monkeypatch.setattr(websocket, "create_connection", create_connection)
    monkeypatch.setattr("retrace.transport.requests.post", lambda url, json=None, **kw: http["post"].append(json))
    monkeypatch.setattr(
        "retrace.transport.requests.patch", lambda url, json=None, **kw: http["patch"].append((url, json)),
    )
    yield pool, http
    cfg.configure(flush_timeout=5.0)


def _record_run(t: WSTransport, spans=3, trace_id="t1"):
    t.send("trace_started", {"id": trace_id, "name": "run", "status": "running", "started_at": "2026-10-01T10:00:00Z"})
    for i in range(spans):
        sid = f"{trace_id}-s{i}"
        t.send("span_started", {"id": sid, "trace_id": trace_id, "name": "search", "span_type": "tool_call",
                                "started_at": "2026-10-01T10:00:00Z"})
        t.send("span_ended", {"id": sid, "trace_id": trace_id, "ended_at": "2026-10-01T10:00:01Z", "output": "x"})
    t.send("trace_ended", {"id": trace_id, "ended_at": "2026-10-01T10:00:02Z", "status": "failed",
                           "output": "gave up", "span_count": spans})


def test_close_waits_for_every_ack_before_closing(servers):
    pool, http = servers
    server = FakeServer(ack_delay=0.3)  # the server is still ingesting when the last frame is written
    pool.append(server)
    t = WSTransport()
    _record_run(t)
    t.close()
    assert server.auth["acks"] is True
    expected = ["trace_started"] + ["span_started", "span_ended"] * 3 + ["trace_ended"]
    assert [f["type"] for f in server.frames] == expected
    assert [f["seq"] for f in server.frames] == sorted(f["seq"] for f in server.frames)
    assert not t.has_pending_data()  # every frame confirmed before the socket closed
    assert server.closed
    assert http == {"post": [], "patch": []}


def test_reconnect_resends_what_the_dead_socket_never_confirmed(servers):
    pool, http = servers
    first = FakeServer(acks=False, die_after=3)  # takes 3 frames, confirms none, then the link drops
    second = FakeServer()
    pool.extend([first, second])
    t = WSTransport()
    _record_run(t)
    t.close()
    # The new socket got the unconfirmed frames first (same seq, in order), then the rest.
    assert [f["seq"] for f in second.frames[:3]] == [f["seq"] for f in first.frames]
    assert sorted(f["seq"] for f in second.frames) == list(range(1, 9))
    assert not t.has_pending_data()
    assert http == {"post": [], "patch": []}


def test_frames_a_dead_socket_never_confirmed_go_over_http_at_exit(servers, caplog):
    pool, http = servers
    pool.append(FakeServer(acks=False, die_after=4))  # no reconnect possible afterwards
    cfg.configure(flush_timeout=0.5)
    t = WSTransport()
    _record_run(t)
    with caplog.at_level(logging.WARNING, logger="retrace"):
        t.close()
    (body,) = http["post"]
    assert body["id"] == "t1" and body["status"] == "failed" and body["output"] == "gave up"
    assert len(body["spans"]) == 3
    assert not [r for r in caplog.records if "INCOMPLETE" in r.getMessage()]


def test_sent_but_unconfirmed_events_are_reported_not_assumed_delivered(servers, caplog):
    pool, http = servers
    pool.append(FakeServer(acks=False))  # connected, but the server never confirms
    cfg.configure(flush_timeout=0.4)
    t = WSTransport()
    _record_run(t, spans=1)
    with caplog.at_level(logging.WARNING, logger="retrace"):
        t.close()
    msgs = [r.getMessage() for r in caplog.records]
    assert any("4 event(s) of 1 trace(s) were sent but not confirmed" in m and "t1" in m for m in msgs), msgs
    assert http == {"post": [], "patch": []}  # still on the server's socket: no duplicate over HTTP


def test_undeliverable_capture_is_reported_as_incomplete(servers, caplog, monkeypatch):
    pool, _http = servers  # no server at all
    cfg.configure(flush_timeout=0.3)

    class Refused:
        status_code = 503

    monkeypatch.setattr("retrace.transport.requests.post", lambda *a, **k: Refused())
    t = WSTransport()
    _record_run(t, spans=1)
    with caplog.at_level(logging.WARNING, logger="retrace"):
        t.close()
    assert any("capture INCOMPLETE" in r.getMessage() and "t1" in r.getMessage() for r in caplog.records)


def test_server_acks_without_seq_are_matched_in_order_by_id(servers):
    """Older servers ack by id only, in ingest order (span_started and span_ended share the id)."""
    t = WSTransport()
    t._remember_unacked('{"type":"span_started","data":{"id":"s1"},"seq":1}')
    t._remember_unacked('{"type":"span_ended","data":{"id":"s1"},"seq":2}')
    t._dispatch({"type": "ack", "id": "s1"})
    assert list(t._unacked) == [2]
    t._dispatch({"type": "ack", "id": "s1"})
    assert not t._unacked


class _Capture:
    def __init__(self):
        self.events = []

    def send(self, event_type, data):
        self.events.append((event_type, data))

    def close(self):
        pass


def test_trace_ended_reports_the_spans_emitted_and_no_terminal_patch_is_raced(monkeypatch):
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    patches = []
    monkeypatch.setattr("requests.patch", lambda *a, **k: patches.append(a))
    cap = _Capture()
    monkeypatch.setattr("retrace.recorder._get_shared_transport", lambda: cap)
    rec = TraceRecorder(name="run")
    rec.start_trace()
    for _ in range(2):
        s = rec.start_span("search", SpanType.TOOL_CALL, input={"q": 1})
        rec.end_span(s.id, output="x")
    rec.end_trace(output="done")
    time.sleep(0.05)
    (ended,) = [d for e, d in cap.events if e == "trace_ended"]
    assert ended["span_count"] == 2
    assert patches == []


def test_record_context_manager_keeps_session_id_and_exposes_the_recorder(monkeypatch):
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    cap = _Capture()
    monkeypatch.setattr("retrace.recorder._get_shared_transport", lambda: cap)
    with retrace.record(name="chat", session_id="sess-1") as t:
        s = t.start_span("lookup", SpanType.TOOL_CALL, input={"q": 1})
        t.end_span(s.id, output=4)
        t.output = 4
    (started,) = [d for e, d in cap.events if e == "trace_started"]
    assert started["session_id"] == "sess-1"
    assert [e for e, _ in cap.events] == ["trace_started", "span_started", "span_ended", "trace_ended"]


def test_add_span_leaves_unknown_usage_out_of_span_ended(monkeypatch):
    """A completed span without cost/tokens must not send them as null (the server rejected the frame)."""
    import retrace
    from retrace import SpanType
    from retrace.trace import Span
    from retrace.utils import utcnow

    sent = []
    with retrace.record(name="usage-null") as t:
        rec = t._recorder
        monkeypatch.setattr(rec, "_send", lambda et, data: sent.append((et, data)))
        s = Span(trace_id=rec._trace.id, span_type=SpanType.TOOL_CALL, name="lookup", input={"i": 1}, output=1)
        s.ended_at = utcnow()
        rec.add_span(s)
    ended = [d for et, d in sent if et == "span_ended"]
    assert len(ended) == 1
    assert "cost" not in ended[0] and "output_tokens" not in ended[0]
    assert ended[0]["output"] == 1
