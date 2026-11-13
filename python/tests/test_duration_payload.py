"""Regression tests for the P0 "duration stored as 0 ms / null" defect.

The recorder measured durations locally but never put them on the terminal events, so the server
persisted totalDurationMs=0 and span durationMs=null (reproduced with a 650 ms span on 0.16.17).
These tests pin the wire contract: terminal events carry the measured values.
"""
import json
import time
from unittest.mock import MagicMock, patch

import retrace.config as cfg
from retrace import Span, SpanType, TraceRecorder
from retrace.transport import HTTPTransport, WSTransport
from retrace.utils import utcnow


def _events(m, name):
    return [c[0][1] for c in m.send.call_args_list if c[0][0] == name]


@patch("retrace.recorder.create_transport")
def test_terminal_events_carry_measured_durations(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)

    rec = TraceRecorder(name="duration-regression")
    rec.start_trace()
    s = rec.start_span("synthetic-sleep", SpanType.TOOL_CALL, input={"x": 1})
    time.sleep(0.12)
    rec.end_span(s.id, output={"ok": True})
    rec.end_trace(output={"done": True})

    (span_end,) = _events(m, "span_ended")
    assert span_end["duration_ms"] >= 110, span_end
    assert span_end["duration_ms"] == s.duration_ms

    (trace_end,) = _events(m, "trace_ended")
    assert trace_end["total_duration_ms"] >= 110, trace_end
    assert trace_end["total_duration_ms"] == rec.trace.total_duration_ms
    # started_at lets a death-flush that only holds this frame build a valid ingest payload.
    assert trace_end["started_at"].endswith("Z")


@patch("retrace.recorder.create_transport")
def test_completed_add_span_derives_duration(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)

    rec = TraceRecorder(name="add-span-duration")
    rec.start_trace()
    sp = Span(span_type=SpanType.TOOL_CALL, name="done")
    time.sleep(0.05)
    sp.ended_at = utcnow()
    rec.add_span(sp)
    rec.end_trace(output=None)  # release the context-bound active recorder (test isolation)
    (span_end,) = _events(m, "span_ended")
    assert span_end["duration_ms"] >= 40


def test_http_transport_upload_merges_terminal_durations(monkeypatch):
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    sent = []
    t = HTTPTransport()
    monkeypatch.setattr(t, "_upload", lambda payload, exit_mode: sent.append(payload) or "ok")
    t.send("trace_started", {"id": "t1", "started_at": "2026-09-28T10:00:00Z", "total_duration_ms": 0,
                             "status": "running"})
    t.send("span_started", {"id": "s1", "trace_id": "t1", "name": "x", "span_type": "tool_call",
                            "started_at": "2026-09-28T10:00:00Z"})
    t.send("span_ended", {"id": "s1", "trace_id": "t1", "ended_at": "2026-09-28T10:00:00.650Z", "duration_ms": 650})
    t.send("trace_ended", {"id": "t1", "ended_at": "2026-09-28T10:00:00.653Z", "status": "completed",
                           "total_duration_ms": 653})
    t.flush()  # wait for the background uploader
    (payload,) = sent
    assert payload["total_duration_ms"] == 653
    assert payload["spans"][0]["duration_ms"] == 650


def test_ws_death_flush_includes_inflight_frame(monkeypatch):
    """The WS worker holds the dequeued head frame (usually trace_started) while it retries a failed
    connect. The exit-time HTTP flush must include it, or the POST has no started_at → HTTP 400."""
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    t = WSTransport()
    t._inflight = json.dumps({"type": "trace_started", "data": {
        "id": "t1", "name": "n", "started_at": "2026-09-28T10:00:00Z", "status": "running"}})
    t._send_queue.put_nowait(json.dumps({"type": "trace_ended", "data": {
        "id": "t1", "ended_at": "2026-09-28T10:00:01Z", "status": "completed", "total_duration_ms": 1000}}))
    posted = []
    monkeypatch.setattr("retrace.transport.requests.post", lambda url, json=None, **kw: posted.append(json))
    t._flush_queue_via_http()
    (body,) = posted
    assert body["started_at"] == "2026-09-28T10:00:00Z"
    assert body["total_duration_ms"] == 1000
    assert t._inflight is None


def test_ws_death_flush_patches_already_started_trace(monkeypatch):
    """trace_started already delivered over WS → the trace exists; finish it with a PATCH, not a
    duplicate POST (which the server would 409)."""
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    t = WSTransport()
    t._send_queue.put_nowait(json.dumps({"type": "trace_ended", "data": {
        "id": "t1", "ended_at": "2026-09-28T10:00:01Z", "status": "completed", "total_duration_ms": 1000}}))
    posted, patched = [], []
    monkeypatch.setattr("retrace.transport.requests.post", lambda url, json=None, **kw: posted.append(json))
    monkeypatch.setattr("retrace.transport.requests.patch", lambda url, json=None, **kw: patched.append((url, json)))
    t._flush_queue_via_http()
    assert posted == []
    (url, body) = patched[0]
    assert url.endswith("/api/v1/traces/t1")
    assert body["status"] == "completed" and body["total_duration_ms"] == 1000


def test_ws_connect_suppresses_origin(monkeypatch):
    """websocket-client synthesizes an Origin header by default; API servers reject it (4003)."""
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    import websocket

    seen = {}

    def fake_create(url, **kw):
        seen.update(kw)
        raise ConnectionError("stop here")

    monkeypatch.setattr(websocket, "create_connection", fake_create)
    WSTransport()._connect()
    assert seen.get("suppress_origin") is True
