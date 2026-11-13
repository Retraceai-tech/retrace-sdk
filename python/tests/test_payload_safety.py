"""Delivery survives what a traced function returns, and a span flood never evicts lifecycle frames.

Pass 8: returning an SDK response object (not JSON-serializable) poisoned the HTTP backlog (retried
as a "network error" before every later upload) and dropped trace_ended over WebSocket, and the
bounded send queue dropped trace_started / trace_ended frames first during a flood, so traces stayed
"running" with no spans.
"""
import dataclasses
import json
import queue
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import retrace
import retrace.config as cfg
from retrace.transport import WSTransport
from retrace.utils import to_jsonable

pydantic = pytest.importorskip("pydantic")


class Reply(pydantic.BaseModel):
    text: str
    when: datetime


@dataclasses.dataclass
class Step:
    name: str
    tags: set


def test_to_jsonable_handles_sdk_objects():
    when = datetime(2026, 10, 3, tzinfo=timezone.utc)
    value = {"r": Reply(text="hi", when=when), "s": Step("a", {"x"}), "b": b"\xffok", "o": object()}
    out = to_jsonable(value)
    json.dumps(out)  # serializable
    assert out["r"] == {"text": "hi", "when": "2026-10-03T00:00:00Z"}
    assert out["s"] == {"name": "a", "tags": ["x"]}
    assert out["b"] == "\ufffdok"
    assert out["o"].startswith("<object object")


@patch("retrace.recorder.create_transport")
def test_an_sdk_object_as_output_still_ends_the_trace(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)

    @retrace.record(name="returns-object")
    def run():
        return Reply(text="hi", when=datetime(2026, 1, 1, tzinfo=timezone.utc))

    run()
    ended = [c[0][1] for c in m.send.call_args_list if c[0][0] == "trace_ended"]
    assert len(ended) == 1
    assert ended[0]["output"] == {"text": "hi", "when": "2026-01-01T00:00:00Z"}
    json.dumps(ended[0])


def test_http_upload_of_an_unencodable_value_is_not_a_network_error():
    from retrace.transport import HTTPTransport

    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    t = HTTPTransport()
    sent = []

    def post(url, **kw):
        sent.append(kw)
        return MagicMock(status_code=200)

    with patch("retrace.transport.requests.post", side_effect=post):
        assert t._upload({"id": "x", "output": object()}, exit_mode=True) == "ok"
    assert json.loads(sent[0]["data"])["output"].startswith("<object object")


def test_a_full_send_queue_evicts_span_frames_before_lifecycle_frames():
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    t = WSTransport()
    t._send_queue = queue.Queue(maxsize=3)
    t._ensure_worker = lambda: None  # keep frames in the queue
    t.send("trace_started", {"id": "t1", "name": "run"})
    t.send("span_started", {"id": "s1", "trace_id": "t1"})
    t.send("span_started", {"id": "s2", "trace_id": "t1"})
    t.send("trace_ended", {"id": "t1", "status": "completed"})  # queue full: evicts s1, not trace_started
    t.send("span_started", {"id": "s3", "trace_id": "t1"})      # evicts s2
    kinds = [json.loads(m)["type"] for m in list(t._send_queue.queue)]
    assert kinds == ["trace_started", "trace_ended", "span_started"]
    assert "s1" in t._dropped_open_spans and "s2" in t._dropped_open_spans
