"""One span per tool call (pass 7: an app's own tool span + the SDK's record of the requested call
were two tool_call spans, read as a repeated step), tool results recorded once per trace (a
process-wide dedupe lost a span when two concurrent runs shared a tool-call id), end_span(span)."""
from unittest.mock import MagicMock, patch

import retrace.config as cfg
from retrace import SpanType, TraceRecorder


def _rec(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    r = TraceRecorder(name="tools")
    r.start_trace()
    return r, m


def _requested(rec, name="lookup_order", args=None, call_id="call_1"):
    rec._handle_intercepted_span({
        "span_type": "tool_call", "name": name, "input": args or {"order_id": "A-1"},
        "metadata": {"tool_call_id": call_id}, "duration_ms": 0,
    })


@patch("retrace.recorder.create_transport")
def test_app_span_fills_in_the_requested_call(mt):
    rec, m = _rec(mt)
    _requested(rec)
    span = rec.start_span("lookup_order", SpanType.TOOL_CALL, input={"order_id": "A-1"})
    rec.end_span(span, output={"carrier": "DHL"})  # accepts the Span itself
    started = [c[0][1] for c in m.send.call_args_list if c[0][0] == "span_started"]
    ended = [c[0][1] for c in m.send.call_args_list if c[0][0] == "span_ended"]
    assert [s["name"] for s in started] == ["lookup_order"]  # one tool_call, not two
    assert ended[-1]["id"] == started[0]["id"] and ended[-1]["output"] == {"carrier": "DHL"}
    # A different call (other arguments) is its own span.
    other = rec.start_span("lookup_order", SpanType.TOOL_CALL, input={"order_id": "B-2"})
    assert other.id != started[0]["id"]
    rec.end_trace()


@patch("retrace.recorder.create_transport")
def test_tool_results_once_per_trace_and_independent_across_traces(mt):
    rec_a, m = _rec(mt)
    rec_b = TraceRecorder(name="other")
    rec_b.start_trace()
    result = {
        "span_type": "tool_result", "name": "lookup_order", "output": "{}", "metadata": {"tool_call_id": "call_same"},
    }
    rec_a._handle_intercepted_span(dict(result))
    rec_a._handle_intercepted_span(dict(result))  # the next model call re-sends the same history
    rec_b._handle_intercepted_span(dict(result))  # another run whose provider reused the id
    results = [
        c[0][1] for c in m.send.call_args_list if c[0][0] == "span_started" and c[0][1]["span_type"] == "tool_result"
    ]
    assert sorted(r["trace_id"] for r in results) == sorted([rec_a.trace.id, rec_b.trace.id])
    # End in reverse start order: each end_trace() restores the context the trace started in.
    rec_b.end_trace()
    rec_a.end_trace()
