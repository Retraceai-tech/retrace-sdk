"""The `retrace.agent(...)` context tags every span created inside it with the agent id/role and
sets the delegation parent from the enclosing block — the data the topology graph is drawn from."""
from unittest.mock import MagicMock, patch

import retrace.config as cfg
from retrace import SpanType, TraceRecorder, agent, current_agent


@patch("retrace.recorder.create_transport")
def test_agent_context_tags_spans(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)

    rec = TraceRecorder(name="agent-test")
    rec.start_trace()
    with agent("planner", role="Planner"):
        s = rec.start_span("think", SpanType.LLM_CALL, input={"q": 1})
    rec.end_span(s.id, output={"ok": True})
    rec.end_trace(output={"done": True})

    started = [c[0][1] for c in m.send.call_args_list if c[0][0] == "span_started"]
    assert any(e.get("agent_id") == "planner" and e.get("agent_role") == "Planner" for e in started)


@patch("retrace.recorder.create_transport")
def test_nested_agent_sets_parent(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)

    rec = TraceRecorder(name="agent-nest")
    rec.start_trace()
    with agent("orchestrator"):
        with agent("worker", role="Worker"):
            s = rec.start_span("do", SpanType.TOOL_CALL, input={})
    rec.end_span(s.id, output={})
    rec.end_trace(output={})

    started = [c[0][1] for c in m.send.call_args_list if c[0][0] == "span_started"]
    worker = next(e for e in started if e.get("agent_id") == "worker")
    assert worker["parent_agent_id"] == "orchestrator"


def test_current_agent_resets_after_block():
    assert current_agent() is None
    with agent("a"):
        assert current_agent()["agent_id"] == "a"
    assert current_agent() is None
