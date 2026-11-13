"""Raw-response calls and tool-calling turns are recorded with their tokens and output (pass 8).

langchain-openai calls ``with_raw_response`` and got an LLM span with 0 tokens and empty output; a
tool-calling turn (no text content) stored an empty output; and adapter spans inside ``@record`` were
lost because adapters only saw the ambient ``init()`` trace.
"""
import importlib
import json
from unittest.mock import MagicMock, patch

import pytest

import retrace
import retrace.config as cfg
from retrace.trace import SpanType


def _hx(pkg):
    base = importlib.import_module(f"{pkg.__name__}._base_client")
    return getattr(base, "httpx2", None) or base.httpx


def test_openai_raw_response_and_tool_call_turn_are_recorded():
    openai = pytest.importorskip("openai")
    from retrace.interceptors import openai as oi

    hx = _hx(openai)
    tool_turn = {"id": "c", "object": "chat.completion", "created": 1, "model": "gpt-4o",
                 "choices": [{"index": 0, "finish_reason": "tool_calls", "message": {
                     "role": "assistant", "content": None,
                     "tool_calls": [{"id": "call_1", "type": "function",
                                     "function": {"name": "get_weather", "arguments": "{\"city\": \"Paris\"}"}}]}}],
                 "usage": {"prompt_tokens": 120, "completion_tokens": 30, "total_tokens": 150}}
    spans = []
    oi.install_openai_interceptor(on_span=spans.append)
    try:
        transport = hx.MockTransport(lambda r: hx.Response(200, json=tool_turn))
        client = openai.OpenAI(api_key="x", base_url="http://mock/v1", http_client=hx.Client(transport=transport))
        msgs = [{"role": "user", "content": "q"}]
        raw = client.chat.completions.with_raw_response.create(model="gpt-4o", messages=msgs)
        assert raw.parse().choices[0].finish_reason == "tool_calls"  # the caller's object still works
        (llm,) = [s for s in spans if s["span_type"] == "llm_call"]
        assert (llm["input_tokens"], llm["output_tokens"]) == (120, 30)
        assert llm["output"] == "[tool calls] get_weather({\"city\": \"Paris\"})"
        assert [s["name"] for s in spans if s["span_type"] == "tool_call"] == ["get_weather"]
    finally:
        oi.uninstall_openai_interceptor()


def test_anthropic_tool_use_turn_output_is_not_empty():
    from retrace.interceptors.tool_spans import anthropic_output_text

    content = [{"type": "tool_use", "id": "t1", "name": "lookup", "input": {"q": "x"}}]
    assert anthropic_output_text(content) == "[tool calls] lookup({\"q\": \"x\"})"
    mixed = [{"type": "text", "text": "a"}, {"type": "tool_use", "name": "n"}, {"type": "text", "text": "b"}]
    assert anthropic_output_text(mixed) == "a\nb"


@patch("retrace.recorder.create_transport")
def test_adapter_spans_go_to_the_record_run_not_only_the_ambient_trace(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)

    @retrace.record(name="run-with-tools")
    def run():
        rec = retrace.get_active_recorder()
        assert rec is not None
        span = rec.start_span(name="get_weather", span_type=SpanType.TOOL_CALL, input={"city": "Paris"})
        rec.end_span(span.id, output="21C")
        return rec._trace.id

    trace_id = run()
    assert retrace.get_active_recorder() is None  # nothing ambient outside the run
    started = [c[0][1] for c in m.send.call_args_list if c[0][0] == "span_started"]
    assert [(s["name"], s["trace_id"]) for s in started] == [("get_weather", trace_id)]
    json.dumps(started)
