"""What an LLM span records as its input (pass 7: the first 10 messages were kept, so the newest —
the question and the tool results the answer quoted — were dropped, and correct answers were judged
"unsupported"). Also: an intercepted span starts when the call STARTED, not when it returned."""
from datetime import timedelta
from unittest.mock import MagicMock, patch

import retrace.config as cfg
from retrace import TraceRecorder
from retrace.interceptors._messages import MAX_RECORDED_MESSAGES, recorded_contents, recorded_messages


def _conversation(n: int) -> list[dict]:
    msgs = [{"role": "system", "content": "You are a shipping assistant."}]
    for i in range(n):
        msgs += [{"role": "user", "content": f"old question {i}"}, {"role": "assistant", "content": f"old answer {i}"}]
    msgs += [{"role": "user", "content": "Where is my order A-1?"},
             {"role": "tool", "content": '{"carrier": "DHL", "eta": "2026-10-04"}'}]
    return msgs


def test_keeps_the_system_message_and_the_newest_messages():
    rec = recorded_messages(_conversation(6))
    msgs = rec["messages"]
    assert len(msgs) == MAX_RECORDED_MESSAGES
    assert msgs[0] == {"role": "system", "content": "You are a shipping assistant."}
    assert msgs[-1]["content"] == '{"carrier": "DHL", "eta": "2026-10-04"}'
    assert msgs[-2]["content"] == "Where is my order A-1?"
    assert rec["messages_omitted"] == 15 - MAX_RECORDED_MESSAGES


def test_short_conversations_are_recorded_whole_and_unmarked():
    rec = recorded_messages(_conversation(1))
    assert len(rec["messages"]) == 5
    assert "messages_omitted" not in rec


def test_accepts_message_objects_and_cuts_long_text():
    class M:
        role = "user"
        content = "x" * 5000
    rec = recorded_messages([M()])
    assert rec["messages"][0]["role"] == "user"
    assert len(rec["messages"][0]["content"]) == 1000


def test_gemini_contents_keep_the_newest_items_and_the_end_of_long_text():
    items = [f"turn {i}" for i in range(30)]
    assert "turn 29" in recorded_contents(items) and "turn 0," not in recorded_contents(items)
    long = "start " + "y" * 5000 + " THE-END"
    out = recorded_contents(long)
    assert out.startswith("start") and out.endswith("THE-END") and len(out) <= 2000
    assert recorded_contents(None) is None


@patch("retrace.recorder.create_transport")
def test_intercepted_span_starts_when_the_call_started(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    rec = TraceRecorder(name="timing")
    rec.start_trace()
    rec._handle_intercepted_span(
        {"span_type": "llm_call", "name": "openai.chat.completions.create", "duration_ms": 1500, "output": "hi"},
    )
    span = rec._trace.spans[-1]
    assert span.ended_at - span.started_at == timedelta(milliseconds=1500)
    rec.end_trace()
