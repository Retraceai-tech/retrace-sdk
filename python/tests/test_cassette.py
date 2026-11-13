"""The golden-cassette writer serializes a recorded run into the committed CI contract shape."""
import json
import os
import tempfile
from unittest.mock import MagicMock, patch

import retrace.config as cfg
from retrace import SpanType, TraceRecorder, write_golden_cassette


@patch("retrace.recorder.create_transport")
def test_write_golden_cassette(mt):
    mt.return_value = MagicMock()
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)

    rec = TraceRecorder(name="agent", input={"q": 1})
    s = rec.start_span("call-model", SpanType.LLM_CALL, input={"p": "hi"}, model="gpt-4")
    rec.end_span(s.id, output="the answer")

    path = os.path.join(tempfile.gettempdir(), f"retrace-cas-{os.getpid()}.json")
    try:
        cas = write_golden_cassette(
            path, recorder=rec, tolerance={"default": "exact", "steps": {"call-model": "semantic"}}
        )
        assert cas["version"] == 1
        assert cas["name"] == "agent"
        assert len(cas["steps"]) == 1
        assert cas["steps"][0]["span_type"] == "llm_call"
        assert cas["steps"][0]["model"] == "gpt-4"
        assert cas["steps"][0]["output"] == "the answer"
        with open(path, encoding="utf-8") as f:
            disk = json.load(f)
        assert disk["tolerance"]["steps"]["call-model"] == "semantic"
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_write_golden_cassette_without_recorder_raises():
    import pytest

    with pytest.raises(RuntimeError, match="no active recorder"):
        write_golden_cassette("/tmp/never-written.json")
