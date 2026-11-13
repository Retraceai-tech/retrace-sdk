"""A failed run stores why it failed (pass 7: a run that raised ValueError("boom") was stored as
"failed" with no message)."""
from unittest.mock import MagicMock, patch

import pytest

import retrace
import retrace.config as cfg
from retrace import TraceRecorder
from retrace.utils import describe_exception


def _ended(m):
    return [c[0][1] for c in m.send.call_args_list if c[0][0] == "trace_ended"]


@patch("retrace.recorder.create_transport")
def test_decorator_records_the_exception(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)

    @retrace.record(name="fails")
    def run():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        run()
    (end,) = _ended(m)
    assert end["status"] == "failed" and end["error"] == "ValueError: boom"


@patch("retrace.recorder.create_transport")
def test_context_manager_records_the_exception_and_success_sends_none(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    with pytest.raises(KeyError):
        with retrace.record(name="cm"):
            raise KeyError("missing")
    with retrace.record(name="ok"):
        pass
    failed, ok = _ended(m)
    assert failed["error"] == "KeyError: 'missing'"
    assert "error" not in ok


@patch("retrace.recorder.create_transport")
def test_end_trace_accepts_a_message(mt):
    m = MagicMock()
    mt.return_value = m
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    rec = TraceRecorder(name="manual")
    rec.start_trace()
    rec.end_trace(status=retrace.TraceStatus.FAILED, error="x" * 5000)
    (end,) = _ended(m)
    assert len(end["error"]) == 2000


def test_describe_exception():
    assert describe_exception(RuntimeError()) == "RuntimeError"
    assert describe_exception(None) is None
