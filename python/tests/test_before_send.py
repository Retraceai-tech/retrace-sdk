"""``before_send`` redacts or drops events before they leave the process (pass 8: no hook existed)."""
from unittest.mock import MagicMock, patch

import retrace
import retrace.config as cfg


def _events(m):
    return [(c[0][0], c[0][1]) for c in m.send.call_args_list]


@patch("retrace.recorder.create_transport")
def test_before_send_can_redact_and_drop(mt):
    m = MagicMock()
    mt.return_value = m

    def scrub(event_type, data):
        if data.get("output") == "drop me":
            return None
        if "input" in data:
            data = {**data, "input": "[redacted]"}
        return data

    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True, before_send=scrub)
    try:
        @retrace.record(name="secret")
        def run(password):
            return "ok"

        run("hunter2")
        sent = _events(m)
        assert all("hunter2" not in str(d) for _, d in sent)
        started = [d for t, d in sent if t == "trace_started"]
        assert started and started[0]["input"] == "[redacted]"

        @retrace.record(name="dropped")
        def run2():
            return "drop me"

        m.send.reset_mock()
        run2()
        assert [t for t, _ in _events(m)] == ["trace_started"]  # trace_ended dropped by the hook
    finally:
        cfg.configure(before_send=None)


@patch("retrace.recorder.create_transport")
def test_a_failing_hook_never_sends_the_raw_event(mt):
    m = MagicMock()
    mt.return_value = m

    def broken(event_type, data):
        raise RuntimeError("bug in redaction")

    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True, before_send=broken)
    try:
        @retrace.record(name="x")
        def run(secret):
            return secret

        run("topsecret")
        assert m.send.call_count == 0
    finally:
        cfg.configure(before_send=None)
