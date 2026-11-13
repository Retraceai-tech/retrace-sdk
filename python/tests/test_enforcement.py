"""Unit tests for the client-side enforcement gate (local limits, offline — no network)."""
import pytest

from retrace.config import RetraceConfig
from retrace.enforcement import EnforcementGate, hash_tool_args
from retrace.errors import RetraceEnforcementError


def _gate(**limits) -> EnforcementGate:
    cfg = RetraceConfig(api_key="rt_test", **limits)
    return EnforcementGate(cfg, run_id="run-test")


def test_inactive_when_no_limits():
    gate = _gate()
    assert gate.active is False
    # No-op: never raises regardless of usage.
    for _ in range(1000):
        gate.record_and_check(tokens=10_000, usd=100.0)


def test_step_ceiling_trips_after_limit():
    gate = _gate(max_steps_per_run=3)
    assert gate.active is True
    gate.record_and_check()  # 1
    gate.record_and_check()  # 2
    gate.record_and_check()  # 3
    with pytest.raises(RetraceEnforcementError) as exc:
        gate.record_and_check()  # 4 > 3
    assert exc.value.verdict == "block"
    assert "step" in exc.value.reason.lower()


def test_token_ceiling_accumulates():
    gate = _gate(max_tokens_per_run=100)
    gate.record_and_check(tokens=60)  # 60 <= 100
    with pytest.raises(RetraceEnforcementError):
        gate.record_and_check(tokens=50)  # 110 > 100


def test_usd_ceiling_accumulates():
    gate = _gate(max_usd_per_run=1.0)
    gate.record_and_check(usd=0.5)
    with pytest.raises(RetraceEnforcementError) as exc:
        gate.record_and_check(usd=0.7)  # 1.2 > 1.0
    assert "usd" in exc.value.reason.lower()


def test_local_limit_applies_without_server_enforcement():
    # server_enforcement defaults False → no network is ever attempted; local limit still fires.
    gate = _gate(max_steps_per_run=1)
    gate.record_and_check()
    with pytest.raises(RetraceEnforcementError):
        gate.record_and_check()


def test_hash_tool_args_is_stable_and_short():
    a = hash_tool_args({"q": "weather", "n": 3})
    b = hash_tool_args({"n": 3, "q": "weather"})  # key order independent
    assert a == b
    assert len(a) == 32


def test_explicit_arg_beats_env(monkeypatch):
    monkeypatch.setenv("RETRACE_MAX_STEPS_PER_RUN", "999")
    cfg = RetraceConfig(api_key="rt_test", max_steps_per_run=2)  # explicit wins over env
    assert cfg.max_steps_per_run == 2


def test_env_used_when_no_explicit_arg(monkeypatch):
    monkeypatch.setenv("RETRACE_MAX_TOKENS_PER_RUN", "500")
    cfg = RetraceConfig(api_key="rt_test")
    assert cfg.max_tokens_per_run == 500


def test_server_check_reports_the_span_id(monkeypatch):
    """The server's ingest backstop evaluates the same recorded span: the /check body names it so the
    step is counted once (it used to count every gated step twice)."""
    import retrace.enforcement as enf

    sent = []

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return {"verdict": "allow"}

    def post(url, json, headers, timeout, allow_redirects=True):
        assert allow_redirects is False  # the key is never forwarded to a redirect target
        sent.append(json)
        return _Resp()

    monkeypatch.setattr(enf.requests, "post", post)
    gate = _gate(server_enforcement=True)
    gate.record_and_check(tokens=5, trace_id="t-1", span_id="s-1")
    assert sent == [
        {"run_id": "run-test", "proposed_tokens": 5, "proposed_usd": 0.0, "trace_id": "t-1", "span_id": "s-1"},
    ]
