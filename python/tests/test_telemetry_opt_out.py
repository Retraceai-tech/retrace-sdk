"""Telemetry can be turned off (pass 7: it could not)."""
import threading

import retrace
from retrace import telemetry
from retrace.config import telemetry_from_env
from retrace.telemetry import track as real_track  # bound before conftest stubs telemetry.track


def _count_threads(monkeypatch):
    started = []
    real = threading.Thread

    def fake_thread(*args, **kwargs):
        t = real(target=lambda: None)
        started.append(kwargs.get("target"))
        return t

    monkeypatch.setattr(telemetry.threading, "Thread", fake_thread)
    return started


def test_nothing_is_sent_when_turned_off(monkeypatch):
    started = _count_threads(monkeypatch)
    retrace.configure(telemetry=False)
    real_track("probe")
    assert started == []
    retrace.configure(telemetry=True)
    real_track("probe")
    assert len(started) == 1
    retrace.configure(telemetry=False)


def test_env_switches():
    assert telemetry_from_env({}) is True
    for v in ("0", "false", "OFF", " no "):
        assert telemetry_from_env({"RETRACE_TELEMETRY": v}) is False
    assert telemetry_from_env({"RETRACE_TELEMETRY": "1"}) is True
    assert telemetry_from_env({"DO_NOT_TRACK": "1"}) is False
    assert telemetry_from_env({"DO_NOT_TRACK": "0"}) is True
