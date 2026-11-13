"""Shared pytest fixtures."""
import atexit
import importlib

import pytest

import retrace.recorder as _recorder
import retrace.telemetry as _telemetry


@pytest.fixture(scope="session", autouse=True)
def _neutralize_sdk_exit_hooks():
    """The SDK registers atexit flush hooks (recorder._flush_on_exit, init._shutdown_atexit) whenever
    it is configured. Across 90+ tests these accumulate and then run during interpreter shutdown —
    flushing via a `retrace-http-flush` daemon thread that does socket I/O while CPython is tearing
    down, which crashes with a native SIGSEGV (core dumped → pytest exit 139) on a clean all-pass exit.
    Remove them at session end so interpreter shutdown has nothing to run. Production is unaffected."""
    yield
    for mod, name in (("retrace.recorder", "_flush_on_exit"), ("retrace.init", "_shutdown_atexit")):
        try:
            fn = getattr(importlib.import_module(mod), name, None)
            if fn is not None:
                atexit.unregister(fn)
        except Exception:
            pass


@pytest.fixture(autouse=True)
def _disable_telemetry(monkeypatch):
    """Anonymous SDK telemetry (telemetry.py) fires on record success/failure via a daemon thread
    that POSTs to `/api/v1/internal/logs` (anonymous, no auth header, by design). In tests that thread
    races the assertions and pollutes `requests.post` mocks — landing as the LAST post, so
    `mock_post.call_args` sees the telemetry `{service, events}` body instead of the trace-ingestion
    call. Silence it so tests observe only the real trace/HTTP calls. Production behavior is unaffected.
    """
    for name in ("track_record", "track", "_send"):
        monkeypatch.setattr(_telemetry, name, lambda *a, **k: None)


@pytest.fixture(autouse=True)
def _reset_shared_transport():
    """The recorder caches a module-level shared transport, so without a reset the per-test
    `@patch("retrace.recorder.create_transport")` only takes effect for the first test to
    create a recorder. Reset it around every test so each test gets its own (mocked) transport.
    """
    _recorder._shared_transport = None
    yield
    _recorder._shared_transport = None
