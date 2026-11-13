"""HTTP transport uploads on a background thread (pass 7: uploads ran in the caller's thread while
holding the transport lock, so concurrent runs waited for each other's ~2 s uploads and were stored
with those inflated durations)."""
import threading
import time

import retrace.config as cfg
from retrace.transport import HTTPTransport


def _trace(t: HTTPTransport, tid: str):
    t.send("trace_started", {"id": tid, "started_at": "2026-10-02T10:00:00Z", "status": "running"})
    t.send("span_started", {"id": f"{tid}-s", "trace_id": tid, "name": "x", "span_type": "tool_call",
                            "started_at": "2026-10-02T10:00:00Z"})
    t.send("span_ended", {"id": f"{tid}-s", "ended_at": "2026-10-02T10:00:00.100Z", "output": "ok"})
    t.send("trace_ended", {"id": tid, "ended_at": "2026-10-02T10:00:00.200Z", "status": "completed"})


def test_concurrent_runs_never_wait_for_uploads(monkeypatch):
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    t = HTTPTransport()
    uploaded: list[str] = []

    def slow_upload(payload, exit_mode):
        time.sleep(0.2)  # a distant API
        uploaded.append(payload["id"])
        return "ok"

    monkeypatch.setattr(t, "_upload", slow_upload)
    ended: list[float] = []

    def run(i: int):
        t0 = time.monotonic()
        _trace(t, f"t{i}")
        ended.append(time.monotonic() - t0)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(8)]
    [th.start() for th in threads]
    [th.join() for th in threads]
    # Every run finished its trace_ended without waiting for any upload (8 × 0.2 s serialized before).
    assert max(ended) < 0.1
    assert t.has_pending_data()
    t.flush()
    assert sorted(uploaded) == sorted(f"t{i}" for i in range(8))
    assert not t.has_pending_data()


def test_close_uploads_traces_that_never_ended_and_waits_for_the_queue(monkeypatch):
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    t = HTTPTransport()
    uploaded: list[tuple[str, bool]] = []
    monkeypatch.setattr(t, "_upload", lambda p, exit_mode: uploaded.append((p["id"], exit_mode)) or "ok")
    _trace(t, "done")
    t.send("trace_started", {"id": "open", "started_at": "2026-10-02T10:00:00Z", "status": "running"})
    t.close()
    assert ("done", False) in uploaded and ("open", True) in uploaded


def test_a_failed_upload_is_retained_and_retried_later(monkeypatch):
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    t = HTTPTransport()
    # first upload fails, its retry at flush fails too (API still down), then the API is back.
    results = iter(["retry", "retry", "ok", "ok"])
    uploaded: list[str] = []
    monkeypatch.setattr(t, "_upload", lambda p, exit_mode: (uploaded.append(p["id"]), next(results))[1])
    _trace(t, "first")
    t.flush()
    assert t._backlog and t._backlog[0]["id"] == "first"
    _trace(t, "second")
    t.flush()
    assert uploaded == ["first", "first", "first", "second"] and not t._backlog


def test_synchronous_mode_uploads_in_the_callers_thread(monkeypatch):
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True)
    t = HTTPTransport(background=False)
    seen: list[str] = []
    monkeypatch.setattr(t, "_upload", lambda p, exit_mode: seen.append(threading.current_thread().name) or "ok")
    _trace(t, "sync")
    assert seen == [threading.current_thread().name]
