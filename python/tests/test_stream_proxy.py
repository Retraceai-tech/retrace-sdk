"""Regression tests for the streaming proxy (provider ``Stream`` surface preserved) and the WebSocket
listener idle poll. Provider-independent: no provider SDK needs to be installed."""
import asyncio
import copy
import json
import logging
import socket
import threading
import time
from types import SimpleNamespace

import websocket

from retrace.interceptors import anthropic as a
from retrace.interceptors._dispatch import AsyncStreamProxy, SyncStreamProxy
from retrace.transport import WSTransport


def _delta(text):
    return SimpleNamespace(type="content_block_delta", delta=SimpleNamespace(text=text, partial_json=None))


class _FakeStream:
    """Minimal stand-in for a provider ``Stream``: iterable, has ``.response`` and ``close()``."""

    def __init__(self, items):
        self._items = list(items)
        self.response = SimpleNamespace(status_code=200)
        self.closed = False

    def __iter__(self):
        return iter(self._items)

    def close(self):
        self.closed = True


class _FakeAsyncStream:
    def __init__(self, items):
        self._items = list(items)
        self.response = SimpleNamespace(status_code=200)
        self.closed = False

    async def __aiter__(self):
        for it in self._items:
            yield it

    async def close(self):
        self.closed = True


def _capture(monkeypatch):
    spans = []
    monkeypatch.setattr(a, "_on_span", lambda s: spans.append(s))
    return spans


MSGS = [{"role": "user", "content": "hi"}]


def test_sync_proxy_context_manager_early_exit_is_partial_and_closes_stream(monkeypatch):
    spans = _capture(monkeypatch)
    raw = _FakeStream([_delta("hel"), _delta("lo")])
    proxy = SyncStreamProxy(raw, a._wrap_anthropic_stream(raw, "sp1", "claude-x", MSGS, time.time()))
    with proxy as st:
        assert st.response.status_code == 200  # forwarded to the real stream
        next(iter(st))  # consume one chunk, then leave the block early
    assert raw.closed is True
    llm = next(s for s in spans if s["span_type"] == "llm_call")
    assert llm["metadata"]["capture_complete"] is False and llm["metadata"]["partial"] is True


def test_sync_proxy_full_drain_is_complete(monkeypatch):
    spans = _capture(monkeypatch)
    raw = _FakeStream([_delta("hel"), _delta("lo")])
    with SyncStreamProxy(raw, a._wrap_anthropic_stream(raw, "sp2", "claude-x", MSGS, time.time())) as st:
        assert len(list(st)) == 2
    llm = next(s for s in spans if s["span_type"] == "llm_call")
    assert llm["output"] == "hello" and llm["metadata"]["capture_complete"] is True
    assert raw.closed is True


def test_async_proxy_context_manager_and_aclose(monkeypatch):
    spans = _capture(monkeypatch)
    raw = _FakeAsyncStream([_delta("hel"), _delta("lo")])

    async def run():
        proxy = AsyncStreamProxy(raw, a._wrap_anthropic_async_stream(raw, "sp3", "claude-x", MSGS, time.time()))
        async with proxy as st:
            assert st.response.status_code == 200
            got = [c async for c in st]
        assert len(got) == 2
        await proxy.aclose()  # idempotent after the context manager already closed it

    asyncio.run(run())
    assert raw.closed is True
    llm = next(s for s in spans if s["span_type"] == "llm_call")
    assert llm["output"] == "hello" and llm["metadata"]["capture_complete"] is True


def test_proxy_copy_does_not_recurse():
    raw = _FakeStream([])
    proxy = SyncStreamProxy(raw, iter(()))
    clone = copy.copy(proxy)  # would RecursionError if __getattr__ resolved _rt_* via itself
    assert clone.response is raw.response


class _FakeWS:
    """recv() times out (websocket-client style) a few times, delivers a server ping, then returns ""
    (what websocket-client's recv() yields for a server close frame)."""

    def __init__(self, idle_polls):
        self._script = [websocket.WebSocketTimeoutException("timed out")] * idle_polls
        self._script += [json.dumps({"type": "ping"}), ""]
        self.sent = []
        self.recv_after_close = 0
        self.pong = threading.Event()

    def settimeout(self, _t):
        pass

    def recv(self):
        if not self._script:
            self.recv_after_close += 1
            raise websocket.WebSocketConnectionClosedException("closed")
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def send(self, data):
        self.sent.append(data)
        if json.loads(data).get("type") == "pong":
            self.pong.set()

    def close(self):
        pass


def test_ws_listener_survives_idle_timeouts_and_answers_ping(caplog):
    """websocket-client raises WebSocketTimeoutException (not a TimeoutError subclass) on an idle
    recv; the listener must treat it as an idle poll instead of exiting after the first second.
    A server close frame ("" from recv) ends the listener quietly instead of warning about a bad frame."""
    t = WSTransport()
    ws = _FakeWS(idle_polls=3)
    t._ws = ws
    t._connected = True
    try:
        with caplog.at_level(logging.WARNING, logger="retrace"):
            t._start_listener()
            assert ws.pong.wait(5.0), "listener died on the idle timeout and never answered the ping"
            t._listener_thread.join(timeout=5.0)
        assert not t._listener_thread.is_alive()
        assert ws.recv_after_close == 0  # stopped at the close frame
        assert not [r for r in caplog.records if "unhandled server frame" in r.getMessage()]
    finally:
        t._closed = True
        t._connected = False
        if t._listener_thread:
            t._listener_thread.join(timeout=5.0)


class _OverlapWS:
    """Counts any send that runs while a recv is in progress on the same socket (or vice versa).
    Python's SSL sockets are not safe for concurrent read + write from two threads: over wss:// the
    overlap intermittently tore the connection down and lost every frame already written.

    `sock` is a real socketpair end kept readable by a feeder thread, so the transport can poll it
    exactly as it polls a websocket-client socket."""

    def __init__(self):
        self.sock, self._peer = socket.socketpair()
        self._busy = threading.Lock()
        self.overlaps = 0
        self.sent = []
        self.stop = threading.Event()
        self._feeder = threading.Thread(target=self._feed, daemon=True)
        self._feeder.start()

    def _feed(self):
        while not self.stop.is_set():
            try:
                self._peer.send(b"x")
            except OSError:
                return
            time.sleep(0.001)

    def _io(self):
        if not self._busy.acquire(blocking=False):
            self.overlaps += 1
            return
        try:
            time.sleep(0.002)
        finally:
            self._busy.release()

    def settimeout(self, _t):
        pass

    def recv(self):
        self._io()
        self.sock.recv(1)
        raise websocket.WebSocketTimeoutException("no complete frame yet")

    def send(self, data):
        self._io()
        self.sent.append(data)

    def close(self):
        self.stop.set()
        self.sock.close()
        self._peer.close()


class _SlowReadWS(_OverlapWS):
    """Every read holds the socket for 50 ms (a busy connection), and data is always pending."""

    def recv(self):
        self._io()
        time.sleep(0.05)
        self.sock.recv(1)
        raise websocket.WebSocketTimeoutException("no complete frame yet")


def test_ws_sender_is_not_held_behind_reads():
    """Queued frames go out back to back. The listener re-took the shared lock after every read, so
    each frame waited for one more read: 20 frames behind 50 ms reads took >= 1 s (one read each)."""
    t = WSTransport()
    ws = _SlowReadWS()
    t._ws = ws
    t._connected = True
    frames = [json.dumps({"type": "span_started", "data": {"i": i}}) for i in range(20)]
    try:
        t._start_listener()
        time.sleep(0.1)  # the listener is mid-read when the frames arrive
        started = time.monotonic()
        for f in frames:
            t._send_queue.put(f)
        t._ensure_worker()
        while len(ws.sent) < len(frames) and time.monotonic() - started < 10:
            time.sleep(0.005)
        elapsed = time.monotonic() - started
        assert ws.sent == frames
        assert ws.overlaps == 0
        assert elapsed < 0.6, f"20 frames took {elapsed:.2f}s behind 50 ms reads"
    finally:
        t._closed = True
        t._connected = False
        for th in (t._listener_thread, t._worker_thread):
            if th:
                th.join(timeout=5.0)
        ws.close()


def test_ws_listener_never_reads_while_the_sender_writes():
    t = WSTransport()
    ws = _OverlapWS()
    t._ws = ws
    t._connected = True
    frames = [json.dumps({"type": "span_started", "data": {"i": i}}) for i in range(150)]
    try:
        t._start_listener()
        for f in frames:
            t._send_queue.put(f)
        t._ensure_worker()
        deadline = time.time() + 10
        while len(ws.sent) < len(frames) and time.time() < deadline:
            time.sleep(0.01)
        assert ws.sent == frames
        assert ws.overlaps == 0, f"{ws.overlaps} concurrent send/recv on one socket"
    finally:
        t._closed = True
        t._connected = False
        for th in (t._listener_thread, t._worker_thread):
            if th:
                th.join(timeout=5.0)
        ws.close()
