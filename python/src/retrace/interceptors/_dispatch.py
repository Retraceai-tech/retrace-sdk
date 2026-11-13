"""
Context-isolated routing for auto-instrumented spans.

The interceptors (openai/anthropic/gemini) are patched globally and call a single module
callback. Previously that callback was a module-global set per recorder (last-writer-wins),
so under concurrency intercepted LLM spans could be routed to the wrong trace. We instead
install ONE stable dispatcher whose target recorder lives in a ContextVar — each thread /
async task resolves its own active recorder, eliminating cross-trace contamination.
"""
from __future__ import annotations

import contextvars
from typing import Any, Callable, Optional

_SpanCallback = Callable[[dict[str, Any]], Any]

_active_recorder_cb: contextvars.ContextVar[Optional[_SpanCallback]] = contextvars.ContextVar(
    "retrace_active_recorder_cb", default=None
)
# The recorder acting as the open-span sink for the current context (two-phase streaming spans).
_active_open_sink: contextvars.ContextVar[Optional[Any]] = contextvars.ContextVar(
    "retrace_active_open_sink", default=None
)

# Returned by set_active_recorder (reset_active_recorder also accepts a bare callback token).
_ActiveRecorderToken = tuple[
    contextvars.Token[Optional[_SpanCallback]], Optional[contextvars.Token[Optional[Any]]]
]


def dispatch_intercepted_span(span_data: dict[str, Any]) -> None:
    """Stable interceptor callback — routes to the recorder active in the current context."""
    cb = _active_recorder_cb.get()
    if cb is not None:
        cb(span_data)


def capture_active_emit() -> Optional[_SpanCallback]:
    """Capture the span sink active in THIS context (at invocation) so a deferred streaming
    finalizer can emit to the right recorder even when later resumed in a context where the
    ContextVar is absent (generator .close()/GeneratorExit, trace-end, atexit)."""
    return _active_recorder_cb.get()


def current_recorder() -> Optional[Any]:
    """The recorder of the trace running in THIS context (a ``record()`` / ``@record`` run), if any."""
    return _active_open_sink.get()


def register_open_span(span_id: str, finalize: Callable[[str], None]) -> None:
    """Register an open streaming span's finalizer with the active recorder (two-phase, model (b))."""
    sink = _active_open_sink.get()
    if sink is not None:
        sink.register_open_span(span_id, finalize)


def unregister_open_span(span_id: str) -> None:
    sink = _active_open_sink.get()
    if sink is not None:
        sink.unregister_open_span(span_id)


def set_active_recorder(cb: _SpanCallback, sink: Any = None) -> _ActiveRecorderToken:
    """Make `cb` the active intercepted-span handler (and `sink` the open-span recorder) for the
    current context. Returns a token for reset_active_recorder."""
    t1 = _active_recorder_cb.set(cb)
    t2 = _active_open_sink.set(sink) if sink is not None else None
    return (t1, t2)


def reset_active_recorder(token: _ActiveRecorderToken | contextvars.Token[Optional[_SpanCallback]]) -> None:
    try:
        if isinstance(token, tuple):
            t1, t2 = token
            _active_recorder_cb.reset(t1)
            if t2 is not None:
                _active_open_sink.reset(t2)
        else:
            _active_recorder_cb.reset(token)  # backward-compat (older single-token callers)
    except Exception:
        pass


class SyncStreamProxy:
    """Wrap a provider ``Stream`` so iteration goes through Retrace's capturing generator while every
    other attribute (``.response``, ``.close()``, context-manager protocol, provider helpers) still
    reaches the real stream. Returning a bare generator broke ``with create(stream=True) as s:``,
    ``s.response`` and openai's ``chat.completions.stream()`` helper (reads ``raw_stream.response``)."""

    def __init__(self, stream: Any, gen: Any) -> None:
        self._rt_stream = stream
        self._rt_gen = gen

    def __iter__(self) -> Any:
        return self._rt_gen

    def __next__(self) -> Any:
        return next(self._rt_gen)

    def __enter__(self) -> "SyncStreamProxy":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._rt_gen.close()  # finalize("partial") if not fully drained
        finally:
            close = getattr(self._rt_stream, "close", None)
            if callable(close):
                close()

    def __getattr__(self, name: str) -> Any:
        # Only reached for attributes not found normally. Guard our own slots so a half-built proxy
        # (copy/pickle before __init__) raises AttributeError instead of recursing forever.
        if name.startswith("_rt_"):
            raise AttributeError(name)
        return getattr(self._rt_stream, name)


class AsyncStreamProxy:
    """Async counterpart of :class:`SyncStreamProxy` (``AsyncStream``)."""

    def __init__(self, stream: Any, agen: Any) -> None:
        self._rt_stream = stream
        self._rt_gen = agen

    def __aiter__(self) -> Any:
        return self._rt_gen

    async def __anext__(self) -> Any:
        return await self._rt_gen.__anext__()

    async def __aenter__(self) -> "AsyncStreamProxy":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        try:
            await self._rt_gen.aclose()  # finalize("partial") if not fully drained
        finally:
            close = getattr(self._rt_stream, "close", None)
            if callable(close):
                res = close()
                if hasattr(res, "__await__"):
                    await res

    aclose = close

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_rt_"):
            raise AttributeError(name)
        return getattr(self._rt_stream, name)


# ── Patch markers ───────────────────────────────────────────────────────────────────────────────
# A provider method we replaced carries this attribute, so a second install (or a reload that reset
# the module globals) never captures our own wrapper as the "original" and recurses into itself.
_WRAPPER_ATTR = "_retrace_wrapper"


def mark_retrace_wrapper(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Mark ``fn`` as a Retrace patch and return it."""
    setattr(fn, _WRAPPER_ATTR, True)
    return fn


def is_retrace_wrapper(fn: object) -> bool:
    """True when ``fn`` is a Retrace patch (see ``mark_retrace_wrapper``)."""
    return bool(getattr(fn, _WRAPPER_ATTR, False))
