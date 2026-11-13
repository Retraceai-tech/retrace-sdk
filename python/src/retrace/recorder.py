from __future__ import annotations

import functools
import inspect
import logging
import os
import random
import threading
from datetime import timedelta
from types import FrameType, TracebackType
from typing import TYPE_CHECKING, Any, Callable, Literal

from .agents import current_agent
from .config import get_config, require_api_key
from .trace import Span, SpanType, Trace, TraceStatus
from .transport import HTTPTransport, WSTransport, create_transport
from .utils import MAX_ERROR_CHARS, describe_exception, to_jsonable, utcnow

if TYPE_CHECKING:
    import contextvars

    from .interceptors._dispatch import _ActiveRecorderToken

logger = logging.getLogger("retrace")


def _apply_agent_ctx(span: Span) -> None:
    """Inherit the active ``retrace.agent(...)`` context onto a span (explicit values win)."""
    ctx = current_agent()
    if not ctx:
        return
    if not span.agent_id:
        span.agent_id = ctx.get("agent_id")
    if not span.agent_role:
        span.agent_role = ctx.get("agent_role")
    if not span.parent_agent_id:
        span.parent_agent_id = ctx.get("parent_agent_id")


def _should_sample(rate: float, seed: str | None = None, key: str | None = None) -> bool:
    """Deterministic sampling using FNV-1a hash when seed is provided."""
    if rate >= 1.0:
        return True
    if rate <= 0.0:
        return False
    if not seed:
        return random.random() < rate
    # FNV-1a hash for deterministic decision
    input_str = f"{seed}:{key or ''}"
    h = 2166136261
    for ch in input_str:
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF
    return (h / 4294967296.0) < rate


# Shared transport — stays open across multiple traces for resume/replay listening
_shared_transport: WSTransport | HTTPTransport | None = None
_shared_lock = threading.Lock()

# Pre-exit hooks run BEFORE the transport drains on the SIGTERM path — e.g. the ambient trace
# finishing itself (emitting a terminated_early trace_ended) so it's in the buffer for the flush.
_pre_exit_hooks: list[Callable[[str], Any]] = []
_installed_sigterm_handler: Callable[[int, FrameType | None], None] | None = None


def on_process_exit(fn: Callable[[str], Any]) -> None:
    """Register a callable(reason: str) run on the SIGTERM exit path before the transport drains."""
    _pre_exit_hooks.append(fn)


def _run_pre_exit_hooks(reason: str) -> None:
    for h in list(_pre_exit_hooks):
        try:
            h(reason)
        except Exception:
            pass


def _get_shared_transport() -> WSTransport | HTTPTransport:
    global _shared_transport
    with _shared_lock:
        if _shared_transport is None:
            _shared_transport = create_transport(get_config().transport)
            cfg = get_config()
            # Guard A: only register the network exit hook when actually configured — an
            # imported-but-unconfigured SDK installs no atexit handler and makes no exit-time call.
            if cfg.enabled and cfg.api_key:
                import atexit
                atexit.register(_flush_on_exit)
                _install_sigterm_handler()
            # Fork awareness: gunicorn/uvicorn pre-fork is the dominant Python deploy. A child
            # inherits the parent's buffered events + dead worker/socket refs; without this it could
            # re-emit the parent's buffer. Clear the child's transport state right after fork.
            if hasattr(os, "register_at_fork"):
                os.register_at_fork(after_in_child=_reset_transport_in_child)
        return _shared_transport


def _install_sigterm_handler() -> None:
    """Flush the buffer on SIGTERM (docker stop / k8s pod termination) — atexit does NOT run on
    SIGTERM, so a containerized job would otherwise silently lose its buffered trace.

    Sole-owner + main-thread gate (the exact analog of TS's listenerCount('SIGTERM') === 0): install
    ONLY if SIGTERM is still SIG_DFL (no framework owns it) and we're on the main thread (signal.signal
    off the main thread raises ValueError). If gunicorn/uvicorn/celery already installed a SIGTERM
    handler, getsignal != SIG_DFL → we skip and never clobber their graceful shutdown. We chain by
    restoring SIG_DFL and re-raising, so the process still terminates with normal SIGTERM semantics.
    SIGINT is intentionally left to atexit — seizing Ctrl-C/KeyboardInterrupt is the surprising case.
    """
    import signal
    if threading.current_thread() is not threading.main_thread():
        return
    try:
        if signal.getsignal(signal.SIGTERM) is not signal.SIG_DFL:
            return  # a framework (or the user) owns SIGTERM — stay a guest, don't clobber

        def _sigterm_handler(signum: int, frame: FrameType | None) -> None:
            _run_pre_exit_hooks("signal")  # finish ambient trace (terminated_early) into the buffer
            _flush_on_exit()               # drain the bounded buffer (Guard B: no-op if empty)
            # Exit 143 (128+SIGTERM) directly rather than restoring SIG_DFL + re-raising: under a
            # container the SDK is often PID 1, and Linux IGNORES SIG_DFL signals for PID 1 — a
            # re-raise would hang. os._exit is immediate and PID-1-safe (we only install this when
            # SIGTERM was SIG_DFL, so there is no prior handler to chain to).
            os._exit(143)

        signal.signal(signal.SIGTERM, _sigterm_handler)
        global _installed_sigterm_handler
        _installed_sigterm_handler = _sigterm_handler
    except (ValueError, OSError):
        pass  # not main thread / not permitted — never crash the user's import


def _reset_transport_in_child() -> None:
    if _shared_transport is not None and hasattr(_shared_transport, "reset_for_child"):
        try:
            _shared_transport.reset_for_child()
        except Exception:
            pass
    # Belt-and-suspenders: drop the SIGTERM handler we installed in the parent so a raw os.fork /
    # multiprocessing child never carries the master's handler (which would finalize the master's
    # ambient trace from the child). gunicorn/uvicorn re-establish their own signals in the worker
    # post-fork anyway; this just covers the no-framework case.
    if _installed_sigterm_handler is not None:
        try:
            import signal
            if signal.getsignal(signal.SIGTERM) is _installed_sigterm_handler:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
        except (ValueError, OSError):
            pass


def _flush_on_exit() -> None:
    """Flush any pending data before process exits (no-op when nothing is pending — Guard B)."""
    t = _shared_transport
    if t is None:
        return
    # Guard B: zero-event run → nothing to drain, no exit-time network call.
    if hasattr(t, "has_pending_data") and not t.has_pending_data():
        return
    try:
        t.close()
    except Exception:
        pass


class TraceRecorder:
    """Manages recording of a single trace and its spans."""

    def __init__(
        self, name: str | None = None, input: Any = None, metadata: dict[str, Any] | None = None,
        resumable: bool = False, session_id: str | None = None, fork_point_span_id: str | None = None,
        fork_point_index: int | None = None,
    ) -> None:
        require_api_key()
        self._trace = Trace(
            name=name,
            input=input,
            metadata=metadata or {},
            project_id=get_config().project_id,
            session_id=session_id,
        )
        if resumable:
            self._trace.metadata["_resumable"] = True
        self._lock = threading.Lock()
        self._transport = _get_shared_transport()
        self._interceptors_installed = False
        # Fork point filtering: suppress pre-fork spans during cascade replay.
        # Server copies pre-fork spans; SDK only emits from fork point onward.
        self._fork_point_span_id = fork_point_span_id
        # 0-based ordinal of the fork-point span in the original trace. Cascade replay re-executes the
        # whole function with NEW span ids, so suppression of pre-fork spans must be positional: emit
        # only from this index onward. None => no suppression (emit everything).
        self._fork_point_index = fork_point_index
        self._fork_point_reached = fork_point_span_id is None or fork_point_index is None
        self._span_counter = 0
        # Spans actually emitted (span_started sent). Reported on trace_ended as span_count so the
        # server can tell a complete capture from one that lost spans in transit.
        self._emitted_spans = 0
        self._emitted_lock = threading.Lock()
        self._cv_token: _ActiveRecorderToken | None = None
        self._tp_token: contextvars.Token[tuple[str, str] | None] | None = None
        # span_id -> finalize(reason) for two-phase streaming spans
        self._open_spans: dict[str, Callable[[str], None]] = {}
        # Tool results already recorded for this trace (keyed by provider tool-call id): each model call
        # re-sends the conversation's earlier tool results, and only the first sighting is a step. Per
        # trace — a process-wide set made two concurrent runs that shared an id lose one of them.
        self._seen_tool_results: set[str] = set()
        # Tool calls the model requested (recorded by the interceptors, no result yet), waiting for the
        # app's own span that runs them: start_span adopts the matching one instead of adding a second
        # tool_call for the same call.
        self._requested_tool_calls: list[Span] = []
        # Client-side circuit breaker for this run (no-op unless a local limit or server enforcement
        # is configured). Trips between calls — see enforcement.EnforcementGate.
        from .enforcement import EnforcementGate
        self._enforcement = EnforcementGate(get_config(), self._trace.id)

    @property
    def trace(self) -> Trace:
        return self._trace

    @property
    def output(self) -> Any:
        return self._trace.output

    @output.setter
    def output(self, value: Any) -> None:
        self._trace.output = value

    def _install_interceptors(self) -> None:
        if self._interceptors_installed:
            return
        # Install ONE stable dispatcher; the active recorder is resolved per-context (see
        # interceptors/_dispatch.py) so concurrent traces don't cross-route spans. Only providers the
        # app imports are patched (now, or right after their import): importing all three here cost
        # 0.9-2 s on the first record().
        from .interceptors._dispatch import dispatch_intercepted_span
        from .interceptors._lazy import when_imported
        from .interceptors.anthropic import install_anthropic_interceptor
        from .interceptors.gemini import install_gemini_interceptor
        from .interceptors.openai import install_openai_interceptor

        when_imported("google.genai", lambda: install_gemini_interceptor(dispatch_intercepted_span))
        when_imported("openai", lambda: install_openai_interceptor(dispatch_intercepted_span))
        when_imported("anthropic", lambda: install_anthropic_interceptor(dispatch_intercepted_span))
        self._interceptors_installed = True

    def _handle_intercepted_span(self, span_data: dict[str, Any]) -> None:
        call_id = (span_data.get("metadata") or {}).get("tool_call_id")
        if span_data.get("span_type") == "tool_result" and call_id:
            with self._lock:
                if call_id in self._seen_tool_results:
                    return
                if len(self._seen_tool_results) > 10_000:
                    self._seen_tool_results.clear()
                self._seen_tool_results.add(call_id)
        span = Span(
            trace_id=self._trace.id,
            span_type=SpanType(span_data.get("span_type", "llm_call")),
            name=span_data.get("name", ""),
            parent_id=span_data.get("parent_id"),
            model=span_data.get("model"),
            input=span_data.get("input"),
            output=span_data.get("output"),
            input_tokens=span_data.get("input_tokens"),
            output_tokens=span_data.get("output_tokens"),
            cost=span_data.get("cost"),
            duration_ms=span_data.get("duration_ms"),
            metadata=span_data.get("metadata") or {},
            error=span_data.get("error"),
        )
        if span_data.get("id"):
            span.id = span_data["id"]
        _apply_agent_ctx(span)
        span.ended_at = utcnow()
        # The interceptors report the call when it RETURNS: the span used to start when it ended, so an
        # LLM call appeared after the tool results recorded during it, and the server's grounding
        # context ("what the step could know": steps that STARTED before it) missed its inputs. The
        # measured duration places the start where the call began.
        duration = span_data.get("duration_ms")
        if isinstance(duration, (int, float)) and not isinstance(duration, bool) and duration > 0:
            span.started_at = span.ended_at - timedelta(milliseconds=duration)
        self.add_span(span)
        if span.span_type == SpanType.TOOL_CALL and call_id and span.output is None:
            with self._lock:
                self._requested_tool_calls.append(span)
                del self._requested_tool_calls[:-100]

    def start_trace(self, name: str | None = None, input: Any = None, metadata: dict[str, Any] | None = None) -> None:
        if name:
            self._trace.name = name
        if input is not None:
            self._trace.input = input
        if metadata:
            self._trace.metadata.update(metadata)
        self._install_interceptors()
        # Route auto-instrumented spans to THIS recorder for the current context only.
        from .interceptors._dispatch import set_active_recorder
        self._cv_token = set_active_recorder(self._handle_intercepted_span, self)
        # Propagate W3C trace context for outbound HTTP calls made during the trace.
        from .traceparent import set_trace_context
        self._tp_token = set_trace_context(self._trace.id, self._trace.id)
        self._send("trace_started", self._trace.to_dict())

    def register_open_span(self, span_id: str, finalize: Callable[[str], None]) -> None:
        """Two-phase streaming spans: register an open span's finalizer (model (b))."""
        self._open_spans[span_id] = finalize

    def unregister_open_span(self, span_id: str) -> None:
        self._open_spans.pop(span_id, None)

    def _finalize_open_spans(self) -> None:
        """Close any still-open streaming spans as partial (capture_complete=False) — a stream that
        was abandoned mid-drain (or never reached a clean end) is emitted into the trace, flagged
        not-byte-replayable, rather than lost."""
        if not self._open_spans:
            return
        for finalize in list(self._open_spans.values()):
            try:
                finalize("partial")
            except Exception:
                pass
        self._open_spans.clear()

    def end_trace(
        self, output: Any = None, status: TraceStatus = TraceStatus.COMPLETED, terminated_early: bool = False,
        error: str | BaseException | None = None,
    ) -> None:
        """End the trace. ``error``: why a failed run failed (an exception, or its message); it is
        stored with the trace (it was dropped: a run that raised showed only "failed")."""
        # Close dangling streaming spans BEFORE the terminal event so they land in this trace.
        self._finalize_open_spans()
        self._trace.output = output if output is not None else self._trace.output
        self._trace.status = status
        self._trace.ended_at = utcnow()
        if self._trace.started_at:
            self._trace.total_duration_ms = int(
                (self._trace.ended_at - self._trace.started_at).total_seconds() * 1000
            )
        trace_ended_payload = {
            "id": self._trace.id,
            # started_at lets an HTTP death-flush that only still holds the terminal frame build a
            # valid trace payload (the ingest schema requires it).
            "started_at": self._trace.started_at.isoformat().replace("+00:00", "Z") if self._trace.started_at else None,
            "ended_at": self._trace.ended_at.isoformat().replace("+00:00", "Z"),
            "output": self._trace.output,
            "status": status.value,
            "total_tokens": self._trace.total_tokens,
            "total_cost": self._trace.total_cost,
            # Measured locally above. Omitting it made every stored trace read 0 ms.
            "total_duration_ms": self._trace.total_duration_ms,
            "span_count": self._emitted_spans,
        }
        if trace_ended_payload["started_at"] is None:
            del trace_ended_payload["started_at"]
        # Force-closed by exit/interrupted finalization: a synthesized terminal must NOT look clean
        # to the replay-guard. terminated_early ⇒ refuse byte-deterministic replay (same as
        # no-terminal / lossy / capture_complete:false). Only a naturally-finished run is clean.
        if terminated_early:
            trace_ended_payload["terminated_early"] = True
        if isinstance(error, BaseException):
            error_text = describe_exception(error)
        else:
            error_text = str(error)[:MAX_ERROR_CHARS] if error else None
        if error_text:
            trace_ended_payload["error"] = error_text
        self._send("trace_ended", trace_ended_payload)
        # Anonymous usage analytics (fire-and-forget; never raises): records success/failure + which
        # provider families ran, so we can see what works across the SDK. No PII, no user content.
        try:
            from .telemetry import track_record
            track_record(status.value, self)
        except Exception:
            pass
        # No separate HTTP PATCH of the terminal state: the transport confirms delivery of trace_ended
        # itself (WS acks, HTTP response) and falls back to HTTP only when it could not. The old
        # unconditional PATCH raced the WS frames and usually hit 404 (the trace row did not exist yet).
        # Release this context's active-recorder binding (restores any enclosing trace).
        if self._cv_token is not None:
            from .interceptors._dispatch import reset_active_recorder
            reset_active_recorder(self._cv_token)
            self._cv_token = None
        # Restore the enclosing trace context (or clear it).
        if self._tp_token is not None:
            from .traceparent import clear_trace_context
            clear_trace_context(self._tp_token)
            self._tp_token = None

    def add_span(self, span: Span) -> None:
        self._span_counter += 1
        # Fork-point filtering: during cascade replay, suppress the pre-fork spans (the server already
        # has them / they replay from the cassette) and emit only from the fork point onward. The
        # fork point is the (fork_point_index)-th span (0-based), i.e. the (index+1)-th emitted here,
        # so suppress while span_counter <= index and emit once we reach it. (Previously this compared
        # span_counter >= 1, which is always true after the increment => zero suppression / no-op.)
        if not self._fork_point_reached:
            if self._fork_point_index is not None and self._span_counter > self._fork_point_index:
                self._fork_point_reached = True
            else:
                return  # Suppress pre-fork span

        span.trace_id = self._trace.id
        # Inherit the active retrace.agent(...) context onto directly-added spans too, so the
        # "every span inherits agent attribution" contract holds on the low-level add_span path
        # (MA-SDK-002) — matching start_span and the provider-intercepted path.
        _apply_agent_ctx(span)
        with self._lock:
            self._trace.spans.append(span)
            self._trace.total_tokens += (span.input_tokens or 0) + (span.output_tokens or 0)
            self._trace.total_cost += span.cost or 0.0

        if span.ended_at:
            if span.duration_ms is None and span.started_at:
                span.duration_ms = max(0, int((span.ended_at - span.started_at).total_seconds() * 1000))
            # Span is complete — send both started and ended
            self._send("span_started", span.to_dict())
            ended: dict[str, Any] = {
                "id": span.id,
                "trace_id": self._trace.id,
                "ended_at": span.ended_at.isoformat().replace("+00:00", "Z"),
                "output": span.output,
            }
            # Unknown usage is left out, not sent as null: API ≤ 1.28.24 rejected `cost: null`, so the
            # span_ended frame was never acked and the span was never checked by the detectors.
            if span.output_tokens is not None:
                ended["output_tokens"] = span.output_tokens
            if span.cost is not None:
                ended["cost"] = span.cost
            if span.duration_ms is not None:
                ended["duration_ms"] = span.duration_ms
            if span.error:
                ended["error"] = span.error
            self._send("span_ended", ended)
        else:
            self._send("span_started", span.to_dict())

        # Circuit breaker: record this span's usage and enforce BEFORE the next call. Raises
        # RetraceEnforcementError on a tripped local limit / server policy (recording is already done
        # above, so the trace keeps the offending span; only the run is stopped).
        if self._enforcement.active:
            is_tool = span.span_type == SpanType.TOOL_CALL
            from .enforcement import hash_tool_args
            self._enforcement.record_and_check(
                tokens=(span.input_tokens or 0) + (span.output_tokens or 0),
                usd=span.cost or 0.0,
                trace_id=self._trace.id,
                tool_name=span.name if is_tool else None,
                tool_args_hash=hash_tool_args(span.input) if is_tool else None,
                span_id=span.id,
            )

    def start_span(
        self,
        name: str,
        span_type: SpanType = SpanType.LLM_CALL,
        input: Any = None,
        model: str | None = None,
        parent_id: str | None = None,
    ) -> Span:
        if span_type == SpanType.TOOL_CALL:
            requested = self._take_requested_tool_call(name, input)
            if requested is not None:
                # The app's span for a call the model requested: one step, not two. The requested
                # call (already sent, with the provider's tool_call_id) gets this run's result when
                # end_span is called; its timing starts now.
                requested.started_at = utcnow()
                requested.ended_at = None
                requested.duration_ms = None
                return requested
        span = Span(
            trace_id=self._trace.id,
            span_type=span_type,
            name=name,
            input=input,
            model=model,
            parent_id=parent_id,
        )
        _apply_agent_ctx(span)
        with self._lock:
            self._trace.spans.append(span)
        self._send("span_started", span.to_dict())
        return span

    def _take_requested_tool_call(self, name: str, input: Any) -> Span | None:
        key = _canonical(input)
        with self._lock:
            for i, s in enumerate(self._requested_tool_calls):
                if s.name == name and _canonical(s.input) == key:
                    return self._requested_tool_calls.pop(i)
        return None

    def end_span(self, span: str | Span, output: Any = None, error: str | None = None) -> None:
        """End a span started with ``start_span``. Takes the span or its id."""
        span_id = span.id if isinstance(span, Span) else span
        with self._lock:
            found = next((s for s in self._trace.spans if s.id == span_id), None)
        if found is None:
            return
        found.output = output
        found.error = error
        found.ended_at = utcnow()
        if found.started_at:
            found.duration_ms = int((found.ended_at - found.started_at).total_seconds() * 1000)
        ended = {
            "id": found.id,
            "trace_id": self._trace.id,
            "ended_at": found.ended_at.isoformat().replace("+00:00", "Z"),
            "output": output,
        }
        # Measured locally above. Omitting it made every stored span read null duration.
        if found.duration_ms is not None:
            ended["duration_ms"] = found.duration_ms
        if error:
            ended["error"] = error
        self._send("span_ended", ended)

    def _send(self, event_type: str, data: dict[str, Any]) -> None:
        try:
            # Every event is JSON-safe before it reaches a transport (see utils.to_jsonable).
            payload = to_jsonable(data)
            hook = get_config().before_send
            if hook is not None:
                try:
                    redacted = hook(event_type, payload)
                except Exception as e:
                    # Fail closed: an error in the user's redaction must never send the raw event.
                    logger.warning("[retrace] before_send raised for %s; event dropped: %s", event_type, e)
                    return
                if redacted is None:
                    return
                payload = redacted
            self._transport.send(event_type, payload)
        except Exception as e:
            # Not debug: a dropped trace_ended leaves the run "running" on the dashboard.
            logger.warning("[retrace] could not send %s: %s", event_type, e)
            return
        if event_type == "span_started":
            with self._emitted_lock:
                self._emitted_spans += 1

    # Context manager support
    def __enter__(self) -> TraceRecorder:
        self.start_trace()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None,
    ) -> Literal[False]:
        if exc_type:
            self.end_trace(status=TraceStatus.FAILED, error=exc_val)
        else:
            self.end_trace(status=TraceStatus.COMPLETED)
        return False


def _canonical(value: Any) -> str:
    """Key-order-independent JSON for comparing tool arguments."""
    import json
    try:
        return json.dumps(value, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return repr(value)


def record(
    name: str | None = None, input: Any = None, metadata: dict[str, Any] | None = None,
    resumable: bool = False, session_id: str | None = None,
) -> Any:
    """Decorator and context manager for recording agent executions.

    Usage as decorator:
        @retrace.record(name="my-agent")
        def my_agent(prompt):
            ...

        @retrace.record(name="my-agent", resumable=True)
        def my_agent(prompt):
            ...  # Supports fork-and-replay from any span

    Usage as context manager:
        with retrace.record(name="my-agent", input={"prompt": "hi"}) as t:
            result = agent.run("hi")
            t.output = result
    """
    cfg = get_config()

    # If called with a function directly: @record without parens
    if callable(name):
        fn = name
        if not cfg.enabled:
            return fn

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not _should_sample(cfg.sample_rate, cfg.sample_seed, fn.__name__):
                return fn(*args, **kwargs)
            recorder = TraceRecorder(
                name=fn.__name__, input={"args": list(args), "kwargs": kwargs}, resumable=resumable
            )
            recorder.start_trace()
            try:
                result = fn(*args, **kwargs)
                recorder.end_trace(output=result, status=TraceStatus.COMPLETED)
                return result
            except Exception as exc:
                recorder.end_trace(status=TraceStatus.FAILED, error=exc)
                raise

        return wrapper

    # Called with arguments: @record(name="...") or as context manager
    def decorator(fn: Callable[..., Any] | None = None) -> Any:
        if fn is None:
            # Context manager usage
            return TraceRecorder(name=name, input=input, metadata=metadata, resumable=resumable, session_id=session_id)

        if not cfg.enabled:
            return fn

        # Register for cascade replay if resumable
        if resumable:
            from .resume import register_resumable
            register_resumable(name or fn.__name__, fn)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not _should_sample(cfg.sample_rate, cfg.sample_seed, name or fn.__name__):
                return fn(*args, **kwargs)
            recorder = TraceRecorder(
                name=name or fn.__name__,
                input=input if input is not None else {"args": list(args), "kwargs": kwargs},
                metadata=metadata,
                session_id=session_id or kwargs.get("session_id"),
            )
            recorder.start_trace()
            try:
                result = fn(*args, **kwargs)
                recorder.end_trace(output=result, status=TraceStatus.COMPLETED)
                return result
            except Exception as exc:
                recorder.end_trace(status=TraceStatus.FAILED, error=exc)
                raise

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            if not _should_sample(cfg.sample_rate, cfg.sample_seed, name or fn.__name__):
                return await fn(*args, **kwargs)
            recorder = TraceRecorder(
                name=name or fn.__name__,
                input=input if input is not None else {"args": list(args), "kwargs": kwargs},
                metadata=metadata,
                session_id=session_id or kwargs.get("session_id"),
            )
            recorder.start_trace()
            try:
                result = await fn(*args, **kwargs)
                recorder.end_trace(output=result, status=TraceStatus.COMPLETED)
                return result
            except Exception as exc:
                recorder.end_trace(status=TraceStatus.FAILED, error=exc)
                raise

        return async_wrapper if inspect.iscoroutinefunction(fn) else wrapper

    # If no function passed, could be context manager or decorator
    if not cfg.enabled:
        # Return a no-op context manager
        class _NoOp:
            output = None
            def __enter__(self) -> _NoOp: return self
            def __exit__(self, *a: Any) -> Literal[False]: return False
            def __call__(self, fn: Callable[..., Any]) -> Callable[..., Any]: return fn
        return _NoOp()

    # Return something that works as both decorator and context manager
    class _RecordProxy:
        def __init__(self) -> None:
            # session_id was dropped here, so `with record(session_id=...)` traces had no session.
            self._recorder = TraceRecorder(
                name=name, input=input, metadata=metadata, resumable=resumable, session_id=session_id,
            )
            self._sampled = _should_sample(cfg.sample_rate, cfg.sample_seed, name)

        def __getattr__(self, attr: str) -> Any:
            # `with record(...) as t:` hands back this proxy; expose the recorder's API on it
            # (t.start_span / t.end_span / t.add_span / t.trace) instead of an AttributeError.
            if attr == "_recorder":  # not set yet (construction failed): no recursion
                raise AttributeError(attr)
            return getattr(self._recorder, attr)

        @property
        def output(self) -> Any:
            return self._recorder.output

        @output.setter
        def output(self, value: Any) -> None:
            self._recorder.output = value

        def __call__(self, fn: Callable[..., Any]) -> Any:
            return decorator(fn)

        def __enter__(self) -> _RecordProxy:
            if not self._sampled:
                return self
            self._recorder.start_trace()
            return self

        def __exit__(
            self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None,
        ) -> Literal[False]:
            if not self._sampled:
                return False
            if exc_type:
                self._recorder.end_trace(status=TraceStatus.FAILED, error=exc_val)
            else:
                self._recorder.end_trace(status=TraceStatus.COMPLETED)
            return False

    return _RecordProxy()
