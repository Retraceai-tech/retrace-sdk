"""Full cascade replay for Retrace.

When @record(resumable=True) is used, the SDK:
1. Stores the decorated function reference
2. Listens for 'resume' commands on the WebSocket
3. On resume: re-executes the function from the fork point with modified input
4. Streams new spans back to the API

This enables "Git branching for AI agent execution."
"""
from __future__ import annotations

import inspect
import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional

from .config import get_config

logger = logging.getLogger("retrace")

# Registry of resumable functions
_resumable_functions: dict[str, Callable[..., Any]] = {}
_resume_lock = threading.Lock()


def register_resumable(trace_name: str, fn: Callable[..., Any]) -> None:
    """Register a function as resumable for cascade replay."""
    with _resume_lock:
        _resumable_functions[trace_name] = fn


def get_resumable(trace_name: str) -> Optional[Callable[..., Any]]:
    """Get a registered resumable function."""
    with _resume_lock:
        return _resumable_functions.get(trace_name)


@dataclass
class ResumeCommand:
    """Command received from API to resume execution."""
    fork_id: str
    trace_id: str
    trace_name: str
    fork_point_span_id: str
    modified_input: Any
    fork_point_index: Optional[int] = None
    original_args: Any = None
    original_kwargs: Any = None
    cassette_data: list[dict[str, Any]] | None = None  # Pre-recorded spans for deterministic replay
    execution_id: Optional[str] = None  # server replay-lease generation token (echoed on the result trace)


def _claim_execution(fork_id: str, execution_id: str) -> bool:
    """Best-effort execution-start / heartbeat claim. Returns False ONLY on a definitive 409 (a newer
    replay superseded this one). ANY other outcome — network/timeout, 401/403, deploy-window 404, 429,
    5xx — returns True and PROCEEDS: best-effort by design; the server's completion CAS is the final
    authority that rejects a stale result."""
    try:
        import requests as _req

        _cfg = get_config()
        _resp = _req.post(
            f"{_cfg.base_url}/api/v1/forks/{fork_id}/replay/claim",
            json={"execution_id": execution_id},
            headers={"x-retrace-key": _cfg.api_key, "Content-Type": "application/json"},
            timeout=5,
            allow_redirects=False,
        )
        return _resp.status_code != 409
    except Exception:
        return True


def handle_resume(command: ResumeCommand) -> bool:
    """Handle a resume command — re-execute the agent function.

    Returns True if the function was found and re-execution started.
    """
    fn = get_resumable(command.trace_name)
    if not fn:
        logger.warning(f"[retrace] No resumable function registered for '{command.trace_name}'")
        return False

    # Re-execute in a background thread to not block the WebSocket
    def _run() -> None:
        # Execution-start fence BEFORE creating the recorder — a superseded replay leaves no leaked
        # recorder/trace/interceptors. Best-effort: aborts only on a definitive 409.
        if command.execution_id and not _claim_execution(command.fork_id, command.execution_id):
            logger.warning("[retrace] Cascade replay superseded by a newer replay — aborting before execution.")
            return
        stop_hb = threading.Event()
        try:
            from .recorder import TraceRecorder
            from .replay import activate_cassette, deactivate_cassette
            from .trace import TraceStatus
            from .transport import HTTPTransport

            # Use HTTP transport for fork replays — ensures trace_ended is delivered
            # before the thread exits (WS is async and may not flush in time)
            recorder = TraceRecorder(
                name=f"Fork: {command.trace_name}",
                input=command.modified_input,
                metadata={
                    "_fork_id": command.fork_id,
                    "_fork_of": command.trace_id,
                    "_fork_point": command.fork_point_span_id,
                    "_fork_execution_id": command.execution_id,
                    "_cascade_replay": True,
                },
                fork_point_span_id=command.fork_point_span_id,
                fork_point_index=command.fork_point_index,
            )
            # Synchronous uploads: the replay's trace must be delivered before this thread exits.
            recorder._transport = HTTPTransport(background=False)
            recorder.start_trace()
            recorder._install_interceptors()

            # Keep-alive heartbeat: refresh our generation while the (possibly long) replay runs so a
            # new dispatch can't reclaim it as stale (which would let a second replay run concurrently).
            execution_id = command.execution_id
            if execution_id:
                def _heartbeat() -> None:
                    while not stop_hb.wait(300):
                        _claim_execution(command.fork_id, execution_id)
                threading.Thread(target=_heartbeat, daemon=True, name=f"retrace-replay-hb-{command.fork_id}").start()

            # Enable deterministic replay if cassette data is provided. This routes through the
            # SAME context-isolated mechanism the interceptors read (replay.py), so recorded
            # outputs are actually returned during cascade replay. Previously it set a separate
            # cassette store (cassette.py) the interceptors never consulted — i.e. dead wiring,
            # so cascade replay silently made real (billed) LLM calls instead of replaying.
            if command.cassette_data and get_config().allow_remote_cassette:
                activate_cassette(command.cassette_data)
                logger.info(f"[retrace] Deterministic replay enabled ({len(command.cassette_data)} entries)")
            elif command.cassette_data:
                logger.info("[retrace] Ignoring the server's recorded responses (allow_remote_cassette is off); "
                            "model calls in this replay run for real")

            args, kwargs = replay_call_arguments(fn, command)
            result = fn(*args, **kwargs)
            recorder.end_trace(output=result, status=TraceStatus.COMPLETED)
            deactivate_cassette()  # Clear cassette after replay
            logger.info(f"[retrace] Cascade replay completed for fork {command.fork_id}")
        except Exception as e:
            logger.error(f"[retrace] Cascade replay failed: {e}")
            deactivate_cassette()
            try:
                recorder.end_trace(status=TraceStatus.FAILED, error=e)
            except Exception:
                pass
        finally:
            stop_hb.set()

    thread = threading.Thread(target=_run, daemon=True, name=f"retrace-replay-{command.fork_id}")
    thread.start()
    return True


def replay_call_arguments(fn: Callable[..., Any], command: ResumeCommand) -> tuple[list[Any], dict[str, Any]]:
    """The arguments a replay calls ``fn`` with: the recorded ones, with the server's modified input
    applied only to parameters ``fn`` declares by name. A dict input used to be merged into the
    keyword arguments as-is, so the server could pass a keyword ``fn`` does not declare (one its
    ``**kwargs`` forwards, such as ``allow_shell=True``). A declared parameter can still be set: that is
    what a fork changes, so only enable remote replay for functions whose parameters are safe to vary."""
    args = list(command.original_args or [])
    kwargs = dict(command.original_kwargs or {})
    params: Mapping[str, inspect.Parameter]
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        params = {}
    named = {n for n, p in params.items() if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)}
    if params:
        kwargs = {k: v for k, v in kwargs.items() if k in named}
    if isinstance(command.modified_input, str):
        args = [command.modified_input] + args[1:] if args else [command.modified_input]
    elif isinstance(command.modified_input, dict):
        unknown = sorted(k for k in command.modified_input if k not in named)
        if unknown:
            logger.warning("[retrace] replay input ignored for %s: not parameters of %s",
                           unknown, getattr(fn, "__name__", "fn"))
        kwargs.update({k: v for k, v in command.modified_input.items() if k in named})
    return args, kwargs


def parse_resume_message(msg: dict[str, Any]) -> Optional[ResumeCommand]:
    """Parse a WebSocket message into a ResumeCommand."""
    if msg.get("type") != "resume":
        return None
    data = msg.get("data", {})
    return ResumeCommand(
        fork_id=data.get("forkId", ""),
        trace_id=data.get("traceId", ""),
        trace_name=data.get("traceName", ""),
        fork_point_span_id=data.get("forkPointSpanId", ""),
        fork_point_index=data.get("forkPointIndex"),
        modified_input=data.get("modifiedInput"),
        original_args=data.get("originalArgs"),
        original_kwargs=data.get("originalKwargs"),
        cassette_data=data.get("cassette"),
        execution_id=data.get("executionId"),
    )
