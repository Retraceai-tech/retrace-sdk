"""Client-side enforcement gate (circuit breakers).

LOCAL limits (max steps / tokens / USD per run) are enforced entirely offline — zero network, so the
breaker trips even when the API is unreachable. When ``server_enforcement`` is enabled the gate ALSO
consults the server ``/enforcement/check`` endpoint for centrally-managed policies; that call is
best-effort from the SDK's point of view (a connection failure leaves the local limits in force and
is logged, never swallowed) and authoritative only at ingest.

The gate accumulates usage as spans complete and checks AFTER recording each span, so a tripped
breaker raises before the agent's NEXT call — it never silently skips a call (see the cassette-miss
strict/warn precedent).
"""
import hashlib
import logging
from typing import Any, Optional

import requests

from .config import RetraceConfig
from .errors import RetraceEnforcementError

logger = logging.getLogger("retrace")


def hash_tool_args(args: Any) -> str:
    """Stable short hash of tool arguments so the server never needs the raw args for a loop/debounce
    check. Mirrors the TS ``hashToolArgs``."""
    try:
        serialized = args if isinstance(args, str) else __import__("json").dumps(args, sort_keys=True, default=str)
    except Exception:
        serialized = str(args)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:32]


class EnforcementGate:
    """Per-run circuit breaker. One gate per trace/run; accumulators are run-local."""

    def __init__(self, config: RetraceConfig, run_id: str):
        self._config = config
        self._run_id = run_id
        self._steps = 0
        self._tokens = 0
        self._usd = 0.0

    @property
    def active(self) -> bool:
        """True when any local ceiling is set or server enforcement is enabled — otherwise the gate
        is a no-op and the hot path skips it entirely."""
        c = self._config
        return (
            c.max_steps_per_run is not None
            or c.max_tokens_per_run is not None
            or c.max_usd_per_run is not None
            or c.server_enforcement
        )

    def record_and_check(
        self,
        *,
        tokens: int = 0,
        usd: float = 0.0,
        trace_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        tool_args_hash: Optional[str] = None,
        span_id: Optional[str] = None,
    ) -> None:
        """Accumulate this span's usage, then enforce. Raises RetraceEnforcementError on a block/hold.

        LOCAL limits are checked first (offline, always authoritative for the local cap). The optional
        server check runs only when enabled and never downgrades a local block.
        """
        if not self.active:
            return
        self._steps += 1
        self._tokens += max(0, tokens)
        self._usd += max(0.0, usd)

        c = self._config
        if c.max_steps_per_run is not None and self._steps > c.max_steps_per_run:
            raise RetraceEnforcementError(
                f"Local step ceiling reached: {self._steps} > {c.max_steps_per_run} per run."
            )
        if c.max_tokens_per_run is not None and self._tokens > c.max_tokens_per_run:
            raise RetraceEnforcementError(
                f"Local token ceiling reached: {self._tokens} > {c.max_tokens_per_run} per run."
            )
        if c.max_usd_per_run is not None and self._usd > c.max_usd_per_run:
            raise RetraceEnforcementError(
                f"Local USD ceiling reached: ${self._usd:.4f} > ${c.max_usd_per_run} per run."
            )

        if c.server_enforcement:
            self._server_check(
                tokens=tokens, usd=usd, trace_id=trace_id, tool_name=tool_name, tool_args_hash=tool_args_hash,
                span_id=span_id,
            )

    def _server_check(
        self,
        *,
        tokens: int,
        usd: float,
        trace_id: Optional[str],
        tool_name: Optional[str],
        tool_args_hash: Optional[str],
        span_id: Optional[str] = None,
    ) -> None:
        """Consult the server /check endpoint. Best-effort: a transport failure logs and returns
        (local limits already applied); only a definitive block/hold verdict raises."""
        c = self._config
        if not c.api_key:
            return
        body: dict[str, Any] = {"run_id": self._run_id, "proposed_tokens": tokens, "proposed_usd": usd}
        if c.project_id:
            body["project_id"] = c.project_id
        if trace_id:
            body["trace_id"] = trace_id
        if span_id:
            # The server's ingest backstop evaluates the same recorded span; with its id it counts once.
            body["span_id"] = span_id
        if tool_name:
            body["tool_name"] = tool_name
        if tool_args_hash:
            body["tool_args_hash"] = tool_args_hash
        try:
            resp = requests.post(
                f"{c.base_url}/api/v1/enforcement/check",
                json=body,
                headers={"x-retrace-key": c.api_key, "Content-Type": "application/json"},
                timeout=5,
                allow_redirects=False,
            )
        except Exception as e:
            logger.warning("Retrace server enforcement check unreachable (local limits still apply): %s", e)
            return
        if resp.status_code not in (200, 202):
            logger.warning("Retrace server enforcement check returned %s (local limits still apply)", resp.status_code)
            return
        data = resp.json()
        verdict = data.get("verdict", "allow")
        if verdict == "hold":
            self._handle_hold(data)
            return
        if verdict == "block":
            raise RetraceEnforcementError(
                data.get("reason", "Blocked by server policy."),
                verdict="block",
                policy_id=data.get("policy_id"),
            )

    def _handle_hold(self, data: dict[str, Any]) -> None:
        """A held action: optionally block synchronously polling for a human decision, then apply the
        fail-closed timeout verdict. With wait=0 (default) the hold raises immediately."""
        c = self._config
        reason = data.get("reason", "Held by server policy.")
        policy_id = data.get("policy_id")
        hold_id = data.get("hold_id")
        wait = c.enforcement_hold_wait_seconds
        if hold_id and wait > 0:
            import time
            deadline = time.monotonic() + wait
            poll_interval = min(2.0, max(0.5, wait / 10))
            while time.monotonic() < deadline:
                status = self._poll_hold(hold_id)
                if status == "approved":
                    return  # released — the action proceeds
                if status in ("denied", "expired"):
                    raise RetraceEnforcementError(f"Held action {status}.", verdict="hold", policy_id=policy_id)
                time.sleep(poll_interval)
            raise RetraceEnforcementError(
                "Hold timed out (default verdict applied).", verdict="hold", policy_id=policy_id
            )
        raise RetraceEnforcementError(reason, verdict="hold", policy_id=policy_id)

    def _poll_hold(self, hold_id: str) -> str:
        """Read a hold's current status; returns 'pending' (or 'unknown') on any transport error so
        the wait loop keeps trying rather than failing the run on a transient blip."""
        c = self._config
        try:
            resp = requests.get(
                f"{c.base_url}/api/v1/enforcement/holds/{hold_id}",
                headers={"x-retrace-key": c.api_key},
                timeout=5,
                allow_redirects=False,
            )
            if resp.status_code == 200:
                return str(resp.json().get("status", "pending"))
        except Exception as e:
            logger.warning("Retrace hold poll failed (still waiting): %s", e)
        return "pending"
