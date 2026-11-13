from dataclasses import dataclass


class RetraceError(Exception):
    """Base error for Retrace SDK."""
    pass

class RetraceAuthError(RetraceError):
    """Invalid or missing API key."""
    def __init__(self, message: str = "Invalid or missing API key") -> None:
        super().__init__(message)

class RetraceCreditsExhaustedError(RetraceError):
    """Monthly trace/AI request limit reached."""
    def __init__(self, message: str = "Monthly limit reached. Upgrade at retraceai.tech/pricing") -> None:
        super().__init__(message)

class RetraceConnectionError(RetraceError):
    """Failed to connect to Retrace API."""
    def __init__(self, message: str = "Failed to connect to Retrace API") -> None:
        super().__init__(message)

class RetraceRateLimitError(RetraceError):
    """Rate limited by the API."""
    def __init__(self, retry_after: int = 60):
        self.retry_after = retry_after
        super().__init__(f"Rate limited. Retry after {retry_after}s")


class RetraceEnforcementError(RetraceError):
    """An enforcement circuit breaker blocked (or held) the action.

    Raised by the pre-call gate when a LOCAL limit (max steps / tokens / USD per run) is exceeded, or
    when an opt-in server-side policy returns a block/hold verdict. NEVER raised silently — the agent
    loop stops here rather than continuing past a runaway condition.

    verdict:   "block" or "hold".
    reason:    human-readable explanation (the ceiling that tripped, or the server reason).
    policy_id: the server policy that tripped, when the verdict came from the server (else None).
    """
    def __init__(self, reason: str, verdict: str = "block", policy_id: str | None = None):
        self.verdict = verdict
        self.reason = reason
        self.policy_id = policy_id
        super().__init__(f"Enforcement {verdict}: {reason}")


@dataclass
class RetraceServerSignal:
    """Structured server-originated signal handed to ``on_error``. Actionable WITHOUT string-matching:
    branch on ``code``, decide retry from ``retryable``, decide whether recording is still alive from
    ``fatal``. Mirrors the TypeScript ``RetraceServerSignal`` exactly (parity is the point of #5).

    code:      one of "credits_exhausted" | "rate_limited" | "halt" | "error" — branch on THIS.
    message:   human-readable detail from the server.
    retryable: will retrying / backing off plausibly succeed? rate_limited=True; others False.
    fatal:     did this STOP recording? halt=True (transport closed); others leave recording alive.
    """
    code: str
    message: str
    retryable: bool
    fatal: bool


def classify_server_signal(raw_type: str, message: str) -> RetraceServerSignal:
    """Map a raw server frame to a structured signal. Single source of truth for category + retryable
    + fatal — identical mapping to the TS ``classifyServerSignal``."""
    if raw_type == "halt":
        return RetraceServerSignal(code="halt", message=message or "Guardrail triggered", retryable=False, fatal=True)
    if message and "limit reached" in message:
        return RetraceServerSignal(code="credits_exhausted", message=message, retryable=False, fatal=False)
    if message and "Rate limit" in message:
        return RetraceServerSignal(code="rate_limited", message=message, retryable=True, fatal=False)
    return RetraceServerSignal(code="error", message=message or "Server error", retryable=False, fatal=False)
