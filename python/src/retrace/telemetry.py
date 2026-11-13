"""Anonymous diagnostic telemetry (on by default, can be turned off).

Sends NO user code, API keys, prompts, responses, or personal information — only the SDK version,
the runtime/OS family, an anonymous per-process id, and event/error *categories*. Events are POSTed
to the Retrace API's internal ingest, which forwards them to PostHog (and the logging platform)
under service="sdk-python" — so the SDK NEVER holds any analytics key (SDK → Retrace API → PostHog).

Fire-and-forget: never blocks the user's program, never raises. Anonymous by construction (no PII,
random per-process id). Turn it off with ``configure(telemetry=False)``, RETRACE_TELEMETRY=0 or
DO_NOT_TRACK=1: nothing is sent at all then.
"""
from __future__ import annotations

import platform
import threading
import uuid
from typing import Any

# Per-process random id — anonymous, not linked to any account, user, or machine identity.
_ANON_ID = uuid.uuid4().hex[:16]


def _sdk_version() -> str:
    try:
        from . import __version__
        return __version__
    except Exception:
        return "unknown"


def provider_of(model: str | None) -> str:
    """Coarse provider family from a model name (analytics only — never the key or content)."""
    m = (model or "").lower()
    if not m:
        return ""
    if m.startswith("gemini") or "google" in m:
        return "google"
    if m.startswith(("gpt", "o1", "o3", "o4", "chatgpt", "text-embedding", "davinci")):
        return "openai"
    if "claude" in m:
        return "anthropic"
    return "other"


def _send(event: str, fields: dict[str, Any]) -> None:
    try:
        import requests  # already an SDK dependency

        from .config import get_config

        cfg = get_config()
        base = (getattr(cfg, "base_url", "") or "https://api.retraceai.tech").rstrip("/")
        # Only safe scalar fields are forwarded — never user content.
        safe = {k: v for k, v in fields.items() if isinstance(v, (str, int, float, bool))}
        payload: dict[str, Any] = {
            "service": "sdk-python",
            "events": [{
                "level": "info",
                "message": f"sdk-python {event}",
                "event": event,
                "sdk_version": _sdk_version(),
                "python": platform.python_version(),
                "os": platform.system().lower(),
                "anon_id": _ANON_ID,
                **safe,
            }],
        }
        requests.post(f"{base}/api/v1/internal/logs", json=payload, timeout=3)
    except Exception:
        pass  # telemetry must never affect the user's program


def _enabled() -> bool:
    from .config import get_config

    return bool(getattr(get_config(), "telemetry", True))


def track(event: str, **fields: Any) -> None:
    """Fire-and-forget anonymous telemetry unless turned off; never raises, never blocks."""
    try:
        if not _enabled():
            return
        threading.Thread(target=_send, args=(event, fields), daemon=True).start()
    except Exception:
        pass


def track_record(status: str, recorder: Any) -> None:
    """Emit record_success / record_failure with anonymous, aggregate-only fields (span count,
    provider families used, whether the run was resumable / a cascade replay). Never raises."""
    try:
        trace = getattr(recorder, "_trace", None)
        spans = getattr(trace, "spans", []) or []
        meta = getattr(trace, "metadata", {}) or {}
        providers = sorted({p for p in (provider_of(getattr(s, "model", None)) for s in spans) if p})
        track(
            "record_success" if status == "completed" else "record_failure",
            spans=len(spans),
            providers=",".join(providers) or "none",
            resumable=bool(meta.get("_resumable")),
            cascade=bool(meta.get("_cascade_replay")),
        )
    except Exception:
        pass
