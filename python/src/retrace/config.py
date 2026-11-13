import logging
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger("retrace")


@dataclass
class RetraceConfig:
    # repr=False: the config is often logged or captured by error trackers; its repr printed the key.
    api_key: str = field(default="", repr=False)
    base_url: str = ""
    project_id: str | None = None
    environment: str | None = None
    ws_url: str = ""
    flush_interval: float = 2.0
    enabled: bool = True
    sample_rate: float = 1.0
    sample_seed: str | None = None
    max_payload_size: int = 2000
    # Transport mode: "auto" (default, WebSocket with offline buffer), or "http"
    # (request/response only — recommended for short scripts/serverless: no open socket,
    # full trace+spans POST, and upload errors are always logged).
    transport: str = "auto"
    # How long the exit-time flush waits for the server to confirm every event (WebSocket acks)
    # before falling back to HTTP / reporting an incomplete capture. Env: RETRACE_FLUSH_TIMEOUT.
    flush_timeout: float = 5.0
    # Replay safety. On a cassette MISS during deterministic replay, the SDK either raises
    # (fail-closed) or falls through to a REAL provider call (live cost + non-determinism, driven by a
    # server-supplied cassette). Default True (fail-closed). Set strict_replay=False (or
    # RETRACE_STRICT_REPLAY=false) for the legacy warn + fall-through. Env: RETRACE_STRICT_REPLAY.
    strict_replay: bool = True
    # C2: honor SERVER-INITIATED resume/replay commands (dashboard-driven cascade replay that
    # re-executes YOUR registered function in THIS process with server-supplied input/cassette).
    # Default False — ignored unless explicitly opted in, since acting on them means trusting the
    # server/transport to invoke code in your environment. Env: RETRACE_ALLOW_REMOTE_REPLAY.
    allow_remote_replay: bool = False
    # With remote replay on, also accept the server's RECORDED responses (a cassette) and return them
    # instead of calling the model. Off by default: a cassette is model output the server chooses,
    # so trusting it lets whoever can trigger a fork decide what your agent "hears" (and so which
    # tools it calls). Without it every model call in a replay runs for real. Env:
    # RETRACE_ALLOW_REMOTE_CASSETTE.
    allow_remote_cassette: bool = False
    # Server-signal callback — receives a STRUCTURED retrace.errors.RetraceServerSignal
    # (code/retryable/fatal), mirroring the TS `onError`. None ⇒ transport throttled-warns by
    # default (never silent). Set in configure() before the first trace.
    on_error: Optional[Callable[[Any], None]] = None
    # Redaction hook: called with (event_type, data) for every event before it leaves the process
    # (trace_started / span_started / span_ended / trace_ended). Return the (possibly edited) data to
    # send, or None to drop the event. Use it to scrub prompts, outputs or tool arguments. An error in
    # the hook drops the event (never sends unredacted data). Default None: events are sent as recorded.
    before_send: Optional[Callable[[str, dict[str, Any]], Optional[dict[str, Any]]]] = None
    # Enforcement (circuit breakers). LOCAL limits are enforced offline with zero network — they
    # always apply. None = that ceiling is unset. Precedence: explicit configure() arg > env var >
    # unset. `server_enforcement=True` additionally consults the server /check endpoint for
    # centrally-managed policies (best-effort from the SDK; authoritative at ingest).
    max_steps_per_run: Optional[int] = None
    max_tokens_per_run: Optional[int] = None
    max_usd_per_run: Optional[float] = None
    server_enforcement: bool = False
    # On a server `hold` verdict, block synchronously up to this many seconds polling for a human
    # decision, then apply the fail-closed timeout verdict (raise). 0 = don't wait (raise at once).
    enforcement_hold_wait_seconds: int = 0
    # Anonymous diagnostic telemetry (SDK version, runtime, event categories; never content or keys).
    # On by default; False turns it off. Env: RETRACE_TELEMETRY=0|false|off|no, or DO_NOT_TRACK=1.
    telemetry: bool = True

    def __post_init__(self) -> None:
        if not self.api_key:
            self.api_key = os.environ.get("RETRACE_API_KEY", "")
        if not self.base_url:
            self.base_url = os.environ.get("RETRACE_BASE_URL", "https://api.retraceai.tech")
        if not self.project_id:
            self.project_id = os.environ.get("RETRACE_PROJECT_ID") or None
        if self.environment is None:
            self.environment = os.environ.get("RETRACE_ENVIRONMENT") or None
        if not self.ws_url:
            self.ws_url = self.base_url.replace("https://", "wss://").replace("http://", "ws://")
        enabled_env = os.environ.get("RETRACE_ENABLED", "true").lower()
        if enabled_env in ("false", "0", "no"):
            self.enabled = False
        sample_env = os.environ.get("RETRACE_SAMPLE_RATE")
        if sample_env:
            self.sample_rate = float(sample_env)
        seed_env = os.environ.get("RETRACE_SAMPLE_SEED")
        if seed_env:
            self.sample_seed = seed_env
        transport_env = os.environ.get("RETRACE_TRANSPORT")
        if transport_env in ("auto", "ws", "http"):
            self.transport = transport_env
        flush_env = os.environ.get("RETRACE_FLUSH_TIMEOUT")
        if flush_env:
            try:
                self.flush_timeout = max(0.0, float(flush_env))
            except ValueError:
                logger.warning("[retrace] ignoring RETRACE_FLUSH_TIMEOUT=%r (not a number of seconds)", flush_env)
        if os.environ.get("RETRACE_STRICT_REPLAY", "").lower() in ("false", "0", "no"):
            self.strict_replay = False
        if os.environ.get("RETRACE_ALLOW_REMOTE_REPLAY", "").lower() in ("true", "1", "yes"):
            self.allow_remote_replay = True
        if os.environ.get("RETRACE_ALLOW_REMOTE_CASSETTE", "").lower() in ("true", "1", "yes"):
            self.allow_remote_cassette = True
        # Enforcement: explicit arg (already set on the dataclass) wins; otherwise fall back to env.
        if self.max_steps_per_run is None and os.environ.get("RETRACE_MAX_STEPS_PER_RUN"):
            self.max_steps_per_run = int(os.environ["RETRACE_MAX_STEPS_PER_RUN"])
        if self.max_tokens_per_run is None and os.environ.get("RETRACE_MAX_TOKENS_PER_RUN"):
            self.max_tokens_per_run = int(os.environ["RETRACE_MAX_TOKENS_PER_RUN"])
        if self.max_usd_per_run is None and os.environ.get("RETRACE_MAX_USD_PER_RUN"):
            self.max_usd_per_run = float(os.environ["RETRACE_MAX_USD_PER_RUN"])
        if not self.server_enforcement:
            _se = os.environ.get("RETRACE_SERVER_ENFORCEMENT", "").lower()
            if _se in ("true", "1", "yes"):
                self.server_enforcement = True
        if self.enforcement_hold_wait_seconds == 0 and os.environ.get("RETRACE_ENFORCEMENT_HOLD_WAIT_SECONDS"):
            self.enforcement_hold_wait_seconds = int(os.environ["RETRACE_ENFORCEMENT_HOLD_WAIT_SECONDS"])
        if not telemetry_from_env(os.environ):
            self.telemetry = False


def telemetry_from_env(env: Any) -> bool:
    """Telemetry is on unless RETRACE_TELEMETRY is 0/false/off/no or DO_NOT_TRACK is set (consoledonottrack.com)."""
    if (env.get("RETRACE_TELEMETRY") or "").strip().lower() in ("0", "false", "off", "no"):
        return False
    return (env.get("DO_NOT_TRACK") or "").strip().lower() not in ("1", "true", "yes")

_config: RetraceConfig | None = None


def _is_loopback_host(host: str) -> bool:
    h = (host or "").lower().strip("[]")
    return h in ("localhost", "127.0.0.1", "::1") or h.endswith(".localhost")


def _assert_secure_ws_url(ws_url: str) -> None:
    """The WebSocket URL carries the API key in its auth frame, so it must be wss:// (ws:// only for
    localhost). An explicit ``ws_url`` (or RETRACE_WS_URL) used to skip the base-URL check."""
    from urllib.parse import urlparse
    if not ws_url:
        return
    u = urlparse(ws_url)
    if u.scheme == "wss" or (u.scheme == "ws" and _is_loopback_host(u.hostname or "")):
        return
    raise ValueError(
        f"Retrace: refusing insecure WebSocket URL {ws_url!r}. Use wss:// (ws:// is allowed only for "
        "localhost): the API key is sent in the first frame."
    )


def _assert_secure_base_url(base_url: str) -> None:
    """C3: refuse cleartext transports for non-loopback hosts. Over http:// -> ws:// the auth frame
    sends the API key in plaintext and server command frames (resume/replay/halt) are injectable on
    the wire (the transport half of the C2 chain). https:// (and http://localhost for local dev) are
    allowed; anything else raises before the API key ever leaves the process."""
    from urllib.parse import urlparse
    try:
        u = urlparse(base_url)
    except Exception:
        raise ValueError(f"Retrace: invalid base URL {base_url!r} (set RETRACE_BASE_URL to an https:// URL).")
    if u.scheme == "https":
        return
    if u.scheme == "http" and _is_loopback_host(u.hostname or ""):
        return  # local dev only
    raise ValueError(
        f"Retrace: refusing insecure base URL {base_url!r}. Use https:// (http:// is allowed only for "
        "localhost). Over cleartext the API key is transmitted in plaintext and server replay/resume "
        "command frames can be injected on the wire."
    )


def get_config() -> RetraceConfig:
    global _config
    if _config is None:
        _config = RetraceConfig()
    return _config


def configure(**kwargs: Any) -> RetraceConfig:
    global _config
    if _config is None:
        _config = RetraceConfig(**kwargs)
    else:
        for k, v in kwargs.items():
            setattr(_config, k, v)
        if "base_url" in kwargs and "ws_url" not in kwargs:
            _config.ws_url = _config.base_url.replace("https://", "wss://").replace("http://", "ws://")
    if _config.api_key and (
        not _config.api_key.startswith("rt_")
        or _config.api_key.startswith("rt_live_")
        or _config.api_key.startswith("rt_test_")
    ):
        raise ValueError("Invalid Retrace API key. Keys must start with 'rt_'. Get yours at https://dash.retraceai.tech/workspace/api-keys")
    _assert_secure_base_url(_config.base_url)
    _assert_secure_ws_url(_config.ws_url)
    try:
        from .telemetry import track
        track("sdk_init", transport=str(getattr(_config, "transport", "auto")), has_api_key=bool(_config.api_key))
    except Exception:
        pass
    return _config


def require_api_key() -> str:
    """Ensure a valid API key is configured. Raises if not."""
    cfg = get_config()
    if not cfg.api_key:
        raise RuntimeError(
            "Retrace API key required. Call retrace.configure(api_key='rt_...') "
            "or set RETRACE_API_KEY. Get yours at https://dash.retraceai.tech/workspace/api-keys"
        )
    # Validate the transport is encrypted before the key is ever used on the wire (covers env-only
    # config that never called configure()).
    _assert_secure_base_url(cfg.base_url)
    _assert_secure_ws_url(cfg.ws_url)
    return cfg.api_key
