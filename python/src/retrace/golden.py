"""Golden trace marking (Phase 2E) for regression replay."""
from .config import get_config, require_api_key


def mark_golden(trace_id: str, golden: bool = True) -> None:
    """Mark (or unmark) a recorded trace as a GOLDEN regression baseline.

    Golden traces are the reference for regression replay — later runs that structurally
    diverge from their golden baseline are flagged as regressions.
    """
    import requests

    cfg = get_config()
    # require_api_key also refuses a cleartext base URL (this call used to skip that check and could
    # send the key over http:// to a non-local host).
    api_key = require_api_key()
    resp = requests.post(
        f"{cfg.base_url}/api/v1/traces/{trace_id}/golden",
        json={"golden": golden},
        headers={"x-retrace-key": api_key, "Content-Type": "application/json"},
        timeout=10,
        allow_redirects=False,  # never forward the key to wherever a redirect points
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"mark_golden failed: HTTP {resp.status_code}")
