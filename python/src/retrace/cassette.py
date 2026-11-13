"""Golden cassette writer for CI regression replay (Phase 4b).

After recording a run, write its cassette to a file you commit to your repo; ``retrace ci replay``
diffs a fresh run against it in CI. The JSON shape is the cross-language contract in
``@retrace/shared`` (CassetteSchema) — keep this writer in sync with it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Optional

from .utils import utcnow

if TYPE_CHECKING:
    from .recorder import TraceRecorder


def write_golden_cassette(
    path: str,
    recorder: Optional["TraceRecorder"] = None,
    tolerance: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Write the active (or given) recorder's run to ``path`` as a golden cassette.

    Call it inside/after your traced function, before the process exits. Raises if no recorder is
    active. ``tolerance`` is written through for CI divergence budgets.
    """
    if recorder is None:
        from .init import get_active_recorder

        recorder = get_active_recorder()
    if recorder is None:
        raise RuntimeError("write_golden_cassette: no active recorder — call inside a traced run")

    trace = recorder.trace
    steps: list[dict[str, Any]] = []
    for i, span in enumerate(trace.spans):
        span_type = span.span_type.value if hasattr(span.span_type, "value") else str(span.span_type)
        steps.append({
            "index": i,
            "span_type": span_type,
            "name": span.name,
            "model": span.model,
            "input": span.input,
            "output": span.output,
            "error": span.error,
        })

    status = trace.status.value if hasattr(trace.status, "value") else str(trace.status)
    cassette: dict[str, Any] = {
        "version": 1,
        "name": trace.name or "trace",
        "trace_id": trace.id,
        "recorded_at": utcnow().isoformat().replace("+00:00", "Z"),
        "status": status,
        "steps": steps,
    }
    if tolerance:
        cassette["tolerance"] = tolerance

    with open(path, "w", encoding="utf-8") as f:
        json.dump(cassette, f, indent=2, default=str)
    return cassette
