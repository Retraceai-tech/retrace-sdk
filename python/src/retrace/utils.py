import dataclasses
import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


def gen_id() -> str:
    return str(uuid4())


def now_iso() -> str:
    return utcnow().isoformat().replace("+00:00", "Z")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def truncate_json(obj: Any, max_bytes: int = 10240) -> Any:
    try:
        s = json.dumps(obj)
        if len(s.encode()) <= max_bytes:
            return obj
        return json.loads(s.encode()[:max_bytes].decode(errors="ignore"))
    except (TypeError, ValueError):
        return str(obj)[:max_bytes]


MAX_ERROR_CHARS = 2000

# How deep to_jsonable walks a value before it stops and keeps a string (bounds cyclic structures).
_JSONABLE_DEPTH = 32


def to_jsonable(value: Any, _depth: int = 0) -> Any:
    """A JSON-serializable copy of ``value``. A traced function can return anything (an SDK response
    object, a dataclass, a set, bytes); it used to go into the payload as-is, and JSON encoding failed:
    over HTTP the trace was retried forever as a "network error" in front of every later upload, and
    over WebSocket its terminal event was dropped, so the trace stayed "running". Pydantic models and
    dataclasses become dicts; anything else unknown becomes its string form."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if _depth >= _JSONABLE_DEPTH:
        return str(value)
    if isinstance(value, dict):
        return {str(k): to_jsonable(v, _depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_jsonable(v, _depth + 1) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    dump = getattr(value, "model_dump", None)  # pydantic v2 (OpenAI / Anthropic / Gemini responses)
    if callable(dump):
        try:
            return to_jsonable(dump(mode="json"), _depth + 1)
        except Exception:
            return str(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        try:
            return to_jsonable(dataclasses.asdict(value), _depth + 1)
        except Exception:
            return str(value)
    for name in ("to_dict", "dict"):  # pydantic v1 and other SDKs
        fn = getattr(value, name, None)
        if callable(fn):
            try:
                return to_jsonable(fn(), _depth + 1)
            except Exception:
                break
    return str(value)


def describe_exception(exc: BaseException | None) -> str | None:
    """The failure a run ended with, as one line: ``ValueError: boom`` (no traceback — frames carry
    file paths and local values). ``None`` when there is no exception."""
    if exc is None:
        return None
    text = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
    return text[:MAX_ERROR_CHARS]

