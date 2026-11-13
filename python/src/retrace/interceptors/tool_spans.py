"""Tool-span extraction (Phase 1C) for the Retrace Python SDK.

Provider interceptors historically emitted only a single ``llm_call`` span and dropped the
model's tool calls (the most common agent-failure class). These helpers derive structured
``tool_call`` and ``tool_result`` spans from a provider request/response so tool usage is
captured with no manual instrumentation:

- ``tool_call`` spans come from the model response (the calls it requested), arguments parsed
  into structured JSON.
- ``tool_result`` spans come from the tool outputs fed back on the next request (verbatim,
  including errors/empty), tagged with the provider tool-call id (the recorder records each once
  per trace).

Downstream detectors (schema validation, loop detection, tool-output hallucination) rely on
these spans + the ``tool_call_id`` carried in metadata.
"""
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional

# Receives each derived span dict (the recorder's intercepted-span handler).
_Emit = Callable[[dict[str, Any]], Any]


def _get(obj: Any, key: str, default: Any = None) -> Any:
    """Read an attribute from either a dict or an SDK object."""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _parse_args(args: Any) -> Any:
    if isinstance(args, str):
        try:
            return json.loads(args)
        except (ValueError, TypeError):
            return args
    return args


def extract_tool_schemas(provider: str, tools: Any) -> Optional[dict[str, Any]]:
    """Extract declared tool parameter schemas (name → JSON schema) from a request's tools."""
    out: dict[str, Any] = {}
    if provider == "gemini":
        if not isinstance(tools, (list, tuple)):
            return None
        for group in tools:
            decls = _get(group, "function_declarations") or _get(group, "functionDeclarations") or []
            for fd in decls:
                name, params = _get(fd, "name"), _get(fd, "parameters")
                if name and params:
                    out[name] = params
    elif isinstance(tools, (list, tuple)):
        for t in tools:
            if provider == "openai":
                fn = _get(t, "function")
                name, params = _get(fn, "name"), _get(fn, "parameters")
                if name and params:
                    out[name] = params
            elif provider == "anthropic":
                name, schema = _get(t, "name"), _get(t, "input_schema")
                if name and schema:
                    out[name] = schema
    return out or None


def extract_sampling_params(provider: str, kwargs: Any) -> Optional[dict[str, Any]]:
    """Capture the sampling/determinism envelope so replay-divergence (2A) + regression (2E) can
    compare sampling config, not just the model. Normalizes to
    {temperature, top_p, top_k, seed, max_tokens}. Returns None if none set."""
    cfg = _get(kwargs, "config") or {} if provider == "gemini" else (kwargs or {})
    out: dict[str, Any] = {}

    def put(k: str, v: Any) -> None:
        if v is not None:
            out[k] = v

    if provider == "gemini":
        put("temperature", _get(cfg, "temperature"))
        put("top_p", _get(cfg, "top_p") or _get(cfg, "topP"))
        put("top_k", _get(cfg, "top_k") or _get(cfg, "topK"))
        put("seed", _get(cfg, "seed"))
        put("max_tokens", _get(cfg, "max_output_tokens") or _get(cfg, "maxOutputTokens"))
    else:
        # anthropic>=1.0 removed temperature/top_p/top_k from messages.create(); callers now pass them
        # via extra_body — fall back to it so the sampling envelope is still captured.
        eb = _get(cfg, "extra_body")
        eb = eb if isinstance(eb, dict) else {}

        def pick(k: str) -> object:
            v = _get(cfg, k)
            return v if v is not None else eb.get(k)

        put("temperature", pick("temperature"))
        put("top_p", pick("top_p"))
        put("top_k", pick("top_k"))
        put("seed", pick("seed"))
        put("max_tokens", pick("max_tokens") or pick("max_completion_tokens"))
    return out or None


def _truncate(val: Any, limit: int = 4000) -> Any:
    """Cap large string payloads; structured values are stored as-is (server uses jsonb)."""
    if isinstance(val, str):
        return val[:limit]
    return val


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _tool_call_span(
    name: Any, args: Any, parent_id: Optional[str], model: Optional[str], tool_call_id: Any = None,
) -> dict[str, Any]:
    now = _now_iso()
    d: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "span_type": "tool_call",
        "name": name or "tool",
        "parent_id": parent_id,
        "input": _truncate(args),
        "started_at": now,
        "ended_at": now,
        "duration_ms": 0,
    }
    if model:
        d["model"] = model
    if tool_call_id:
        d["metadata"] = {"tool_call_id": tool_call_id}
    return d


def _tool_result_span(name: Any, output: Any, is_error: bool, tool_call_id: Any = None) -> dict[str, Any]:
    now = _now_iso()
    d: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "span_type": "tool_result",
        "name": name or "tool_result",
        "output": _truncate(output),
        "started_at": now,
        "ended_at": now,
        "duration_ms": 0,
    }
    if is_error:
        d["error"] = output if isinstance(output, str) else json.dumps(output, default=str)
    if tool_call_id:
        d["metadata"] = {"tool_call_id": tool_call_id}
    return d


# ─── OpenAI ───────────────────────────────────────────────────────────────────
def emit_openai_tool_calls(message: Any, parent_id: Optional[str], model: Optional[str], emit: _Emit) -> None:
    calls = _get(message, "tool_calls")
    if not calls:
        return
    for c in calls:
        fn = _get(c, "function")
        emit(_tool_call_span(
            _get(fn, "name") or "tool", _parse_args(_get(fn, "arguments")),
            parent_id, model, _get(c, "id"),
        ))


def emit_openai_tool_results(messages: Any, emit: _Emit) -> None:
    if not isinstance(messages, (list, tuple)):
        return
    name_by_id: dict[str, str] = {}
    for m in messages:
        if _get(m, "role") == "assistant":
            for c in (_get(m, "tool_calls") or []):
                cid = _get(c, "id")
                if cid:
                    name_by_id[cid] = _get(_get(c, "function"), "name") or "tool"
    for m in messages:
        if _get(m, "role") != "tool":
            continue
        cid = _get(m, "tool_call_id")
        if not cid:
            continue
        content = _get(m, "content")
        is_error = isinstance(content, str) and any(w in content.lower() for w in ("error", "exception", "failed"))
        emit(_tool_result_span(name_by_id.get(cid) or _get(m, "name") or "tool_result", content, is_error, cid))


# ─── Anthropic ─────────────────────────────────────────────────────────────────
def emit_anthropic_tool_calls(content: Any, parent_id: Optional[str], model: Optional[str], emit: _Emit) -> None:
    if not isinstance(content, (list, tuple)):
        return
    for block in content:
        if _get(block, "type") != "tool_use":
            continue
        emit(_tool_call_span(_get(block, "name") or "tool", _get(block, "input"), parent_id, model, _get(block, "id")))


def emit_anthropic_tool_results(messages: Any, emit: _Emit) -> None:
    if not isinstance(messages, (list, tuple)):
        return
    for m in messages:
        content = _get(m, "content")
        if not isinstance(content, (list, tuple)):
            continue
        for block in content:
            if _get(block, "type") != "tool_result":
                continue
            tid = _get(block, "tool_use_id")
            if not tid:
                continue
            emit(_tool_result_span("tool_result", _get(block, "content"), bool(_get(block, "is_error")), tid))


# ─── Gemini ────────────────────────────────────────────────────────────────────
def emit_gemini_tool_calls(candidates: Any, parent_id: Optional[str], model: Optional[str], emit: _Emit) -> None:
    if not isinstance(candidates, (list, tuple)):
        return
    for cand in candidates:
        parts = _get(_get(cand, "content"), "parts") or []
        for p in parts:
            fc = _get(p, "function_call")
            if not fc:
                continue
            emit(_tool_call_span(_get(fc, "name") or "tool", _get(fc, "args"), parent_id, model, _get(fc, "name")))


def emit_gemini_tool_results(contents: Any, emit: _Emit) -> None:
    if contents is None:
        return
    items = contents if isinstance(contents, (list, tuple)) else [contents]
    for c in items:
        parts = _get(c, "parts")
        if not isinstance(parts, (list, tuple)):
            continue
        for p in parts:
            fr = _get(p, "function_response")
            if not fr:
                continue
            name = _get(fr, "name")
            resp = _get(fr, "response")
            # The recorder records each tool result once per trace, keyed by this id.
            call_id = _get(fr, "id") or name or f"response:{str(resp)[:64]}"
            emit(_tool_result_span(name or "tool_result", resp, False, call_id))


# ─── Response helpers (shared by the provider interceptors) ─────────────────────
def parsed_response(result: Any) -> Any:
    """The parsed model response behind ``result``. A raw-response call (``with_raw_response``, which
    langchain-openai uses) returns an ``APIResponse`` wrapper, and reading usage/choices from the
    wrapper recorded 0 tokens and an empty output. ``parse()`` is cached by the SDK, so the caller
    still gets the same object when it parses."""
    # The wrapper (LegacyAPIResponse / APIResponse) carries the HTTP response and a parse() method;
    # its `content` is the raw body bytes, so it is not mistaken for an Anthropic message.
    if not hasattr(result, "http_response") or isinstance(getattr(result, "choices", None), list):
        return result
    parse = getattr(result, "parse", None)
    if not callable(parse):
        return result
    try:
        return parse()
    except Exception:
        return result


def _tool_call_text(name: Any, args: Any) -> str:
    if isinstance(args, str):
        rendered = args
    else:
        try:
            rendered = json.dumps(args, default=str)
        except Exception:
            rendered = str(args)
    return f"{name or 'tool'}({rendered})"


def openai_output_text(message: Any) -> str:
    """The assistant text of an OpenAI message, or a summary of the tools it called. A tool-calling
    turn has no text content, so its LLM span used to store an empty output."""
    text = _get(message, "content") or ""
    if text:
        return text if isinstance(text, str) else str(text)
    calls = _get(message, "tool_calls") or []
    parts = [_tool_call_text(_get(_get(c, "function"), "name"), _get(_get(c, "function"), "arguments")) for c in calls]
    return f"[tool calls] {'; '.join(parts)}" if parts else ""


def anthropic_output_text(content: Any) -> str:
    """All text blocks of an Anthropic response, or a summary of its tool_use blocks (the first block
    of a tool-calling turn is often not text, so the span stored an empty output)."""
    if not isinstance(content, (list, tuple)):
        return ""
    texts = [str(_get(b, "text") or "") for b in content if _get(b, "type") == "text" and _get(b, "text")]
    if texts:
        return "\n".join(texts)
    calls = [_tool_call_text(_get(b, "name"), _get(b, "input")) for b in content if _get(b, "type") == "tool_use"]
    return f"[tool calls] {'; '.join(calls)}" if calls else ""

