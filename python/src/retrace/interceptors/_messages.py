"""What an LLM span records as its input messages.

The interceptors recorded the FIRST 10 messages of a call. In a longer conversation that dropped the
newest ones — the user's latest question and the tool results the model answered from — so the
recorded input no longer contained what the answer was grounded in, and the hallucination detector
flagged correct answers (pass 7: "0% of claims supported" for an answer copied from the tool result).
Now the conversation's leading system message (its instructions) plus the NEWEST messages are kept;
how many were left out is recorded alongside.
"""
from __future__ import annotations

from typing import Any

MAX_RECORDED_MESSAGES = 10
MAX_MESSAGE_CHARS = 1000
MAX_CONTENTS_CHARS = 2000


def _get(m: Any, key: str) -> Any:
    return m.get(key) if isinstance(m, dict) else getattr(m, key, None)


def _message(m: Any) -> dict[str, Any]:
    content = _get(m, "content")
    text = "" if content is None else content if isinstance(content, str) else str(content)
    return {"role": _get(m, "role") or "", "content": text[:MAX_MESSAGE_CHARS]}


def recorded_messages(messages: Any) -> dict[str, Any]:
    """`{"messages": [...]}` for a span's input: the leading system message + the newest messages."""
    msgs = list(messages) if isinstance(messages, (list, tuple)) else []
    if len(msgs) <= MAX_RECORDED_MESSAGES:
        return {"messages": [_message(m) for m in msgs]}
    head = msgs[:1] if _get(msgs[0], "role") == "system" else []
    tail = msgs[-(MAX_RECORDED_MESSAGES - len(head)):]
    return {"messages": [_message(m) for m in head + tail], "messages_omitted": len(msgs) - len(head) - len(tail)}


def recorded_contents(contents: Any) -> str | None:
    """Gemini `contents` as recorded text: the newest items, and the end of the text when it is long."""
    if not contents:
        return None
    items = list(contents) if isinstance(contents, (list, tuple)) else None
    if items is not None and len(items) > MAX_RECORDED_MESSAGES:
        items = items[-MAX_RECORDED_MESSAGES:]
    text = str(items if items is not None else contents)
    if len(text) <= MAX_CONTENTS_CHARS:
        return text
    head = MAX_CONTENTS_CHARS // 4
    return f"{text[:head]} […] {text[-(MAX_CONTENTS_CHARS - head - 5):]}"
