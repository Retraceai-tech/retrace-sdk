"""Provider-SDK compatibility canary: exercises the real openai / anthropic / google-genai classes that the
interceptors patch, over mock HTTP transports (no network). Each block skips if that SDK isn't installed,
so the same file runs against the lowest supported and the latest provider majors."""
import asyncio
import importlib
import json

import pytest


def _hx(pkg):
    # openai>=3 / anthropic>=1 are built on httpx2; older majors (and google-genai) on httpx.
    base = importlib.import_module(f"{pkg.__name__}._base_client")
    return getattr(base, "httpx2", None) or base.httpx


def _sse(events, named=False):
    if named:
        return "".join(f"event: {n}\ndata: {json.dumps(d)}\n\n" for n, d in events).encode()
    return ("".join(f"data: {json.dumps(e)}\n\n" for e in events) + "data: [DONE]\n\n").encode()


@pytest.fixture
def spans():
    return []


def test_openai_create_and_stream(spans):
    openai = pytest.importorskip("openai")
    from retrace.interceptors import openai as oi

    hx = _hx(openai)
    completion = {"id": "c", "object": "chat.completion", "created": 1, "model": "gpt-4o",
                  "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": "hi"}}],
                  "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}}
    chunk = {"id": "c", "object": "chat.completion.chunk", "created": 1, "model": "gpt-4o",
             "choices": [{"index": 0, "delta": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}]}

    def handler(req):
        if json.loads(req.content or b"{}").get("stream"):
            return hx.Response(200, headers={"content-type": "text/event-stream"}, content=_sse([chunk]))
        return hx.Response(200, json=completion)

    oi.install_openai_interceptor(on_span=spans.append)
    try:
        client = openai.OpenAI(api_key="x", base_url="http://mock/v1",
                               http_client=hx.Client(transport=hx.MockTransport(handler)))
        msgs = [{"role": "user", "content": "q"}]
        assert client.chat.completions.create(model="gpt-4o", messages=msgs).choices[0].message.content == "hi"
        with client.chat.completions.create(model="gpt-4o", messages=msgs, stream=True) as st:
            assert st.response is not None  # real Stream attributes stay reachable through the proxy
            assert "".join(c.choices[0].delta.content or "" for c in st if c.choices) == "hi"
        if hasattr(client.chat.completions, "stream"):  # openai>=1.4x helper reads raw_stream.response
            with client.chat.completions.stream(model="gpt-4o", messages=msgs) as st:
                list(st)
        llm = [s for s in spans if s["span_type"] == "llm_call"]
        assert len(llm) >= 2 and llm[0]["input_tokens"] == 3
    finally:
        oi.uninstall_openai_interceptor()


def test_anthropic_create_and_stream(spans):
    anthropic = pytest.importorskip("anthropic")
    from retrace.interceptors import anthropic as ai

    hx = _hx(anthropic)
    msg = {"id": "m", "type": "message", "role": "assistant", "model": "claude-sonnet-4-6",
           "content": [{"type": "text", "text": "hi"}], "stop_reason": "end_turn", "stop_sequence": None,
           "usage": {"input_tokens": 3, "output_tokens": 2}}
    events = [
        ("message_start", {"type": "message_start",
                           "message": {**msg, "content": [], "usage": {"input_tokens": 3, "output_tokens": 0}}}),
        ("content_block_start", {"type": "content_block_start", "index": 0,
                                 "content_block": {"type": "text", "text": ""}}),
        ("content_block_delta", {"type": "content_block_delta", "index": 0,
                                 "delta": {"type": "text_delta", "text": "hi"}}),
        ("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                           "usage": {"output_tokens": 2}}),
        ("message_stop", {"type": "message_stop"}),
    ]

    def handler(req):
        if json.loads(req.content or b"{}").get("stream"):
            return hx.Response(200, headers={"content-type": "text/event-stream"}, content=_sse(events, named=True))
        return hx.Response(200, json=msg)

    ai.install_anthropic_interceptor(on_span=spans.append)
    try:
        client = anthropic.Anthropic(api_key="x", base_url="http://mock",
                                     http_client=hx.Client(transport=hx.MockTransport(handler)))
        msgs = [{"role": "user", "content": "q"}]
        r = client.messages.create(model="claude-sonnet-4-6", max_tokens=8, messages=msgs,
                                   extra_body={"temperature": 0.2})
        assert r.content[0].text == "hi"
        with client.messages.create(model="claude-sonnet-4-6", max_tokens=8, messages=msgs, stream=True) as st:
            list(st)
        llm = [s for s in spans if s["span_type"] == "llm_call"]
        assert len(llm) == 2
        assert llm[0]["metadata"]["sampling"]["temperature"] == 0.2  # anthropic>=1 moved sampling to extra_body
        assert llm[1]["output"] == "hi" and llm[1]["output_tokens"] == 2
    finally:
        ai.uninstall_anthropic_interceptor()


def test_gemini_generate_and_stream(spans):
    pytest.importorskip("google.genai")
    try:
        import httpx
    except ImportError:  # google-genai>=3 is built on httpx2 (a drop-in httpx fork) and no longer pulls in httpx
        import httpx2 as httpx
    from google import genai
    from google.genai import types

    from retrace.interceptors import gemini as gi

    resp = {"candidates": [{"content": {"role": "model", "parts": [{"text": "hi"}]}, "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": 3, "candidatesTokenCount": 2, "totalTokenCount": 5}}

    def handler(req):
        if "streamGenerateContent" in str(req.url):
            return httpx.Response(200, headers={"content-type": "text/event-stream"},
                                  content=f"data: {json.dumps(resp)}\r\n\r\n".encode())
        return httpx.Response(200, json=resp)

    gi.install_gemini_interceptor(on_span=spans.append)
    try:
        client = genai.Client(api_key="x", http_options=types.HttpOptions(
            base_url="http://mock/", httpx_client=httpx.Client(transport=httpx.MockTransport(handler))))
        assert client.models.generate_content(model="gemini-2.5-flash", contents="q").text == "hi"
        assert "".join(c.text or "" for c in client.models.generate_content_stream(
            model="gemini-2.5-flash", contents="q")) == "hi"
        llm = [s for s in spans if s["span_type"] == "llm_call"]
        assert len(llm) == 2 and llm[0]["input_tokens"] == 3
    finally:
        gi.uninstall_gemini_interceptor()


def test_openai_async_stream(spans):
    openai = pytest.importorskip("openai")
    from retrace.interceptors import openai as oi

    hx = _hx(openai)
    chunk = {"id": "c", "object": "chat.completion.chunk", "created": 1, "model": "gpt-4o",
             "choices": [{"index": 0, "delta": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}]}

    def handler(req):
        return hx.Response(200, headers={"content-type": "text/event-stream"}, content=_sse([chunk]))

    async def run():
        client = openai.AsyncOpenAI(api_key="x", base_url="http://mock/v1",
                                    http_client=hx.AsyncClient(transport=hx.MockTransport(handler)))
        msgs = [{"role": "user", "content": "q"}]
        async with await client.chat.completions.create(model="gpt-4o", messages=msgs, stream=True) as st:
            assert st.response is not None  # AsyncStream attributes stay reachable through the proxy
            out = "".join([c.choices[0].delta.content or "" async for c in st if c.choices])
        assert out == "hi"
        has_helper = hasattr(client.chat.completions, "stream")  # async helper reads raw_stream.response
        if has_helper:
            async with client.chat.completions.stream(model="gpt-4o", messages=msgs) as st:
                async for _ in st:
                    pass
        return has_helper

    oi.install_openai_interceptor(on_span=spans.append)
    try:
        has_helper = asyncio.run(run())
        llm = [s for s in spans if s["span_type"] == "llm_call"]
        assert len(llm) == (2 if has_helper else 1)
        assert llm[0]["output"] == "hi" and llm[0]["metadata"]["capture_complete"] is True
    finally:
        oi.uninstall_openai_interceptor()


def test_anthropic_async_stream(spans):
    anthropic = pytest.importorskip("anthropic")
    from retrace.interceptors import anthropic as ai

    hx = _hx(anthropic)
    start_msg = {"id": "m", "type": "message", "role": "assistant", "model": "claude-sonnet-4-6", "content": [],
                 "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 3, "output_tokens": 0}}
    events = [
        ("message_start", {"type": "message_start", "message": start_msg}),
        ("content_block_start", {"type": "content_block_start", "index": 0,
                                 "content_block": {"type": "text", "text": ""}}),
        ("content_block_delta", {"type": "content_block_delta", "index": 0,
                                 "delta": {"type": "text_delta", "text": "hi"}}),
        ("message_delta", {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                           "usage": {"output_tokens": 2}}),
        ("message_stop", {"type": "message_stop"}),
    ]

    def handler(req):
        return hx.Response(200, headers={"content-type": "text/event-stream"}, content=_sse(events, named=True))

    async def run():
        client = anthropic.AsyncAnthropic(api_key="x", base_url="http://mock",
                                          http_client=hx.AsyncClient(transport=hx.MockTransport(handler)))
        kw = {"model": "claude-sonnet-4-6", "max_tokens": 8, "messages": [{"role": "user", "content": "q"}]}
        async with await client.messages.create(stream=True, **kw) as st:
            assert st.response is not None
            async for _ in st:
                pass
        early = await client.messages.create(stream=True, **kw)
        async for _ in early:
            break  # abandon after the first event
        await early.close()

    ai.install_anthropic_interceptor(on_span=spans.append)
    try:
        asyncio.run(run())
        llm = [s for s in spans if s["span_type"] == "llm_call"]
        assert len(llm) == 2
        assert llm[0]["output"] == "hi" and llm[0]["metadata"]["capture_complete"] is True
        assert llm[1]["metadata"]["partial"] is True  # early close => partial span, not dropped
    finally:
        ai.uninstall_anthropic_interceptor()
