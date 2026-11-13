"""Concurrent first calls in a fresh process (pass 8, HIGH): the provider patch was installed without
a lock, so two threads both patched, the second captured the first's wrapper as the "original", and
every call recursed until RecursionError (12 of 12 calls, ~990 error spans each)."""
import subprocess
import sys
import textwrap

import pytest

pytest.importorskip("openai")
pytest.importorskip("anthropic")

MOCK = """
import json, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
CHAT = {"id": "c", "object": "chat.completion", "created": 1, "model": "m",
        "choices": [{"index": 0, "finish_reason": "stop", "message": {"role": "assistant", "content": "hi"}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}}
MSG = {"id": "m", "type": "message", "role": "assistant", "model": "m", "stop_reason": "end_turn",
       "stop_sequence": None, "content": [{"type": "text", "text": "hi"}],
       "usage": {"input_tokens": 1, "output_tokens": 1}}
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_POST(self):
        self.rfile.read(int(self.headers.get("content-length") or 0))
        body = CHAT if self.path.endswith("/chat/completions") else MSG if self.path.endswith("/messages") else {}
        b = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)
srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
threading.Thread(target=srv.serve_forever, daemon=True).start()
BASE = f"http://127.0.0.1:{srv.server_address[1]}"
"""


def _run(body: str) -> str:
    env = "import os\nos.environ['RETRACE_TELEMETRY'] = '0'\nos.environ['RETRACE_TRANSPORT'] = 'http'\n"
    code = env + MOCK + textwrap.dedent(body)
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=180, check=True)
    return out.stdout.strip().splitlines()[-1]


@pytest.mark.parametrize("provider", ["openai", "anthropic"])
def test_concurrent_first_calls_in_a_fresh_process_never_recurse(provider):
    line = _run(f"""
        import sys, threading, collections
        import retrace, openai, anthropic
        # Build the providers' response models on this thread first: pydantic's lazy first build is
        # not thread-safe either (it can fail without Retrace). Retrace's patch is still installed by
        # the threads' first calls below, which is the race under test.
        openai.types.chat.ChatCompletion.model_validate(CHAT)
        anthropic.types.Message.model_validate(MSG)
        retrace.configure(api_key="rt_testkeytestkeytestkey", base_url=BASE)
        oa = openai.OpenAI(api_key="sk-x", base_url=BASE + "/v1", max_retries=0)
        an = anthropic.Anthropic(api_key="sk-x", base_url=BASE, max_retries=0)
        msg = [{{"role": "user", "content": "x"}}]
        errors = collections.Counter()

        @retrace.record(name="cold")
        def run():
            if "{provider}" == "openai":
                return oa.chat.completions.create(model="m", messages=msg).choices[0].message.content
            return an.messages.create(model="m", max_tokens=5, messages=msg).content[0].text

        def worker():
            for _ in range(3):
                try:
                    assert run() == "hi"
                except BaseException as e:
                    errors[type(e).__name__] += 1

        threads = [threading.Thread(target=worker) for _ in range(12)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        print(dict(errors))
    """)
    assert line == "{}"
