"""The first trace patches only the providers the app imports (pass 7: it imported all three, 0.9-2 s)."""
import subprocess
import sys
import textwrap

import pytest

pytest.importorskip("openai")
pytest.importorskip("anthropic")


def _run(code: str) -> str:
    env_code = "import os\nos.environ['RETRACE_TELEMETRY'] = '0'\nos.environ['RETRACE_TRANSPORT'] = 'http'\n"
    out = subprocess.run(
        [sys.executable, "-c", env_code + textwrap.dedent(code)],
        capture_output=True, text=True, timeout=120, check=True,
    )
    return out.stdout.strip().splitlines()[-1]


def test_first_trace_does_not_import_unused_providers():
    line = _run("""
        import sys, retrace
        retrace.configure(api_key="rt_testkeytestkeytestkey", base_url="http://127.0.0.1:9")
        with retrace.record("t"):
            pass
        print(sorted(n for n in ("openai", "anthropic", "google.genai") if n in sys.modules))
    """)
    assert line == "[]"


def test_provider_imported_before_or_after_the_trace_is_patched():
    line = _run("""
        import sys, retrace
        retrace.configure(api_key="rt_testkeytestkeytestkey", base_url="http://127.0.0.1:9")
        import anthropic  # imported before the first trace
        with retrace.record("t"):
            import openai  # imported while a trace is open
        from anthropic.resources.messages import Messages
        from openai.resources.chat.completions import Completions
        print(Messages.create.__name__, Completions.create.__name__, type(openai.__spec__.loader).__name__)
    """)
    assert line == "patched_create patched_create SourceFileLoader"
