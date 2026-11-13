"""The API key never leaves over cleartext or through a redirect, and is not printed (pass 8)."""
from unittest.mock import MagicMock, patch

import pytest

import retrace.config as cfg
from retrace.golden import mark_golden


def test_config_repr_does_not_print_the_key():
    c = cfg.RetraceConfig(api_key="rt_supersecretkeyvalue", base_url="https://api.retraceai.tech")
    assert "rt_supersecretkeyvalue" not in repr(c)


def test_a_cleartext_websocket_url_is_refused():
    with pytest.raises(ValueError, match="insecure WebSocket URL"):
        cfg.configure(api_key="rt_t", base_url="https://api.retraceai.tech", ws_url="ws://evil.example")
    cfg.configure(api_key="rt_t", base_url="https://api.retraceai.tech", ws_url="wss://api.retraceai.tech")
    cfg.configure(api_key="rt_t", base_url="http://localhost:3001", ws_url="ws://localhost:3001")


def test_mark_golden_checks_the_url_and_does_not_follow_redirects():
    c = cfg.get_config()
    c.api_key, c.base_url, c.ws_url = "rt_t", "http://api.example.com", "wss://api.example.com"
    with patch("requests.post") as post:
        with pytest.raises(ValueError, match="insecure base URL"):
            mark_golden("t1")
        post.assert_not_called()
    cfg.configure(api_key="rt_t", base_url="https://api.retraceai.tech")
    with patch("requests.post", return_value=MagicMock(status_code=200)) as post:
        mark_golden("t1")
    assert post.call_args.kwargs["allow_redirects"] is False


def test_every_key_bearing_http_call_disables_redirects():
    import inspect
    import re

    import retrace.enforcement as enforcement
    import retrace.resume as resume
    import retrace.transport as transport
    for mod in (transport, enforcement, resume):
        src = inspect.getsource(mod)
        calls = re.findall(r"(?:requests|_req)\.(?:post|get|patch)\((?:[^()]|\([^()]*\))*\)", src, re.S)
        keyed = [c for c in calls if "headers" in c]
        assert keyed, mod.__name__
        assert all("allow_redirects=False" in c for c in keyed), [c[:80] for c in keyed if "allow_redirects" not in c]
    assert "redirect_limit=0" in inspect.getsource(transport.WSTransport._connect)
