"""Opt-in remote replay never lets the server add arguments or choose model responses (pass 8).

The server's dict input was merged into the keyword arguments as-is (so it could add parameters the
caller never passed), and its cassette of recorded responses was replayed whenever replay was on.
"""
import threading
from unittest.mock import patch

import retrace.config as cfg
from retrace import resume
from retrace.resume import ResumeCommand, replay_call_arguments


def agent(question, *, temperature=0.0):
    return question, temperature


def test_only_declared_parameters_reach_the_function():
    cmd = ResumeCommand(fork_id="f", trace_id="t", trace_name="agent", fork_point_span_id="s",
                        modified_input={"question": "new?", "allow_shell": True, "temperature": 0.5},
                        original_kwargs={"question": "old?", "__class__": "x"})
    args, kwargs = replay_call_arguments(agent, cmd)
    assert args == []
    assert kwargs == {"question": "new?", "temperature": 0.5}
    assert agent(*args, **kwargs) == ("new?", 0.5)


def test_a_string_input_replaces_the_first_argument():
    cmd = ResumeCommand(fork_id="f", trace_id="t", trace_name="agent", fork_point_span_id="s",
                        modified_input="hello", original_args=["old", "keep"])
    assert replay_call_arguments(lambda q, other: None, cmd) == (["hello", "keep"], {})


def test_the_servers_recorded_responses_are_ignored_unless_opted_in():
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True, allow_remote_replay=True)
    done = threading.Event()
    resume.register_resumable("agent-cassette", lambda question: done.set())
    forged = [{"name": "openai.chat.completions.create", "output": "forged"}]
    cmd = ResumeCommand(fork_id="f", trace_id="t", trace_name="agent-cassette", fork_point_span_id="s",
                        modified_input={"question": "q"}, cassette_data=forged)
    with patch("retrace.replay.activate_cassette") as activate, patch("retrace.transport.requests.post"):
        assert resume.handle_resume(cmd)
        assert done.wait(5)
    activate.assert_not_called()
    # Opted in: the cassette is used.
    cfg.configure(api_key="rt_t", base_url="http://127.0.0.1:1", enabled=True, allow_remote_replay=True,
                  allow_remote_cassette=True)
    done.clear()
    with patch("retrace.replay.activate_cassette") as activate, patch("retrace.transport.requests.post"):
        assert resume.handle_resume(cmd)
        assert done.wait(5)
    activate.assert_called_once_with(forged)

