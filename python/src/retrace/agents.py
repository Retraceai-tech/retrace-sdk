"""Multi-agent topology helpers — tag spans with the agent that produced them.

Framework-agnostic: wrap a sub-agent's work in ``with retrace.agent(...)`` and every span recorded
inside (including auto-instrumented LLM calls) inherits the agent id / role / parent, so the trace
page can draw the agent topology. Nested ``agent(...)`` blocks set ``parent_agent_id`` automatically.

Framework mapping (explicit, because deep auto-detection would couple the SDK to each framework):
    - LangGraph: use the node name as the agent id.
    - CrewAI: use the agent's role as both id and role.

    with retrace.agent("planner", role="Planner"):
        plan = planner_node(state)          # spans tagged agent_id="planner"
        with retrace.agent("researcher", role="Researcher"):
            facts = researcher_node(state)  # parent_agent_id="planner"
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator, Optional

# ContextVar (not an instance attribute) so concurrent agents in async/threaded runs don't clobber
# each other's context. Reconstructible state only — never the source of truth.
_current_agent: ContextVar[Optional[dict[str, Optional[str]]]] = ContextVar("retrace_current_agent", default=None)


def current_agent() -> Optional[dict[str, Optional[str]]]:
    """The agent context spans are currently tagged with, or None outside any ``agent(...)`` block."""
    return _current_agent.get()


@contextmanager
def agent(agent_id: str, *, role: Optional[str] = None, parent: Optional[str] = None) -> Iterator[None]:
    """Scope every span created inside the block to ``agent_id``.

    ``parent`` defaults to the enclosing ``agent(...)`` block's id (the delegation edge), so nesting
    expresses the handoff graph without repeating ids.
    """
    prev = _current_agent.get()
    parent_id = parent if parent is not None else (prev.get("agent_id") if prev else None)
    token = _current_agent.set(
        {"agent_id": agent_id, "agent_role": role, "parent_agent_id": parent_id}
    )
    try:
        yield
    finally:
        _current_agent.reset(token)
