"""Dialectic agent — agentic tool-calling loop for answering peer queries.

Public surface is intentionally small: the FastAPI router calls `run_agent_stream`
to drive the loop and consume SSE events.
"""

from foreman.app.dialectic.agent import AgentEvent, run_agent_stream
from foreman.app.dialectic.reasoning_levels import LEVELS, ReasoningLevel

__all__ = ["AgentEvent", "LEVELS", "ReasoningLevel", "run_agent_stream"]
