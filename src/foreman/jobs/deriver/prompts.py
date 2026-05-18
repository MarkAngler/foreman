"""Deriver prompts for explicit-fact extraction.

Ported from honcho upstream `src/deriver/prompts.py` — `minimal_deriver_prompt`.
The body of the prompt is preserved verbatim so that fact-extraction quality
stays in lockstep with honcho's own evaluation set.

Source:
  https://github.com/plastic-labs/honcho/blob/main/src/deriver/prompts.py
"""

from __future__ import annotations

from datetime import datetime
from inspect import cleandoc

JSON_OUTPUT_INSTRUCTIONS = cleandoc(
    """
    Return a single JSON object with this exact shape and nothing else:
    {"explicit": [{"content": "<fact>"}, ...]}
    No prose, no markdown fences, no commentary. If there is nothing to extract,
    return {"explicit": []}.
    """
)


def format_message_turn(content: str, created_at: datetime, peer_name: str) -> str:
    """Render one message for the prompt, matching honcho's turn format."""
    ts = created_at.replace(microsecond=0).isoformat()
    return f"[{ts}] {peer_name}: {content}"


def minimal_deriver_prompt(
    *,
    peer_id: str,
    messages: str,
    custom_instructions: str | None = None,
) -> str:
    """Honcho's minimal deriver prompt — verbatim — wrapped with a JSON contract.

    The body of the rules / examples block matches the upstream string exactly.
    We append explicit JSON-output instructions because honcho enforces shape via
    `response_model=PromptRepresentation` in their LLM client; here we go through
    the generic `chat_sync` so the contract has to live in the prompt.
    """
    custom_section = _custom_instructions_section(custom_instructions)
    return cleandoc(
        f"""
        Analyze messages from {peer_id} to extract **explicit atomic facts** about them.

        [EXPLICIT] DEFINITION: Facts about {peer_id} that can be derived directly from their messages.
           - Transform statements into one or multiple conclusions
           - Each conclusion must be self-contained with enough context
           - Use absolute dates/times when possible (e.g. "June 26, 2025" not "yesterday")

        RULES:
        - Properly attribute observations to the correct subject: if it is about {peer_id}, say so. If {peer_id} is referencing someone or something else, make that clear.
        - Observations should make sense on their own. Each observation will be used in the future to better understand {peer_id}.
        - Extract ALL observations from {peer_id} messages, using others as context.
        - Contextualize each observation sufficiently (e.g. "Ann is nervous about the job interview at the pharmacy" not just "Ann is nervous")

        EXAMPLES:
        - EXPLICIT: "I just had my 25th birthday last Saturday" -> "{peer_id} is 25 years old", "{peer_id}'s birthday is June 21st"
        - EXPLICIT: "I took my dog for a walk in NYC" -> "{peer_id} has a dog", "{peer_id} lives in NYC"
        - EXPLICIT: "{peer_id} attended college" + general knowledge -> "{peer_id} completed high school or equivalent"

        {custom_section}

        Messages to analyze:
        <messages>
        {messages}
        </messages>

        {JSON_OUTPUT_INSTRUCTIONS}
        """
    )


def _custom_instructions_section(custom_instructions: str | None) -> str:
    if custom_instructions is None:
        return ""
    stripped = custom_instructions.strip()
    if not stripped:
        return ""
    return f"CUSTOM INSTRUCTIONS:\n{stripped}"
