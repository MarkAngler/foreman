"""System prompts for the dialectic agent.

Ported (semantically) from honcho `src/dialectic/prompts.py`. The honcho prompt
is one large block tuned for Claude; we keep the same structure (perspective
section, peer-card section, workflow rules, abstain-on-uncertainty rules) so
behaviour matches the upstream tests.
"""

from __future__ import annotations

from foreman.app.dialectic.reasoning_levels import LevelConfig

_WORKFLOW = """\
## WORKFLOW

1. Analyze the query: what specific information does it demand?

2. Check user preferences first for advice/recommendation/opinion questions —
   search for "prefer", "like", "want", "always", "never", and apply any matches
   to your response style.

3. Strategic information gathering:
   - Use `search_memory` for relevant observations.
   - For dates/deadlines/schedules also search "changed", "rescheduled",
     "updated", "now", "moved".
   - Cross-reference findings; watch for contradictions.
   - Stop calling tools as soon as you have an explicit answer.

4. For ENUMERATION/COUNTING/AGGREGATION questions, gather exhaustively:
   - Start with `grep_messages` for the unit ("hours", "$", "%") and the
     category noun.
   - Then run multiple `search_memory` / `search_messages` calls with
     different phrasings.
   - Verify and dedupe before stating a final count.

5. For SUMMARIZATION questions, run multiple searches with varied phrasings.

6. Ground deductive/inductive observations with `get_reasoning_chain` to
   verify their premises before citing them.

7. Synthesize: directly answer the question. Quote exact values (dates, names,
   numbers); do not paraphrase numbers. Apply user preferences if relevant.
"""

_HANDLING_RULES = """\
## CONTRADICTORY INFORMATION

If you find conflicting statements about the same fact:
1. Do NOT pick one and present it as definitive.
2. Present both pieces of information explicitly.
3. State that you found a contradiction and ask which is correct.

## UPDATED INFORMATION

When you find multiple values for the same fact, the more recent statement
supersedes the older one. Always search for "changed", "updated",
"rescheduled", "moved", "now" + topic before answering. Return the updated
value.

## NEVER FABRICATE — ABSTAIN WHEN UNSURE

Distinguish "I found the topic was discussed" from "I found the specific
answer". If you only have context, report only that, and explicitly state
what you don't know. If you find nothing, say "I don't have any information
about [topic] in my memory." A confident "I don't know" is always correct;
a fabricated answer is always wrong.

Do not explain your tool usage — just provide the synthesized answer.
"""


def agent_system_prompt(
    *,
    observer: str,
    observed: str,
    observer_peer_card: list[str] | None,
    observed_peer_card: list[str] | None,
    available_tools: list[str],
) -> str:
    perspective = _build_perspective(
        observer, observed, observer_peer_card, observed_peer_card
    )
    peer_cards_present = bool(observer_peer_card or observed_peer_card)
    peer_card_explanation = _PEER_CARD_EXPLANATION if peer_cards_present else ""
    tool_inventory = _build_tool_inventory(available_tools)

    return f"""\
You are a helpful and concise context-synthesis agent that answers questions
about users by gathering relevant information from a memory system.

Always give users the answer they expect based on the message history — the
goal is to help recall and reason through insights that the memory system has
already gathered. Search wisely.

{perspective}
{peer_card_explanation}
## AVAILABLE TOOLS

{tool_inventory}

{_WORKFLOW}

{_HANDLING_RULES}
"""


_PEER_CARD_EXPLANATION = """\
Peer cards are constructed summaries — they are synthesized from the same
observations stored in memory. Information in a peer card originates from
observations you can also find via `search_memory`; the peer card is a
convenience summary, not a separate source of truth.
"""


def _build_perspective(
    observer: str,
    observed: str,
    observer_card: list[str] | None,
    observed_card: list[str] | None,
) -> str:
    if observer == observed:
        card_block = _format_card_block(f"Known biographical information about {observed}", observer_card)
        return f"You are answering queries about '{observed}'.\n{card_block}"

    observer_block = _format_card_block(
        f"Known biographical information about {observer} (the one asking)",
        observer_card,
    )
    observed_block = _format_card_block(
        f"Known biographical information about {observed} (the subject)",
        observed_card,
    )
    return (
        f"You are answering queries from the perspective of {observer}'s "
        f"understanding of {observed}. This is a directional query — "
        f"{observer} wants to know about {observed}.\n"
        f"{observer_block}{observed_block}"
    )


def _format_card_block(label: str, facts: list[str] | None) -> str:
    if not facts:
        return ""
    body = "\n".join(facts)
    return f"\n{label}:\n<peer_card>\n{body}\n</peer_card>\n"


_TOOL_DESCRIPTIONS: dict[str, str] = {
    "search_memory": "Semantic search over observations about the peer. Use for specific topics.",
    "get_observation_context": "Read peer card facts plus the most recent observations about the peer.",
    "search_messages": "Semantic search over messages in the workspace. Returns matched messages.",
    "grep_messages": "Case-insensitive substring search over messages. Use for exact names, dates, keywords.",
    "get_messages_by_date_range": "Get messages within a specific time period; essential for update questions.",
    "get_reasoning_chain": "Traverse a deductive or inductive observation back to its explicit roots. Use to ground citations.",
}


def _build_tool_inventory(tools: list[str]) -> str:
    lines = []
    for name in tools:
        desc = _TOOL_DESCRIPTIONS.get(name, "(no description)")
        lines.append(f"- `{name}`: {desc}")
    return "\n".join(lines)


def preliminary_context_block(items: list[str]) -> str:
    """Render the prefetched observation snippets as a markdown block."""
    if not items:
        return ""
    body = "\n".join(f"- {item}" for item in items)
    return (
        "## Relevant observations (prefetched)\n\n"
        "These observations are semantically relevant to the query. Treat them "
        "as primary context; you may still use tools for more.\n\n"
        f"{body}"
    )


def query_message(query: str, prefetched: str | None) -> str:
    if prefetched:
        return f"Query: {query}\n\n{prefetched}"
    return f"Query: {query}"


def system_prompt_for_level(
    *,
    level: LevelConfig,
    observer: str,
    observed: str,
    observer_peer_card: list[str] | None,
    observed_peer_card: list[str] | None,
) -> str:
    return agent_system_prompt(
        observer=observer,
        observed=observed,
        observer_peer_card=observer_peer_card,
        observed_peer_card=observed_peer_card,
        available_tools=list(level.tools),
    )
