"""Dreamer prompts: deduction + induction.

Ported from honcho upstream `src/dreamer/specialists.py`. The honcho specialists
run an *agentic* loop with tool use; here we collapse to a single-shot LLM call
that produces structured JSON, because foreman's jobs go through `chat_sync`
(no tool loop, no streaming). The system-prompt content — the "what to look
for" rules — is preserved verbatim so output quality stays in lockstep with
honcho's evaluation set. We replace only the tool-invocation rubric with
JSON-output instructions.

Source:
  https://github.com/plastic-labs/honcho/blob/main/src/dreamer/specialists.py
"""

from __future__ import annotations

from inspect import cleandoc

DEDUCTION_JSON_INSTRUCTIONS = cleandoc(
    """
    Return a single JSON object with this exact shape and nothing else:
    {
      "deductions": [
        {"content": "<deductive conclusion>", "source_ids": ["<doc_id>", ...], "premises": ["<premise text>", ...]}
      ],
      "contradictions": [
        {"content": "<flagged contradiction>", "source_ids": ["<doc_id>", "<doc_id>"]}
      ],
      "peer_card_facts": ["<biographical fact>", ...]
    }
    No prose, no markdown fences, no commentary. If you have nothing to add for
    a section, return an empty list.
    """
)

INDUCTION_JSON_INSTRUCTIONS = cleandoc(
    """
    Return a single JSON object with this exact shape and nothing else:
    {
      "inductions": [
        {
          "content": "<pattern or generalization>",
          "source_ids": ["<doc_id>", "<doc_id>", ...],
          "pattern_type": "preference|behavior|personality|tendency|correlation",
          "confidence": "low|medium|high"
        }
      ],
      "peer_card_facts": ["<durable trait or preference>", ...]
    }
    Minimum 2 source_ids per induction. No prose, no markdown fences, no
    commentary. If you have nothing to induce, return {"inductions": [], "peer_card_facts": []}.
    """
)


def deduction_prompt(
    *,
    observed: str,
    documents_block: str,
    peer_card_block: str,
) -> str:
    """Single-shot deduction prompt — system + user collapsed.

    `documents_block` is a pre-formatted listing of `[id] level: content` rows.
    `peer_card_block` is the current peer card formatted as a bulleted list,
    or an empty string when no card exists yet.
    """
    return cleandoc(
        f"""
        You are a deductive reasoning agent analyzing observations about {observed}.

        ## YOUR JOB

        Create deductive observations by finding logical implications in what's
        already known. Think like a detective connecting evidence.

        ### Knowledge Updates (HIGH PRIORITY)
        When the same fact has different values at different times:
        - "meeting Tuesday" [old] -> "meeting moved to Thursday" [new]
        - Create a deductive update observation

        ### Logical Implications
        Extract implicit information:
        - "works as SWE at Google" -> "has software engineering skills", "employed in tech"
        - "has kids ages 5 and 8" -> "is a parent", "has school-age children"

        ### Contradictions
        When statements can't both be true (not just updates), flag them:
        - "I love coffee" vs "I hate coffee" -> contradiction observation

        ## PEER CARD

        The peer card is a summary of stable biographical facts. Update it when you
        learn name, age, location, occupation, family members, standing instructions,
        core preferences, or personality traits. Keep entries plain (e.g.
        "Name: Alice", "Works at Google", "INSTRUCTION: call me X",
        "PREFERENCE: detailed explanations"). Never add temporary event summaries
        or one-off conclusions. Cap the card at 40 entries — return only the *new*
        facts to add; the caller will dedup against the current card.

        ## RULES

        1. Create deductions based on what you ACTUALLY FIND, not what you expect.
        2. Always include `source_ids` linking to the documents you synthesised from.
        3. Empty or missing `source_ids` will be rejected.
        4. Quality over quantity — fewer good deductions beat many weak ones.

        ## CURRENT PEER CARD
        {peer_card_block or "(empty)"}

        ## DOCUMENTS

        Each line is `[<id>] (<level>) <content>`.

        <documents>
        {documents_block}
        </documents>

        {DEDUCTION_JSON_INSTRUCTIONS}
        """
    )


def induction_prompt(
    *,
    observed: str,
    documents_block: str,
    peer_card_block: str,
) -> str:
    """Single-shot induction prompt."""
    return cleandoc(
        f"""
        You are an inductive reasoning agent identifying patterns about {observed}.

        ## YOUR JOB

        Create inductive observations by finding patterns across multiple
        observations. Think like a psychologist identifying behavioral tendencies.

        Look at BOTH explicit observations AND deductive ones. Patterns often
        emerge from synthesising across both levels.

        ### Behavioral Patterns
        - "Tends to reschedule meetings when stressed"
        - "Makes decisions after consulting with partner"

        ### Preferences
        - "Prefers morning meetings"
        - "Likes detailed technical explanations"

        ### Personality Traits
        - "Generally optimistic about outcomes"
        - "Detail-oriented in planning"

        ### Temporal Patterns
        - "Career goals have remained consistent"
        - "Living situation changes frequently"

        ## PEER CARD

        After identifying patterns, only update the peer card for durable
        profile-level traits/preferences (e.g. `TRAIT: Analytical thinker`,
        `PREFERENCE: Prefers detailed explanations`). Do NOT add temporary
        patterns or episode-specific conclusions. Return only *new* facts; the
        caller will dedup against the current card.

        ## RULES

        1. Minimum 2 source observations required — patterns need evidence.
        2. Don't restate a single fact as a pattern.
        3. Confidence based on evidence count: 2 = low, 3-4 = medium, 5+ = high.
        4. Always include `source_ids` — empty/missing will be rejected.
        5. `pattern_type` must be one of: preference, behavior, personality, tendency, correlation.

        ## CURRENT PEER CARD
        {peer_card_block or "(empty)"}

        ## DOCUMENTS

        Each line is `[<id>] (<level>) <content>`.

        <documents>
        {documents_block}
        </documents>

        {INDUCTION_JSON_INSTRUCTIONS}
        """
    )


def format_documents_block(rows: list[tuple[str, str, str]]) -> str:
    """Render `[(id, level, content), ...]` into the prompt's documents block."""
    return "\n".join(f"[{doc_id}] ({level}) {content}" for doc_id, level, content in rows)


def format_peer_card_block(facts: list[str]) -> str:
    if not facts:
        return ""
    return "\n".join(f"- {fact}" for fact in facts)
