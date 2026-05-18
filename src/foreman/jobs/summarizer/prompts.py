"""Summarizer prompts.

Ported from honcho upstream `src/utils/summarizer.py` — the short and long
summary prompt bodies are preserved verbatim so summary quality stays in
lockstep with honcho's evaluation set.

Source:
  https://github.com/plastic-labs/honcho/blob/main/src/utils/summarizer.py
"""

from __future__ import annotations

from datetime import datetime
from inspect import cleandoc

# Honcho's defaults from `settings.SUMMARY.MESSAGES_PER_*_SUMMARY`.
MESSAGES_PER_SHORT_SUMMARY = 20
MESSAGES_PER_LONG_SUMMARY = 60

# Hard token caps lifted from honcho's `settings.SUMMARY.MAX_TOKENS_*`.
MAX_TOKENS_SHORT = 1000
MAX_TOKENS_LONG = 4000


def format_message_turn(content: str, created_at: datetime, peer_name: str) -> str:
    """Render one message for the summary prompt — matches honcho's turn format."""
    ts = created_at.replace(microsecond=0).isoformat()
    return f"[{ts}] {peer_name}: {content}"


def short_summary_prompt(
    *,
    formatted_messages: str,
    output_words: int,
    previous_summary_text: str,
) -> str:
    return cleandoc(
        f"""
        You are a system that summarizes parts of a conversation to create a concise and accurate summary. Focus on capturing:

        1. Key facts and information shared (**Capture as many explicit facts as possible**)
        2. User preferences, opinions, and questions
        3. Important context and requests
        4. Core topics discussed

        If there is a previous summary, ALWAYS make your new summary inclusive of both it and the new messages, therefore capturing the ENTIRE conversation. Prioritize key facts across the entire conversation.

        Provide a concise, factual summary that captures the essence of the conversation. Your summary should be detailed enough to serve as context for future messages, but brief enough to be helpful. Prefer a thorough chronological narrative over a list of bullet points.

        Return only the summary without any explanation or meta-commentary.

        <previous_summary>
        {previous_summary_text}
        </previous_summary>

        <conversation>
        {formatted_messages}
        </conversation>

        Hard limit: {output_words} words maximum. If needed, drop lower-priority detail to stay within the limit.
        """
    )


def long_summary_prompt(
    *,
    formatted_messages: str,
    output_words: int,
    previous_summary_text: str,
) -> str:
    return cleandoc(
        f"""
        You are a system that creates thorough, comprehensive summaries of conversations. Focus on capturing:

        1. Key facts and information shared (**Capture as many explicit facts as possible**)
        2. User preferences, opinions, and questions
        3. Important context and requests
        4. Core topics discussed in detail
        5. User's apparent emotional state and personality traits
        6. Important themes and patterns across the conversation

        If there is a previous summary, ALWAYS make your new summary inclusive of both it and the new messages, therefore capturing the ENTIRE conversation. Prioritize key facts across the entire conversation.

        Provide a thorough and detailed summary that captures the essence of the conversation. Your summary should serve as a comprehensive record of the important information in this conversation. Prefer an exhaustive chronological narrative over a list of bullet points.

        Return only the summary without any explanation or meta-commentary.

        <previous_summary>
        {previous_summary_text}
        </previous_summary>

        <conversation>
        {formatted_messages}
        </conversation>

        Hard limit: {output_words} words maximum. If needed, drop lower-priority detail to stay within the limit.
        """
    )


NO_PREVIOUS_SUMMARY = (
    "There is no previous summary -- the messages are the beginning of the conversation."
)
