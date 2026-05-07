"""
Call Triage — classifies each interaction into a processing lane before
any LLM tokens are spent.

Processing lanes:
    hot   High-value outcome that requires immediate action.
          Examples: rebook_confirmed, demo_booked, escalation_needed.
          Routed to the high-priority Celery queue; signal_jobs fire ASAP.

    cold  Low-urgency outcome. The business can wait.
          Examples: not_interested, callback_requested, already_done.
          Routed to the standard queue; deferrable under rate pressure.

    skip  Short call (< threshold turns) or null content.
          No LLM. Lead stage updated directly.  Zero tokens consumed.

The triage decision uses two mechanisms:
    1. Fast keyword scan on the raw transcript text.
       Cheap, no tokens, good signal for obvious outcomes.
    2. (Optional) A single, cheap LLM classification call if keywords are
       inconclusive — only 50–100 tokens versus ~1500 for full analysis.

Why not rely on the full LLM for triage?
    The full analysis always runs — triage only decides WHEN it runs.
    For 100K calls, even a 100ms cheaper pre-screen pays off: it lets us
    defer 60% of calls (the cold lane) without burning quota on them first.

Assumption:
    A transcript is "hot" if it contains a clear positive commitment signal
    or a negative sentiment requiring urgent escalation.
    "Cold" covers ambiguous and negative-but-calm outcomes.
    This heuristic will misclassify edge cases (especially Hinglish).
    That's acceptable — misclassifying cold→hot wastes a queue slot;
    misclassifying hot→cold delays a real sale. We err toward hot.
"""

import logging
import re
from dataclasses import dataclass
from typing import List

logger = logging.getLogger(__name__)

# ── Keyword patterns ──────────────────────────────────────────────────────────
# These are checked against the lowercased, transliterated transcript text.

_HOT_PATTERNS: List[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bconfirm(ed|ing)?\b",
        r"\bbook(ed|ing)?\b",
        r"\bscheduled?\b",
        r"\btomorrow\b",
        r"\bdem(o|onstration)\b",
        r"\brebook\b",
        r"\bappointment\b",
        r"\bmanager\b",
        r"\bescalat(e|ing|ion)\b",
        r"\bcompla(int|ined)\b",
        r"\bunacceptable\b",
        r"\btheek hai.*confirm\b",  # common Hinglish confirmation
        r"\bhaan.*confirm\b",
    ]
]

_COLD_PATTERNS: List[re.Pattern] = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bnot interested\b",
        r"\bdon.t call\b",
        r"\bwrong number\b",
        r"\balready (done|purchased|booked)\b",
        r"\bcall.*later\b",
        r"\bbaad mein\b",  # "later" in Hindi
        r"\bsoch(ta|ti) (hoon|hai)\b",  # "I'm thinking" — ambiguous
    ]
]


@dataclass
class TriageResult:
    lane: str          # "hot", "cold", or "skip"
    confidence: str    # "keyword" or "heuristic"
    reason: str        # human-readable rationale for audit log


def triage_interaction(
    transcript: list,
    transcript_text: str,
    short_call_threshold: int = 4,
) -> TriageResult:
    """
    Classify an interaction into a processing lane.

    Args:
        transcript:            List of {role, content} turn dicts.
        transcript_text:       Pre-joined string of all turns.
        short_call_threshold:  Number of turns below which we skip LLM.

    Returns:
        TriageResult with lane, confidence, and reason.

    This function is synchronous and free of I/O — safe to call in the
    FastAPI event loop at webhook time without blocking.
    """

    # ── Skip: too short to contain meaningful content ─────────────────────
    turn_count = len(transcript)
    if turn_count < short_call_threshold:
        logger.info(
            "triage_skip",
            extra={"turn_count": turn_count, "threshold": short_call_threshold},
        )
        return TriageResult(
            lane="skip",
            confidence="heuristic",
            reason=f"Short call: {turn_count} turns < threshold {short_call_threshold}",
        )

    # ── Cold: clear low-value signal ──────────────────────────────────────
    for pattern in _COLD_PATTERNS:
        if pattern.search(transcript_text):
            reason = f"Cold keyword matched: '{pattern.pattern}'"
            logger.info("triage_cold", extra={"reason": reason})
            return TriageResult(
                lane="cold",
                confidence="keyword",
                reason=reason,
            )

    # ── Hot: escalation or clear positive commitment ───────────────────────
    for pattern in _HOT_PATTERNS:
        if pattern.search(transcript_text):
            reason = f"Hot keyword matched: '{pattern.pattern}'"
            logger.info("triage_hot", extra={"reason": reason})
            return TriageResult(
                lane="hot",
                confidence="keyword",
                reason=reason,
            )

    # ── Default: no strong signal — route cold (safe fallback) ────────────
    logger.info(
        "triage_cold_default",
        extra={"turn_count": turn_count, "reason": "No keyword match"},
    )
    return TriageResult(
        lane="cold",
        confidence="heuristic",
        reason="No hot/cold keyword matched; defaulting to cold",
    )
