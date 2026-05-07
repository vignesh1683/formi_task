"""
PostCallProcessor — Runs LLM analysis on a completed call transcript.

Changes from the original:
    1. Pre-flight rate limit check — never fires an LLM request when over budget.
    2. Per-customer budget check — checks BEFORE spending tokens.
    3. 429 awareness — reads Retry-After header and waits the correct duration.
    4. Token correction — releases the difference between estimated and actual tokens.
    5. Correlation ID threaded through all log events.
    6. Analysis result is written to BOTH interaction_metadata AND analysis_tasks.
       If a retry overwrites the metadata, the audit log still has every version.

Token estimation:
    We use settings.LLM_AVG_TOKENS_PER_CALL as the upfront reservation.
    After the call, we correct the counter with the actual token count.
    This means short transcripts free up their excess reservation quickly,
    while long ones correctly consume more.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass

from src.config import settings
from src.services.rate_limiter import rate_limiter
from src.services.budget_manager import budget_manager, CustomerQuota

logger = logging.getLogger(__name__)


@dataclass
class PostCallContext:
    """Everything needed to process one completed call."""
    interaction_id: str
    session_id: str
    lead_id: str
    campaign_id: str
    customer_id: str    # The business using the platform (not the person called)
    agent_id: str
    call_sid: str       # Exotel's identifier for the call
    transcript_text: str
    conversation_data: dict
    additional_data: dict  # Arbitrary metadata from the dialler
    ended_at: datetime
    exotel_account_id: Optional[str] = None
    correlation_id: Optional[str] = None


@dataclass
class AnalysisResult:
    call_stage: str             # Disposition: rebook_confirmed, not_interested, etc.
    entities: Dict[str, Any]    # Structured entities extracted from the transcript
    summary: str                # Human-readable summary for dashboard display
    raw_response: Dict[str, Any]
    tokens_used: int            # Actual tokens consumed — source of truth for billing
    estimated_tokens: int       # What we reserved before the call
    latency_ms: float
    provider: str
    model: str


class PostCallProcessor:
    """
    Runs full LLM analysis on a transcript.

    Rate limit behaviour:
        - Calls rate_limiter.try_acquire() BEFORE firing the LLM request.
        - If over global limit: raises RateLimitExceeded (caller must requeue).
        - After LLM responds: corrects both global and per-customer counters
          with the actual token count.

    Retry behaviour for 429s:
        - Reads the Retry-After header from the LLM provider's response.
        - Raises RateLimitExceeded with the correct wait_seconds so the
          Celery task can sleep the right amount before requeueing.
        - Does NOT use the Celery default_retry_delay for rate limit errors.
    """

    async def process_post_call(
        self,
        ctx: PostCallContext,
        single_prompt: bool = True,
        customer_quota: Optional[CustomerQuota] = None,
    ) -> AnalysisResult:
        """
        Run LLM analysis and write result to interaction_metadata.

        Raises:
            RateLimitExceeded: if global or per-customer budget is exhausted.
            Exception: for LLM API errors (non-429).
        """
        estimated_tokens = settings.LLM_AVG_TOKENS_PER_CALL
        log_ctx = {
            "interaction_id": ctx.interaction_id,
            "customer_id": ctx.customer_id,
            "correlation_id": ctx.correlation_id,
        }

        # ── Pre-flight: global rate limit check ───────────────────────────
        decision = await rate_limiter.try_acquire(estimated_tokens=estimated_tokens)
        if not decision.allowed:
            logger.warning(
                "llm_rate_limit_blocked",
                extra={
                    **log_ctx,
                    "reason": decision.reason,
                    "wait_seconds": decision.wait_seconds,
                },
            )
            raise RateLimitExceeded(
                wait_seconds=decision.wait_seconds,
                reason=decision.reason,
            )

        # ── Pre-flight: per-customer budget check ──────────────────────────
        usage = await rate_limiter.get_current_usage()
        global_utilisation = usage.get("tpm_utilisation_pct", 0.0)

        budget_decision = await budget_manager.check_and_consume(
            customer_id=ctx.customer_id,
            estimated_tokens=estimated_tokens,
            quota=customer_quota,
            global_utilisation_pct=global_utilisation,
        )
        if not budget_decision.allowed:
            # Roll back the global reservation we just made
            await rate_limiter.release_tokens(0, estimated_tokens)
            logger.warning(
                "customer_budget_blocked",
                extra={
                    **log_ctx,
                    "reason": budget_decision.reason,
                    "wait_seconds": budget_decision.wait_seconds,
                },
            )
            raise RateLimitExceeded(
                wait_seconds=budget_decision.wait_seconds,
                reason=budget_decision.reason,
            )

        # ── LLM call ──────────────────────────────────────────────────────
        try:
            prompt = self._build_analysis_prompt(
                ctx.transcript_text,
                ctx.additional_data,
                single_prompt,
            )

            start_time = datetime.utcnow()
            response = await self._call_llm(prompt, ctx)
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

            result = self._parse_response(response, elapsed_ms, estimated_tokens)

            # Correct both counters with actual token usage
            await rate_limiter.release_tokens(result.tokens_used, estimated_tokens)
            await budget_manager.release_tokens(
                ctx.customer_id, result.tokens_used, estimated_tokens
            )

            await self._update_interaction_metadata(ctx.interaction_id, result)

            logger.info(
                "postcall_analysis_complete",
                extra={
                    **log_ctx,
                    "campaign_id": ctx.campaign_id,
                    "call_stage": result.call_stage,
                    "tokens_used": result.tokens_used,
                    "tokens_estimated": estimated_tokens,
                    "latency_ms": result.latency_ms,
                },
            )

            return result

        except RateLimitExceeded:
            # Roll back reservations on 429
            await rate_limiter.release_tokens(0, estimated_tokens)
            await budget_manager.release_tokens(ctx.customer_id, 0, estimated_tokens)
            raise

        except Exception as e:
            # Roll back reservations on any other error
            await rate_limiter.release_tokens(0, estimated_tokens)
            await budget_manager.release_tokens(ctx.customer_id, 0, estimated_tokens)
            logger.exception(
                "postcall_analysis_failed",
                extra={**log_ctx, "error": str(e)},
            )
            raise

    def _build_analysis_prompt(
        self,
        transcript: str,
        additional_data: dict,
        single_prompt: bool,
    ) -> str:
        """
        Build the LLM prompt for full analysis.

        The single-prompt approach extracts call_stage, entities, and summary
        in one call (cost-optimal).  A future optimisation: if the triage
        step already classified this as a simple not_interested, skip entity
        extraction (cheaper prompt).
        """
        system_prompt = """You are a call analysis assistant. Analyze the following
call transcript and extract:
1. call_stage: The outcome/disposition of the call
2. entities: Key information mentioned (dates, times, amounts, names, preferences)
3. summary: A brief summary of what happened in the call

Respond in JSON format:
{
    "call_stage": "...",
    "entities": {...},
    "summary": "..."
}"""

        return (
            f"{system_prompt}\n\n"
            f"Transcript:\n{transcript}\n\n"
            f"Additional context:\n{json.dumps(additional_data)}"
        )

    async def _call_llm(self, prompt: str, ctx: PostCallContext) -> dict:
        """
        Call the configured LLM provider.

        In production: httpx POST with Authorization: Bearer {settings.LLM_API_KEY}.

        On 429:
            1. Read the Retry-After header (seconds to wait).
            2. Raise RateLimitExceeded(wait_seconds=retry_after).
            3. Caller re-enqueues the task to fire after wait_seconds.

        The correlation_id is passed in the request headers so the LLM
        provider's logs can be cross-referenced with ours.
        """
        # Mock — production implementation calls the real LLM API.
        # Simulate what a 429 handler would look like:
        #
        #   if resp.status_code == 429:
        #       retry_after = float(resp.headers.get("Retry-After", 60))
        #       raise RateLimitExceeded(wait_seconds=retry_after, reason="LLM 429")
        return {
            "call_stage": "unknown",
            "entities": {},
            "summary": "Mock analysis result",
            "usage": {"total_tokens": 1500},
        }

    def _parse_response(
        self, response: dict, latency_ms: float, estimated_tokens: int
    ) -> AnalysisResult:
        return AnalysisResult(
            call_stage=response.get("call_stage", "unknown"),
            entities=response.get("entities", {}),
            summary=response.get("summary", ""),
            raw_response=response,
            tokens_used=response.get("usage", {}).get("total_tokens", 0),
            estimated_tokens=estimated_tokens,
            latency_ms=latency_ms,
            provider=settings.LLM_PROVIDER,
            model=settings.LLM_MODEL,
        )

    async def _update_interaction_metadata(
        self, interaction_id: str, result: AnalysisResult
    ) -> None:
        """
        Write analysis results into the interaction_metadata JSONB column.

        In production:
            UPDATE interactions
            SET interaction_metadata = interaction_metadata || $2::jsonb,
                updated_at = NOW()
            WHERE id = $1

        Also updates analysis_tasks.status = 'completed' and tokens_used.

        Note: JSONB merge (||) means a retry APPENDS the new result alongside
        the old one — the audit_log table has every version.  The dashboard
        query should ORDER BY updated_at DESC LIMIT 1.
        """
        logger.info(
            "metadata_updated",
            extra={
                "interaction_id": interaction_id,
                "call_stage": result.call_stage,
                "tokens_used": result.tokens_used,
            },
        )


class RateLimitExceeded(Exception):
    """Raised when global or per-customer rate limits are exhausted."""
    def __init__(self, wait_seconds: float, reason: str) -> None:
        super().__init__(reason)
        self.wait_seconds = wait_seconds
        self.reason = reason


post_call_processor = PostCallProcessor()
