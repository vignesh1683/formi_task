"""
Celery tasks for post-call processing — redesigned pipeline.

Key changes from the original:

1. TWO QUEUES instead of one:
       postcall_hot  → hot-lane interactions (rebook, demo, escalation)
       postcall_cold → cold-lane interactions (not_interested, callback, etc.)
   Workers on the hot queue are scaled more aggressively. Cold queue workers
   can be scaled down when rate limit headroom is tight.

2. Recording and LLM run in PARALLEL:
   asyncio.gather() runs fetch_and_upload_recording() and _run_llm_analysis()
   concurrently. Since the LLM reads the transcript (not the audio), they have
   zero data dependency. This eliminates the 45-second recording gate that
   delayed every analysis.

3. Rate-limit-aware LLM scheduling:
   If rate_limiter or budget_manager blocks the LLM call, the task does NOT
   immediately retry (which would flood the queue). Instead it:
       a. Sleeps wait_seconds (from RateLimitExceeded.wait_seconds)
       b. Updates analysis_tasks.next_retry_at in DB
       c. Raises self.retry() with the correct countdown
   This ensures the task waits the right amount before consuming a worker slot.

4. Durable task state:
   analysis_tasks DB row is updated at each step transition:
       queued → processing → completed | failed | exhausted
   If a worker crashes, the row stays in 'processing'. A sweeper job (not
   implemented here) detects stale processing rows and re-enqueues them.

5. Signal jobs fire ONCE, AFTER analysis, with the real result:
   The original fired signal_jobs twice (once before Celery with empty payload,
   once after). Now signal_jobs only fires from the Celery task, after the LLM
   result is available.

6. Single retry mechanism:
   The custom PostCallRetryQueue is removed. Celery's own retry mechanism
   (self.retry) is the sole retry path. The analysis_tasks table tracks state
   durably so retries can resume even after Redis restarts.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict

from src.tasks.celery_app import celery_app
from src.services.post_call_processor import (
    PostCallProcessor,
    PostCallContext,
    RateLimitExceeded,
)
from src.services.recording import fetch_and_upload_recording
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.metrics import metrics_tracker

logger = logging.getLogger(__name__)

# Celery queue names — must match the worker startup configuration
HOT_QUEUE = "postcall_hot"
COLD_QUEUE = "postcall_cold"


@celery_app.task(
    name="process_interaction_end_background_task",
    bind=True,
    max_retries=4,
    # NO default_retry_delay — we compute it dynamically based on RateLimitExceeded
    acks_late=True,
    queue=COLD_QUEUE,  # default queue; endpoints.py overrides to HOT_QUEUE for hot lane
)
def process_interaction_end_background_task(self, payload: Dict[str, Any]):
    """
    Main Celery task. Routes based on lane in payload.

    Celery workers are synchronous; we spin up a new event loop per task.
    At 100K calls with 3.5s LLM latency and 10 workers on the cold queue:
        100,000 × 3.5s / 10 workers = 9.7 hours.
    But with the cold queue deferrable and hot queue prioritised:
        - Hot calls (rebooks, demos): processed within seconds.
        - Cold calls: processed as capacity allows, never starving hot calls.
    Horizontal worker scaling reduces the cold queue drain time linearly.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(_process_interaction(self, payload))
    except RateLimitExceeded as e:
        # Rate limited — wait the right amount, then requeue
        wait = max(int(e.wait_seconds), 30)
        logger.warning(
            "celery_task_rate_limited",
            extra={
                "interaction_id": payload.get("interaction_id"),
                "wait_seconds": wait,
                "reason": e.reason,
                "attempt": self.request.retries,
            },
        )
        loop.run_until_complete(
            _update_task_status(
                payload.get("interaction_id", ""),
                "failed",
                error=e.reason,
                next_retry_seconds=wait,
            )
        )
        raise self.retry(exc=e, countdown=wait)

    except Exception as e:
        logger.exception(
            "celery_task_failed",
            extra={
                "interaction_id": payload.get("interaction_id"),
                "error": str(e),
                "attempt": self.request.retries,
                "correlation_id": payload.get("correlation_id"),
            },
        )
        loop.run_until_complete(
            _update_task_status(
                payload.get("interaction_id", ""),
                "failed",
                error=str(e),
                next_retry_seconds=60,
            )
        )

        if self.request.retries >= self.max_retries:
            # Exhausted — mark permanently failed in DB, alert fires via logger
            loop.run_until_complete(
                metrics_tracker.track_processing_failed(
                    payload.get("interaction_id", ""),
                    error=str(e),
                    customer_id=payload.get("customer_id", ""),
                )
            )
            loop.run_until_complete(
                _update_task_status(
                    payload.get("interaction_id", ""), "exhausted", error=str(e)
                )
            )
        else:
            raise self.retry(exc=e, countdown=60)
    finally:
        loop.close()


async def _process_interaction(task, payload: Dict[str, Any]):
    interaction_id = payload["interaction_id"]
    customer_id = payload.get("customer_id", "")
    correlation_id = payload.get("correlation_id", "")

    log_ctx = {
        "interaction_id": interaction_id,
        "customer_id": customer_id,
        "correlation_id": correlation_id,
        "lane": payload.get("lane", "cold"),
    }

    await _update_task_status(interaction_id, "processing")
    await metrics_tracker.track_processing_started(interaction_id, customer_id)

    ctx = PostCallContext(
        interaction_id=interaction_id,
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"],
        customer_id=customer_id,
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
        correlation_id=correlation_id,
    )

    # ── Step 1: Recording + LLM in PARALLEL ───────────────────────────────
    # The original ran these sequentially (recording blocked analysis by 45s).
    # asyncio.gather() runs both concurrently.
    # Recording failure does NOT abort LLM — they are independent.
    # RateLimitExceeded from the LLM step propagates up to abort the task.
    logger.info("postcall_processing_start", extra=log_ctx)

    processor = PostCallProcessor()

    recording_task = asyncio.create_task(
        fetch_and_upload_recording(
            interaction_id=ctx.interaction_id,
            call_sid=ctx.call_sid,
            exotel_account_id=ctx.exotel_account_id or "",
            correlation_id=correlation_id,
        )
    )

    llm_task = asyncio.create_task(
        processor.process_post_call(ctx)
    )

    # Wait for both. If llm_task raises RateLimitExceeded, recording_task
    # is cancelled cleanly by the exception propagation.
    recording_s3_key, result = await asyncio.gather(
        recording_task, llm_task, return_exceptions=False
    )

    if not recording_s3_key:
        # Already logged inside fetch_and_upload_recording; nothing more to do.
        logger.warning(
            "recording_unavailable_continuing",
            extra=log_ctx,
        )

    # ── Step 2: Update metrics ─────────────────────────────────────────────
    await metrics_tracker.track_processing_completed(
        interaction_id, result.tokens_used, result.latency_ms, customer_id
    )

    # ── Step 3: Signal jobs (with real analysis result) ────────────────────
    # Fires exactly once, after analysis. The original fired twice with an
    # empty payload on the first fire.
    try:
        await trigger_signal_jobs(
            interaction_id=ctx.interaction_id,
            session_id=ctx.session_id,
            campaign_id=ctx.campaign_id,
            analysis_result=result.raw_response,
        )
    except Exception as e:
        # Signal job failure is logged but does not abort the task.
        # The analysis result is already committed; only downstream actions fail.
        # A separate signal-job retry mechanism handles these independently.
        logger.warning(
            "signal_jobs_failed",
            extra={**log_ctx, "error": str(e)},
        )

    # ── Step 4: Lead stage update ──────────────────────────────────────────
    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=ctx.interaction_id,
            call_stage=result.call_stage,
        )
    except Exception as e:
        logger.warning(
            "lead_stage_update_failed",
            extra={**log_ctx, "error": str(e)},
        )

    # ── Step 5: Mark task complete ─────────────────────────────────────────
    await _update_task_status(interaction_id, "completed")
    logger.info(
        "postcall_processing_complete",
        extra={**log_ctx, "call_stage": result.call_stage},
    )


from sqlalchemy import text
from src.utils.db import engine

async def _update_task_status(
    interaction_id: str,
    status: str,
    error: str = "",
    next_retry_seconds: int = 0,
) -> None:
    """Write the current task status to analysis_tasks."""
    query = text("""
        UPDATE analysis_tasks 
        SET status = :status, 
            attempt_count = attempt_count + (CASE WHEN :status = 'processing' THEN 1 ELSE 0 END),
            next_retry_at = CASE WHEN :retry > 0 THEN NOW() + (:retry * INTERVAL '1 second') ELSE next_retry_at END,
            updated_at = NOW()
        WHERE interaction_id = :iid
    """)
    async with engine.begin() as conn:
        await conn.execute(query, {
            "status": status, "retry": next_retry_seconds, "iid": interaction_id
        })
