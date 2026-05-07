"""
Recording pipeline — fetches the call recording from Exotel and uploads to S3.

Replaces the 45-second sleep with a proper polling loop.

How Exotel works:
    After a call ends, Exotel processes the audio and makes a recording URL
    available via their REST API. The time between call-end and URL availability
    varies: typically 10–30 seconds, but can be 60–90s under load on their end.

    GET /v1/Accounts/{account_sid}/Calls/{call_sid}/Recording
    Returns 200 + recording_url if ready, 404 if not yet available.

    Exotel does NOT rate-limit the status polling endpoint, so checking every
    10s is fine.

New approach — poll with exponential backoff:
    Attempt  1: wait  10s  (total:  10s)
    Attempt  2: wait  15s  (total:  25s)
    Attempt  3: wait  20s  (total:  45s)
    Attempt  4: wait  30s  (total:  75s)
    Attempt  5: wait  45s  (total: 120s)
    → give up after ~120s, emit an alertable structured log event.

Key improvements over the original:
    - Recording fetch is fully decoupled from LLM analysis (parallel tasks).
    - Every failure emits a structured log at WARNING level with interaction_id,
      making Grafana alerting straightforward.
    - The interaction's recording_status in analysis_tasks is always updated
      (uploaded / failed / skipped), so ops can query for recording gaps.
    - The 404-vs-error distinction is now explicit: 404 = not ready yet
      (retry); non-404 HTTP errors = bail immediately and alert.

Note: S3 and DB writes are still mocked for local dev. In production, the
_upload_to_s3 and _update_recording_status functions call boto3 and asyncpg
respectively.
"""

import asyncio
import logging
from typing import Optional

import httpx

from src.config import settings

logger = logging.getLogger(__name__)

# Poll schedule in seconds. Each value is the sleep BEFORE that attempt.
_POLL_DELAYS = [10, 15, 20, 30, 45]


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
    correlation_id: Optional[str] = None,
) -> Optional[str]:
    """
    Poll Exotel for the recording URL and upload to S3.

    Returns the S3 key on success, None on failure.
    All failures produce a WARNING-level structured log — never silent.

    This function is designed to run concurrently with LLM analysis
    (launched via asyncio.gather in celery_tasks.py).  The recording
    has zero dependency on the LLM result, and vice versa.
    """
    log_ctx = {
        "interaction_id": interaction_id,
        "call_sid": call_sid,
        "correlation_id": correlation_id,
    }

    for attempt, delay in enumerate(_POLL_DELAYS, start=1):
        logger.info(
            "recording_poll_attempt",
            extra={**log_ctx, "attempt": attempt, "sleeping_seconds": delay},
        )
        await asyncio.sleep(delay)

        result = await _fetch_exotel_recording_url(
            call_sid, exotel_account_id, interaction_id=interaction_id
        )

        if result == "not_ready":
            # 404 from Exotel: recording not yet available, keep polling
            logger.debug(
                "recording_not_ready_yet",
                extra={**log_ctx, "attempt": attempt},
            )
            continue

        if result is None:
            # Hard error (non-404 HTTP failure, network issue, etc.)
            # Don't bother retrying — it won't get better without intervention.
            logger.warning(
                "recording_fetch_error",
                extra={
                    **log_ctx,
                    "attempt": attempt,
                    "result": "hard_error",
                    "alert": True,
                },
            )
            await _update_recording_status(interaction_id, status="failed")
            return None

        # We have a URL — upload to S3
        try:
            s3_key = await _upload_to_s3(result, interaction_id)
            logger.info(
                "recording_uploaded",
                extra={**log_ctx, "s3_key": s3_key, "attempt": attempt},
            )
            await _update_recording_status(
                interaction_id, status="uploaded", s3_key=s3_key
            )
            return s3_key

        except Exception as exc:
            logger.warning(
                "recording_s3_upload_failed",
                extra={**log_ctx, "error": str(exc), "alert": True},
            )
            await _update_recording_status(interaction_id, status="failed")
            return None

    # Exhausted all poll attempts
    logger.warning(
        "recording_poll_exhausted",
        extra={
            **log_ctx,
            "attempts": len(_POLL_DELAYS),
            "total_wait_seconds": sum(_POLL_DELAYS),
            "alert": True,
            # alert=True is a convention — a log-based Grafana alert can
            # filter on this field and page on-call when recordings go missing.
        },
    )
    await _update_recording_status(interaction_id, status="failed")
    return None


async def _fetch_exotel_recording_url(
    call_sid: str,
    account_id: str,
    interaction_id: str = "",
) -> Optional[str]:
    """
    Hit the Exotel API to get the recording URL for a completed call.

    Returns:
        str:          The recording URL if ready.
        "not_ready":  HTTP 404 — recording not yet available, poll again.
        None:         Hard error — bail out, do not retry this interaction.
    """
    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)

        if resp.status_code == 200:
            data = resp.json()
            recording_url = data.get("recording_url")
            if not recording_url:
                # 200 but no URL — treat as not-ready
                return "not_ready"
            return recording_url

        if resp.status_code == 404:
            # Not yet available — normal during the first few attempts
            return "not_ready"

        # Unexpected status code (403 auth error, 500 Exotel internal, etc.)
        logger.warning(
            "exotel_unexpected_status",
            extra={
                "interaction_id": interaction_id,
                "call_sid": call_sid,
                "status_code": resp.status_code,
            },
        )
        return None

    except httpx.TimeoutException:
        logger.warning(
            "exotel_timeout",
            extra={"interaction_id": interaction_id, "call_sid": call_sid},
        )
        return "not_ready"  # Timeout is transient — keep polling

    except httpx.HTTPError as exc:
        logger.warning(
            "exotel_http_error",
            extra={"interaction_id": interaction_id, "error": str(exc)},
        )
        return None  # Network error — bail


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Download the recording from Exotel's URL and upload to S3.

    In production:
        async with httpx.AsyncClient() as client:
            audio_bytes = await client.get(recording_url)
        s3_client.upload_fileobj(BytesIO(audio_bytes.content), S3_BUCKET, s3_key)

    Raises on failure — caller handles retry or failure logging.

    After upload: UPDATE analysis_tasks SET recording_s3_key = $1 WHERE interaction_id = $2
    Also updates the interactions.recording_s3_key column for dashboard display.

    Race condition note: if the worker crashes after the S3 upload but before
    the DB write, the file exists in S3 but the DB doesn't know about it.
    A nightly reconciliation job comparing S3 keys against analysis_tasks is
    the correct fix. Filed as a known weakness.
    """
    s3_key = f"recordings/{interaction_id}.mp3"

    logger.info(
        "recording_s3_upload",
        extra={
            "interaction_id": interaction_id,
            "s3_bucket": settings.S3_BUCKET,
            "s3_key": s3_key,
        },
    )
    # Mock: return key immediately
    return s3_key


from sqlalchemy import text
from src.utils.db import engine

async def _update_recording_status(
    interaction_id: str,
    status: str,
    s3_key: Optional[str] = None,
) -> None:
    """Write the recording outcome back to analysis_tasks."""
    query = text("""
        UPDATE analysis_tasks 
        SET recording_status = :status, recording_s3_key = :key, updated_at = NOW()
        WHERE interaction_id = :iid
    """)
    async with engine.begin() as conn:
        await conn.execute(query, {
            "status": status, "key": s3_key, "iid": interaction_id
        })
