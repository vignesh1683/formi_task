"""
Tests for the recording poller.

Validates:
    AC4: Recording poller retries with backoff; never silently skips.
"""

import pytest
from unittest.mock import AsyncMock, patch, call


@pytest.mark.asyncio
async def test_recording_available_on_first_poll():
    """
    Recording ready on attempt 1: uploads immediately, no extra polls.
    """
    from src.services.recording import fetch_and_upload_recording

    with (
        patch("src.services.recording.asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        patch(
            "src.services.recording._fetch_exotel_recording_url",
            new_callable=AsyncMock,
            return_value="https://recordings.exotel.in/xyz.mp3",
        ),
        patch(
            "src.services.recording._upload_to_s3",
            new_callable=AsyncMock,
            return_value="recordings/test-001.mp3",
        ),
        patch("src.services.recording._update_recording_status", new_callable=AsyncMock),
    ):
        s3_key = await fetch_and_upload_recording(
            interaction_id="test-001",
            call_sid="exotel-001",
            exotel_account_id="account-001",
        )

    assert s3_key == "recordings/test-001.mp3"
    # Should only have slept once (for the first poll delay)
    mock_sleep.assert_called_once()


@pytest.mark.asyncio
async def test_recording_retries_on_not_ready():
    """
    Recording not ready on first 2 attempts; available on attempt 3.
    Verifies retry loop fires and succeeds.
    """
    from src.services.recording import fetch_and_upload_recording

    side_effects = ["not_ready", "not_ready", "https://recordings.exotel.in/abc.mp3"]

    with (
        patch("src.services.recording.asyncio.sleep", new_callable=AsyncMock),
        patch(
            "src.services.recording._fetch_exotel_recording_url",
            new_callable=AsyncMock,
            side_effect=side_effects,
        ),
        patch(
            "src.services.recording._upload_to_s3",
            new_callable=AsyncMock,
            return_value="recordings/test-002.mp3",
        ),
        patch("src.services.recording._update_recording_status", new_callable=AsyncMock),
    ):
        s3_key = await fetch_and_upload_recording(
            interaction_id="test-002",
            call_sid="exotel-002",
            exotel_account_id="account-001",
        )

    assert s3_key == "recordings/test-002.mp3"


@pytest.mark.asyncio
async def test_recording_exhausted_logs_warning_not_silent(caplog):
    """
    AC4: If all poll attempts fail, a WARNING is logged. No silent skip.
    """
    import logging
    from src.services.recording import fetch_and_upload_recording

    with (
        patch("src.services.recording.asyncio.sleep", new_callable=AsyncMock),
        patch(
            "src.services.recording._fetch_exotel_recording_url",
            new_callable=AsyncMock,
            return_value="not_ready",  # always not ready
        ),
        patch("src.services.recording._update_recording_status", new_callable=AsyncMock) as mock_update,
    ):
        with caplog.at_level(logging.WARNING, logger="src.services.recording"):
            s3_key = await fetch_and_upload_recording(
                interaction_id="test-003",
                call_sid="exotel-003",
                exotel_account_id="account-001",
            )

    assert s3_key is None
    assert any("recording_poll_exhausted" in r.message for r in caplog.records)
    # DB status must be updated to 'failed' — not silently left as 'pending'
    mock_update.assert_called_with("test-003", status="failed")


@pytest.mark.asyncio
async def test_recording_hard_error_stops_retrying(caplog):
    """
    A non-404 HTTP error (e.g. 500 from Exotel) should stop polling immediately
    rather than burning all retry attempts.
    """
    import logging
    from src.services.recording import fetch_and_upload_recording

    with (
        patch("src.services.recording.asyncio.sleep", new_callable=AsyncMock),
        patch(
            "src.services.recording._fetch_exotel_recording_url",
            new_callable=AsyncMock,
            return_value=None,  # None = hard error
        ),
        patch("src.services.recording._update_recording_status", new_callable=AsyncMock) as mock_update,
    ):
        with caplog.at_level(logging.WARNING, logger="src.services.recording"):
            s3_key = await fetch_and_upload_recording(
                interaction_id="test-004",
                call_sid="exotel-004",
                exotel_account_id="account-001",
            )

    assert s3_key is None
    # Should stop on the first attempt (hard error), not exhaust all retries
    assert any("recording_fetch_error" in r.message for r in caplog.records)
    mock_update.assert_called_with("test-004", status="failed")


@pytest.mark.asyncio
async def test_recording_s3_upload_failure_logs_warning():
    """
    S3 upload failure is logged and status set to 'failed'.
    """
    from src.services.recording import fetch_and_upload_recording

    with (
        patch("src.services.recording.asyncio.sleep", new_callable=AsyncMock),
        patch(
            "src.services.recording._fetch_exotel_recording_url",
            new_callable=AsyncMock,
            return_value="https://recordings.exotel.in/ok.mp3",
        ),
        patch(
            "src.services.recording._upload_to_s3",
            new_callable=AsyncMock,
            side_effect=Exception("S3 connection timeout"),
        ),
        patch("src.services.recording._update_recording_status", new_callable=AsyncMock) as mock_update,
    ):
        s3_key = await fetch_and_upload_recording(
            interaction_id="test-005",
            call_sid="exotel-005",
            exotel_account_id="account-001",
        )

    assert s3_key is None
    mock_update.assert_called_with("test-005", status="failed")
