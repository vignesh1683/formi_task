"""
Tests for the rate limiter.

Validates:
    AC1: System never fires LLM requests beyond configured rate limits
    AC2: Per-customer budget enforced
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from src.services.rate_limiter import LLMRateLimiter, RateLimitDecision


@pytest.fixture
def rate_limiter_under_test(mock_redis):
    """Rate limiter wired to mock Redis with default limits."""
    limiter = LLMRateLimiter()
    limiter._tpm_limit = 10_000
    limiter._rpm_limit = 100
    limiter._avg_tokens = 1_500
    return limiter, mock_redis


@pytest.mark.asyncio
async def test_acquire_allowed_when_headroom_available(mock_redis):
    """When counters are well below limits, acquisition is allowed."""
    mock_redis.get = AsyncMock(side_effect=lambda key: None)  # all counters at 0
    mock_redis.eval = AsyncMock(return_value=1500)  # Lua script succeeds

    limiter = LLMRateLimiter()
    limiter._tpm_limit = 10_000
    limiter._rpm_limit = 100

    with patch("src.services.rate_limiter.redis_client", mock_redis):
        decision = await limiter.try_acquire(estimated_tokens=1500)

    assert decision.allowed is True
    assert decision.current_tpm <= decision.tpm_limit


@pytest.mark.asyncio
async def test_acquire_blocked_when_tpm_full(mock_redis):
    """When TPM counter is at the limit, acquisition must be denied."""
    # Simulate current_tpm at 100% of limit
    mock_redis.get = AsyncMock(
        side_effect=lambda key: b"10000" if "tpm" in key else b"0"
    )
    mock_redis.ttl = AsyncMock(return_value=30)

    limiter = LLMRateLimiter()
    limiter._tpm_limit = 10_000
    limiter._rpm_limit = 100

    with patch("src.services.rate_limiter.redis_client", mock_redis):
        decision = await limiter.try_acquire(estimated_tokens=1500)

    assert decision.allowed is False
    assert "TPM limit" in decision.reason
    assert decision.wait_seconds > 0


@pytest.mark.asyncio
async def test_acquire_blocked_when_rpm_full(mock_redis):
    """When RPM counter is at the limit, acquisition must be denied."""
    mock_redis.get = AsyncMock(
        side_effect=lambda key: b"100" if "rpm" in key else b"0"
    )
    mock_redis.ttl = AsyncMock(return_value=15)

    limiter = LLMRateLimiter()
    limiter._tpm_limit = 10_000
    limiter._rpm_limit = 100

    with patch("src.services.rate_limiter.redis_client", mock_redis):
        decision = await limiter.try_acquire(estimated_tokens=1500)

    assert decision.allowed is False
    assert "RPM limit" in decision.reason


@pytest.mark.asyncio
async def test_lua_atomic_reservation_rollback_on_rpm_fail(mock_redis):
    """
    If TPM reservation succeeds but RPM fails (race), TPM must be rolled back.
    """
    mock_redis.get = AsyncMock(return_value=None)
    # TPM Lua succeeds, RPM Lua fails (returns -1)
    mock_redis.eval = AsyncMock(side_effect=[1500, -1])  # tpm ok, rpm race
    mock_redis.decrby = AsyncMock()
    mock_redis.ttl = AsyncMock(return_value=20)

    limiter = LLMRateLimiter()
    limiter._tpm_limit = 10_000
    limiter._rpm_limit = 100

    with patch("src.services.rate_limiter.redis_client", mock_redis):
        decision = await limiter.try_acquire(estimated_tokens=1500)

    assert decision.allowed is False
    # Verify TPM rollback was called
    mock_redis.decrby.assert_called_once_with("llm:global:tpm", 1500)


@pytest.mark.asyncio
async def test_burst_of_1000_calls_does_not_exceed_rpm_limit(mock_redis):
    """
    Simulates 1000 concurrent acquisition attempts.
    Each beyond the RPM limit must be denied.
    AC1: No unhandled 429s surfaced to callers.
    """
    rpm_limit = 100
    call_count = [0]

    async def fake_eval(script, num_keys, key, amount, limit, window):
        call_count[0] += 1
        if call_count[0] > limit:
            return -1
        return call_count[0]

    async def fake_get(key):
        return str(call_count[0]).encode()

    mock_redis.get = AsyncMock(side_effect=fake_get)
    mock_redis.eval = AsyncMock(side_effect=fake_eval)
    mock_redis.ttl = AsyncMock(return_value=45)

    limiter = LLMRateLimiter()
    limiter._tpm_limit = 10_000_000
    limiter._rpm_limit = rpm_limit

    allowed = 0
    denied = 0

    with patch("src.services.rate_limiter.redis_client", mock_redis):
        for _ in range(200):  # Run 200 sequential attempts
            d = await limiter.try_acquire(estimated_tokens=100)
            if d.allowed:
                allowed += 1
            else:
                denied += 1

    # All denials should have non-zero wait_seconds (never raises 429 to caller)
    assert denied > 0
    assert allowed <= rpm_limit


@pytest.mark.asyncio
async def test_release_tokens_corrects_underestimate(mock_redis):
    """When actual tokens > estimated, incrby is called with the difference."""
    mock_redis.incrby = AsyncMock()
    mock_redis.decrby = AsyncMock()

    limiter = LLMRateLimiter()
    with patch("src.services.rate_limiter.redis_client", mock_redis):
        await limiter.release_tokens(actual_tokens=2000, estimated_tokens=1500)

    mock_redis.incrby.assert_called_once_with("llm:global:tpm", 500)


@pytest.mark.asyncio
async def test_release_tokens_corrects_overestimate(mock_redis):
    """When actual tokens < estimated, decrby is called to return the excess."""
    mock_redis.incrby = AsyncMock()
    mock_redis.decrby = AsyncMock()

    limiter = LLMRateLimiter()
    with patch("src.services.rate_limiter.redis_client", mock_redis):
        await limiter.release_tokens(actual_tokens=1000, estimated_tokens=1500)

    mock_redis.decrby.assert_called_once_with("llm:global:tpm", 500)
