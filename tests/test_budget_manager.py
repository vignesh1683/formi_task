"""
Tests for the per-customer budget manager.

Validates:
    AC2: Customer A's budget does not consume Customer B's allocation.
"""

import pytest
from unittest.mock import AsyncMock, patch
from src.services.budget_manager import CustomerBudgetManager, CustomerQuota


def make_quota(customer_id: str, allocated_tpm: int = 2000, burst: float = 1.5) -> CustomerQuota:
    return CustomerQuota(
        customer_id=customer_id,
        allocated_tpm=allocated_tpm,
        burst_factor=burst,
    )


@pytest.mark.asyncio
async def test_customer_a_allowed_within_budget(mock_redis):
    """Customer A can process calls up to their effective limit."""
    mock_redis.get = AsyncMock(return_value=None)  # no usage yet
    mock_redis.incrby = AsyncMock(return_value=1000)
    mock_redis.expire = AsyncMock()

    mgr = CustomerBudgetManager()
    quota = make_quota("customer-a", allocated_tpm=2000, burst=1.5)

    with patch("src.services.budget_manager.redis_client", mock_redis):
        decision = await mgr.check_and_consume(
            customer_id="customer-a",
            estimated_tokens=1000,
            quota=quota,
            global_utilisation_pct=30.0,  # low global load → burst allowed
        )

    assert decision.allowed is True
    assert decision.effective_limit == int(2000 * 1.5)  # burst active


@pytest.mark.asyncio
async def test_customer_a_exhausted_does_not_affect_customer_b(mock_redis):
    """
    AC2: Exhausting Customer A's budget must not block Customer B.
    """
    # Simulate Customer A's TPM at limit
    async def fake_get(key):
        if "customer-a" in key:
            return b"3001"  # above 2000 × 1.5 = 3000 effective limit
        return b"0"

    mock_redis.get = AsyncMock(side_effect=fake_get)
    mock_redis.ttl = AsyncMock(return_value=30)
    mock_redis.incrby = AsyncMock(return_value=1500)
    mock_redis.expire = AsyncMock()

    mgr = CustomerBudgetManager()
    quota_a = make_quota("customer-a", allocated_tpm=2000, burst=1.5)
    quota_b = make_quota("customer-b", allocated_tpm=1000, burst=1.2)

    with patch("src.services.budget_manager.redis_client", mock_redis):
        # Customer A is blocked
        decision_a = await mgr.check_and_consume(
            customer_id="customer-a",
            estimated_tokens=1500,
            quota=quota_a,
            global_utilisation_pct=40.0,
        )

        # Customer B should still be allowed (request fits within their budget)
        decision_b = await mgr.check_and_consume(
            customer_id="customer-b",
            estimated_tokens=500,  # well within customer-b's 1000×1.2=1200 effective limit
            quota=quota_b,
            global_utilisation_pct=40.0,
        )

    assert decision_a.allowed is False
    assert "Customer budget reached" in decision_a.reason
    assert decision_b.allowed is True


@pytest.mark.asyncio
async def test_burst_limited_under_high_global_load(mock_redis):
    """
    When global utilisation > 70%, effective limit drops to allocated floor.
    """
    mock_redis.get = AsyncMock(return_value=b"1900")  # near floor, not burst
    mock_redis.ttl = AsyncMock(return_value=20)
    mock_redis.incrby = AsyncMock(return_value=2000)
    mock_redis.expire = AsyncMock()

    mgr = CustomerBudgetManager()
    quota = make_quota("customer-c", allocated_tpm=2000, burst=1.5)

    with patch("src.services.budget_manager.redis_client", mock_redis):
        # At 75% global utilisation, burst is disabled
        # current=1900, request=200, floor=2000 → 2100 > 2000 → blocked
        decision = await mgr.check_and_consume(
            customer_id="customer-c",
            estimated_tokens=200,
            quota=quota,
            global_utilisation_pct=75.0,  # constrained → burst disabled
        )

    assert decision.allowed is False
    assert decision.effective_limit == quota.allocated_tpm  # floor, not burst


@pytest.mark.asyncio
async def test_burst_allowed_under_low_global_load(mock_redis):
    """
    When global utilisation <= 70%, burst headroom is available.
    """
    mock_redis.get = AsyncMock(return_value=b"2500")  # above floor, within burst
    mock_redis.incrby = AsyncMock(return_value=3000)
    mock_redis.expire = AsyncMock()

    mgr = CustomerBudgetManager()
    quota = make_quota("customer-d", allocated_tpm=2000, burst=1.5)

    with patch("src.services.budget_manager.redis_client", mock_redis):
        # 50% global load → burst active; effective_limit = 3000
        # current=2500, request=499 → 2999 <= 3000 → allowed
        decision = await mgr.check_and_consume(
            customer_id="customer-d",
            estimated_tokens=499,
            quota=quota,
            global_utilisation_pct=50.0,
        )

    assert decision.allowed is True
    assert decision.effective_limit == int(2000 * 1.5)


@pytest.mark.asyncio
async def test_daily_token_limit_enforced(mock_redis):
    """A customer with a daily token cap is blocked when the cap is exceeded."""
    async def fake_get(key):
        if "daily" in key:
            return b"50000"  # daily limit reached
        return b"0"

    mock_redis.get = AsyncMock(side_effect=fake_get)
    mock_redis.ttl = AsyncMock(return_value=10)
    mock_redis.incrby = AsyncMock()
    mock_redis.expire = AsyncMock()

    mgr = CustomerBudgetManager()
    quota = CustomerQuota(
        customer_id="customer-e",
        allocated_tpm=2000,
        burst_factor=1.5,
        daily_token_limit=50_000,
    )

    with patch("src.services.budget_manager.redis_client", mock_redis):
        decision = await mgr.check_and_consume(
            customer_id="customer-e",
            estimated_tokens=1500,
            quota=quota,
            global_utilisation_pct=30.0,
        )

    assert decision.allowed is False
    assert "Daily token limit" in decision.reason
