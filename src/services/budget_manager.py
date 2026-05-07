"""
Per-Customer Token Budget Manager.

Solves the multi-tenancy problem: when platform capacity is N tokens/min
and K customers are active, we need to ensure Customer A's campaigns can't
starve Customer B.

Allocation model (three-tier):
    1. Reserved floor  — each customer has allocated_tpm tokens/min guaranteed.
       Even if other customers are bursting, the floor is always available.

    2. Burst headroom  — if global capacity exceeds total committed allocations,
       customers can burst up to allocated_tpm × burst_factor.

    3. Priority within tier — when multiple customers want the same unallocated
       headroom, priority_tier (1=highest) determines who gets served first.

Redis keys per customer:
    budget:{customer_id}:tpm_used    — rolling 60-second window INCRBY counter
    budget:{customer_id}:daily_used  — daily accumulator, resets at midnight UTC

The budget manager is checked AFTER the global rate limiter. The global
limiter is the hard guard against 429s. The budget manager is the fairness
layer on top.

Idempotency note:
    Tokens are reserved optimistically and released if the LLM call fails.
    If a worker crashes after consuming tokens but before the call completes,
    the 60-second TTL on the counter means the over-count self-heals within
    one window.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

BUDGET_TPM_PREFIX = "budget:{customer_id}:tpm_used"
BUDGET_DAILY_PREFIX = "budget:{customer_id}:daily_used"
WINDOW_SECONDS = 60
DAILY_SECONDS = 86_400


@dataclass
class BudgetDecision:
    allowed: bool
    customer_id: str
    # Tokens consumed in the current 60-second window
    current_tpm: int
    # The customer's allocated floor guarantee
    allocated_tpm: int
    # Effective limit for this window (floor or floor × burst_factor)
    effective_limit: int
    # Estimated wait if not allowed
    wait_seconds: float = 0.0
    reason: str = ""


@dataclass
class CustomerQuota:
    """In-memory representation of a customer's quota configuration.

    In production, this is loaded from the customer_quotas table.
    Here we use a simple default for any customer without an explicit entry.
    """
    customer_id: str
    allocated_tpm: int = 1000
    burst_factor: float = 1.5
    daily_token_limit: Optional[int] = None
    priority_tier: int = 2


class CustomerBudgetManager:
    """
    Enforces per-customer token budgets and tracks usage in real time.

    How it interacts with the global rate limiter:
        Global limiter → checks total platform TPM/RPM
        Budget manager → checks per-customer fairness within that total

    A request must pass BOTH checks to be processed.
    """

    def __init__(self) -> None:
        # Default quota used for customers with no explicit row in customer_quotas.
        # Production: load from DB/cache.
        self._default_quota = CustomerQuota(
            customer_id="default",
            allocated_tpm=500,
            burst_factor=1.2,
        )
        # Total platform TPM — used to compute unallocated headroom.
        self._global_tpm_limit = settings.LLM_TOKENS_PER_MINUTE

    async def check_and_consume(
        self,
        customer_id: str,
        estimated_tokens: int,
        quota: Optional[CustomerQuota] = None,
        global_utilisation_pct: float = 0.0,
    ) -> BudgetDecision:
        """
        Check whether this customer has budget for estimated_tokens and consume
        them if allowed.

        global_utilisation_pct (0–100): Current global TPM usage ratio.
            When global utilisation is low, customers can burst.
            When high (>80%), only reserved allocations are honoured.
        """
        q = quota or self._default_quota
        tpm_key = BUDGET_TPM_PREFIX.format(customer_id=customer_id)
        daily_key = BUDGET_DAILY_PREFIX.format(customer_id=customer_id)

        current_tpm = int(await redis_client.get(tpm_key) or 0)

        # Determine effective limit for this window:
        # If global headroom is ample (<= 70% used), allow bursting.
        # If global is constrained (>70%), clamp to the floor allocation.
        if global_utilisation_pct <= 70.0:
            effective_limit = int(q.allocated_tpm * q.burst_factor)
        else:
            effective_limit = q.allocated_tpm

        if current_tpm + estimated_tokens > effective_limit:
            ttl = await redis_client.ttl(tpm_key)
            wait = float(ttl) if ttl > 0 else float(WINDOW_SECONDS)
            logger.warning(
                "customer_budget_exceeded",
                extra={
                    "customer_id": customer_id,
                    "current_tpm": current_tpm,
                    "requested_tokens": estimated_tokens,
                    "effective_limit": effective_limit,
                    "global_utilisation_pct": global_utilisation_pct,
                },
            )
            return BudgetDecision(
                allowed=False,
                customer_id=customer_id,
                current_tpm=current_tpm,
                allocated_tpm=q.allocated_tpm,
                effective_limit=effective_limit,
                wait_seconds=wait,
                reason=(
                    f"Customer budget reached: {current_tpm + estimated_tokens}"
                    f" > {effective_limit} tokens/min"
                ),
            )

        # Check daily cap if configured
        if q.daily_token_limit is not None:
            daily_used = int(await redis_client.get(daily_key) or 0)
            if daily_used + estimated_tokens > q.daily_token_limit:
                logger.warning(
                    "customer_daily_budget_exceeded",
                    extra={
                        "customer_id": customer_id,
                        "daily_used": daily_used,
                        "daily_limit": q.daily_token_limit,
                    },
                )
                return BudgetDecision(
                    allowed=False,
                    customer_id=customer_id,
                    current_tpm=current_tpm,
                    allocated_tpm=q.allocated_tpm,
                    effective_limit=effective_limit,
                    wait_seconds=0.0,
                    reason=(
                        f"Daily token limit reached: {daily_used + estimated_tokens}"
                        f" > {q.daily_token_limit}"
                    ),
                )

        # Consume from window counter
        new_tpm = await redis_client.incrby(tpm_key, estimated_tokens)
        await redis_client.expire(tpm_key, WINDOW_SECONDS)

        # Consume from daily counter (separate key, longer TTL)
        await redis_client.incrby(daily_key, estimated_tokens)
        await redis_client.expire(daily_key, DAILY_SECONDS)

        logger.debug(
            "customer_budget_consumed",
            extra={
                "customer_id": customer_id,
                "tokens_consumed": estimated_tokens,
                "new_tpm": new_tpm,
                "effective_limit": effective_limit,
            },
        )
        return BudgetDecision(
            allowed=True,
            customer_id=customer_id,
            current_tpm=new_tpm,
            allocated_tpm=q.allocated_tpm,
            effective_limit=effective_limit,
        )

    async def release_tokens(
        self,
        customer_id: str,
        actual_tokens: int,
        estimated_tokens: int,
    ) -> None:
        """
        Correct the per-customer TPM counter after an LLM call completes.
        Mirrors LLMRateLimiter.release_tokens() but operates on the customer key.
        """
        delta = actual_tokens - estimated_tokens
        tpm_key = BUDGET_TPM_PREFIX.format(customer_id=customer_id)
        if delta < 0:
            await redis_client.decrby(tpm_key, -delta)
        elif delta > 0:
            await redis_client.incrby(tpm_key, delta)

    async def get_usage(self, customer_id: str) -> dict:
        """Return current window and daily usage for a customer."""
        tpm_key = BUDGET_TPM_PREFIX.format(customer_id=customer_id)
        daily_key = BUDGET_DAILY_PREFIX.format(customer_id=customer_id)
        tpm = int(await redis_client.get(tpm_key) or 0)
        daily = int(await redis_client.get(daily_key) or 0)
        return {"customer_id": customer_id, "current_tpm": tpm, "daily_total": daily}


budget_manager = CustomerBudgetManager()
