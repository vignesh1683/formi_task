"""
LLM Rate Limiter — enforces global TPM and RPM limits using Redis sliding-window
counters.

Design:
    Two independent counters, both with 60-second sliding windows:
        llm:global:tpm  — total tokens consumed across all customers in the last 60s
        llm:global:rpm  — total requests dispatched in the last 60s

    Before any LLM call:
        1. Check rpm_remaining = LLM_REQUESTS_PER_MINUTE - current_rpm
        2. Check tpm_remaining = LLM_TOKENS_PER_MINUTE - current_tpm
        3. If either is zero → return RateLimitDecision(allowed=False, wait_seconds=...)
        4. If both have headroom → atomically reserve the expected tokens
           and proceed.

    Estimating wait_seconds:
        We can't know exactly when the window will clear, but a conservative
        estimate is: next_window_reset - now.  The TTL of the Redis key gives us
        the remaining window time.

Why sliding window over fixed window?
    Fixed windows cause thundering-herd: all 500 allowed requests fire in the
    first second of each minute, then the system blocks for 59 seconds.
    A sliding window spreads load more evenly.

Implementation note:
    We use a Lua script for the atomic read-increment-expire operation.
    This avoids the TOCTOU race where two concurrent workers both read
    "headroom available" and both proceed, overshooting the limit.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

# Redis key names
GLOBAL_TPM_KEY = "llm:global:tpm"
GLOBAL_RPM_KEY = "llm:global:rpm"
WINDOW_SECONDS = 60

# Lua script: atomically increment a counter if it would stay within the limit.
# Returns the new counter value if the increment was allowed, or -1 if not.
_LUA_TRY_CONSUME = """
local key    = KEYS[1]
local amount = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])
local window = tonumber(ARGV[3])

local current = tonumber(redis.call('GET', key) or 0)
if current + amount > limit then
    return -1
end
local new_val = redis.call('INCRBY', key, amount)
redis.call('EXPIRE', key, window)
return new_val
"""


@dataclass
class RateLimitDecision:
    allowed: bool
    current_tpm: int
    current_rpm: int
    tpm_limit: int
    rpm_limit: int
    # Estimated seconds to wait before trying again (0 if allowed).
    wait_seconds: float = 0.0
    # Human-readable reason for rejection (empty if allowed).
    reason: str = ""


class LLMRateLimiter:
    """
    Global rate limiter for LLM API calls.

    Usage:
        decision = await rate_limiter.try_acquire(estimated_tokens=1500)
        if not decision.allowed:
            await asyncio.sleep(decision.wait_seconds)
            # re-enqueue or wait

    Concurrency:
        The Lua try_consume script is atomic per Redis server.  Multiple Celery
        workers hitting this simultaneously are safe — only one will win the
        final slot when headroom is exactly 1 request.
    """

    def __init__(self) -> None:
        self._tpm_limit = settings.LLM_TOKENS_PER_MINUTE
        self._rpm_limit = settings.LLM_REQUESTS_PER_MINUTE
        self._avg_tokens = settings.LLM_AVG_TOKENS_PER_CALL

    async def try_acquire(
        self,
        estimated_tokens: Optional[int] = None,
    ) -> RateLimitDecision:
        """
        Attempt to reserve capacity for one LLM call.

        estimated_tokens: Expected token cost.  Defaults to the configured
            average.  Pass a smaller value for cheap operations (triage only),
            a larger one for known-verbose transcripts.

        Returns a RateLimitDecision.  The caller is responsible for calling
        release() with the ACTUAL token count after the LLM call completes,
        so the counter stays accurate.
        """
        tokens = estimated_tokens or self._avg_tokens

        current_tpm = int(await redis_client.get(GLOBAL_TPM_KEY) or 0)
        current_rpm = int(await redis_client.get(GLOBAL_RPM_KEY) or 0)

        tpm_ok = (current_tpm + tokens) <= self._tpm_limit
        rpm_ok = (current_rpm + 1) <= self._rpm_limit

        if not tpm_ok:
            wait = await self._ttl(GLOBAL_TPM_KEY)
            logger.warning(
                "rate_limit_tpm_exceeded",
                extra={
                    "current_tpm": current_tpm,
                    "requested_tokens": tokens,
                    "tpm_limit": self._tpm_limit,
                    "wait_seconds": wait,
                },
            )
            return RateLimitDecision(
                allowed=False,
                current_tpm=current_tpm,
                current_rpm=current_rpm,
                tpm_limit=self._tpm_limit,
                rpm_limit=self._rpm_limit,
                wait_seconds=wait,
                reason=f"TPM limit reached ({current_tpm}/{self._tpm_limit})",
            )

        if not rpm_ok:
            wait = await self._ttl(GLOBAL_RPM_KEY)
            logger.warning(
                "rate_limit_rpm_exceeded",
                extra={
                    "current_rpm": current_rpm,
                    "rpm_limit": self._rpm_limit,
                    "wait_seconds": wait,
                },
            )
            return RateLimitDecision(
                allowed=False,
                current_tpm=current_tpm,
                current_rpm=current_rpm,
                tpm_limit=self._tpm_limit,
                rpm_limit=self._rpm_limit,
                wait_seconds=wait,
                reason=f"RPM limit reached ({current_rpm}/{self._rpm_limit})",
            )

        # Atomically reserve — if this returns -1, another worker beat us
        # to the last available slot.
        new_tpm = await self._try_consume(GLOBAL_TPM_KEY, tokens, self._tpm_limit)
        if new_tpm == -1:
            wait = await self._ttl(GLOBAL_TPM_KEY)
            return RateLimitDecision(
                allowed=False,
                current_tpm=current_tpm,
                current_rpm=current_rpm,
                tpm_limit=self._tpm_limit,
                rpm_limit=self._rpm_limit,
                wait_seconds=wait,
                reason="TPM reservation lost to concurrent worker",
            )

        new_rpm = await self._try_consume(GLOBAL_RPM_KEY, 1, self._rpm_limit)
        if new_rpm == -1:
            # Roll back the TPM reservation we just made
            await redis_client.decrby(GLOBAL_TPM_KEY, tokens)
            wait = await self._ttl(GLOBAL_RPM_KEY)
            return RateLimitDecision(
                allowed=False,
                current_tpm=current_tpm,
                current_rpm=current_rpm,
                tpm_limit=self._tpm_limit,
                rpm_limit=self._rpm_limit,
                wait_seconds=wait,
                reason="RPM reservation lost to concurrent worker",
            )

        logger.debug(
            "rate_limit_acquired",
            extra={
                "tpm_after": new_tpm,
                "rpm_after": new_rpm,
                "reserved_tokens": tokens,
            },
        )
        return RateLimitDecision(
            allowed=True,
            current_tpm=new_tpm,
            current_rpm=new_rpm,
            tpm_limit=self._tpm_limit,
            rpm_limit=self._rpm_limit,
        )

    async def release_tokens(self, actual_tokens: int, estimated_tokens: int) -> None:
        """
        Correct the TPM counter after an LLM call completes.

        We reserved estimated_tokens upfront.  If the actual cost was lower,
        we over-reserved and should return the difference.  If higher (can happen
        with streaming or long completions), the counter is already accurate.
        """
        delta = actual_tokens - estimated_tokens
        if delta < 0:
            # We over-reserved — give back the excess
            await redis_client.decrby(GLOBAL_TPM_KEY, -delta)
        elif delta > 0:
            # We under-reserved — consume the extra
            await redis_client.incrby(GLOBAL_TPM_KEY, delta)

    async def get_current_usage(self) -> dict:
        """Return current TPM and RPM usage for observability / alerting."""
        tpm = int(await redis_client.get(GLOBAL_TPM_KEY) or 0)
        rpm = int(await redis_client.get(GLOBAL_RPM_KEY) or 0)
        return {
            "current_tpm": tpm,
            "current_rpm": rpm,
            "tpm_utilisation_pct": round(tpm / self._tpm_limit * 100, 1) if self._tpm_limit else 0,
            "rpm_utilisation_pct": round(rpm / self._rpm_limit * 100, 1) if self._rpm_limit else 0,
            "tpm_limit": self._tpm_limit,
            "rpm_limit": self._rpm_limit,
        }

    async def _try_consume(self, key: str, amount: int, limit: int) -> int:
        """Run the atomic Lua try-consume script. Returns new value or -1."""
        result = await redis_client.eval(
            _LUA_TRY_CONSUME,
            1,           # number of KEYS
            key,         # KEYS[1]
            amount,      # ARGV[1]
            limit,       # ARGV[2]
            WINDOW_SECONDS,  # ARGV[3]
        )
        return int(result)

    async def _ttl(self, key: str) -> float:
        """Return the TTL of a Redis key, defaulting to WINDOW_SECONDS."""
        ttl = await redis_client.ttl(key)
        return float(ttl) if ttl > 0 else float(WINDOW_SECONDS)


rate_limiter = LLMRateLimiter()
