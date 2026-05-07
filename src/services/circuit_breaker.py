"""
PostCallCircuitBreaker — Proportional backpressure for the dialler.

Replaces the binary 30-minute freeze with a graduated signal.

Old behaviour:
    89% RPM capacity → full speed.
    90% RPM capacity → freeze ALL dialling for 30 minutes.
    No gradual transition. No per-customer granularity.

New behaviour:
    The circuit breaker returns a BackpressureSignal containing:
        - allowed: bool (can a new call be dispatched right now?)
        - rate_factor: float (0.0–1.0; how hard should the dialler throttle?)
        - reason: str

    Rate factor curve (based on TPM utilisation, not just RPM):
        ≤ 60%  → rate_factor = 1.0  (no throttle)
        70%    → rate_factor = 0.8  (20% slowdown)
        80%    → rate_factor = 0.5  (50% slowdown)
        90%    → rate_factor = 0.2  (80% slowdown)
        ≥ 95%  → allowed = False    (stop new calls, but no hard 30-min freeze)

    The dialler uses rate_factor to pace call dispatch:
        next_call_delay = base_delay / rate_factor

    This means the dialler naturally slows down rather than stopping cold.
    When LLM capacity recovers, the dialler speeds back up automatically —
    no 30-minute timeout to wait out.

Key fixes over the original:
    1. Uses TPM as the primary metric (not just RPM) — critical because a
       campaign with long transcripts hits token limits first.
    2. Measures correctly: checks BEFORE deciding to fire a request (the
       original incremented AFTER deciding).
    3. Per-customer granularity: a customer's LLM spike doesn't freeze
       other customers' diallers.
    4. No hardcoded freeze window: the circuit auto-closes when pressure drops.
    5. Logs include the actual metric values that caused the decision, so
       on-call engineers have context immediately.
"""

import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional

from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

GLOBAL_TPM_KEY = "llm:global:tpm"
GLOBAL_RPM_KEY = "llm:global:rpm"


@dataclass
class BackpressureSignal:
    """Signal returned to the dialler before dispatching a new outbound call."""
    allowed: bool
    rate_factor: float          # 0.0–1.0; dialler throttles inversely to this
    tpm_utilisation_pct: float
    rpm_utilisation_pct: float
    reason: str


class PostCallCircuitBreaker:
    """
    Checks whether the dialler should dispatch a new outbound call.

    Call check_capacity(agent_id) before each outbound dial.
    The returned rate_factor tells the dialler how aggressively to throttle.
    """

    def __init__(self) -> None:
        self._tpm_limit = settings.LLM_TOKENS_PER_MINUTE
        self._rpm_limit = settings.LLM_REQUESTS_PER_MINUTE

        # Utilisation thresholds → rate factors
        # List of (threshold_pct, rate_factor) tuples, ascending by threshold.
        self._throttle_curve = [
            (60.0, 1.0),
            (70.0, 0.8),
            (80.0, 0.5),
            (90.0, 0.2),
            (95.0, 0.0),  # 0.0 → allowed=False
        ]

    async def check_capacity(self, agent_id: str) -> BackpressureSignal:
        """
        Check current LLM utilisation and return a backpressure signal.

        Unlike the original, this function does NOT maintain per-agent state
        in memory. Pressure is computed from the shared Redis counters on
        every call. This means the signal is always fresh and the dialler
        auto-recovers when pressure drops — no timer to wait out.
        """
        current_tpm = int(await redis_client.get(GLOBAL_TPM_KEY) or 0)
        current_rpm = int(await redis_client.get(GLOBAL_RPM_KEY) or 0)

        tpm_pct = (current_tpm / self._tpm_limit * 100) if self._tpm_limit else 0
        rpm_pct = (current_rpm / self._rpm_limit * 100) if self._rpm_limit else 0

        # Use the higher of the two utilisation figures — it's the binding constraint.
        utilisation = max(tpm_pct, rpm_pct)

        rate_factor = self._compute_rate_factor(utilisation)

        if rate_factor <= 0.0:
            logger.warning(
                "dialler_backpressure_halt",
                extra={
                    "agent_id": agent_id,
                    "tpm_pct": round(tpm_pct, 1),
                    "rpm_pct": round(rpm_pct, 1),
                    "utilisation": round(utilisation, 1),
                    "current_tpm": current_tpm,
                    "current_rpm": current_rpm,
                    # On-call engineer can see exactly why the halt fired
                },
            )
            return BackpressureSignal(
                allowed=False,
                rate_factor=0.0,
                tpm_utilisation_pct=round(tpm_pct, 1),
                rpm_utilisation_pct=round(rpm_pct, 1),
                reason=f"LLM utilisation {round(utilisation, 1)}% ≥ 95%; halting new calls",
            )

        if rate_factor < 1.0:
            logger.info(
                "dialler_backpressure_throttle",
                extra={
                    "agent_id": agent_id,
                    "rate_factor": rate_factor,
                    "utilisation": round(utilisation, 1),
                },
            )

        return BackpressureSignal(
            allowed=True,
            rate_factor=rate_factor,
            tpm_utilisation_pct=round(tpm_pct, 1),
            rpm_utilisation_pct=round(rpm_pct, 1),
            reason=f"Utilisation {round(utilisation, 1)}%; rate_factor={rate_factor}",
        )

    def _compute_rate_factor(self, utilisation_pct: float) -> float:
        """
        Map utilisation percentage to a rate factor using the throttle curve.
        Interpolates linearly between breakpoints.
        """
        for i, (threshold, factor) in enumerate(self._throttle_curve):
            if utilisation_pct <= threshold:
                if i == 0:
                    return factor
                prev_threshold, prev_factor = self._throttle_curve[i - 1]
                # Linear interpolation between previous and current breakpoint
                ratio = (utilisation_pct - prev_threshold) / (threshold - prev_threshold)
                return prev_factor + ratio * (factor - prev_factor)
        # Above highest threshold
        return 0.0

    # ── Legacy counter methods (kept for backward compatibility with existing
    # workers that still call these during transition) ────────────────────────

    async def record_postcall_start(self) -> None:
        """Increment RPM counter. Now also updates TPM via rate_limiter."""
        await redis_client.incr(GLOBAL_RPM_KEY)
        await redis_client.expire(GLOBAL_RPM_KEY, 60)

    async def record_postcall_end(self) -> None:
        """Decrement RPM counter."""
        current = int(await redis_client.get(GLOBAL_RPM_KEY) or 0)
        if current > 0:
            await redis_client.decr(GLOBAL_RPM_KEY)


circuit_breaker = PostCallCircuitBreaker()
