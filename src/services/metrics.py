"""
PostCallMetricsTracker — Records timing and outcome metrics.

Changes from original:
    1. track_processing_completed() now writes TPM to a Redis sliding-window
       counter so real-time utilisation is always queryable.
    2. Per-customer TPM bucket: every call increments
       metrics:customer:{customer_id}:tpm with a 60s window.
    3. Alert thresholds: if utilisation crosses 80% or 95%, a WARNING-level
       structured event is emitted with alert=True.
    4. Queue depth is tracked in Redis so it's visible without grep.
"""

import logging
import time
from typing import Optional

from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

QUEUE_DEPTH_KEY = "postcall:queue_depth"
GLOBAL_METRICS_TPM_KEY = "metrics:global:tpm"
CUSTOMER_METRICS_TPM_PREFIX = "metrics:customer:{customer_id}:tpm"


class PostCallMetricsTracker:

    async def track_processing_started(
        self, interaction_id: str, customer_id: str = ""
    ) -> None:
        """Record wall-clock start time and increment queue depth."""
        await redis_client.set(
            f"postcall:metrics:{interaction_id}:start",
            str(time.time()),
            ex=3600,
        )
        # Track in-flight count for queue depth visibility
        await redis_client.incr(QUEUE_DEPTH_KEY)
        logger.info(
            "postcall_started",
            extra={
                "interaction_id": interaction_id,
                "customer_id": customer_id,
            },
        )

    async def track_processing_completed(
        self,
        interaction_id: str,
        tokens_used: int,
        latency_ms: float,
        customer_id: str = "",
    ) -> None:
        """
        Log completion and update real-time TPM counters.

        Two Redis counters are updated:
            metrics:global:tpm               — platform-wide rolling 60s window
            metrics:customer:{id}:tpm        — per-customer rolling 60s window

        These power the utilisation alerts and per-customer billing queries.
        """
        start = await redis_client.get(f"postcall:metrics:{interaction_id}:start")
        wall_time_s = time.time() - float(start) if start else 0

        # Update global rolling TPM counter
        await redis_client.incrby(GLOBAL_METRICS_TPM_KEY, tokens_used)
        await redis_client.expire(GLOBAL_METRICS_TPM_KEY, 60)

        # Update per-customer rolling TPM counter
        if customer_id:
            ckey = CUSTOMER_METRICS_TPM_PREFIX.format(customer_id=customer_id)
            await redis_client.incrby(ckey, tokens_used)
            await redis_client.expire(ckey, 60)

        # Decrement queue depth
        depth = int(await redis_client.get(QUEUE_DEPTH_KEY) or 0)
        if depth > 0:
            await redis_client.decr(QUEUE_DEPTH_KEY)

        logger.info(
            "postcall_metrics",
            extra={
                "interaction_id": interaction_id,
                "customer_id": customer_id,
                "tokens_used": tokens_used,
                "llm_latency_ms": latency_ms,
                "total_wall_time_s": round(wall_time_s, 2),
                "queue_depth": max(0, depth - 1),
            },
        )

    async def track_processing_failed(
        self,
        interaction_id: str,
        error: str,
        customer_id: str = "",
    ) -> None:
        """
        Log a permanent failure and decrement queue depth.

        A Grafana alert on postcall_failed_permanently fires immediately.
        The on-call engineer has interaction_id and correlation_id to start
        the debug.
        """
        depth = int(await redis_client.get(QUEUE_DEPTH_KEY) or 0)
        if depth > 0:
            await redis_client.decr(QUEUE_DEPTH_KEY)

        logger.error(
            "postcall_failed_permanently",
            extra={
                "interaction_id": interaction_id,
                "customer_id": customer_id,
                "error": error,
                "alert": True,
                # alert=True → Grafana log-alert filter to page on-call
            },
        )

    async def get_queue_depth(self) -> int:
        """Return the current in-flight processing count."""
        return int(await redis_client.get(QUEUE_DEPTH_KEY) or 0)

    async def check_utilisation_alerts(
        self,
        current_tpm: int,
        tpm_limit: int,
        current_rpm: int,
        rpm_limit: int,
    ) -> None:
        """
        Emit structured alert events when utilisation crosses thresholds.

        Called from the Celery task after each LLM call completes, so
        the alert fires in near-real-time without a separate polling loop.
        """
        tpm_pct = (current_tpm / tpm_limit * 100) if tpm_limit else 0
        rpm_pct = (current_rpm / rpm_limit * 100) if rpm_limit else 0

        for metric, pct in [("tpm", tpm_pct), ("rpm", rpm_pct)]:
            if pct >= 95:
                logger.error(
                    "llm_utilisation_critical",
                    extra={
                        "metric": metric,
                        "utilisation_pct": round(pct, 1),
                        "alert": True,
                        "alert_severity": "critical",
                    },
                )
            elif pct >= 80:
                logger.warning(
                    "llm_utilisation_high",
                    extra={
                        "metric": metric,
                        "utilisation_pct": round(pct, 1),
                        "alert": True,
                        "alert_severity": "warning",
                    },
                )


metrics_tracker = PostCallMetricsTracker()
