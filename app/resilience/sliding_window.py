"""Redis-backed Sliding Window for provider health tracking."""

import json
import time
import uuid
from typing import Dict, List, Optional
from pydantic import BaseModel
import redis.asyncio as aioredis
from app.config import settings
from app.core.errors import ErrorTaxonomy
from app.core.redis_client import get_redis_client


class WindowMetrics(BaseModel):
    provider: str
    total_samples: int
    successful_samples: int
    failed_samples: int
    success_rate: float
    failure_rate: float
    error_counts: Dict[str, int]
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    window_duration_seconds: int


class SlidingWindowHealthTracker:
    """Tracks provider request outcomes over a rolling time window using Redis sorted sets."""

    def __init__(self, window_seconds: Optional[int] = None):
        self.window_seconds = window_seconds or settings.HEALTH_WINDOW_SECONDS

    def _get_key(self, provider_name: str) -> str:
        return f"gateway:health:window:{provider_name}"

    async def record_outcome(
        self,
        provider_name: str,
        success: bool,
        latency_ms: float,
        error_category: Optional[ErrorTaxonomy] = None,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> None:
        """Records a request outcome to the provider's sliding window."""
        client = redis_client or await get_redis_client()
        now = time.time()
        key = self._get_key(provider_name)

        payload = {
            "id": str(uuid.uuid4()),
            "ts": now,
            "success": success,
            "latency_ms": latency_ms,
            "error_category": error_category.value if error_category else None,
        }

        # Pipeline: Add new entry, remove expired entries, update key TTL
        pipe = client.pipeline()
        pipe.zadd(key, {json.dumps(payload): now})
        min_ts = now - self.window_seconds
        pipe.zremrangebyscore(key, "-inf", min_ts)
        pipe.expire(key, self.window_seconds * 2)
        await pipe.execute()

    async def get_metrics(
        self,
        provider_name: str,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> WindowMetrics:
        """Computes rolling health and latency metrics from the current sliding window."""
        client = redis_client or await get_redis_client()
        now = time.time()
        key = self._get_key(provider_name)
        min_ts = now - self.window_seconds

        # Clean expired and fetch remaining records
        pipe = client.pipeline()
        pipe.zremrangebyscore(key, "-inf", min_ts)
        pipe.zrangebyscore(key, min_ts, "+inf")
        results = await pipe.execute()
        raw_items: List[str] = results[1] if len(results) > 1 else []

        total_samples = len(raw_items)
        if total_samples == 0:
            return WindowMetrics(
                provider=provider_name,
                total_samples=0,
                successful_samples=0,
                failed_samples=0,
                success_rate=1.0,  # Default healthy when idle
                failure_rate=0.0,
                error_counts={cat.value: 0 for cat in ErrorTaxonomy},
                p50_latency_ms=0.0,
                p95_latency_ms=0.0,
                p99_latency_ms=0.0,
                window_duration_seconds=self.window_seconds,
            )

        successful_samples = 0
        failed_samples = 0
        error_counts: Dict[str, int] = {cat.value: 0 for cat in ErrorTaxonomy}
        latencies: List[float] = []

        for item_str in raw_items:
            try:
                item = json.loads(item_str)
                if item.get("success", False):
                    successful_samples += 1
                else:
                    failed_samples += 1
                    cat = item.get("error_category")
                    if cat in error_counts:
                        error_counts[cat] += 1
                    else:
                        error_counts[ErrorTaxonomy.UNKNOWN.value] += 1

                lat = item.get("latency_ms", 0.0)
                latencies.append(lat)
            except Exception:
                continue

        failure_rate = (failed_samples / total_samples) if total_samples > 0 else 0.0
        success_rate = (successful_samples / total_samples) if total_samples > 0 else 1.0

        latencies.sort()
        p50 = self._calculate_percentile(latencies, 50)
        p95 = self._calculate_percentile(latencies, 95)
        p99 = self._calculate_percentile(latencies, 99)

        return WindowMetrics(
            provider=provider_name,
            total_samples=total_samples,
            successful_samples=successful_samples,
            failed_samples=failed_samples,
            success_rate=round(success_rate, 4),
            failure_rate=round(failure_rate, 4),
            error_counts=error_counts,
            p50_latency_ms=round(p50, 2),
            p95_latency_ms=round(p95, 2),
            p99_latency_ms=round(p99, 2),
            window_duration_seconds=self.window_seconds,
        )

    @staticmethod
    def _calculate_percentile(sorted_list: List[float], percentile: int) -> float:
        if not sorted_list:
            return 0.0
        k = (len(sorted_list) - 1) * (percentile / 100.0)
        f = int(k)
        c = f + 1
        if c < len(sorted_list):
            d0 = sorted_list[f] * (c - k)
            d1 = sorted_list[c] * (k - f)
            return d0 + d1
        return sorted_list[-1]
