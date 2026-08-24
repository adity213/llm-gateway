"""Chaos injection engine for resiliency and failover testing."""

import asyncio
import json
import random
import time
from typing import Dict, Optional
import redis.asyncio as aioredis
from app.config import settings
from app.core.errors import ErrorTaxonomy, ProviderCallException
from app.core.redis_client import get_redis_client
from app.models.schemas import ChaosModeEnum, ChaosRequest, ChaosStatus


class ChaosManager:
    """Manages injected faults (failures, rate limiting, latencies) per provider."""

    def __init__(self):
        # In-memory fallback if Redis is unavailable
        self._local_chaos: Dict[str, Dict] = {}

    def _get_key(self, provider_name: str) -> str:
        return f"gateway:chaos:{provider_name}"

    async def inject_chaos(
        self,
        provider_name: str,
        chaos_req: ChaosRequest,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> ChaosStatus:
        """Sets a timed chaos configuration for a provider."""
        now = time.time()
        expires_at = now + chaos_req.duration_seconds

        payload = {
            "provider": provider_name,
            "mode": chaos_req.mode.value,
            "fail_rate": chaos_req.fail_rate,
            "add_latency_ms": chaos_req.add_latency_ms,
            "expires_at": expires_at,
        }

        client = redis_client or await get_redis_client()
        key = self._get_key(provider_name)
        await client.set(key, json.dumps(payload), ex=chaos_req.duration_seconds)
        self._local_chaos[provider_name] = payload

        return ChaosStatus(
            provider=provider_name,
            active=True,
            mode=chaos_req.mode,
            fail_rate=chaos_req.fail_rate,
            add_latency_ms=chaos_req.add_latency_ms,
            expires_at=expires_at,
            remaining_seconds=float(chaos_req.duration_seconds),
        )

    async def clear_chaos(
        self,
        provider_name: str,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> None:
        """Clears active chaos for a specific provider."""
        client = redis_client or await get_redis_client()
        key = self._get_key(provider_name)
        await client.delete(key)
        self._local_chaos.pop(provider_name, None)

    async def get_chaos_status(
        self,
        provider_name: str,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> ChaosStatus:
        """Checks if chaos is currently active for a provider."""
        client = redis_client or await get_redis_client()
        key = self._get_key(provider_name)
        raw = await client.get(key)
        now = time.time()

        if not raw:
            # Check local fallback
            local = self._local_chaos.get(provider_name)
            if local and local["expires_at"] > now:
                return ChaosStatus(
                    provider=provider_name,
                    active=True,
                    mode=ChaosModeEnum(local["mode"]),
                    fail_rate=local.get("fail_rate"),
                    add_latency_ms=local.get("add_latency_ms"),
                    expires_at=local["expires_at"],
                    remaining_seconds=round(local["expires_at"] - now, 1),
                )
            self._local_chaos.pop(provider_name, None)
            return ChaosStatus(provider=provider_name, active=False)

        try:
            data = json.loads(raw)
            exp = data.get("expires_at", 0)
            if exp <= now:
                return ChaosStatus(provider=provider_name, active=False)
            return ChaosStatus(
                provider=provider_name,
                active=True,
                mode=ChaosModeEnum(data["mode"]),
                fail_rate=data.get("fail_rate"),
                add_latency_ms=data.get("add_latency_ms"),
                expires_at=exp,
                remaining_seconds=round(exp - now, 1),
            )
        except Exception:
            return ChaosStatus(provider=provider_name, active=False)

    async def apply_chaos_if_active(self, provider_name: str) -> None:
        """
        Executes active chaos behavior (delay or injected fault exception).
        Called right before/during the provider execution.
        """
        status = await self.get_chaos_status(provider_name)
        if not status.active:
            return

        if status.mode == ChaosModeEnum.FAIL_ALL:
            raise ProviderCallException(
                provider_name=provider_name,
                message=f"[CHAOS ACTIVE] Injected total failure for provider {provider_name}",
                category=ErrorTaxonomy.SERVER_ERROR,
                status_code=500,
            )

        elif status.mode == ChaosModeEnum.FAIL_RATE:
            rate = status.fail_rate or 0.5
            if random.random() < rate:
                raise ProviderCallException(
                    provider_name=provider_name,
                    message=f"[CHAOS ACTIVE] Injected probabilistic failure ({rate*100}%) for {provider_name}",
                    category=ErrorTaxonomy.RATE_LIMIT,
                    status_code=429,
                )

        elif status.mode == ChaosModeEnum.ADD_LATENCY_MS:
            extra_ms = status.add_latency_ms or 3000
            await asyncio.sleep(extra_ms / 1000.0)


chaos_manager = ChaosManager()
