"""Idempotency management, atomic in-flight locking, and Redis result caching."""

import asyncio
import json
import time
from typing import Dict, Optional
import redis.asyncio as aioredis
from app.config import settings
from app.core.redis_client import get_redis_client
from app.models.schemas import ChatCompletionResponse


class IdempotencyManager:
    """
    Manages client idempotency keys with atomic in-flight locks to ensure
    concurrent duplicate requests never execute duplicate provider calls.
    """

    def __init__(self, ttl_seconds: int = settings.IDEMPOTENCY_TTL_SECONDS):
        self.ttl_seconds = ttl_seconds
        self._local_cache: Dict[str, Dict] = {}
        self._local_inflight_locks: set[str] = set()

    def _get_key(self, idempotency_key: str) -> str:
        return f"gateway:idempotency:{idempotency_key}"

    def _get_lock_key(self, idempotency_key: str) -> str:
        return f"gateway:idempotency:lock:{idempotency_key}"

    async def get_cached_response(
        self,
        idempotency_key: str,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> Optional[ChatCompletionResponse]:
        """Returns previously computed response if idempotency key was already completed."""
        if not idempotency_key:
            return None

        client = redis_client or await get_redis_client()
        key = self._get_key(idempotency_key)
        raw = await client.get(key)

        if not raw:
            # Check local fallback
            local = self._local_cache.get(idempotency_key)
            if local and local.get("expires_at", 0) > time.time():
                resp_data = local.get("response")
                if resp_data:
                    resp = ChatCompletionResponse.model_validate(resp_data)
                    if resp.gateway_metadata:
                        resp.gateway_metadata.cached_idempotent = True
                    return resp
            return None

        try:
            data = json.loads(raw)
            resp = ChatCompletionResponse.model_validate(data)
            if resp.gateway_metadata:
                resp.gateway_metadata.cached_idempotent = True
            return resp
        except Exception:
            return None

    async def acquire_in_flight_lock(
        self,
        idempotency_key: str,
        lock_ttl_seconds: int = 30,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> bool:
        """
        Atomically claims execution right for this idempotency key.
        Returns True if this caller acquired the lock, False if another concurrent caller holds it.
        """
        if not idempotency_key:
            return True

        client = redis_client or await get_redis_client()
        lock_key = self._get_lock_key(idempotency_key)
        acquired = await client.set(lock_key, "in_flight", nx=True, ex=lock_ttl_seconds)

        if acquired:
            self._local_inflight_locks.add(idempotency_key)
            return True

        # Fallback check for local
        if idempotency_key in self._local_inflight_locks:
            return False

        self._local_inflight_locks.add(idempotency_key)
        return True

    async def release_in_flight_lock(
        self,
        idempotency_key: str,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> None:
        """Releases the in-flight lock after execution concludes."""
        if not idempotency_key:
            return

        client = redis_client or await get_redis_client()
        lock_key = self._get_lock_key(idempotency_key)
        await client.delete(lock_key)
        self._local_inflight_locks.discard(idempotency_key)

    async def wait_for_in_flight_result(
        self,
        idempotency_key: str,
        timeout_seconds: float = 10.0,
        poll_interval_seconds: float = 0.05,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> Optional[ChatCompletionResponse]:
        """
        Waits for an active in-flight request on the same key to complete and store its result.
        """
        start = time.time()
        while time.time() - start < timeout_seconds:
            cached = await self.get_cached_response(idempotency_key, redis_client=redis_client)
            if cached is not None:
                return cached
            await asyncio.sleep(poll_interval_seconds)
        return None

    async def store_response(
        self,
        idempotency_key: str,
        response: ChatCompletionResponse,
        redis_client: Optional[aioredis.Redis] = None,
    ) -> None:
        """Saves completed response to Redis and releases in-flight lock."""
        if not idempotency_key:
            return

        client = redis_client or await get_redis_client()
        key = self._get_key(idempotency_key)
        serialized = response.model_dump_json()

        pipe = client.pipeline()
        pipe.set(key, serialized, ex=self.ttl_seconds)
        pipe.delete(self._get_lock_key(idempotency_key))
        await pipe.execute()

        self._local_inflight_locks.discard(idempotency_key)
        self._local_cache[idempotency_key] = {
            "response": response.model_dump(),
            "expires_at": time.time() + self.ttl_seconds,
        }


idempotency_manager = IdempotencyManager()
