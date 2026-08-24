"""Async Redis client wrapper and lifecycle management."""

from typing import Optional
import redis.asyncio as aioredis
from app.config import settings

_redis_pool: Optional[aioredis.Redis] = None


async def get_redis_client() -> aioredis.Redis:
    """Returns the global async Redis client instance."""
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            max_connections=50,
        )
    return _redis_pool


def set_redis_client(client: aioredis.Redis) -> None:
    """Allows injection of test/mock Redis client (e.g. fakeredis)."""
    global _redis_pool
    _redis_pool = client


async def close_redis_client() -> None:
    """Gracefully closes the Redis connection pool."""
    global _redis_pool
    if _redis_pool is not None:
        await _redis_pool.close()
        _redis_pool = None
