import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Set environment variables before any module imports
os.environ["ENVIRONMENT"] = "test"
os.environ["LITELLM_TELEMETRY"] = "False"
os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
os.environ["ENABLE_CHAOS_ENDPOINT"] = "true"

import asyncio
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
import fakeredis.aioredis as fake_aioredis
from app.config import settings
from app.core.provider_client import reset_provider_stub_handler
from app.core.redis_client import set_redis_client
from app.main import app
from app.resilience.chaos import chaos_manager
from app.resilience.circuit_breaker import CircuitState
from app.resilience.router import router


@pytest_asyncio.fixture(autouse=True)
async def setup_test_environment():
    """Sets up a clean fakeredis instance, test environment, and resets all gateway state before each test."""
    settings.ENABLE_CHAOS_ENDPOINT = True
    settings.ENVIRONMENT = "test"
    reset_provider_stub_handler()

    # Use fakeredis for fast, isolated, deterministic in-memory Redis testing
    fake_redis = fake_aioredis.FakeRedis(decode_responses=True)
    set_redis_client(fake_redis)

    # Reset all breakers & chaos state
    for name, breaker in router.breakers.items():
        breaker._local_state = CircuitState.CLOSED
        breaker._local_opened_at = 0.0
        breaker._local_consecutive_probes = 0
        breaker._probe_in_flight = False
    chaos_manager._local_chaos.clear()

    yield fake_redis

    reset_provider_stub_handler()
    await fake_redis.flushall()
    await fake_redis.close()


@pytest_asyncio.fixture
async def client(setup_test_environment):
    """Async HTTP client targeting the FastAPI application."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
