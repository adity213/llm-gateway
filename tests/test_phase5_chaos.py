"""Tests for Phase 5: Chaos endpoint fault injection and expiration."""

import asyncio
import time
import pytest
from httpx import AsyncClient
from app.config import settings
from app.resilience.circuit_breaker import CircuitState
from app.resilience.router import router


@pytest.mark.asyncio
async def test_chaos_fail_all_injection_and_expiration(client: AsyncClient):
    """
    Verifies that POST /chaos/{provider} with fail_all forces failure on that provider,
    and deleting/expiring chaos restores normal operation.
    """
    settings.ENABLE_CHAOS_ENDPOINT = True
    provider = "mock-provider-a"
    router.breakers[provider]._local_state = CircuitState.CLOSED

    # 1. Inject chaos for 2 seconds
    chaos_payload = {
        "mode": "fail_all",
        "duration_seconds": 2,
    }
    set_resp = await client.post(f"/chaos/{provider}", json=chaos_payload)
    assert set_resp.status_code == 200
    assert set_resp.json()["active"] is True

    # 2. Get chaos status
    status_resp = await client.get(f"/chaos/{provider}")
    assert status_resp.status_code == 200
    assert status_resp.json()["mode"] == "fail_all"

    # 3. Clear chaos manually
    del_resp = await client.delete(f"/chaos/{provider}")
    assert del_resp.status_code == 200

    status_resp2 = await client.get(f"/chaos/{provider}")
    assert status_resp2.json()["active"] is False


@pytest.mark.asyncio
async def test_chaos_endpoint_gated_by_env_flag(client: AsyncClient):
    """Verifies that chaos endpoints return 403 Forbidden when ENABLE_CHAOS_ENDPOINT is False."""
    settings.ENABLE_CHAOS_ENDPOINT = False

    resp = await client.post("/chaos/mock-provider-a", json={"mode": "fail_all", "duration_seconds": 30})
    assert resp.status_code == 403
    assert "Chaos endpoint is disabled" in resp.json()["detail"]

    # Reset flag for other tests
    settings.ENABLE_CHAOS_ENDPOINT = True
