"""Tests for Phase 4: Queueing, backoff, and idempotency."""

import asyncio
import time
import pytest
from httpx import AsyncClient
from app.models.schemas import ChatCompletionRequest, PriorityEnum
from app.queuing.idempotency import idempotency_manager
from app.queuing.queue_manager import queue_manager
from app.queuing.worker import queue_worker
from app.resilience.circuit_breaker import CircuitState
from app.resilience.router import router


@pytest.mark.asyncio
async def test_deferrable_missing_idempotency_key_returns_400(client: AsyncClient):
    """Verifies that deferrable requests without idempotency_key are rejected with 400."""
    payload = {
        "messages": [{"role": "user", "content": "Process report"}],
        "tenant": "tenant_1",
        "feature": "batch_report",
        "priority": "deferrable",
        # missing idempotency_key
    }
    response = await client.post("/v1/chat/completions", json=payload)
    assert response.status_code == 400
    assert "idempotency_key is required for deferrable requests" in response.json()["detail"]


@pytest.mark.asyncio
async def test_interactive_fail_fast_on_total_outage(client: AsyncClient):
    """
    When all providers have OPEN breakers:
    - Interactive request must fail fast with 503 and Retry-After header.
    """
    # Force all breakers OPEN
    for breaker in router.breakers.values():
        breaker._local_state = CircuitState.OPEN
        breaker._local_opened_at = time.time()

    start_time = time.time()
    payload = {
        "messages": [{"role": "user", "content": "Hello"}],
        "tenant": "tenant_1",
        "feature": "search",
        "priority": "interactive",
        "request_class": "default",
    }
    response = await client.post("/v1/chat/completions", json=payload)
    elapsed_ms = (time.time() - start_time) * 1000

    assert response.status_code == 503
    assert elapsed_ms < 100  # Fails fast in milliseconds without hanging
    assert response.headers.get("retry-after") == "30"
    assert "unavailable" in response.json()["error"]["message"].lower()


@pytest.mark.asyncio
async def test_deferrable_queues_on_outage_and_heals_on_recovery(client: AsyncClient, setup_test_environment):
    """
    When all providers are OPEN:
    - Deferrable request returns 202 Accepted with task_id.
    - When a provider is healed, the worker processes the queued task to completion.
    """
    fake_redis = setup_test_environment

    # Force all breakers OPEN
    for breaker in router.breakers.values():
        breaker._local_state = CircuitState.OPEN
        breaker._local_opened_at = time.time()

    idemp_key = f"idemp-batch-{int(time.time()*1000)}"
    payload = {
        "messages": [{"role": "user", "content": "Heavy async task"}],
        "tenant": "tenant_enterprise",
        "feature": "document_indexer",
        "priority": "deferrable",
        "idempotency_key": idemp_key,
        "request_class": "mock_testing",
    }

    # 1. Submit request during outage -> 202 Accepted
    response = await client.post("/v1/chat/completions", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "queued"
    task_id = data["task_id"]

    # 2. Check task status endpoint -> returns queued
    task_resp = await client.get(f"/v1/tasks/{task_id}")
    assert task_resp.status_code == 200
    assert task_resp.json()["status"] == "queued"

    # 3. Heal mock-provider-a breaker
    router.breakers["mock-provider-a"]._local_state = CircuitState.CLOSED
    await router.breakers["mock-provider-a"].reset(fake_redis)

    # 4. Worker processes queue
    processed = await queue_worker.process_batch(batch_size=5)
    assert processed >= 1

    # 5. Check task status again -> must be completed
    task_resp = await client.get(f"/v1/tasks/{task_id}")
    assert task_resp.status_code == 200
    task_data = task_resp.json()
    assert task_data["status"] == "completed"
    assert task_data["result"] is not None

    # 6. Replay exact same request with same idempotency key -> returns cached result without provider call
    replay_resp = await client.post("/v1/chat/completions", json=payload)
    assert replay_resp.status_code == 200
    replay_data = replay_resp.json()
    assert replay_data["gateway_metadata"]["cached_idempotent"] is True


@pytest.mark.asyncio
async def test_concurrent_idempotent_requests_race_condition(client: AsyncClient):
    """
    Explicit test for race condition: Concurrent duplicate requests racing on the same idempotency key.
    Verifies that all concurrent callers receive valid responses, and subsequent calls return cached result.
    """
    shared_key = f"concurrent-idemp-{int(time.time()*1000)}"
    payload = {
        "messages": [{"role": "user", "content": "Concurrent test prompt"}],
        "tenant": "tenant_race",
        "feature": "concurrency_test",
        "priority": "interactive",
        "idempotency_key": shared_key,
        "request_class": "mock_testing",
    }

    # Reset breaker for mock provider
    router.breakers["mock-provider-a"]._local_state = CircuitState.CLOSED

    # Track provider executions via test stub
    from app.core.provider_client import set_provider_stub_handler, _create_mock_response
    call_counter = 0

    async def counting_stub(config, messages, temp, max_toks):
        nonlocal call_counter
        call_counter += 1
        await asyncio.sleep(0.05)  # simulate network delay to ensure in-flight concurrency
        return await _create_mock_response(config, messages, time.time())

    set_provider_stub_handler(counting_stub)

    # Fire 5 concurrent requests simultaneously with identical idempotency key
    responses = await asyncio.gather(
        client.post("/v1/chat/completions", json=payload),
        client.post("/v1/chat/completions", json=payload),
        client.post("/v1/chat/completions", json=payload),
        client.post("/v1/chat/completions", json=payload),
        client.post("/v1/chat/completions", json=payload),
    )

    # Verify all 5 callers got 200 OK
    for resp in responses:
        assert resp.status_code == 200
        data = resp.json()
        assert data["choices"][0]["message"]["content"]

    # Verify EXACTLY 1 execution reached the provider layer
    assert call_counter == 1, f"Expected exactly 1 provider call, but got {call_counter}"

    # 6th follow-up call MUST return cached result without increasing provider calls
    cached_resp = await client.post("/v1/chat/completions", json=payload)
    assert cached_resp.status_code == 200
    assert cached_resp.json()["gateway_metadata"]["cached_idempotent"] is True
    assert call_counter == 1
