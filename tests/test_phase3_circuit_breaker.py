"""Tests for Phase 3: Circuit Breaker state machine, preference lists, and routing."""

import asyncio
import json
import time
import pytest
from httpx import AsyncClient
from app.core.errors import ErrorTaxonomy
from app.models.providers import DEFAULT_PREFERENCE_LISTS, get_provider_config
from app.models.schemas import ChatCompletionRequest, PriorityEnum
from app.resilience.circuit_breaker import CircuitBreaker, CircuitState
from app.resilience.router import GatewayRouter
from app.resilience.sliding_window import SlidingWindowHealthTracker


@pytest.mark.asyncio
async def test_circuit_breaker_trips_to_open_on_threshold(setup_test_environment):
    """
    Verifies that CircuitBreaker transitions CLOSED -> OPEN only after >= 10 samples
    and failure rate exceeds 30%.
    """
    fake_redis = setup_test_environment
    tracker = SlidingWindowHealthTracker(window_seconds=60)
    breaker = CircuitBreaker(
        provider_name="test-prov-1",
        health_tracker=tracker,
        min_samples=10,
        failure_rate_threshold=0.30,
        cooldown_seconds=5,
    )

    # Initial state is CLOSED
    state = await breaker.get_state(fake_redis)
    assert state == CircuitState.CLOSED

    # 1. Send 6 successes and 3 failures (9 samples total < 10 min_samples)
    for _ in range(6):
        await tracker.record_outcome("test-prov-1", success=True, latency_ms=50.0, redis_client=fake_redis)
        await breaker.record_success(fake_redis)

    for _ in range(3):
        await tracker.record_outcome("test-prov-1", success=False, latency_ms=100.0, error_category=ErrorTaxonomy.SERVER_ERROR, redis_client=fake_redis)
        await breaker.record_failure(fake_redis)

    # Failure rate = 3/9 = 33% > 30%, but samples < 10, so breaker MUST stay CLOSED
    state = await breaker.get_state(fake_redis)
    assert state == CircuitState.CLOSED

    # 2. Add 1 more failure (10th sample: 4 failures / 10 total = 40% > 30%)
    await tracker.record_outcome("test-prov-1", success=False, latency_ms=100.0, error_category=ErrorTaxonomy.SERVER_ERROR, redis_client=fake_redis)
    await breaker.record_failure(fake_redis)

    # Now breaker must be OPEN
    state = await breaker.get_state(fake_redis)
    assert state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_recovery_and_probe_failure(setup_test_environment):
    """
    Verifies:
    - OPEN -> HALF_OPEN after cooldown.
    - HALF_OPEN -> OPEN immediately on first probe failure.
    - HALF_OPEN -> CLOSED on 3 consecutive successful probe requests.
    """
    fake_redis = setup_test_environment
    tracker = SlidingWindowHealthTracker(window_seconds=60)
    breaker = CircuitBreaker(
        provider_name="test-prov-2",
        health_tracker=tracker,
        cooldown_seconds=1,  # 1s for fast test
        successful_probes_required=3,
    )

    # Trip breaker to OPEN in Redis and local
    now = time.time()
    await fake_redis.set(breaker._get_key(), json.dumps({"state": CircuitState.OPEN.value, "opened_at": now - 5.0}))
    breaker._local_state = CircuitState.OPEN
    breaker._local_opened_at = now - 5.0

    # Breaker should now evaluate to HALF_OPEN
    state = await breaker.get_state(fake_redis)
    assert state == CircuitState.HALF_OPEN

    # Test immediate trip back to OPEN on probe failure
    await breaker.record_failure(fake_redis)
    state = await breaker.get_state(fake_redis)
    assert state == CircuitState.OPEN

    # Let cooldown elapse again in Redis and local
    await fake_redis.set(breaker._get_key(), json.dumps({"state": CircuitState.OPEN.value, "opened_at": time.time() - 5.0}))
    breaker._local_opened_at = time.time() - 5.0
    state = await breaker.get_state(fake_redis)
    assert state == CircuitState.HALF_OPEN

    # Test 3 consecutive successful probes healing breaker back to CLOSED
    await breaker.record_success(fake_redis)
    state = await breaker.get_state(fake_redis)
    assert state == CircuitState.HALF_OPEN

    await breaker.record_success(fake_redis)
    state = await breaker.get_state(fake_redis)
    assert state == CircuitState.HALF_OPEN

    await breaker.record_success(fake_redis)
    state = await breaker.get_state(fake_redis)
    assert state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_concurrent_probe_gating_in_half_open(setup_test_environment):
    """
    Explicit test for race condition: Concurrent requests arriving during the same Half-Open window.
    Confirms only ONE probe gets through to test the provider, while concurrent sibling requests
    are rejected/bypassed.
    """
    fake_redis = setup_test_environment
    tracker = SlidingWindowHealthTracker(window_seconds=60)
    breaker = CircuitBreaker(
        provider_name="test-prov-concurrent",
        health_tracker=tracker,
        cooldown_seconds=0,
        successful_probes_required=3,
    )

    # Force breaker into HALF_OPEN in Redis and local
    now = time.time()
    await fake_redis.set(breaker._get_key(), json.dumps({"state": CircuitState.OPEN.value, "opened_at": now - 10.0}))
    breaker._local_state = CircuitState.OPEN
    breaker._local_opened_at = now - 10.0
    assert await breaker.get_state(fake_redis) == CircuitState.HALF_OPEN

    # Simulate 5 concurrent requests attempting admission
    admission_results = await asyncio.gather(
        breaker.can_admit_traffic(fake_redis),
        breaker.can_admit_traffic(fake_redis),
        breaker.can_admit_traffic(fake_redis),
        breaker.can_admit_traffic(fake_redis),
        breaker.can_admit_traffic(fake_redis),
    )

    # Exactly 1 request should be admitted as a probe permit, the other 4 denied
    assert admission_results.count(True) == 1
    assert admission_results.count(False) == 4


@pytest.mark.asyncio
async def test_preference_list_omitted_request_class_fallback():
    """Verifies that an omitted or unknown request_class cleanly falls back to 'default'."""
    router = GatewayRouter()
    default_list = router.get_preference_list(None)
    assert default_list == DEFAULT_PREFERENCE_LISTS["default"]

    unknown_list = router.get_preference_list("some_nonexistent_class")
    assert unknown_list == DEFAULT_PREFERENCE_LISTS["default"]

    custom_list = router.get_preference_list("cheap_classification")
    assert custom_list == DEFAULT_PREFERENCE_LISTS["cheap_classification"]


@pytest.mark.asyncio
async def test_20_concurrent_requests_at_half_open_admit_single_probe_and_reroute_rest(client: AsyncClient, setup_test_environment):
    """
    Explicit test for race condition:
    20+ concurrent requests hitting a provider whose breaker is in HALF_OPEN at the same instant.
    Verifies exactly ONE is admitted as the probe to mock-provider-a, and the remaining 19 are
    correctly rerouted to mock-provider-b.
    """
    fake_redis = setup_test_environment
    from app.resilience.router import router

    # 1. Put mock-provider-a into HALF_OPEN
    now = time.time()
    await fake_redis.set(router.breakers["mock-provider-a"]._get_key(), json.dumps({
        "state": CircuitState.OPEN.value,
        "opened_at": now - 60.0  # Cooldown elapsed -> evaluates to HALF_OPEN
    }))
    router.breakers["mock-provider-a"]._local_state = CircuitState.OPEN
    router.breakers["mock-provider-a"]._local_opened_at = now - 60.0
    router.breakers["mock-provider-a"]._probe_in_flight = False

    # 2. Put mock-provider-b into CLOSED (healthy fallback)
    await fake_redis.set(router.breakers["mock-provider-b"]._get_key(), json.dumps({
        "state": CircuitState.CLOSED.value,
        "opened_at": 0.0
    }))
    router.breakers["mock-provider-b"]._local_state = CircuitState.CLOSED

    # Verify mock-provider-a is in HALF_OPEN
    assert await router.breakers["mock-provider-a"].get_state(fake_redis) == CircuitState.HALF_OPEN

    # 3. Fire 20 concurrent requests simultaneously to mock_testing preference list
    payload = {
        "messages": [{"role": "user", "content": "Concurrent burst"}],
        "tenant": "tenant_burst",
        "feature": "half_open_test",
        "priority": "interactive",
        "request_class": "mock_testing",
    }

    responses = await asyncio.gather(*[
        client.post("/v1/chat/completions", json=payload)
        for _ in range(20)
    ])

    # All 20 requests must succeed
    for resp in responses:
        assert resp.status_code == 200

    # Extract provider serving each request
    providers_served = [
        resp.json()["gateway_metadata"]["provider"]
        for resp in responses
    ]

    probes_admitted = providers_served.count("mock-provider-a")
    rerouted_fallbacks = providers_served.count("mock-provider-b")

    # Exactly 1 probe allowed to mock-provider-a, exactly 19 rerouted to mock-provider-b
    assert probes_admitted == 1, f"Expected exactly 1 probe to mock-provider-a, but got {probes_admitted}"
    assert rerouted_fallbacks == 19, f"Expected 19 rerouted to mock-provider-b, but got {rerouted_fallbacks}"
