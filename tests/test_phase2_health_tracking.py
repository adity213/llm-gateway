"""Tests for Phase 2: Redis-backed health tracking, error taxonomy, and metrics."""

import pytest
from httpx import AsyncClient
from app.core.errors import ErrorTaxonomy, classify_exception
from app.resilience.sliding_window import SlidingWindowHealthTracker


def test_error_taxonomy_classification():
    """Verifies that various exceptions are correctly mapped to the 6 error taxonomy categories."""
    assert classify_exception(Exception("Rate limit exceeded 429")) == ErrorTaxonomy.RATE_LIMIT
    assert classify_exception(TimeoutError("Connection timed out")) == ErrorTaxonomy.TIMEOUT
    assert classify_exception(PermissionError("401 Unauthorized Invalid API Key")) == ErrorTaxonomy.AUTH_FAILURE
    assert classify_exception(Exception("Safety violation content policy trigger")) == ErrorTaxonomy.CONTENT_FILTER
    assert classify_exception(Exception("502 Bad Gateway from upstream cluster")) == ErrorTaxonomy.SERVER_ERROR
    assert classify_exception(Exception("Something completely custom")) == ErrorTaxonomy.UNKNOWN


@pytest.mark.asyncio
async def test_sliding_window_50_requests_mix(setup_test_environment):
    """
    Fires 50 requests with a known mixture of successes (35) and failures (15) across error categories.
    Verifies sliding window calculates exact rates, categories, and percentiles.
    """
    fake_redis = setup_test_environment
    tracker = SlidingWindowHealthTracker(window_seconds=60)
    provider = "test-provider-mix"

    # Send 35 successes with latencies 50ms - 100ms
    for i in range(35):
        await tracker.record_outcome(
            provider_name=provider,
            success=True,
            latency_ms=50.0 + i,
            error_category=None,
            redis_client=fake_redis,
        )

    # Send 5 RateLimit errors
    for _ in range(5):
        await tracker.record_outcome(
            provider_name=provider,
            success=False,
            latency_ms=200.0,
            error_category=ErrorTaxonomy.RATE_LIMIT,
            redis_client=fake_redis,
        )

    # Send 5 Timeout errors
    for _ in range(5):
        await tracker.record_outcome(
            provider_name=provider,
            success=False,
            latency_ms=5000.0,
            error_category=ErrorTaxonomy.TIMEOUT,
            redis_client=fake_redis,
        )

    # Send 5 ServerError errors
    for _ in range(5):
        await tracker.record_outcome(
            provider_name=provider,
            success=False,
            latency_ms=300.0,
            error_category=ErrorTaxonomy.SERVER_ERROR,
            redis_client=fake_redis,
        )

    # Fetch rolling metrics
    metrics = await tracker.get_metrics(provider, redis_client=fake_redis)

    assert metrics.total_samples == 50
    assert metrics.successful_samples == 35
    assert metrics.failed_samples == 15
    assert metrics.success_rate == 0.70
    assert metrics.failure_rate == 0.30

    assert metrics.error_counts[ErrorTaxonomy.RATE_LIMIT.value] == 5
    assert metrics.error_counts[ErrorTaxonomy.TIMEOUT.value] == 5
    assert metrics.error_counts[ErrorTaxonomy.SERVER_ERROR.value] == 5
    assert metrics.error_counts[ErrorTaxonomy.CONTENT_FILTER.value] == 0

    assert metrics.p50_latency_ms > 0
    assert metrics.p95_latency_ms >= metrics.p50_latency_ms
    assert metrics.p99_latency_ms >= metrics.p95_latency_ms


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_prometheus(client: AsyncClient):
    """Verifies that GET /metrics returns valid Prometheus text metrics."""
    response = await client.get("/metrics")
    assert response.status_code == 200
    text = response.text
    assert "llm_gateway_requests_total" in text
    assert "llm_gateway_circuit_state" in text
    assert "llm_gateway_queue_depth" in text
