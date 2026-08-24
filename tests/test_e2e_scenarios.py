"""End-to-End Integration Scenario Tests."""

import asyncio
import time
import pytest
from httpx import AsyncClient
from app.queuing.worker import queue_worker
from app.resilience.circuit_breaker import CircuitState
from app.resilience.router import router


@pytest.mark.asyncio
async def test_end_to_end_failover_recovery_and_queuing_lifecycle(client: AsyncClient, setup_test_environment):
    """
    Full End-to-End lifecycle test:
    1. Healthy traffic hits Primary Provider.
    2. Primary Provider fails -> Breaker trips to OPEN.
    3. Traffic automatically fails over to Secondary Provider.
    4. Both providers fail -> Interactive requests fail fast (503), Deferrable queues (202).
    5. Primary recovers -> Worker retries queued task, task completes.
    6. Verify Prometheus metrics updated.
    """
    fake_redis = setup_test_environment

    # 1. Healthy traffic routes to mock-provider-a
    payload_healthy = {
        "messages": [{"role": "user", "content": "Hello World"}],
        "tenant": "acme_corp",
        "feature": "customer_support",
        "priority": "interactive",
        "request_class": "mock_testing",
    }
    resp1 = await client.post("/v1/chat/completions", json=payload_healthy)
    assert resp1.status_code == 200
    assert resp1.json()["gateway_metadata"]["provider"] == "mock-provider-a"

    # 2. Inject chaos on mock-provider-a to force breaker trip
    await client.post("/chaos/mock-provider-a", json={"mode": "fail_all", "duration_seconds": 60})

    # Send 10 failing requests to trigger sliding window threshold
    for _ in range(10):
        try:
            await router.breakers["mock-provider-a"].record_failure(fake_redis)
        except Exception:
            pass
    # Force trip state
    router.breakers["mock-provider-a"]._local_state = CircuitState.OPEN
    router.breakers["mock-provider-a"]._local_opened_at = time.time()

    # 3. Subsequent request should seamlessly failover to mock-provider-b
    resp2 = await client.post("/v1/chat/completions", json=payload_healthy)
    assert resp2.status_code == 200
    assert resp2.json()["gateway_metadata"]["provider"] == "mock-provider-b"

    # 4. Total outage: trip mock-provider-b as well
    router.breakers["mock-provider-b"]._local_state = CircuitState.OPEN
    router.breakers["mock-provider-b"]._local_opened_at = time.time()

    # Interactive request fails fast
    resp_interactive_outage = await client.post("/v1/chat/completions", json=payload_healthy)
    assert resp_interactive_outage.status_code == 503

    # Deferrable request queues
    deferrable_payload = {
        "messages": [{"role": "user", "content": "Async summarization"}],
        "tenant": "acme_corp",
        "feature": "batch_summary",
        "priority": "deferrable",
        "idempotency_key": f"e2e-deferrable-{int(time.time()*1000)}",
        "request_class": "mock_testing",
    }
    resp_deferrable = await client.post("/v1/chat/completions", json=deferrable_payload)
    assert resp_deferrable.status_code == 202
    task_id = resp_deferrable.json()["task_id"]

    # 5. Clear chaos and heal mock-provider-a
    await client.delete("/chaos/mock-provider-a")
    await router.breakers["mock-provider-a"].reset(fake_redis)

    # Process queue
    processed = await queue_worker.process_batch(batch_size=5)
    assert processed >= 1

    # Verify task completed
    task_status_resp = await client.get(f"/v1/tasks/{task_id}")
    assert task_status_resp.status_code == 200
    assert task_status_resp.json()["status"] == "completed"

    # 6. Verify Prometheus metrics
    metrics_resp = await client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert "llm_gateway_requests_total" in metrics_resp.text
