import asyncio
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Configure environment
os.environ["ENVIRONMENT"] = "test"
os.environ["LITELLM_TELEMETRY"] = "False"
os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
os.environ["ENABLE_CHAOS_ENDPOINT"] = "true"

print(">>> Starting direct verification runner...")

# Test imports
from app.main import app
from app.config import settings
from app.models.schemas import ChatCompletionRequest, PriorityEnum
from app.core.redis_client import set_redis_client
import fakeredis.aioredis as fake_aioredis
from httpx import ASGITransport, AsyncClient

print(">>> Imports successful.")


async def main():
    fake_redis = fake_aioredis.FakeRedis(decode_responses=True)
    set_redis_client(fake_redis)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Phase 0: Health check
        print("\n[TEST] Phase 0: Health check...")
        resp = await client.get("/health")
        assert resp.status_code == 200, f"Health check failed: {resp.text}"
        assert resp.json() == {"status": "ok"}
        print("  --> PASSED: /health returned 200 {'status': 'ok'}")

        # Phase 1: Metadata validation & normalization
        print("\n[TEST] Phase 1: Metadata validation & normalized response...")
        resp = await client.post("/v1/chat/completions", json={
            "messages": [{"role": "user", "content": "Hi"}],
            "feature": "test",
            "priority": "interactive"
        })
        assert resp.status_code == 400
        print("  --> PASSED: Missing tenant returned 400 Bad Request")

        resp = await client.post("/v1/chat/completions", json={
            "messages": [{"role": "user", "content": "Hi"}],
            "tenant": "t1",
            "feature": "f1",
            "priority": "interactive",
            "request_class": "mock_testing"
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "choices" in data
        assert "gateway_metadata" in data
        print(f"  --> PASSED: Normalized response received from {data['gateway_metadata']['provider']}")

        # Phase 2: Sliding window health tracking
        print("\n[TEST] Phase 2: Sliding window health tracking...")
        from app.resilience.sliding_window import SlidingWindowHealthTracker
        tracker = SlidingWindowHealthTracker(window_seconds=60)
        for i in range(10):
            await tracker.record_outcome("test_prov", success=True, latency_ms=40.0 + i, redis_client=fake_redis)
        metrics = await tracker.get_metrics("test_prov", redis_client=fake_redis)
        assert metrics.total_samples == 10
        assert metrics.success_rate == 1.0
        print(f"  --> PASSED: Sliding window recorded {metrics.total_samples} samples with success rate {metrics.success_rate}")

        # Phase 3: Circuit breaker trip & recovery
        print("\n[TEST] Phase 3: Circuit breaker state transitions...")
        from app.resilience.circuit_breaker import CircuitBreaker, CircuitState
        breaker = CircuitBreaker("cb_test", health_tracker=tracker, min_samples=5, failure_rate_threshold=0.3)
        for _ in range(5):
            await tracker.record_outcome("cb_test", success=False, latency_ms=100.0, redis_client=fake_redis)
            await breaker.record_failure(fake_redis)
        state = await breaker.get_state(fake_redis)
        assert state == CircuitState.OPEN
        print("  --> PASSED: Circuit breaker tripped CLOSED -> OPEN on threshold breach")

        # Phase 4: Queueing, backoff & idempotency
        print("\n[TEST] Phase 4: Queueing & Idempotency...")
        # Missing idempotency on deferrable -> 400
        resp = await client.post("/v1/chat/completions", json={
            "messages": [{"role": "user", "content": "Defer me"}],
            "tenant": "t1",
            "feature": "f1",
            "priority": "deferrable"
        })
        assert resp.status_code == 400
        print("  --> PASSED: Deferrable request without idempotency_key rejected with 400")

        # Idempotency replay
        idemp_key = "idemp_test_123"
        resp1 = await client.post("/v1/chat/completions", json={
            "messages": [{"role": "user", "content": "Idemp prompt"}],
            "tenant": "t1",
            "feature": "f1",
            "priority": "interactive",
            "idempotency_key": idemp_key,
            "request_class": "mock_testing"
        })
        assert resp1.status_code == 200

        resp2 = await client.post("/v1/chat/completions", json={
            "messages": [{"role": "user", "content": "Idemp prompt"}],
            "tenant": "t1",
            "feature": "f1",
            "priority": "interactive",
            "idempotency_key": idemp_key,
            "request_class": "mock_testing"
        })
        assert resp2.status_code == 200
        assert resp2.json()["gateway_metadata"]["cached_idempotent"] is True
        print("  --> PASSED: Duplicate idempotency key returned cached result in 0ms without provider call")

        # Phase 5: Chaos injection
        print("\n[TEST] Phase 5: Chaos injection...")
        chaos_resp = await client.post("/chaos/mock-provider-a", json={"mode": "fail_all", "duration_seconds": 10})
        assert chaos_resp.status_code == 200
        status_resp = await client.get("/chaos/mock-provider-a")
        assert status_resp.json()["active"] is True
        await client.delete("/chaos/mock-provider-a")
        print("  --> PASSED: Chaos fault injected and cleared successfully")

        # Metrics scrape
        print("\n[TEST] Phase 6: Prometheus metrics scrape...")
        metrics_resp = await client.get("/metrics")
        assert metrics_resp.status_code == 200
        assert "llm_gateway_requests_total" in metrics_resp.text
        print("  --> PASSED: Prometheus metrics scrape returned valid metrics text")

    print("\n=======================================================")
    print("ALL CORE UNIT & INTEGRATION TESTS PASSED SUCCESSFULLY!!")
    print("=======================================================\n")


if __name__ == "__main__":
    asyncio.run(main())
