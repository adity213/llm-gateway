"""Full test suite executor running all unit, integration, and e2e tests."""

import asyncio
import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

os.environ["ENVIRONMENT"] = "test"
os.environ["LITELLM_TELEMETRY"] = "False"
os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
os.environ["ENABLE_CHAOS_ENDPOINT"] = "true"

import fakeredis.aioredis as fake_aioredis
from httpx import ASGITransport, AsyncClient
from app.config import settings
from app.core.redis_client import set_redis_client
from app.main import app
from app.resilience.chaos import chaos_manager
from app.resilience.circuit_breaker import CircuitState
from app.resilience.router import router

# Import individual test modules
import tests.test_phase0_health as t0
import tests.test_phase1_entrypoint as t1
import tests.test_phase2_health_tracking as t2
import tests.test_phase3_circuit_breaker as t3
import tests.test_phase4_queue_idempotency as t4
import tests.test_phase5_chaos as t5
import tests.test_e2e_scenarios as te2e


async def reset_env():
    fake_redis = fake_aioredis.FakeRedis(decode_responses=True)
    set_redis_client(fake_redis)
    for name, breaker in router.breakers.items():
        breaker._local_state = CircuitState.CLOSED
        breaker._local_opened_at = 0.0
        breaker._local_consecutive_probes = 0
        breaker._probe_in_flight = False
    chaos_manager._local_chaos.clear()
    return fake_redis


async def run_suite():
    print("\n=======================================================")
    print("RUNNING COMPLETE SELF-HEALING LLM GATEWAY TEST SUITE")
    print("=======================================================\n")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Phase 0
        print("[PHASE 0] Health Checks:")
        await reset_env()
        await t0.test_health_check_returns_ok(client)
        print("  -> PASSED: GET /health returns 200 {'status': 'ok'}")

        # Phase 1
        print("\n[PHASE 1] Entrypoint, Providers & Mandatory Metadata:")
        await reset_env()
        await t1.test_missing_tenant_returns_400(client)
        print("  -> PASSED: Missing 'tenant' rejected with 400")
        await t1.test_missing_feature_returns_400(client)
        print("  -> PASSED: Missing 'feature' rejected with 400")
        await t1.test_missing_priority_returns_400(client)
        print("  -> PASSED: Missing 'priority' rejected with 400")
        await t1.test_normalized_response_shape(client)
        print("  -> PASSED: Response normalized to standard OpenAI completion format")

        # Phase 2
        print("\n[PHASE 2] Health Tracking, Sliding Window & Error Taxonomy:")
        t2.test_error_taxonomy_classification()
        print("  -> PASSED: Error taxonomy correctly categorizes all exception classes")
        fake_redis = await reset_env()
        await t2.test_sliding_window_50_requests_mix(fake_redis)
        print("  -> PASSED: Sliding window accurately computes rolling 50-sample rates & percentiles")
        await t2.test_metrics_endpoint_exposes_prometheus(client)
        print("  -> PASSED: Prometheus metrics scraped at /metrics")

        # Phase 3
        print("\n[PHASE 3] Circuit Breaker & Request-Class Router:")
        fake_redis = await reset_env()
        await t3.test_circuit_breaker_trips_to_open_on_threshold(fake_redis)
        print("  -> PASSED: Breaker trips CLOSED -> OPEN only when samples >= 10 and failure rate > 30%")
        fake_redis = await reset_env()
        await t3.test_circuit_breaker_half_open_recovery_and_probe_failure(fake_redis)
        print("  -> PASSED: Auto-transition OPEN -> HALF_OPEN, immediate trip on probe failure, heal on 3 probes")
        fake_redis = await reset_env()
        await t3.test_concurrent_probe_gating_in_half_open(fake_redis)
        print("  -> PASSED: Concurrency-safe single-probe gating strictly permits only 1 in-flight probe")
        fake_redis = await reset_env()
        await t3.test_20_concurrent_requests_at_half_open_admit_single_probe_and_reroute_rest(client, fake_redis)
        print("  -> PASSED: 20 concurrent requests at Half-Open admit exactly 1 probe and reroute 19 to fallback")
        await t3.test_preference_list_omitted_request_class_fallback()
        print("  -> PASSED: Omitted request_class cleanly falls back to 'default' preference list")

        # Phase 4
        print("\n[PHASE 4] Queueing, Exponential Backoff & Idempotency:")
        await reset_env()
        await t4.test_deferrable_missing_idempotency_key_returns_400(client)
        print("  -> PASSED: Missing idempotency_key on deferrable request rejected with 400")
        await reset_env()
        await t4.test_interactive_fail_fast_on_total_outage(client)
        print("  -> PASSED: Interactive priority fails fast in <10ms with 503 + Retry-After during outage")
        fake_redis = await reset_env()
        await t4.test_deferrable_queues_on_outage_and_heals_on_recovery(client, fake_redis)
        print("  -> PASSED: Deferrable request enqueued with 202, retried on recovery, and completed")
        await reset_env()
        await t4.test_concurrent_idempotent_requests_race_condition(client)
        print("  -> PASSED: Concurrent racing duplicate idempotency keys return identical cached results")

        # Phase 5
        print("\n[PHASE 5] Chaos Fault Injection:")
        await reset_env()
        await t5.test_chaos_fail_all_injection_and_expiration(client)
        print("  -> PASSED: POST /chaos/{provider} injects timed fault and deletes/expires cleanly")
        await reset_env()
        await t5.test_chaos_endpoint_gated_by_env_flag(client)
        print("  -> PASSED: Chaos endpoints safely return 403 Forbidden when ENABLE_CHAOS_ENDPOINT=false")

        # End-to-End
        print("\n[PHASE 6-8] Full End-to-End Resilience Lifecycle:")
        fake_redis = await reset_env()
        await te2e.test_end_to_end_failover_recovery_and_queuing_lifecycle(client, fake_redis)
        print("  -> PASSED: Full trip -> reroute -> total outage -> recovery -> queue drain -> metrics scrape verified")

    print("\n=======================================================")
    print("ALL 16 AUTOMATED RESILIENCE & UNIT TESTS PASSED (100% SUCCESS)!")
    print("=======================================================\n")


if __name__ == "__main__":
    asyncio.run(run_suite())
