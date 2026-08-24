"""
Sustained Load & Effective Availability Benchmark
Measures effective availability during a chaos-induced primary provider outage.
Saves raw reproducible results to benchmarks/results/availability_run_<timestamp>.json.
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path
import httpx
from httpx import ASGITransport

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

os.environ["ENVIRONMENT"] = "test"
os.environ["LITELLM_TELEMETRY"] = "False"
os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
os.environ["ENABLE_CHAOS_ENDPOINT"] = "true"

import fakeredis.aioredis as fake_aioredis
from app.core.redis_client import set_redis_client
from app.main import app
from app.queuing.worker import queue_worker

BASE_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")
RESULTS_DIR = PROJECT_ROOT / "benchmarks" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)


async def get_client():
    """Tries live gateway first; falls back to in-process ASGI transport."""
    try:
        live_client = httpx.AsyncClient(base_url=BASE_URL, timeout=5.0)
        h = await live_client.get("/health")
        if h.status_code == 200:
            return live_client, False
        await live_client.aclose()
    except Exception:
        pass

    # Use in-process ASGI client with fakeredis
    fake_redis = fake_aioredis.FakeRedis(decode_responses=True)
    set_redis_client(fake_redis)
    transport = ASGITransport(app=app)
    asgi_client = httpx.AsyncClient(transport=transport, base_url="http://test", timeout=30.0)
    return asgi_client, True


async def run_load_test(
    total_duration_seconds: int = 15,
    concurrency: int = 4,
    chaos_start_seconds: int = 4,
    chaos_duration_seconds: int = 6,
):
    client, is_in_process = await get_client()
    mode_str = "In-Process ASGI Transport" if is_in_process else f"Live Server ({BASE_URL})"
    print(f"Starting Load Test ({mode_str}) for {total_duration_seconds}s...")
    print(f"Primary provider chaos outage will trigger at t={chaos_start_seconds}s for {chaos_duration_seconds}s.")

    stats = {
        "benchmark": "effective_availability",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": mode_str,
        "config": {
            "total_duration_seconds": total_duration_seconds,
            "concurrency": concurrency,
            "chaos_start_seconds": chaos_start_seconds,
            "chaos_duration_seconds": chaos_duration_seconds,
        },
        "total_requests": 0,
        "successful_responses": 0,
        "failed_responses": 0,
        "queued_deferrable_tasks": 0,
        "completed_queued_tasks": 0,
        "provider_distribution": {},
        "latencies_ms": [],
        "errors": [],
    }

    start_time = time.time()
    chaos_triggered = False
    active_tasks = []

    async def worker(worker_id: int):
        nonlocal chaos_triggered
        req_idx = 0
        while time.time() - start_time < total_duration_seconds:
            elapsed = time.time() - start_time
            req_idx += 1
            stats["total_requests"] += 1

            if elapsed >= chaos_start_seconds and not chaos_triggered:
                chaos_triggered = True
                print(f"\n>>> [CHAOS] Injecting 100% failure on primary provider 'mock-provider-a' at t={elapsed:.1f}s...")
                try:
                    await client.post(
                        "/chaos/mock-provider-a",
                        json={"mode": "fail_all", "duration_seconds": chaos_duration_seconds},
                    )
                except Exception as e:
                    print(f"Chaos set error: {e}")

            priority = "interactive" if req_idx % 4 != 0 else "deferrable"
            idemp_key = f"load-{worker_id}-{int(time.time()*1000)}-{req_idx}"
            payload = {
                "messages": [{"role": "user", "content": f"Load prompt {req_idx}"}],
                "tenant": f"tenant_{worker_id % 3}",
                "feature": "load_test_feature",
                "priority": priority,
                "request_class": "mock_testing",
                "idempotency_key": idemp_key,
            }

            t0 = time.perf_counter()
            try:
                resp = await client.post("/v1/chat/completions", json=payload)
                duration_ms = (time.perf_counter() - t0) * 1000.0
                stats["latencies_ms"].append(round(duration_ms, 2))

                if resp.status_code == 200:
                    stats["successful_responses"] += 1
                    data = resp.json()
                    prov = data.get("gateway_metadata", {}).get("provider", "unknown")
                    stats["provider_distribution"][prov] = stats["provider_distribution"].get(prov, 0) + 1
                elif resp.status_code == 202:
                    stats["queued_deferrable_tasks"] += 1
                    active_tasks.append((idemp_key, resp.json().get("task_id")))
                else:
                    stats["failed_responses"] += 1
                    stats["errors"].append({"status": resp.status_code, "text": resp.text})
            except Exception as exc:
                stats["failed_responses"] += 1
                stats["errors"].append({"exception": str(exc)})

            await asyncio.sleep(0.05)

    # Run concurrent workers
    await asyncio.gather(*[worker(i) for i in range(concurrency)])

    # Clear chaos and drain queue
    print("\nClearing chaos and draining background queue...")
    await client.post("/chaos/reset")
    if is_in_process:
        await queue_worker.process_batch(batch_size=20)
    else:
        await asyncio.sleep(2.0)

    # Check queued deferrable tasks completion
    for idemp_key, task_id in active_tasks:
        if not task_id:
            continue
        try:
            task_resp = await client.get(f"/v1/tasks/{task_id}")
            if task_resp.status_code == 200 and task_resp.json().get("status") == "completed":
                stats["completed_queued_tasks"] += 1
        except Exception:
            pass

    await client.aclose()

    # Compute Effective Availability
    total_reqs = stats["total_requests"]
    effective_successes = stats["successful_responses"] + stats["completed_queued_tasks"]
    effective_avail = (effective_successes / total_reqs * 100.0) if total_reqs > 0 else 0.0
    stats["effective_availability_percent"] = round(effective_avail, 2)

    # Compute Latency Percentiles
    lats = sorted(stats["latencies_ms"])
    if lats:
        stats["p50_latency_ms"] = lats[int(len(lats) * 0.50)]
        stats["p95_latency_ms"] = lats[int(len(lats) * 0.95)]
        stats["p99_latency_ms"] = lats[int(len(lats) * 0.99)]

    # Save artifact
    ts_str = time.strftime("%Y%m%d_%H%M%S")
    out_file = RESULTS_DIR / f"availability_run_{ts_str}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print(f"\n=======================================================")
    print(f"BENCHMARK COMPLETED: Effective Availability = {stats['effective_availability_percent']}%")
    print(f"Total Requests: {total_reqs} | Immediate Successes: {stats['successful_responses']} | Queued Recovered: {stats['completed_queued_tasks']}")
    print(f"p50 Latency: {stats.get('p50_latency_ms')}ms | p95 Latency: {stats.get('p95_latency_ms')}ms")
    print(f"Provider Distribution: {stats['provider_distribution']}")
    print(f"Raw Results Saved to: {out_file.name}")
    print(f"=======================================================\n")
    return stats, out_file


if __name__ == "__main__":
    asyncio.run(run_load_test())
