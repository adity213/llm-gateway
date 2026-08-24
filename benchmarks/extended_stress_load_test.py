"""
Extended Stress Load & Cascading Outage Availability Benchmark
Runs a 3-minute sustained mixed-traffic stress test with:
1. Primary provider outage (t=30s to t=80s)
2. Overlapping burst of deferrable requests during outage (t=50s to t=75s)
3. Second cascading outage on fallback provider mid-recovery (t=80s to t=120s)
4. Recovery and queue drain verification (t=120s to t=180s)

Saves raw reproducible results to benchmarks/results/extended_stress_availability_<timestamp>.json.
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
        live_client = httpx.AsyncClient(base_url=BASE_URL, timeout=10.0)
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


async def run_extended_stress_benchmark(
    total_duration_seconds: int = 180,  # 3 minutes
    concurrency: int = 6,
):
    client, is_in_process = await get_client()
    mode_str = "In-Process ASGI Transport" if is_in_process else f"Live Server ({BASE_URL})"
    print(f"================================================================================")
    print(f"STARTING EXTENDED 3-MINUTE STRESS BENCHMARK ({mode_str})")
    print(f"Duration: {total_duration_seconds}s | Workers: {concurrency}")
    print(f"Chaos Schedule:")
    print(f" - t=30s : Primary outage on 'mock-provider-a' (50s duration)")
    print(f" - t=50s : High-frequency deferrable batch request surge")
    print(f" - t=80s : Cascading fallback outage on 'mock-provider-b' mid-recovery (40s duration)")
    print(f" - t=120s: Both providers recovering, queue drain & single-probe gating")
    print(f"================================================================================\n")

    stats = {
        "benchmark": "extended_stress_availability",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": mode_str,
        "config": {
            "total_duration_seconds": total_duration_seconds,
            "concurrency": concurrency,
            "primary_outage_start": 30,
            "primary_outage_duration": 50,
            "deferrable_burst_start": 50,
            "deferrable_burst_end": 75,
            "fallback_outage_start": 80,
            "fallback_outage_duration": 40,
        },
        "total_requests": 0,
        "interactive_requests": 0,
        "deferrable_requests": 0,
        "successful_responses_200": 0,
        "queued_deferrable_responses_202": 0,
        "fast_failed_total_outage_503": 0,
        "failed_responses_other": 0,
        "completed_queued_tasks": 0,
        "lost_or_failed_queued_tasks": 0,
        "provider_distribution": {},
        "latencies_ms": [],
        "errors": [],
    }

    start_time = time.time()
    chaos_a_triggered = False
    chaos_b_triggered = False
    active_tasks = []

    # Periodic background queue runner for in-process mode
    queue_drainer_task = None
    if is_in_process:
        async def background_drainer():
            while time.time() - start_time < total_duration_seconds + 10:
                await queue_worker.process_batch(batch_size=15)
                await asyncio.sleep(0.5)
        queue_drainer_task = asyncio.create_task(background_drainer())

    async def worker(worker_id: int):
        nonlocal chaos_a_triggered, chaos_b_triggered
        req_idx = 0
        while time.time() - start_time < total_duration_seconds:
            elapsed = time.time() - start_time
            req_idx += 1
            stats["total_requests"] += 1

            # Chaos 1: Primary Outage at t=30s
            if elapsed >= 30 and not chaos_a_triggered:
                chaos_a_triggered = True
                print(f"\n>>> [CHAOS EVENT 1] t={elapsed:.1f}s: Primary Outage on 'mock-provider-a' (50s fail_all)")
                try:
                    await client.post("/chaos/mock-provider-a", json={"mode": "fail_all", "duration_seconds": 50})
                except Exception as e:
                    print(f"Chaos error: {e}")

            # Chaos 2: Cascading Fallback Outage at t=80s
            if elapsed >= 80 and not chaos_b_triggered:
                chaos_b_triggered = True
                print(f"\n>>> [CHAOS EVENT 2] t={elapsed:.1f}s: Cascading Fallback Outage on 'mock-provider-b' (40s fail_all)")
                try:
                    await client.post("/chaos/mock-provider-b", json={"mode": "fail_all", "duration_seconds": 40})
                except Exception as e:
                    print(f"Chaos error: {e}")

            # Deferrable burst between t=50s and t=75s
            in_deferrable_burst = (50 <= elapsed <= 75)
            if in_deferrable_burst:
                priority = "deferrable" if (req_idx % 2 == 0) else "interactive"
            else:
                priority = "interactive" if (req_idx % 4 != 0) else "deferrable"

            if priority == "deferrable":
                stats["deferrable_requests"] += 1
            else:
                stats["interactive_requests"] += 1

            idemp_key = f"stress-{worker_id}-{int(time.time()*1000)}-{req_idx}"
            payload = {
                "messages": [{"role": "user", "content": f"Stress prompt {worker_id}-{req_idx}"}],
                "tenant": f"tenant_{worker_id % 4}",
                "feature": "batch_processing" if priority == "deferrable" else "chat_agent",
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
                    stats["successful_responses_200"] += 1
                    data = resp.json()
                    prov = data.get("gateway_metadata", {}).get("provider", "unknown")
                    stats["provider_distribution"][prov] = stats["provider_distribution"].get(prov, 0) + 1
                elif resp.status_code == 202:
                    stats["queued_deferrable_responses_202"] += 1
                    task_id = resp.json().get("task_id")
                    active_tasks.append((idemp_key, task_id))
                elif resp.status_code == 503:
                    stats["fast_failed_total_outage_503"] += 1
                else:
                    stats["failed_responses_other"] += 1
                    stats["errors"].append({"status": resp.status_code, "text": resp.text})
            except Exception as exc:
                stats["failed_responses_other"] += 1
                stats["errors"].append({"exception": str(exc)})

            # Progress log every 30 seconds
            if worker_id == 0 and req_idx % 50 == 0:
                print(f"[t={elapsed:5.1f}s] Requests: {stats['total_requests']:4d} | 200 OK: {stats['successful_responses_200']:4d} | 202 Queued: {stats['queued_deferrable_responses_202']:4d} | 503 Fail-Fast: {stats['fast_failed_total_outage_503']:3d}")

            await asyncio.sleep(0.04)

    # Run all workers
    await asyncio.gather(*[worker(i) for i in range(concurrency)])

    print("\n>>> Sustained traffic generation finished. Resetting chaos and draining queue...")
    await client.post("/chaos/reset")

    # Give worker time to drain all remaining queued tasks
    drain_wait_start = time.time()
    while time.time() - drain_wait_start < 15:
        if is_in_process:
            await queue_worker.process_batch(batch_size=30)
        await asyncio.sleep(0.5)

    if queue_drainer_task:
        queue_drainer_task.cancel()

    # Verify task statuses
    print(">>> Verifying all queued deferrable tasks...")
    for idemp_key, task_id in active_tasks:
        if not task_id:
            stats["lost_or_failed_queued_tasks"] += 1
            continue
        try:
            task_resp = await client.get(f"/v1/tasks/{task_id}")
            if task_resp.status_code == 200:
                t_data = task_resp.json()
                if t_data.get("status") == "completed":
                    stats["completed_queued_tasks"] += 1
                else:
                    stats["lost_or_failed_queued_tasks"] += 1
            else:
                stats["lost_or_failed_queued_tasks"] += 1
        except Exception:
            stats["lost_or_failed_queued_tasks"] += 1

    await client.aclose()

    # Calculate Metrics
    total_reqs = stats["total_requests"]
    total_successes = stats["successful_responses_200"] + stats["completed_queued_tasks"]
    
    # 1. Effective Availability (Did the client get an answer either immediately or via queue)
    # Interactive requests during total outage properly fail-fast (503), preventing infinite hanging.
    effective_avail = (total_successes / (total_reqs - stats["fast_failed_total_outage_503"]) * 100.0) if (total_reqs - stats["fast_failed_total_outage_503"]) > 0 else 0.0
    overall_served_rate = (total_successes / total_reqs * 100.0) if total_reqs > 0 else 0.0
    
    stats["effective_availability_percent"] = round(effective_avail, 2)
    stats["overall_served_rate_percent"] = round(overall_served_rate, 2)
    stats["deferrable_queue_recovery_rate_percent"] = round(
        (stats["completed_queued_tasks"] / stats["queued_deferrable_responses_202"] * 100.0)
        if stats["queued_deferrable_responses_202"] > 0 else 100.0,
        2
    )

    # Latency percentiles
    lats = sorted(stats["latencies_ms"])
    if lats:
        stats["p50_latency_ms"] = lats[int(len(lats) * 0.50)]
        stats["p95_latency_ms"] = lats[int(len(lats) * 0.95)]
        stats["p99_latency_ms"] = lats[int(len(lats) * 0.99)]

    # Save to file
    ts_str = time.strftime("%Y%m%d_%H%M%S")
    out_file = RESULTS_DIR / f"extended_stress_availability_{ts_str}.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)

    print(f"\n================================================================================")
    print(f"EXTENDED STRESS BENCHMARK COMPLETE (3-Minute Run)")
    print(f"================================================================================")
    print(f"Raw Results File                : {out_file.name}")
    print(f"Total Requests Dispatched       : {total_reqs}")
    print(f" - Interactive Requests         : {stats['interactive_requests']}")
    print(f" - Deferrable Requests          : {stats['deferrable_requests']}")
    print(f"Direct 200 OK Responses         : {stats['successful_responses_200']}")
    print(f"Queued 202 Responses            : {stats['queued_deferrable_responses_202']}")
    print(f"Queued Tasks Recovered/Completed: {stats['completed_queued_tasks']} ({stats['deferrable_queue_recovery_rate_percent']}%)")
    print(f"Queued Tasks Lost/Failed        : {stats['lost_or_failed_queued_tasks']}")
    print(f"Fast-Failed 503 (Total Outage)  : {stats['fast_failed_total_outage_503']}")
    print(f"Unexpected / Unhandled Errors   : {stats['failed_responses_other']}")
    print(f"Latency p50 / p95 / p99         : {stats.get('p50_latency_ms')}ms / {stats.get('p95_latency_ms')}ms / {stats.get('p99_latency_ms')}ms")
    print(f"Effective Availability (Healthy): {stats['effective_availability_percent']}%")
    print(f"Overall Traffic Served Rate     : {stats['overall_served_rate_percent']}%")
    print(f"Provider Distribution           : {stats['provider_distribution']}")
    print(f"================================================================================\n")
    return stats, out_file


if __name__ == "__main__":
    asyncio.run(run_extended_stress_benchmark())
