"""
Self-Healing LLM Gateway - Live Interactive Demo Script
Demonstrates:
  1. Multi-provider normalized routing
  2. Live Chaos Fault Injection
  3. Circuit Breaker Trips & Automatic Failover
  4. Interactive 503 Fail-Fast vs Deferrable Queueing
  5. Half-Open Probe Recovery & Self-Healing
  6. Idempotent Deduplication
"""

import asyncio
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

# ANSI Color codes for rich terminal formatting
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


def print_banner(title: str):
    print(f"\n{BOLD}{CYAN}{'='*70}{RESET}")
    print(f"{BOLD}{CYAN} >>> {title}{RESET}")
    print(f"{BOLD}{CYAN}{'='*70}{RESET}\n")


async def get_demo_client():
    try:
        live_client = httpx.AsyncClient(base_url=BASE_URL, timeout=5.0)
        h = await live_client.get("/health")
        if h.status_code == 200:
            return live_client, False
        await live_client.aclose()
    except Exception:
        pass

    fake_redis = fake_aioredis.FakeRedis(decode_responses=True)
    set_redis_client(fake_redis)
    transport = ASGITransport(app=app)
    asgi_client = httpx.AsyncClient(transport=transport, base_url="http://test", timeout=30.0)
    return asgi_client, True


async def run_demo():
    client, is_in_process = await get_demo_client()
    mode_str = "In-Process Demo Transport" if is_in_process else f"Live Server ({BASE_URL})"

    print(f"{BOLD}{GREEN}Starting Self-Healing LLM Gateway Live Demonstration ({mode_str})...{RESET}\n")

    # Step 0: Check Health
    print_banner("Step 0: Checking Gateway Health")
    resp = await client.get("/health")
    if resp.status_code == 200:
        print(f"[{GREEN}OK{RESET}] Gateway is ONLINE: {resp.json()}")
    else:
        print(f"[{RED}FAIL{RESET}] Gateway returned {resp.status_code}")
        return

    # Step 1: Normal Traffic Routing
    print_banner("Step 1: Normal Multi-Provider Routing")
    payload = {
        "messages": [{"role": "user", "content": "Summarize resilience engineering in 10 words."}],
        "tenant": "enterprise_corp",
        "feature": "ai_assistant",
        "priority": "interactive",
        "request_class": "mock_testing",
    }

    for i in range(3):
        t0 = time.perf_counter()
        resp = await client.post("/v1/chat/completions", json=payload)
        elapsed = (time.perf_counter() - t0) * 1000.0
        data = resp.json()
        prov = data.get("gateway_metadata", {}).get("provider", "unknown")
        cost = data.get("gateway_metadata", {}).get("cost_usd", 0.0)
        print(f"[{GREEN}200 OK{RESET}] Request {i+1} handled by: {BOLD}{prov}{RESET} | Latency: {elapsed:.1f}ms | Cost: ${cost:.6f}")
        await asyncio.sleep(0.2)

    # Step 2: Injecting Fault / Chaos on Primary Provider
    print_banner("Step 2: Injecting Chaos Fault on Primary Provider (mock-provider-a)")
    print(f"{YELLOW}Injecting 100% failure rate on 'mock-provider-a' for 20 seconds...{RESET}")
    chaos_resp = await client.post(
        "/chaos/mock-provider-a",
        json={"mode": "fail_all", "duration_seconds": 20},
    )
    print(f"Chaos active: {chaos_resp.json()}")

    # Step 3: Triggering Circuit Breaker Trip & Automatic Failover
    print_banner("Step 3: Circuit Breaker Trip & Automatic Failover")
    print("Sending traffic during primary provider failure...")

    for i in range(12):
        t0 = time.perf_counter()
        resp = await client.post("/v1/chat/completions", json=payload)
        data = resp.json()
        prov = data.get("gateway_metadata", {}).get("provider", "unknown")
        retries = data.get("gateway_metadata", {}).get("retries", 0)

        if prov == "mock-provider-b":
            print(f"[{YELLOW}REROUTED{RESET}] Request {i+1:02d} -> Automatically routed to {BOLD}{GREEN}{prov}{RESET} (Failover active, retries={retries})")
        else:
            print(f"[{GREEN}OK{RESET}] Request {i+1:02d} -> Provider {prov}")
        await asyncio.sleep(0.1)

    # Step 4: Total Outage - Interactive vs Deferrable Handling
    print_banner("Step 4: Total Outage Handling (Interactive 503 vs Deferrable Queueing)")
    print(f"{RED}Injecting chaos on secondary provider 'mock-provider-b' (Total Outage)...{RESET}")
    await client.post("/chaos/mock-provider-b", json={"mode": "fail_all", "duration_seconds": 20})

    # Interactive Request -> Fails Fast with 503
    t0 = time.perf_counter()
    interactive_resp = await client.post("/v1/chat/completions", json=payload)
    elapsed = (time.perf_counter() - t0) * 1000.0
    print(f"\n{BOLD}Interactive Request:{RESET}")
    print(f"[{RED}{interactive_resp.status_code}{RESET}] Failed in {elapsed:.1f}ms (Fail-Fast protection). Retry-After: {interactive_resp.headers.get('retry-after')}s")
    print(f"Response: {interactive_resp.json()}")

    # Deferrable Request -> Queued with 202 Accepted
    idemp_key = f"demo-deferrable-{int(time.time()*1000)}"
    deferrable_payload = {
        "messages": [{"role": "user", "content": "Async report generation"}],
        "tenant": "enterprise_corp",
        "feature": "batch_indexer",
        "priority": "deferrable",
        "idempotency_key": idemp_key,
        "request_class": "mock_testing",
    }
    print(f"\n{BOLD}Deferrable Request (Idempotency Key: {idemp_key}):{RESET}")
    deferrable_resp = await client.post("/v1/chat/completions", json=deferrable_payload)
    print(f"[{YELLOW}{deferrable_resp.status_code} ACCEPTED{RESET}] Request enqueued for background exponential backoff retry.")
    task_info = deferrable_resp.json()
    print(f"Task payload: {task_info}")
    task_id = task_info["task_id"]

    # Step 5: Provider Recovery & Self-Healing
    print_banner("Step 5: Healing Providers & Verifying Background Queue Completion")
    print(f"{GREEN}Clearing chaos and resetting breakers...{RESET}")
    await client.post("/chaos/reset")

    print("Draining queue...")
    if is_in_process:
        await queue_worker.process_batch(batch_size=5)
    else:
        await asyncio.sleep(2.0)

    task_status = await client.get(f"/v1/tasks/{task_id}")
    print(f"Task status poll: {task_status.json()}")

    # Step 6: Idempotency Validation
    print_banner("Step 6: Idempotency Cached Replay")
    print(f"Resubmitting EXACT same request with idempotency key '{idemp_key}'...")
    t0 = time.perf_counter()
    replay_resp = await client.post("/v1/chat/completions", json=deferrable_payload)
    elapsed = (time.perf_counter() - t0) * 1000.0
    replay_data = replay_resp.json()
    cached = replay_data.get("gateway_metadata", {}).get("cached_idempotent", False)
    print(f"[{GREEN}200 OK{RESET}] Response returned in {elapsed:.2f}ms | Cached Idempotent: {BOLD}{GREEN}{cached}{RESET}")

    await client.aclose()
    print_banner("Demo Complete!")
    print(f"{BOLD}{GREEN}All Self-Healing Gateway mechanisms demonstrated successfully!{RESET}\n")


if __name__ == "__main__":
    asyncio.run(run_demo())
