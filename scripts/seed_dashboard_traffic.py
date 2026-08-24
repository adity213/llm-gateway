"""
Dashboard Traffic Generator
Generates realistic continuous traffic across tenants, features, and request classes
to populate Grafana panels for live monitoring.
"""

import asyncio
import random
import time
import httpx

BASE_URL = "http://localhost:8000"

TENANTS = ["tenant_enterprise", "tenant_growth", "tenant_starter", "acme_ai"]
FEATURES = ["document_search", "copilot_assist", "batch_indexer", "chat_agent"]
REQUEST_CLASSES = ["default", "cheap_classification", "long_form_generation", "mock_testing"]


async def send_traffic_worker(worker_id: int, client: httpx.AsyncClient, duration_seconds: int = 120):
    start = time.time()
    count = 0

    while time.time() - start < duration_seconds:
        tenant = random.choice(TENANTS)
        feature = random.choice(FEATURES)
        req_class = random.choice(REQUEST_CLASSES)
        priority = "interactive" if random.random() > 0.15 else "deferrable"
        idemp = f"seed-{worker_id}-{int(time.time()*1000)}-{random.randint(100,999)}"

        payload = {
            "messages": [{"role": "user", "content": f"Sample query {random.randint(1, 1000)}"}],
            "tenant": tenant,
            "feature": feature,
            "priority": priority,
            "request_class": req_class,
            "idempotency_key": idemp,
        }

        try:
            resp = await client.post("/v1/chat/completions", json=payload)
            count += 1
            if count % 10 == 0:
                print(f"[Worker {worker_id}] Sent {count} requests. Latest status: {resp.status_code}")
        except Exception as e:
            print(f"[Worker {worker_id}] Request error: {e}")

        await asyncio.sleep(random.uniform(0.1, 0.6))


async def main():
    print("Starting Grafana dashboard traffic generator (5 concurrent workers for 120 seconds)...")
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=15.0) as client:
        tasks = [send_traffic_worker(i, client) for i in range(5)]
        await asyncio.gather(*tasks)
    print("Traffic generation complete.")


if __name__ == "__main__":
    asyncio.run(main())
