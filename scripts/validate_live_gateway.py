"""
End-to-End Validation Script
Tests live gateway routing, response quality, and fallback behavior.
"""

import httpx
import json
import time

BASE_URL = "http://localhost:8000"

def test_chat(prompt: str, request_class: str = "default", priority: str = "interactive"):
    print(f"\n=======================================================")
    print(f"Testing Prompt: \"{prompt}\"")
    print(f"Request Class: {request_class} | Priority: {priority}")
    print(f"=======================================================")
    
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "tenant": "engineering_qa",
        "feature": "live_validation",
        "priority": priority,
        "request_class": request_class,
        "idempotency_key": f"val-{int(time.time()*1000)}" if priority == "deferrable" else None,
    }
    
    start = time.time()
    try:
        with httpx.Client(base_url=BASE_URL, timeout=30.0) as client:
            resp = client.post("/v1/chat/completions", json=payload)
            elapsed_ms = (time.time() - start) * 1000
            
            print(f"HTTP Status: {resp.status_code} ({elapsed_ms:.1f}ms)")
            if resp.status_code in [200, 202]:
                data = resp.json()
                choice = data.get("choices", [{}])[0]
                content = choice.get("message", {}).get("content", "")
                provider = data.get("gateway_metadata", {}).get("provider", "unknown")
                tokens = data.get("usage", {})
                cost = data.get("gateway_metadata", {}).get("cost_usd", 0.0)
                
                print(f"Selected Provider : {provider}")
                print(f"AI Response       : {content.strip()}")
                print(f"Token Usage       : {tokens.get('total_tokens', 0)} tokens (Prompt: {tokens.get('prompt_tokens', 0)}, Completion: {tokens.get('completion_tokens', 0)})")
                print(f"Calculated Cost   : ${cost:.8f}")
            else:
                print(f"Response Error: {resp.text}")
    except Exception as e:
        print(f"Connection Exception: {e}")

if __name__ == "__main__":
    print("Sending live test requests through the Self-Healing LLM Gateway...")
    
    # 1. Quick Math / Default Class
    test_chat("What is the capital of France? Reply in one sentence.", request_class="default")
    
    # 2. Mock Testing (Instant local validation)
    test_chat("Explain quantum mechanics to a 10 year old.", request_class="mock_testing")
    
    # 3. Deferrable background job
    test_chat("Summarize the history of space exploration in 3 bullet points.", request_class="cheap_classification", priority="deferrable")
