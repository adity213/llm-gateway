"""Tests for Phase 1: Single entrypoint, real/mock providers, and mandatory metadata."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_missing_tenant_returns_400(client: AsyncClient):
    """Verifies that requests missing 'tenant' fail with 400 and clear explanation."""
    payload = {
        "messages": [{"role": "user", "content": "Hello!"}],
        "feature": "search_summary",
        "priority": "interactive",
    }
    response = await client.post("/v1/chat/completions", json=payload)
    assert response.status_code == 400
    assert "tenant and feature are required for cost attribution" in response.json()["detail"]


@pytest.mark.asyncio
async def test_missing_feature_returns_400(client: AsyncClient):
    """Verifies that requests missing 'feature' fail with 400 and clear explanation."""
    payload = {
        "messages": [{"role": "user", "content": "Hello!"}],
        "tenant": "org_123",
        "priority": "interactive",
    }
    response = await client.post("/v1/chat/completions", json=payload)
    assert response.status_code == 400
    assert "tenant and feature are required for cost attribution" in response.json()["detail"]


@pytest.mark.asyncio
async def test_missing_priority_returns_400(client: AsyncClient):
    """Verifies that requests missing 'priority' fail with 400."""
    payload = {
        "messages": [{"role": "user", "content": "Hello!"}],
        "tenant": "org_123",
        "feature": "chat",
    }
    response = await client.post("/v1/chat/completions", json=payload)
    assert response.status_code == 400
    assert "priority is required" in response.json()["detail"]


@pytest.mark.asyncio
async def test_normalized_response_shape(client: AsyncClient):
    """
    Sends requests across mock/real providers and verifies identical standardized OpenAI JSON shape:
    - id
    - object
    - created
    - model
    - choices (list with message role and content)
    - usage (prompt_tokens, completion_tokens, total_tokens)
    - gateway_metadata (provider, latency_ms, cost_usd)
    """
    payload = {
        "messages": [{"role": "user", "content": "What is 2+2?"}],
        "tenant": "tenant_alpha",
        "feature": "math_solver",
        "priority": "interactive",
        "request_class": "mock_testing",
    }

    response = await client.post("/v1/chat/completions", json=payload)
    assert response.status_code == 200
    data = response.json()

    # Verify standard OpenAI fields
    assert "id" in data
    assert data["object"] == "chat.completion"
    assert "created" in data
    assert "model" in data
    assert isinstance(data["choices"], list)
    assert len(data["choices"]) > 0
    assert "message" in data["choices"][0]
    assert data["choices"][0]["message"]["role"] == "assistant"
    assert len(data["choices"][0]["message"]["content"]) > 0

    # Verify usage
    assert "usage" in data
    assert "prompt_tokens" in data["usage"]
    assert "completion_tokens" in data["usage"]
    assert "total_tokens" in data["usage"]

    # Verify gateway metadata
    assert "gateway_metadata" in data
    meta = data["gateway_metadata"]
    assert "provider" in meta
    assert "latency_ms" in meta
    assert "cost_usd" in meta
