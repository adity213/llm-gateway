"""Normalized provider invocation layer using LiteLLM with test stub support."""

import asyncio
import os
import time
from typing import Any, Callable, Dict, List, Optional
from app.config import settings
from app.core.errors import ProviderCallException, classify_exception
from app.models.providers import ProviderConfig, calculate_cost
from app.models.schemas import (
    ChatCompletionChoice,
    ChatCompletionChoiceMessage,
    ChatCompletionResponse,
    GatewayMetadata,
    UsageInfo,
)

# Lazy litellm loader to prevent slow package initialization during imports and tests
_litellm = None


def _get_litellm():
    global _litellm
    if _litellm is None:
        import litellm
        litellm.drop_params = True
        litellm.telemetry = False
        _litellm = litellm
    return _litellm


# Global stub handler for tests/CI to guarantee zero external API calls and zero costs
_provider_stub_handler: Optional[Callable[..., Any]] = None


def set_provider_stub_handler(handler: Optional[Callable[..., Any]]) -> None:
    """Sets an injectable mock/stub handler for provider calls in unit/integration tests."""
    global _provider_stub_handler
    _provider_stub_handler = handler


def reset_provider_stub_handler() -> None:
    """Resets the test stub handler back to None (real provider calling)."""
    global _provider_stub_handler
    _provider_stub_handler = None


async def _create_mock_response(
    config: ProviderConfig,
    messages: List[Dict[str, Any]],
    start_time: float,
) -> ChatCompletionResponse:
    duration_ms = max(5.0, (time.time() - start_time) * 1000)
    prompt_tokens = sum(len(str(m.get("content", "")).split()) for m in messages) + 10
    completion_tokens = 25
    cost = calculate_cost(config.provider_name, prompt_tokens, completion_tokens)

    return ChatCompletionResponse(
        id=f"chatcmpl-{config.provider_name}-{int(time.time() * 1000)}",
        created=int(time.time()),
        model=config.litellm_model_id,
        choices=[
            ChatCompletionChoice(
                index=0,
                message=ChatCompletionChoiceMessage(
                    role="assistant",
                    content=f"Normalized response from {config.provider_name} [{config.litellm_model_id}]",
                ),
                finish_reason="stop",
            )
        ],
        usage=UsageInfo(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
        ),
        gateway_metadata=GatewayMetadata(
            provider=config.provider_name,
            model_used=config.litellm_model_id,
            latency_ms=round(duration_ms, 2),
            cost_usd=round(cost, 8),
        ),
    )


async def call_provider(
    config: ProviderConfig,
    messages: List[Dict[str, Any]],
    temperature: float = 0.7,
    max_tokens: Optional[int] = None,
    stream: bool = False,
    extra_headers: Optional[Dict[str, str]] = None,
) -> ChatCompletionResponse:
    """
    Invokes the requested provider via LiteLLM with strict timeout, normalization, and test stubbing.
    """
    start_time = time.time()
    provider_name = config.provider_name

    # 1. Check custom test stub handler if registered (CI / Mock testing)
    if _provider_stub_handler is not None:
        result = _provider_stub_handler(config, messages, temperature, max_tokens)
        if asyncio.iscoroutine(result):
            return await result
        return result

    # 2. Check if running in test environment or using mock provider
    if settings.ENVIRONMENT == "test" or provider_name.startswith("mock-"):
        await asyncio.sleep(0.01)
        return await _create_mock_response(config, messages, start_time)

    # 3. Setup real API credentials if present
    if settings.GROQ_API_KEY and "GROQ_API_KEY" not in os.environ:
        os.environ["GROQ_API_KEY"] = settings.GROQ_API_KEY
    if settings.ANTHROPIC_API_KEY and "ANTHROPIC_API_KEY" not in os.environ:
        os.environ["ANTHROPIC_API_KEY"] = settings.ANTHROPIC_API_KEY
    if settings.OPENAI_API_KEY and "OPENAI_API_KEY" not in os.environ:
        os.environ["OPENAI_API_KEY"] = settings.OPENAI_API_KEY

    # 4. Real call via LiteLLM
    kwargs: Dict[str, Any] = {
        "model": config.litellm_model_id,
        "messages": messages,
        "temperature": temperature,
        "timeout": config.timeout_seconds,
    }
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    if config.provider_name == "ollama-local":
        kwargs["api_base"] = settings.OLLAMA_API_BASE

    try:
        litellm_lib = _get_litellm()
        response = await litellm_lib.acompletion(**kwargs)
        duration_ms = (time.time() - start_time) * 1000

        # Extract choices & usage
        first_choice = response.choices[0]
        content = first_choice.message.content or ""
        finish_reason = getattr(first_choice, "finish_reason", "stop")

        usage = response.get("usage", {})
        prompt_tokens = usage.get("prompt_tokens", 10)
        completion_tokens = usage.get("completion_tokens", 20)
        total_tokens = usage.get("total_tokens", prompt_tokens + completion_tokens)

        cost = calculate_cost(provider_name, prompt_tokens, completion_tokens)

        return ChatCompletionResponse(
            id=str(response.get("id", f"chatcmpl-{int(time.time()*1000)}")),
            created=int(response.get("created", time.time())),
            model=config.litellm_model_id,
            choices=[
                ChatCompletionChoice(
                    index=0,
                    message=ChatCompletionChoiceMessage(
                        role="assistant",
                        content=content,
                    ),
                    finish_reason=finish_reason,
                )
            ],
            usage=UsageInfo(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
            ),
            gateway_metadata=GatewayMetadata(
                provider=provider_name,
                model_used=config.litellm_model_id,
                latency_ms=round(duration_ms, 2),
                cost_usd=round(cost, 8),
            ),
        )
    except Exception as exc:
        category = classify_exception(exc)
        raise ProviderCallException(
            provider_name=provider_name,
            message=f"Call to provider '{provider_name}' failed: {str(exc)}",
            category=category,
            original_exception=exc,
        ) from exc
