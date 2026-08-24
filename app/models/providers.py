"""Model configurations and provider registry."""

from typing import Dict, List
from pydantic import BaseModel


class ProviderConfig(BaseModel):
    provider_name: str
    litellm_model_id: str
    quality_tier: str  # 'fast', 'premium', 'local', 'economy'
    cost_per_input_token: float  # USD per token
    cost_per_output_token: float  # USD per token
    timeout_seconds: float = 15.0


# Provider registry with realistic pricing
PROVIDER_REGISTRY: Dict[str, ProviderConfig] = {
    "groq-llama": ProviderConfig(
        provider_name="groq-llama",
        litellm_model_id="groq/llama-3.3-70b-versatile",
        quality_tier="fast",
        cost_per_input_token=0.00000059,  # $0.59 per 1M tokens
        cost_per_output_token=0.00000079,  # $0.79 per 1M tokens
        timeout_seconds=10.0,
    ),
    "anthropic-claude": ProviderConfig(
        provider_name="anthropic-claude",
        litellm_model_id="anthropic/claude-3-5-sonnet-20241022",
        quality_tier="premium",
        cost_per_input_token=0.000003,  # $3.00 per 1M tokens
        cost_per_output_token=0.000015,  # $15.00 per 1M tokens
        timeout_seconds=25.0,
    ),
    "ollama-local": ProviderConfig(
        provider_name="ollama-local",
        litellm_model_id="ollama/llama3.2:1b",
        quality_tier="local",
        cost_per_input_token=0.0,  # Free / local compute
        cost_per_output_token=0.0,
        timeout_seconds=30.0,
    ),
    "gemini-flash": ProviderConfig(
        provider_name="gemini-flash",
        litellm_model_id="gemini/gemini-2.0-flash",
        quality_tier="fast",
        cost_per_input_token=0.00000010,  # $0.10 per 1M tokens
        cost_per_output_token=0.00000040,  # $0.40 per 1M tokens
        timeout_seconds=15.0,
    ),
    "puter-grok": ProviderConfig(
        provider_name="puter-grok",
        litellm_model_id="openai/grok-2",
        quality_tier="premium",
        cost_per_input_token=0.0,  # Free tier via Puter
        cost_per_output_token=0.0,
        timeout_seconds=25.0,
    ),
    "puter-claude": ProviderConfig(
        provider_name="puter-claude",
        litellm_model_id="openai/claude-3-5-sonnet",
        quality_tier="premium",
        cost_per_input_token=0.0,  # Free tier via Puter
        cost_per_output_token=0.0,
        timeout_seconds=25.0,
    ),
    "mock-provider-a": ProviderConfig(
        provider_name="mock-provider-a",
        litellm_model_id="mock/model-a",
        quality_tier="fast",
        cost_per_input_token=0.0000005,
        cost_per_output_token=0.000001,
        timeout_seconds=5.0,
    ),
    "mock-provider-b": ProviderConfig(
        provider_name="mock-provider-b",
        litellm_model_id="mock/model-b",
        quality_tier="premium",
        cost_per_input_token=0.000002,
        cost_per_output_token=0.000008,
        timeout_seconds=5.0,
    ),
}

# Request class preference lists per PRD Section 4 Phase 3
DEFAULT_PREFERENCE_LISTS: Dict[str, List[str]] = {
    "cheap_classification": ["gemini-flash", "puter-grok", "ollama-local", "groq-llama", "anthropic-claude"],
    "long_form_generation": ["puter-claude", "anthropic-claude", "gemini-flash", "puter-grok", "groq-llama", "ollama-local"],
    "default": ["gemini-flash", "puter-grok", "puter-claude", "groq-llama", "anthropic-claude", "ollama-local"],
    "mock_testing": ["mock-provider-a", "mock-provider-b"],
}


def get_provider_config(provider_name: str) -> ProviderConfig:
    if provider_name not in PROVIDER_REGISTRY:
        raise KeyError(f"Unknown provider '{provider_name}'. Available: {list(PROVIDER_REGISTRY.keys())}")
    return PROVIDER_REGISTRY[provider_name]


def calculate_cost(provider_name: str, prompt_tokens: int, completion_tokens: int) -> float:
    config = get_provider_config(provider_name)
    return (prompt_tokens * config.cost_per_input_token) + (completion_tokens * config.cost_per_output_token)
