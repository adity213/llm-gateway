"""Application settings and configuration."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    """Central configuration for Self-Healing LLM Gateway."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Server settings
    ENVIRONMENT: str = "development"
    PORT: int = 8000
    HOST: str = "0.0.0.0"

    # Redis settings
    REDIS_URL: str = "redis://localhost:6379/0"

    # Provider API keys / URLs
    GROQ_API_KEY: str = ""
    ANTHROPIC_API_KEY: str = ""
    OPENAI_API_KEY: str = ""
    GEMINI_API_KEY: str = ""
    OLLAMA_API_BASE: str = "http://localhost:11434"

    # Health Tracking & Sliding Window
    HEALTH_WINDOW_SECONDS: int = 60

    # Circuit Breaker settings
    BREAKER_MIN_SAMPLES: int = 10
    BREAKER_FAILURE_RATE_THRESHOLD: float = 0.30
    BREAKER_COOLDOWN_SECONDS: int = 30
    BREAKER_SUCCESSFUL_PROBES_REQUIRED: int = 3

    # Chaos Injection
    ENABLE_CHAOS_ENDPOINT: bool = False

    # Idempotency & Queuing
    IDEMPOTENCY_TTL_SECONDS: int = 86400  # 24 hours
    MAX_DEFERRABLE_RETRIES: int = 5
    MAX_DEFERRABLE_TIMEOUT_SECONDS: int = 600  # 10 minutes
    QUEUE_BACKOFF_BASE_SECONDS: float = 1.0
    QUEUE_BACKOFF_JITTER_SECONDS: float = 0.5


settings = Settings()
