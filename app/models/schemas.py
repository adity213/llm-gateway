"""Pydantic schemas for requests, responses, and gateway models."""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator


class PriorityEnum(str, Enum):
    INTERACTIVE = "interactive"
    DEFERRABLE = "deferrable"


class ChatMessage(BaseModel):
    role: Literal["system", "user", "assistant", "function", "tool"]
    content: Optional[Union[str, List[Dict[str, Any]]]] = ""
    name: Optional[str] = None


class ChatCompletionRequest(BaseModel):
    # Standard OpenAI fields
    model: Optional[str] = "default"
    messages: List[ChatMessage]
    temperature: Optional[float] = 0.7
    top_p: Optional[float] = 1.0
    n: Optional[int] = 1
    stream: Optional[bool] = False
    max_tokens: Optional[int] = None
    presence_penalty: Optional[float] = 0.0
    frequency_penalty: Optional[float] = 0.0

    # Mandatory Gateway metadata (Phase 1)
    tenant: str = Field(
        ...,
        description="Tenant identifier for multi-tenancy and cost attribution.",
    )
    feature: str = Field(
        ...,
        description="Feature/product area initiating the request for cost attribution.",
    )

    # Classification and resilience controls (Phases 3 & 4)
    priority: PriorityEnum = Field(
        ...,
        description="Request priority: 'interactive' (fail-fast) or 'deferrable' (queueable with backoff).",
    )
    request_class: Optional[str] = Field(
        default="default",
        description="Routing class (e.g., 'cheap_classification', 'long_form_generation', 'default').",
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Unique client-generated idempotency key to prevent duplicate side-effects (Required for deferrable priority).",
    )

    @field_validator("tenant")
    @classmethod
    def validate_tenant(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("tenant is required for cost attribution; see README.")
        return v.strip()

    @field_validator("feature")
    @classmethod
    def validate_feature(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("feature is required for cost attribution; see README.")
        return v.strip()

    @model_validator(mode="after")
    def validate_conditional_fields(self) -> "ChatCompletionRequest":
        if self.priority == PriorityEnum.DEFERRABLE and (not self.idempotency_key or not str(self.idempotency_key).strip()):
            raise ValueError("idempotency_key is required for deferrable requests to guarantee safe retries; see README.")
        return self


class UsageInfo(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class ChatCompletionChoiceMessage(BaseModel):
    role: str = "assistant"
    content: str = ""


class ChatCompletionChoice(BaseModel):
    index: int = 0
    message: ChatCompletionChoiceMessage
    finish_reason: Optional[str] = "stop"


class GatewayMetadata(BaseModel):
    provider: str
    model_used: str
    latency_ms: float
    cost_usd: float
    retries: int = 0
    cached_idempotent: bool = False


class ChatCompletionResponse(BaseModel):
    id: str
    object: str = "chat.completion"
    created: int
    model: str
    choices: List[ChatCompletionChoice]
    usage: UsageInfo
    gateway_metadata: Optional[GatewayMetadata] = None


class ChaosModeEnum(str, Enum):
    FAIL_ALL = "fail_all"
    FAIL_RATE = "fail_rate"
    ADD_LATENCY_MS = "add_latency_ms"


class ChaosRequest(BaseModel):
    mode: ChaosModeEnum
    fail_rate: Optional[float] = Field(default=0.5, ge=0.0, le=1.0)
    add_latency_ms: Optional[int] = Field(default=3000, ge=0)
    duration_seconds: int = Field(default=30, ge=1, le=3600)


class ChaosStatus(BaseModel):
    provider: str
    active: bool
    mode: Optional[ChaosModeEnum] = None
    fail_rate: Optional[float] = None
    add_latency_ms: Optional[int] = None
    expires_at: Optional[float] = None
    remaining_seconds: Optional[float] = None


class TaskStatusEnum(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskStatusResponse(BaseModel):
    task_id: str
    status: TaskStatusEnum
    created_at: float
    updated_at: float
    attempts: int = 0
    max_attempts: int
    error: Optional[str] = None
    result: Optional[ChatCompletionResponse] = None
