"""Circuit Breaker state machine for self-healing provider routing."""

from enum import Enum
import json
import time
from typing import Dict, Optional
import redis.asyncio as aioredis
from app.config import settings
from app.core.metrics import set_circuit_state_metric
from app.core.redis_client import get_redis_client
from app.resilience.sliding_window import SlidingWindowHealthTracker


class CircuitState(str, Enum):
    CLOSED = "CLOSED"        # Value 0 in Prometheus
    HALF_OPEN = "HALF_OPEN"  # Value 1 in Prometheus
    OPEN = "OPEN"            # Value 2 in Prometheus


def state_to_int(state: CircuitState) -> int:
    mapping = {
        CircuitState.CLOSED: 0,
        CircuitState.HALF_OPEN: 1,
        CircuitState.OPEN: 2,
    }
    return mapping[state]


import httpx
from app.models.schemas import ChaosModeEnum


async def probe_provider_metadata(provider_name: str) -> bool:
    """
    Performs a lightweight, zero-token HTTP metadata probe (/v1/models or health tags)
    to verify upstream network reachability and authentication without spending money on AI generation tokens.
    """
    # 1. Check if mock provider or running under test environment
    if provider_name.startswith("mock-") or settings.ENVIRONMENT == "test":
        try:
            from app.resilience.chaos import chaos_manager
            if chaos_manager.is_chaos_active(provider_name):
                status = chaos_manager.get_status(provider_name)
                if status.mode == ChaosModeEnum.FAIL_ALL:
                    return False
        except Exception:
            pass
        return True

    # 2. Real provider probe via zero-cost HTTP metadata endpoints
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            if provider_name == "ollama-local":
                resp = await client.get(f"{settings.OLLAMA_API_BASE}/api/tags")
                return resp.status_code == 200
            elif provider_name == "gemini-flash":
                if not settings.GEMINI_API_KEY:
                    return False
                url = f"https://generativelanguage.googleapis.com/v1beta/models?key={settings.GEMINI_API_KEY}"
                resp = await client.get(url)
                return resp.status_code == 200
            elif provider_name.startswith("puter-"):
                if not settings.PUTER_AUTH_TOKEN:
                    return False
                headers = {"Authorization": f"Bearer {settings.PUTER_AUTH_TOKEN}"}
                resp = await client.get("https://api.puter.com/puterai/openai/v1/models", headers=headers)
                return resp.status_code in (200, 404)
            elif provider_name == "groq-llama":
                if not settings.GROQ_API_KEY:
                    return False
                headers = {"Authorization": f"Bearer {settings.GROQ_API_KEY}"}
                resp = await client.get("https://api.groq.com/openai/v1/models", headers=headers)
                return resp.status_code == 200
            elif provider_name == "anthropic-claude":
                if not settings.ANTHROPIC_API_KEY:
                    return False
                headers = {"x-api-key": settings.ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01"}
                resp = await client.get("https://api.anthropic.com/v1/models", headers=headers)
                return resp.status_code in (200, 400)
            elif provider_name.startswith("openai"):
                if not settings.OPENAI_API_KEY:
                    return False
                headers = {"Authorization": f"Bearer {settings.OPENAI_API_KEY}"}
                resp = await client.get("https://api.openai.com/v1/models", headers=headers)
                return resp.status_code == 200
    except Exception:
        return False

    return True


class CircuitBreaker:
    """
    Implements the 3-state Circuit Breaker per PRD specifications:
    - CLOSED -> OPEN: Failure rate > 30% over >= 10 samples.
    - OPEN -> HALF_OPEN: Cooldown period elapsed (default 30s).
    - HALF_OPEN -> CLOSED: 3 consecutive successful probe requests.
    - HALF_OPEN -> OPEN: 1 probe failure immediately.
    """

    def __init__(
        self,
        provider_name: str,
        health_tracker: SlidingWindowHealthTracker,
        min_samples: int = settings.BREAKER_MIN_SAMPLES,
        failure_rate_threshold: float = settings.BREAKER_FAILURE_RATE_THRESHOLD,
        cooldown_seconds: int = settings.BREAKER_COOLDOWN_SECONDS,
        successful_probes_required: int = settings.BREAKER_SUCCESSFUL_PROBES_REQUIRED,
    ):
        self.provider_name = provider_name
        self.health_tracker = health_tracker
        self.min_samples = min_samples
        self.failure_rate_threshold = failure_rate_threshold
        self.cooldown_seconds = cooldown_seconds
        self.successful_probes_required = successful_probes_required

        # In-memory fallback tracking
        self._local_state: CircuitState = CircuitState.CLOSED
        self._local_opened_at: float = 0.0
        self._local_consecutive_probes: int = 0
        self._probe_in_flight: bool = False

    def _get_key(self) -> str:
        return f"gateway:breaker:{self.provider_name}"

    async def get_state(self, redis_client: Optional[aioredis.Redis] = None) -> CircuitState:
        """Determines current circuit breaker state, handling automatic transition from OPEN -> HALF_OPEN."""
        client = redis_client or await get_redis_client()
        key = self._get_key()
        raw = await client.get(key)
        now = time.time()

        if not raw:
            # Check local state fallback
            if self._local_state == CircuitState.OPEN and (now - self._local_opened_at >= self.cooldown_seconds):
                self._local_state = CircuitState.HALF_OPEN
                self._local_consecutive_probes = 0
            set_circuit_state_metric(self.provider_name, state_to_int(self._local_state))
            return self._local_state

        try:
            data = json.loads(raw)
            state = CircuitState(data.get("state", CircuitState.CLOSED.value))
            opened_at = data.get("opened_at", 0.0)

            # Auto transition OPEN -> HALF_OPEN after cooldown
            if state == CircuitState.OPEN and (now - opened_at >= self.cooldown_seconds):
                state = CircuitState.HALF_OPEN
                data["state"] = state.value
                data["consecutive_probes"] = 0
                await client.set(key, json.dumps(data))
                self._local_state = state
                self._local_consecutive_probes = 0

            set_circuit_state_metric(self.provider_name, state_to_int(state))
            return state
        except Exception:
            return CircuitState.CLOSED

    async def can_admit_traffic(self, redis_client: Optional[aioredis.Redis] = None) -> bool:
        """
        Returns True if traffic is allowed to this provider.
        - CLOSED: True
        - OPEN: False
        - HALF_OPEN: True only for a single probe request at a time after verifying zero-cost metadata probe.
        """
        state = await self.get_state(redis_client)
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.OPEN:
            return False

        # State is HALF_OPEN: probe admission check
        client = redis_client or await get_redis_client()
        probe_lock_key = f"gateway:breaker:probe_lock:{self.provider_name}"
        # Try to acquire probe permit lock with 15s expiration
        acquired = await client.set(probe_lock_key, "1", nx=True, ex=15)
        if acquired:
            # Zero-cost metadata / models endpoint pre-check (Option A)
            is_metadata_healthy = await probe_provider_metadata(self.provider_name)
            if not is_metadata_healthy:
                # Upstream metadata probe failed -> trip immediately back to OPEN without wasting prompt tokens!
                await self.record_failure(client)
                return False

            self._probe_in_flight = True
            return True
        return False

    async def record_success(self, redis_client: Optional[aioredis.Redis] = None) -> None:
        """Records a successful request, progressing HALF_OPEN probe count towards CLOSED."""
        client = redis_client or await get_redis_client()
        key = self._get_key()
        probe_lock_key = f"gateway:breaker:probe_lock:{self.provider_name}"
        await client.delete(probe_lock_key)
        self._probe_in_flight = False

        state = await self.get_state(client)
        if state == CircuitState.HALF_OPEN:
            raw = await client.get(key)
            data = json.loads(raw) if raw else {"state": state.value, "consecutive_probes": 0}
            consecutive = data.get("consecutive_probes", 0) + 1

            if consecutive >= self.successful_probes_required:
                # Fully healed: HALF_OPEN -> CLOSED
                data["state"] = CircuitState.CLOSED.value
                data["consecutive_probes"] = 0
                await client.set(key, json.dumps(data))
                self._local_state = CircuitState.CLOSED
                self._local_consecutive_probes = 0
                set_circuit_state_metric(self.provider_name, state_to_int(CircuitState.CLOSED))
            else:
                data["consecutive_probes"] = consecutive
                await client.set(key, json.dumps(data))
                self._local_consecutive_probes = consecutive

    async def record_failure(self, redis_client: Optional[aioredis.Redis] = None) -> None:
        """
        Records a failed request:
        - If HALF_OPEN: immediately trips back to OPEN (no second chance).
        - If CLOSED: checks sliding window failure rate; trips to OPEN if threshold exceeded.
        """
        client = redis_client or await get_redis_client()
        key = self._get_key()
        probe_lock_key = f"gateway:breaker:probe_lock:{self.provider_name}"
        await client.delete(probe_lock_key)
        self._probe_in_flight = False

        state = await self.get_state(client)
        now = time.time()

        if state == CircuitState.HALF_OPEN:
            # Immediate trip back to OPEN
            payload = {
                "state": CircuitState.OPEN.value,
                "opened_at": now,
                "consecutive_probes": 0,
            }
            await client.set(key, json.dumps(payload))
            self._local_state = CircuitState.OPEN
            self._local_opened_at = now
            self._local_consecutive_probes = 0
            set_circuit_state_metric(self.provider_name, state_to_int(CircuitState.OPEN))
            return

        if state == CircuitState.CLOSED:
            metrics = await self.health_tracker.get_metrics(self.provider_name, client)
            if metrics.total_samples >= self.min_samples and metrics.failure_rate > self.failure_rate_threshold:
                # Trip: CLOSED -> OPEN
                payload = {
                    "state": CircuitState.OPEN.value,
                    "opened_at": now,
                    "consecutive_probes": 0,
                }
                await client.set(key, json.dumps(payload))
                self._local_state = CircuitState.OPEN
                self._local_opened_at = now
                set_circuit_state_metric(self.provider_name, state_to_int(CircuitState.OPEN))

    async def reset(self, redis_client: Optional[aioredis.Redis] = None) -> None:
        """Manually forces breaker back to CLOSED state."""
        client = redis_client or await get_redis_client()
        key = self._get_key()
        payload = {"state": CircuitState.CLOSED.value, "consecutive_probes": 0, "opened_at": 0.0}
        await client.set(key, json.dumps(payload))
        self._local_state = CircuitState.CLOSED
        self._local_consecutive_probes = 0
        set_circuit_state_metric(self.provider_name, state_to_int(CircuitState.CLOSED))
