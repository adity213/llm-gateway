"""Preference-list based router with circuit breaker integration."""

import time
from typing import Dict, List, Optional
from app.core.errors import AllProvidersUnavailableException, ProviderCallException
from app.core.metrics import record_error_metrics, record_request_metrics
from app.core.provider_client import call_provider
from app.models.providers import (
    DEFAULT_PREFERENCE_LISTS,
    PROVIDER_REGISTRY,
    get_provider_config,
)
from app.models.schemas import ChatCompletionRequest, ChatCompletionResponse
from app.resilience.chaos import chaos_manager
from app.resilience.circuit_breaker import CircuitBreaker
from app.resilience.sliding_window import SlidingWindowHealthTracker


class GatewayRouter:
    """Routes requests across providers using priority preference lists and circuit breaker health."""

    def __init__(
        self,
        health_tracker: Optional[SlidingWindowHealthTracker] = None,
        preference_lists: Optional[Dict[str, List[str]]] = None,
    ):
        self.health_tracker = health_tracker or SlidingWindowHealthTracker()
        self.preference_lists = preference_lists or DEFAULT_PREFERENCE_LISTS
        self.breakers: Dict[str, CircuitBreaker] = {
            name: CircuitBreaker(provider_name=name, health_tracker=self.health_tracker)
            for name in PROVIDER_REGISTRY
        }

    def get_preference_list(self, request_class: Optional[str]) -> List[str]:
        """Returns the ordered list of provider candidates for a given request class."""
        if request_class and request_class in self.preference_lists:
            return self.preference_lists[request_class]
        return self.preference_lists.get("default", list(PROVIDER_REGISTRY.keys()))

    async def route_and_execute(
        self,
        request: ChatCompletionRequest,
    ) -> ChatCompletionResponse:
        """
        Executes request against the highest-priority healthy provider.
        Automatically fails over to subsequent candidates if a provider error occurs.
        """
        candidates = self.get_preference_list(request.request_class)
        last_exception: Optional[Exception] = None
        attempt_count = 0

        for provider_name in candidates:
            if provider_name not in self.breakers:
                continue

            breaker = self.breakers[provider_name]
            can_admit = await breaker.can_admit_traffic()
            if not can_admit:
                # Breaker is OPEN or HALF_OPEN with probe permit already taken; skip to next
                continue

            # Attempt provider execution
            attempt_count += 1
            start_time = time.time()
            config = get_provider_config(provider_name)
            messages_dict = [m.model_dump() for m in request.messages]

            try:
                # 1. Apply Chaos Fault Injection if active (Phase 5)
                await chaos_manager.apply_chaos_if_active(provider_name)

                # 2. Call provider via LiteLLM normalized wrapper
                response = await call_provider(
                    config=config,
                    messages=messages_dict,
                    temperature=request.temperature or 0.7,
                    max_tokens=request.max_tokens,
                    stream=request.stream or False,
                )

                latency_ms = (time.time() - start_time) * 1000

                # 3. Record success in health tracker and circuit breaker
                await self.health_tracker.record_outcome(
                    provider_name=provider_name,
                    success=True,
                    latency_ms=latency_ms,
                    error_category=None,
                )
                await breaker.record_success()

                # 4. Record Prometheus metrics
                cost = response.gateway_metadata.cost_usd if response.gateway_metadata else 0.0
                record_request_metrics(
                    provider=provider_name,
                    tenant=request.tenant,
                    feature=request.feature,
                    status="success",
                    duration_seconds=latency_ms / 1000.0,
                    cost_usd=cost,
                )

                if response.gateway_metadata:
                    response.gateway_metadata.retries = attempt_count - 1

                return response

            except ProviderCallException as pce:
                last_exception = pce
                latency_ms = (time.time() - start_time) * 1000

                # Record error in health window & breaker
                await self.health_tracker.record_outcome(
                    provider_name=provider_name,
                    success=False,
                    latency_ms=latency_ms,
                    error_category=pce.category,
                )
                await breaker.record_failure()

                # Record error in Prometheus
                record_error_metrics(provider=provider_name, category=pce.category.value)
                record_request_metrics(
                    provider=provider_name,
                    tenant=request.tenant,
                    feature=request.feature,
                    status="error",
                    duration_seconds=latency_ms / 1000.0,
                )
                # Continue loop to failover to next candidate

            except Exception as exc:
                last_exception = exc
                latency_ms = (time.time() - start_time) * 1000
                await breaker.record_failure()
                # Continue loop to next candidate

        # If all candidates were skipped or failed
        raise AllProvidersUnavailableException(
            f"All providers in preference list {candidates} are unavailable. Last error: {str(last_exception)}"
        )


router = GatewayRouter()
