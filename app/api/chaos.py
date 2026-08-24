"""Chaos injection endpoints for resiliency testing and live demonstration."""

from typing import Optional
from fastapi import APIRouter, HTTPException, status
from app.config import settings
from app.models.providers import PROVIDER_REGISTRY
from app.models.schemas import ChaosRequest, ChaosStatus
from app.resilience.chaos import chaos_manager
from app.resilience.router import router

api_router = APIRouter(prefix="/chaos", tags=["Chaos Testing"])


def _check_chaos_enabled():
    if not settings.ENABLE_CHAOS_ENDPOINT:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Chaos endpoint is disabled. Set ENABLE_CHAOS_ENDPOINT=true in configuration.",
        )


@api_router.post("/reset")
async def reset_all_chaos_and_breakers():
    """
    Resets all active chaos and forces all circuit breakers back to CLOSED.
    Must be defined before /{provider_name} to avoid route shadowing.
    """
    _check_chaos_enabled()
    for name, breaker in router.breakers.items():
        await chaos_manager.clear_chaos(name)
        await breaker.reset()
    return {"status": "all_reset", "message": "All chaos cleared and breakers reset to CLOSED."}


@api_router.post("/{provider_name}", response_model=ChaosStatus)
async def set_chaos(provider_name: str, request: ChaosRequest):
    """
    Injects a timed fault into the specified provider (fail_all, fail_rate, or add_latency_ms).
    """
    _check_chaos_enabled()
    if provider_name not in PROVIDER_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Unknown provider '{provider_name}'. Available: {list(PROVIDER_REGISTRY.keys())}",
        )

    status_obj = await chaos_manager.inject_chaos(provider_name, request)
    return status_obj


@api_router.get("/{provider_name}", response_model=ChaosStatus)
async def get_chaos(provider_name: str):
    """
    Retrieves current chaos status for a provider.
    """
    _check_chaos_enabled()
    return await chaos_manager.get_chaos_status(provider_name)


@api_router.delete("/{provider_name}")
async def clear_provider_chaos(provider_name: str):
    """
    Clears active chaos on a specific provider.
    """
    _check_chaos_enabled()
    await chaos_manager.clear_chaos(provider_name)
    return {"status": "cleared", "provider": provider_name}
