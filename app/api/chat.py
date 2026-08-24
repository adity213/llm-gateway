"""Chat completions endpoint with idempotency, metadata enforcement, and priority handling."""

import time
from typing import Optional
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from app.core.errors import AllProvidersUnavailableException, GatewayException
from app.models.schemas import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    PriorityEnum,
    TaskStatusResponse,
)
from app.queuing.idempotency import idempotency_manager
from app.queuing.queue_manager import queue_manager
from app.resilience.router import router

api_router = APIRouter(tags=["Chat Completions"])


@api_router.post(
    "/v1/chat/completions",
    response_model=None,
    responses={
        200: {"description": "Successful completion", "model": ChatCompletionResponse},
        202: {"description": "Deferrable request queued", "model": TaskStatusResponse},
        400: {"description": "Missing metadata or bad request"},
        503: {"description": "All providers unavailable (Interactive priority)"},
    },
)
async def create_chat_completion(
    request_body: dict,
    response: Response,
    idempotency_key_header: Optional[str] = Header(None, alias="Idempotency-Key"),
    tenant_header: Optional[str] = Header(None, alias="X-Tenant-ID"),
    feature_header: Optional[str] = Header(None, alias="X-Feature-ID"),
):
    """
    OpenAI-compatible chat completions endpoint with resilient routing, self-healing, and cost tracking.
    """
    # 1. Enforce mandatory metadata (Phase 1)
    tenant = request_body.get("tenant") or tenant_header
    feature = request_body.get("feature") or feature_header

    if not tenant or not str(tenant).strip() or not feature or not str(feature).strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="tenant and feature are required for cost attribution; see README.",
        )

    # Priority check (Phase 4)
    raw_priority = request_body.get("priority")
    if not raw_priority or raw_priority not in [p.value for p in PriorityEnum]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="priority is required and must be either 'interactive' or 'deferrable'; see README.",
        )

    # Idempotency key extraction
    idempotency_key = request_body.get("idempotency_key") or idempotency_key_header
    if raw_priority == PriorityEnum.DEFERRABLE.value and not idempotency_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="idempotency_key is required for deferrable requests to guarantee safe retries; see README.",
        )

    # Construct validated request
    try:
        req_obj = ChatCompletionRequest(
            model=request_body.get("model", "default"),
            messages=request_body.get("messages", []),
            temperature=request_body.get("temperature", 0.7),
            top_p=request_body.get("top_p", 1.0),
            n=request_body.get("n", 1),
            stream=request_body.get("stream", False),
            max_tokens=request_body.get("max_tokens"),
            tenant=str(tenant).strip(),
            feature=str(feature).strip(),
            priority=PriorityEnum(raw_priority),
            request_class=request_body.get("request_class", "default"),
            idempotency_key=idempotency_key,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid request schema: {str(exc)}",
        )

    # 2. Check Idempotency Cache & In-Flight Lock (Phase 4)
    if idempotency_key:
        cached_resp = await idempotency_manager.get_cached_response(idempotency_key)
        if cached_resp:
            return JSONResponse(status_code=200, content=cached_resp.model_dump())

        # Claim in-flight execution right
        acquired = await idempotency_manager.acquire_in_flight_lock(idempotency_key)
        if not acquired:
            # Another concurrent request is actively processing this idempotency key.
            # Wait for it to complete to return the identical response without duplicate provider call.
            completed_resp = await idempotency_manager.wait_for_in_flight_result(idempotency_key, timeout_seconds=10.0)
            if completed_resp:
                return JSONResponse(status_code=200, content=completed_resp.model_dump())
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A concurrent request with this idempotency_key is currently in-flight; please retry shortly.",
            )

    # 3. Route and Execute
    try:
        completion_response = await router.route_and_execute(req_obj)

        # Store in idempotency cache if key present (releases in-flight lock)
        if idempotency_key:
            await idempotency_manager.store_response(idempotency_key, completion_response)

        return JSONResponse(status_code=200, content=completion_response.model_dump())

    except AllProvidersUnavailableException as exc:
        if idempotency_key:
            await idempotency_manager.release_in_flight_lock(idempotency_key)

        # 4. Handle Outage based on Priority Classification (Phase 4)
        if req_obj.priority == PriorityEnum.INTERACTIVE:
            # Interactive fails fast with 503 and Retry-After
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "error": {
                        "message": "All upstream providers are currently unavailable.",
                        "type": "circuit_breaker_open",
                        "code": 503,
                    }
                },
                headers={"Retry-After": "30"},
            )
        else:
            # Deferrable pushes to backoff queue
            task_status = await queue_manager.enqueue_request(req_obj)
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content=task_status.model_dump(),
            )

    except GatewayException as exc:
        if idempotency_key:
            await idempotency_manager.release_in_flight_lock(idempotency_key)
        raise HTTPException(status_code=exc.status_code, detail=exc.message)

    except Exception as exc:
        if idempotency_key:
            await idempotency_manager.release_in_flight_lock(idempotency_key)
        raise HTTPException(status_code=500, detail=f"Unexpected gateway error: {str(exc)}")
