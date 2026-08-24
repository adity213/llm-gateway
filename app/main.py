"""Main FastAPI application initialization and lifespan management."""

from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from app.api.chaos import api_router as chaos_router
from app.api.chat import api_router as chat_router
from app.api.health import router as health_router
from app.api.metrics import router as metrics_router
from app.api.tasks import router as tasks_router
from app.config import settings
from app.core.errors import GatewayException
from app.core.redis_client import close_redis_client, get_redis_client
from app.queuing.worker import queue_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("llm_gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager: connects Redis, starts background worker on startup and cleans up on shutdown."""
    logger.info("Initializing Self-Healing LLM Gateway...")
    try:
        redis_client = await get_redis_client()
        await redis_client.ping()
        logger.info("Redis connection established successfully.")
    except Exception as exc:
        logger.warning(f"Redis connection warning at startup: {exc}. In-memory fallbacks will be used.")

    # Start deferrable queue background worker
    await queue_worker.start()

    yield

    # Shutdown
    logger.info("Shutting down Self-Healing LLM Gateway...")
    await queue_worker.stop()
    await close_redis_client()
    logger.info("Shutdown complete.")


app = FastAPI(
    title="Self-Healing LLM Gateway",
    version="0.1.0",
    description="Resilient, self-healing LLM API gateway with circuit breakers, sliding window health, backoff queues, and Prometheus metrics.",
    lifespan=lifespan,
)

# Enable CORS for dashboards/UIs
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(GatewayException)
async def gateway_exception_handler(request: Request, exc: GatewayException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "message": exc.message,
                "category": exc.category.value,
                "type": exc.__class__.__name__,
            }
        },
    )


# Mount routers
app.include_router(health_router)
app.include_router(chat_router)
app.include_router(tasks_router)
app.include_router(chaos_router)
app.include_router(metrics_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=True,
    )
