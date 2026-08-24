"""Health check endpoint."""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter(tags=["Health"])


@router.get("/health")
async def health_check():
    """Health check endpoint returning 200 OK with {'status': 'ok'}."""
    return JSONResponse(status_code=200, content={"status": "ok"})
