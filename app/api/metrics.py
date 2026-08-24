"""Prometheus metrics scrape endpoint."""

from fastapi import APIRouter, Response
from app.core.metrics import get_prometheus_metrics

router = APIRouter(tags=["Metrics"])


@router.get("/metrics")
async def metrics_endpoint():
    """
    Exposes metrics in Prometheus text format for scraping.
    """
    data, content_type = get_prometheus_metrics()
    return Response(content=data, media_type=content_type)
