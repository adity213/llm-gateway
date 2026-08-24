"""Prometheus metrics instrumentation for LLM Gateway."""

from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

# 1. Total Requests
REQUESTS_TOTAL = Counter(
    "llm_gateway_requests_total",
    "Total requests processed by LLM gateway",
    ["provider", "tenant", "feature", "status"],
)

# 2. Total Errors categorized by Error Taxonomy
ERRORS_TOTAL = Counter(
    "llm_gateway_errors_total",
    "Total errors categorized by taxonomy",
    ["provider", "category"],
)

# 3. Request Latencies (in seconds for standard Prometheus histogram)
REQUEST_DURATION_SECONDS = Histogram(
    "llm_gateway_request_duration_seconds",
    "Request duration in seconds per provider",
    ["provider"],
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0],
)

# 4. Circuit Breaker State (0 = CLOSED, 1 = HALF_OPEN, 2 = OPEN)
CIRCUIT_STATE = Gauge(
    "llm_gateway_circuit_state",
    "Current circuit breaker state (0=Closed, 1=Half-Open, 2=Open)",
    ["provider"],
)

# 5. Queue Depth
QUEUE_DEPTH = Gauge(
    "llm_gateway_queue_depth",
    "Number of requests currently in deferrable queue",
)

# 6. Cost Accumulator in USD
COST_DOLLARS_TOTAL = Counter(
    "llm_gateway_cost_dollars_total",
    "Total cost accrued in USD per tenant, feature, and provider",
    ["tenant", "feature", "provider"],
)


def record_request_metrics(
    provider: str,
    tenant: str,
    feature: str,
    status: str,
    duration_seconds: float,
    cost_usd: float = 0.0,
) -> None:
    REQUESTS_TOTAL.labels(provider=provider, tenant=tenant, feature=feature, status=status).inc()
    REQUEST_DURATION_SECONDS.labels(provider=provider).observe(duration_seconds)
    if cost_usd > 0:
        COST_DOLLARS_TOTAL.labels(tenant=tenant, feature=feature, provider=provider).inc(cost_usd)


def record_error_metrics(provider: str, category: str) -> None:
    ERRORS_TOTAL.labels(provider=provider, category=category).inc()


def set_circuit_state_metric(provider: str, state_value: int) -> None:
    CIRCUIT_STATE.labels(provider=provider).set(state_value)


def set_queue_depth_metric(depth: int) -> None:
    QUEUE_DEPTH.set(depth)


def get_prometheus_metrics() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST
