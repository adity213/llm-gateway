"""Error taxonomy and exception handling for LLM Gateway."""

from enum import Enum
from typing import Optional


class ErrorTaxonomy(str, Enum):
    """Explicit error categories per PRD Section 4 Phase 2."""
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    SERVER_ERROR = "server_error"
    CONTENT_FILTER = "content_filter"
    AUTH_FAILURE = "auth_failure"
    UNKNOWN = "unknown"


class GatewayException(Exception):
    """Base exception for all gateway errors."""

    def __init__(self, message: str, status_code: int = 500, category: ErrorTaxonomy = ErrorTaxonomy.UNKNOWN):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.category = category


class ProviderCallException(GatewayException):
    """Exception when a provider call fails."""

    def __init__(
        self,
        provider_name: str,
        message: str,
        category: ErrorTaxonomy = ErrorTaxonomy.UNKNOWN,
        status_code: int = 502,
        original_exception: Optional[Exception] = None,
    ):
        super().__init__(message, status_code=status_code, category=category)
        self.provider_name = provider_name
        self.original_exception = original_exception


class AllProvidersUnavailableException(GatewayException):
    """Raised when all candidate providers have Open circuit breakers or are failing."""

    def __init__(self, message: str = "All upstream providers are currently unavailable."):
        super().__init__(message, status_code=503, category=ErrorTaxonomy.SERVER_ERROR)


def classify_exception(exc: Exception) -> ErrorTaxonomy:
    """
    Classifies any thrown exception (LiteLLM, httpx, asyncio, etc.) into the strict ErrorTaxonomy.
    """
    exc_name = exc.__class__.__name__.lower()
    exc_str = str(exc).lower()

    # Rate limiting
    if "ratelimit" in exc_name or "429" in exc_str or "rate limit" in exc_str or "quota" in exc_str:
        return ErrorTaxonomy.RATE_LIMIT

    # Timeouts
    if "timeout" in exc_name or "timed out" in exc_str or "deadline" in exc_str:
        return ErrorTaxonomy.TIMEOUT

    # Authentication / Authorization
    if "auth" in exc_name or "permission" in exc_name or "401" in exc_str or "403" in exc_str or "api key" in exc_str or "unauthorized" in exc_str:
        return ErrorTaxonomy.AUTH_FAILURE

    # Content Moderation / Filters
    if "contentfilter" in exc_name or "moderation" in exc_str or "safety" in exc_str or "content policy" in exc_str:
        return ErrorTaxonomy.CONTENT_FILTER

    # Upstream Server Errors (5xx, Bad Gateway, Service Unavailable)
    if "servererror" in exc_name or "500" in exc_str or "502" in exc_str or "503" in exc_str or "504" in exc_str or "bad gateway" in exc_str or "service unavailable" in exc_str or "connection" in exc_name:
        return ErrorTaxonomy.SERVER_ERROR

    return ErrorTaxonomy.UNKNOWN
