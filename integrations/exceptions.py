class ApiRequestError(Exception):
    """Generic API request error (4xx/5xx not otherwise classified)."""

class ApiAuthError(ApiRequestError):
    """Authentication or authorization failure (401/403)."""

class ApiRateLimitError(ApiRequestError):
    """Rate limiting encountered (429)."""
