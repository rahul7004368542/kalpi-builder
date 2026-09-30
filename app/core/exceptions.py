"""Exception hierarchy. The API layer maps these to HTTP responses."""
from __future__ import annotations


class KalpiError(Exception):
    status_code = 500
    code = "INTERNAL_ERROR"

    def __init__(self, message: str = "", code: str | None = None):
        super().__init__(message)
        self.message = message
        if code:
            self.code = code


class ValidationError(KalpiError):
    status_code, code = 422, "VALIDATION_ERROR"


class ConflictError(KalpiError):
    status_code, code = 409, "CONFLICT"


class NotFoundError(KalpiError):
    status_code, code = 404, "NOT_FOUND"


class UnsupportedBrokerError(NotFoundError):
    code = "UNSUPPORTED_BROKER"


# ---- Broker-side errors: the executor's retry policy keys off these types ----
class BrokerError(KalpiError):
    status_code, code = 502, "BROKER_ERROR"


class AuthenticationError(BrokerError):
    status_code, code = 401, "BROKER_AUTH_FAILED"


class RateLimitError(BrokerError):
    """HTTP 429 / 'too many requests'. Safe to retry: the order was NOT placed."""
    code = "RATE_LIMITED"

    def __init__(self, message: str = "rate limited", retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class TransientBrokerError(BrokerError):
    """Timeout / 5xx. AMBIGUOUS: the order may or may not have been placed."""
    code = "BROKER_UNAVAILABLE"


class OrderRejectedError(BrokerError):
    """Definitive business rejection (RMS, funds, blocked scrip). Never retried."""

    def __init__(self, code: str, message: str):
        super().__init__(message, code=code)


class LiveModeNotImplemented(BrokerError):
    code = "LIVE_MODE_NOT_IMPLEMENTED"
