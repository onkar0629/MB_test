"""Rate limiting and backoff logic for the Slack Web API.

Slack returns HTTP 429 when rate limits are exceeded, along with a
Retry-After header indicating how many seconds to pause.
"""
from __future__ import annotations


def retry_after_seconds(headers: dict[str, str], default: float = 5.0) -> float:
    """Extract seconds to wait from Retry-After header, or return default."""
    norm = {k.lower(): v for k, v in headers.items()}
    val = norm.get("retry-after")
    if val is None:
        return default
    try:
        sec = float(val)
        return max(sec, 0.0)
    except ValueError:
        return default


def backoff_seconds(attempt: int, base: float = 1.0, max_backoff: float = 60.0) -> float:
    """Exponential backoff with cap."""
    factor = min(base * (2 ** (attempt - 1)), max_backoff)
    return factor
