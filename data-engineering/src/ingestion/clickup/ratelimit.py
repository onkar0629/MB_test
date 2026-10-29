"""Pacing and retry logic for the ClickUp REST API.

ClickUp enforces rate limits per minute (typically 100/min or 1,000/min depending
on plan tier). Rate limits are tracked via X-RateLimit-* response headers.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RateLimitState:
    limit: int
    remaining: int
    reset_epoch_seconds: float

    @classmethod
    def from_headers(cls, headers: dict[str, str], now: float) -> RateLimitState | None:
        norm = {k.lower(): v for k, v in headers.items()}
        try:
            limit = int(norm.get("x-ratelimit-limit", ""))
            remaining = int(norm.get("x-ratelimit-remaining", ""))
            reset_sec = float(norm.get("x-ratelimit-reset", ""))
        except ValueError:
            return None

        reset_epoch = reset_sec if reset_sec > 1_000_000 else now + reset_sec
        return cls(limit=limit, remaining=remaining, reset_epoch_seconds=reset_epoch)


def pause_seconds(state: RateLimitState | None, now: float) -> float:
    """Return seconds to sleep before next request to pace within remaining quota."""
    if state is None or state.limit <= 0:
        return 0.0
    if state.remaining <= 2:
        return max(0.0, state.reset_epoch_seconds - now + 0.1)
    if state.remaining < state.limit * 0.1:
        time_to_reset = max(0.0, state.reset_epoch_seconds - now)
        return min(time_to_reset / max(state.remaining, 1), 2.0)
    return 0.0


def backoff_seconds(attempt: int, base: float = 1.0, max_backoff: float = 60.0) -> float:
    """Exponential backoff."""
    factor = min(base * (2 ** (attempt - 1)), max_backoff)
    return factor
