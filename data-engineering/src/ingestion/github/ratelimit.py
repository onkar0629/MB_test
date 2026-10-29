"""GitHub API rate limiting and backoff arithmetic.

GitHub allows 5,000 requests per hour for authenticated requests with a Personal Access Token.
Rate limit headers:
  - X-RateLimit-Limit: Quota per hour (e.g. 5000)
  - X-RateLimit-Remaining: Calls remaining in current window
  - X-RateLimit-Reset: Unix epoch seconds when quota resets
"""
from __future__ import annotations

from dataclasses import dataclass

SAFETY_MARGIN_CALLS = 10
BACKOFF_FACTOR = 2.0
MAX_BACKOFF_SECONDS = 60.0


@dataclass(frozen=True)
class GitHubRateLimitState:
    limit: int
    remaining: int
    reset_epoch_seconds: float
    observed_at: float

    @classmethod
    def from_headers(
        cls, headers: dict[str, str], observed_at: float
    ) -> GitHubRateLimitState:
        # HTTP headers are case-insensitive
        h = {k.lower(): v for k, v in headers.items()}
        limit = int(h.get("x-ratelimit-limit", 5000))
        remaining = int(h.get("x-ratelimit-remaining", 5000))
        reset = float(h.get("x-ratelimit-reset", observed_at + 3600))
        return cls(
            limit=limit,
            remaining=remaining,
            reset_epoch_seconds=reset,
            observed_at=observed_at,
        )


def pause_seconds(
    state: GitHubRateLimitState,
    now: float,
    safety_margin: int = SAFETY_MARGIN_CALLS,
) -> float:
    """Return how many seconds to pause if the quota is critically low."""
    if state.remaining <= safety_margin:
        delay = state.reset_epoch_seconds - now
        return max(delay, 0.0) + 1.0
    return 0.0


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff duration for server errors."""
    return min(BACKOFF_FACTOR**attempt, MAX_BACKOFF_SECONDS)
