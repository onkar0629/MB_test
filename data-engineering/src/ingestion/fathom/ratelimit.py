"""Pure rate-limit arithmetic for the Fathom client.

Fathom allows 60 calls/min overall but only 30/min for "heavy" calls -- and
every call this pipeline makes is heavy, because it sets include_transcript.
Under load that ceiling can drop to 5/min, so the client paces itself from the
response headers rather than waiting to be told off with a 429.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class RateLimitState:
    remaining: int | None
    reset_seconds: float | None

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> RateLimitState:
        lowered = {k.lower(): v for k, v in headers.items()}
        return cls(
            remaining=_as_int(lowered.get("ratelimit-remaining")),
            reset_seconds=_as_float(lowered.get("ratelimit-reset")),
        )


def pause_seconds(state: RateLimitState, floor: int = 2) -> float:
    """Seconds to wait before the next request to stay inside the budget.

    Keeping `floor` calls in reserve leaves room for a retry without tripping
    the limit.
    """
    if state.remaining is None or state.reset_seconds is None:
        return 0.0
    if state.remaining > floor:
        return 0.0
    return max(0.0, state.reset_seconds)


def backoff_seconds(
    attempt: int, jitter: float, base: float = 1.0, cap: float = 60.0
) -> float:
    """Exponential backoff with full jitter.

    `jitter` is supplied by the caller (in [0, 1]) rather than drawn here, so
    the behaviour is deterministic under test.
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    window = min(cap, base * (2 ** (attempt - 1)))
    return window * jitter


def _as_int(value: str | None) -> int | None:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _as_float(value: str | None) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
