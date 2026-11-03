from __future__ import annotations

from src.ingestion.clickup.ratelimit import RateLimitState, pause_seconds


def test_ratelimit_from_headers():
    headers = {
        "X-RateLimit-Limit": "100",
        "X-RateLimit-Remaining": "50",
        "X-RateLimit-Reset": "1700000060",
    }
    state = RateLimitState.from_headers(headers, now=1700000000)
    assert state is not None
    assert state.limit == 100
    assert state.remaining == 50
    assert state.reset_epoch_seconds == 1700000060.0


def test_pause_seconds_when_exhausted():
    state = RateLimitState(limit=100, remaining=1, reset_epoch_seconds=105.0)
    pause = pause_seconds(state, now=100.0)
    assert pause >= 5.0
