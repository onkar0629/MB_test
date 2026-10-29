from __future__ import annotations

from src.ingestion.github.ratelimit import (
    GitHubRateLimitState,
    backoff_seconds,
    pause_seconds,
)


def test_ratelimit_from_headers():
    headers = {
        "x-ratelimit-limit": "5000",
        "x-ratelimit-remaining": "4950",
        "x-ratelimit-reset": "1729600000",
    }
    state = GitHubRateLimitState.from_headers(headers, observed_at=1729590000.0)
    assert state.limit == 5000
    assert state.remaining == 4950
    assert state.reset_epoch_seconds == 1729600000.0


def test_pause_seconds_critical():
    state = GitHubRateLimitState(
        limit=5000,
        remaining=5,
        reset_epoch_seconds=1729600050.0,
        observed_at=1729600000.0,
    )
    delay = pause_seconds(state, now=1729600000.0)
    assert delay >= 51.0


def test_backoff_seconds():
    assert backoff_seconds(1) == 2.0
    assert backoff_seconds(2) == 4.0
    assert backoff_seconds(10) == 60.0
