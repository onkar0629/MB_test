from __future__ import annotations

import pytest

from src.ingestion.fathom.ratelimit import (
    RateLimitState,
    backoff_seconds,
    pause_seconds,
)


def test_reads_headers_case_insensitively():
    state = RateLimitState.from_headers(
        {"ratelimit-remaining": "7", "RateLimit-Reset": "31"}
    )
    assert state == RateLimitState(remaining=7, reset_seconds=31.0)


def test_missing_headers_produce_an_unknown_state():
    assert RateLimitState.from_headers({}) == RateLimitState(None, None)


def test_no_pause_while_budget_remains():
    assert pause_seconds(RateLimitState(remaining=20, reset_seconds=30.0)) == 0.0


def test_pauses_until_reset_when_budget_is_nearly_gone():
    assert pause_seconds(RateLimitState(remaining=1, reset_seconds=30.0)) == 30.0


def test_unknown_state_never_pauses():
    assert pause_seconds(RateLimitState(None, None)) == 0.0


def test_backoff_grows_exponentially_and_is_capped():
    full = 1.0  # full jitter -> the whole window
    assert backoff_seconds(1, jitter=full) == 1.0
    assert backoff_seconds(2, jitter=full) == 2.0
    assert backoff_seconds(3, jitter=full) == 4.0
    assert backoff_seconds(99, jitter=full) == 60.0


def test_jitter_scales_the_window():
    assert backoff_seconds(3, jitter=0.5) == 2.0


def test_attempt_must_be_positive():
    with pytest.raises(ValueError, match="attempt"):
        backoff_seconds(0, jitter=1.0)
