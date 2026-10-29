"""Unit tests for Slack rate limiting and backoff."""
from __future__ import annotations

from src.ingestion.slack.ratelimit import backoff_seconds, retry_after_seconds


def test_retry_after_seconds_present() -> None:
    headers = {"Retry-After": "12"}
    assert retry_after_seconds(headers) == 12.0


def test_retry_after_seconds_case_insensitive() -> None:
    headers = {"retry-after": "5.5"}
    assert retry_after_seconds(headers) == 5.5


def test_retry_after_seconds_missing_uses_default() -> None:
    assert retry_after_seconds({}, default=7.0) == 7.0


def test_retry_after_seconds_invalid_uses_default() -> None:
    assert retry_after_seconds({"Retry-After": "invalid"}, default=3.0) == 3.0


def test_backoff_seconds() -> None:
    assert backoff_seconds(1) == 1.0
    assert backoff_seconds(2) == 2.0
    assert backoff_seconds(3) == 4.0
    assert backoff_seconds(10, max_backoff=30.0) == 30.0
