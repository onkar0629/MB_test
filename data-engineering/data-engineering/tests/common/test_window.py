from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.common.window import compute_window

NOW = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)
DEFAULT_START = datetime(2024, 1, 1, tzinfo=timezone.utc)


def test_first_run_starts_at_the_default():
    start, end = compute_window(None, NOW, DEFAULT_START)
    assert (start, end) == (DEFAULT_START, NOW)


def test_later_runs_reach_back_by_the_overlap():
    last = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)
    start, end = compute_window(last, NOW, DEFAULT_START, overlap_hours=48)
    assert start == last - timedelta(hours=48)
    assert end == NOW


def test_overlap_never_pushes_start_past_end():
    last = NOW + timedelta(days=5)  # clock skew or a bad write
    start, end = compute_window(last, NOW, DEFAULT_START)
    assert start <= end


def test_naive_datetimes_are_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        compute_window(None, datetime(2026, 9, 18, 12, 0), DEFAULT_START)


def test_non_utc_input_is_normalised_to_utc():
    eastern = timezone(timedelta(hours=-4))
    last = datetime(2026, 9, 17, 8, 0, tzinfo=eastern)  # == 12:00Z
    start, _ = compute_window(last, NOW, DEFAULT_START, overlap_hours=1)
    assert start == datetime(2026, 9, 17, 11, 0, tzinfo=timezone.utc)
