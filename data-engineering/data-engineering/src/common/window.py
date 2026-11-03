"""Ingest-window arithmetic.

The window is expressed against Fathom's `created_at`, not the meeting start
time: a meeting's record is created when Fathom finishes processing, which can
be well after the call ended. Watermarking on start time silently drops late
arrivals.

Every run also reaches back by `overlap_hours` because Fathom revises
transcripts and summaries after first publish. That deliberately re-fetches
known data; bronze accepts the duplicates and silver collapses them.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

DEFAULT_OVERLAP_HOURS = 48


def compute_window(
    last_watermark: datetime | None,
    now: datetime,
    default_start: datetime,
    overlap_hours: int = DEFAULT_OVERLAP_HOURS,
) -> tuple[datetime, datetime]:
    """Return the (created_after, created_before) window for this run."""
    now_utc = _to_utc(now, "now")
    default_utc = _to_utc(default_start, "default_start")

    if last_watermark is None:
        start = default_utc
    else:
        start = _to_utc(last_watermark, "last_watermark") - timedelta(
            hours=overlap_hours
        )

    # A watermark ahead of `now` means clock skew or a bad write. Clamp rather
    # than emit an inverted window the API would reject.
    return min(start, now_utc), now_utc


def _to_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)
