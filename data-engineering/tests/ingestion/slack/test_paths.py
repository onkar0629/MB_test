"""Unit tests for Slack landing paths."""
from __future__ import annotations

from src.ingestion.slack.paths import (
    build_channels_page_path,
    build_messages_page_path,
    build_users_page_path,
)


def test_build_users_page_path() -> None:
    path = build_users_page_path(
        volume_root="/Volumes/workspace/raw_slack/landing",
        ingest_date="2026-10-25",
        run_id="run-123",
        page_no=1,
    )
    expected = (
        "/Volumes/workspace/raw_slack/landing/users/ingest_date=2026-10-25/"
        "run_id=run-123/page-0001.json"
    )
    assert path == expected


def test_build_channels_page_path() -> None:
    path = build_channels_page_path(
        volume_root="/Volumes/workspace/raw_slack/landing/",
        ingest_date="2026-10-25",
        run_id="run-456",
        page_no=2,
    )
    expected = (
        "/Volumes/workspace/raw_slack/landing/channels/ingest_date=2026-10-25/"
        "run_id=run-456/page-0002.json"
    )
    assert path == expected


def test_build_messages_page_path() -> None:
    path = build_messages_page_path(
        volume_root="/Volumes/workspace/raw_slack/landing",
        channel_id="C12345",
        ingest_date="2026-10-25",
        run_id="run-789",
        page_no=1,
    )
    expected = (
        "/Volumes/workspace/raw_slack/landing/messages/channel_id=C12345/"
        "ingest_date=2026-10-25/run_id=run-789/page-0001.json"
    )
    assert path == expected
