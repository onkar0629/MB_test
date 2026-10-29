"""Unit tests for PostgreSQL landing paths."""
from __future__ import annotations

from src.ingestion.postgres.paths import build_page_path


def test_build_page_path() -> None:
    path = build_page_path(
        volume_root="/Volumes/workspace/raw_postgres/landing",
        entity="people",
        ingest_date="2026-10-25",
        run_id="run-123",
        page_no=1,
    )
    expected = (
        "/Volumes/workspace/raw_postgres/landing/people/ingest_date=2026-10-25/"
        "run_id=run-123/page-0001.json"
    )
    assert path == expected


def test_build_page_path_trailing_slash() -> None:
    path = build_page_path(
        volume_root="/Volumes/workspace/raw_postgres/landing/",
        entity="workspace_members",
        ingest_date="2026-10-25",
        run_id="run-456",
        page_no=2,
    )
    expected = (
        "/Volumes/workspace/raw_postgres/landing/workspace_members/"
        "ingest_date=2026-10-25/run_id=run-456/page-0002.json"
    )
    assert path == expected
