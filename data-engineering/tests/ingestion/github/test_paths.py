from __future__ import annotations

from src.ingestion.github.paths import build_page_path, parse_page_path


def test_build_and_parse_page_path():
    path = build_page_path(
        volume_root="/Volumes/workspace/raw_github/landing",
        owner="coders-boutique",
        repo="action-os-twenty",
        entity="pull_requests",
        ingest_date="2026-10-23",
        run_id="run_123",
        page_number=1,
    )
    assert path == (
        "/Volumes/workspace/raw_github/landing/repos/coders-boutique/action-os-twenty/"
        "pull_requests/ingest_date=2026-10-23/run_id=run_123/page-0001.json"
    )

    parsed = parse_page_path(path)
    assert parsed is not None
    assert parsed["owner"] == "coders-boutique"
    assert parsed["repo"] == "action-os-twenty"
    assert parsed["entity"] == "pull_requests"
    assert parsed["ingest_date"] == "2026-10-23"
    assert parsed["run_id"] == "run_123"
    assert parsed["page"] == "0001"
