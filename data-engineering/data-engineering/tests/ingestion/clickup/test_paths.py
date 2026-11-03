from __future__ import annotations

from src.ingestion.clickup.paths import build_page_path, team_id_from_path


def test_build_page_path():
    path = build_page_path(
        volume_root="/Volumes/workspace/raw_clickup/landing",
        team_id="901234",
        ingest_date="2026-10-22",
        run_id="run-abc",
        page_no=3,
    )
    expected = (
        "/Volumes/workspace/raw_clickup/landing/tasks/"
        "team_id=901234/ingest_date=2026-10-22/run_id=run-abc/page-0003.json"
    )
    assert path == expected


def test_team_id_from_path():
    path = (
        "/Volumes/workspace/raw_clickup/landing/tasks/"
        "team_id=901234/ingest_date=2026-10-22/run_id=run-abc/page-0001.json"
    )
    assert team_id_from_path(path) == "901234"
    assert team_id_from_path("/invalid/path") is None
