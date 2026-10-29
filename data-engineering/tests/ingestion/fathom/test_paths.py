from __future__ import annotations

from datetime import date

import pytest

from src.ingestion.fathom.paths import build_page_path, source_account_from_path

ROOT = "/Volumes/workspace/raw_fathom/landing"


def test_builds_a_partitioned_page_path():
    assert build_page_path(ROOT, "fenil", date(2026, 9, 18), "4711", 1) == (
        f"{ROOT}/meetings/source_account=fenil/ingest_date=2026-09-18"
        "/run_id=4711/page-0001.json"
    )


def test_page_numbers_are_zero_padded_so_paths_sort():
    path = build_page_path(ROOT, "fenil", date(2026, 9, 18), "4711", 42)
    assert path.endswith("page-0042.json")


def test_trailing_slash_on_the_root_is_tolerated():
    with_slash = build_page_path(ROOT + "/", "fenil", date(2026, 9, 18), "1", 1)
    without = build_page_path(ROOT, "fenil", date(2026, 9, 18), "1", 1)
    assert with_slash == without


def test_page_number_must_be_positive():
    with pytest.raises(ValueError, match="page_no"):
        build_page_path(ROOT, "fenil", date(2026, 9, 18), "1", 0)


def test_recovers_the_account_from_a_landing_path():
    path = build_page_path(ROOT, "usman", date(2026, 9, 18), "9", 3)
    assert source_account_from_path(path) == "usman"


def test_returns_none_for_a_path_without_an_account_partition():
    assert source_account_from_path("/Volumes/x/y/page-0001.json") is None
