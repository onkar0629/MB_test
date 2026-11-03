from __future__ import annotations

from datetime import date, datetime, timezone

from src.ingestion.fathom.land import land_account

AFTER = datetime(2026, 9, 1, tzinfo=timezone.utc)
BEFORE = datetime(2026, 9, 18, 12, 0, tzinfo=timezone.utc)
ROOT = "/Volumes/workspace/raw_fathom/landing"


class StubClient:
    def __init__(self, pages):
        self._pages = pages

    def iter_meeting_pages(self, created_after, created_before, max_pages):
        yield from self._pages


def test_writes_one_file_per_page_and_returns_the_count():
    written = {}
    client = StubClient([
        (1, {"items": [{"recording_id": "a"}], "next_cursor": "c"}),
        (2, {"items": [{"recording_id": "b"}], "next_cursor": None}),
    ])

    pages = land_account(
        client=client,
        writer=lambda path, body: written.__setitem__(path, body),
        volume_root=ROOT,
        slug="fenil",
        window=(AFTER, BEFORE),
        run_id="4711",
        ingest_date=date(2026, 9, 18),
        max_pages=10,
    )

    assert pages == 2
    assert sorted(written) == [
        f"{ROOT}/meetings/source_account=fenil/ingest_date=2026-09-18"
        "/run_id=4711/page-0001.json",
        f"{ROOT}/meetings/source_account=fenil/ingest_date=2026-09-18"
        "/run_id=4711/page-0002.json",
    ]


def test_writes_the_response_body_verbatim_as_json():
    written = {}
    payload = {"items": [{"recording_id": "a", "title": "Acme sync"}], "next_cursor": None}
    client = StubClient([(1, payload)])

    land_account(
        client=client,
        writer=lambda path, body: written.__setitem__(path, body),
        volume_root=ROOT,
        slug="fenil",
        window=(AFTER, BEFORE),
        run_id="1",
        ingest_date=date(2026, 9, 18),
        max_pages=1,
    )

    import json

    assert json.loads(next(iter(written.values()))) == payload


def test_an_empty_window_writes_nothing():
    written = {}
    pages = land_account(
        client=StubClient([]),
        writer=lambda path, body: written.__setitem__(path, body),
        volume_root=ROOT,
        slug="fenil",
        window=(AFTER, BEFORE),
        run_id="1",
        ingest_date=date(2026, 9, 18),
        max_pages=10,
    )
    assert pages == 0
    assert written == {}


def test_raises_when_max_pages_hit_with_cursor_remaining():
    client = StubClient([
        (1, {"items": [{"recording_id": "a"}], "next_cursor": "c1"}),
        (2, {"items": [{"recording_id": "b"}], "next_cursor": "c2"}),
    ])

    import pytest

    with pytest.raises(RuntimeError, match="reached max_pages"):
        land_account(
            client=client,
            writer=lambda path, body: None,
            volume_root=ROOT,
            slug="fenil",
            window=(AFTER, BEFORE),
            run_id="1",
            ingest_date=date(2026, 9, 18),
            max_pages=2,
        )
