"""Landing-zone path construction.

Layout:
    <root>/meetings/source_account=<slug>/ingest_date=<YYYY-MM-DD>/run_id=<id>/page-NNNN.json

Files are never overwritten -- a re-run produces a new run_id directory. The
account partition is what lets two contributors write concurrently without
touching the same path, and `source_account_from_path` is how bronze recovers
the provenance from the file location.
"""
from __future__ import annotations

import re
from datetime import date

_ACCOUNT_PATTERN = re.compile(r"source_account=([^/]+)/")


def build_page_path(
    volume_root: str, slug: str, ingest_date: date, run_id: str, page_no: int
) -> str:
    if page_no < 1:
        raise ValueError("page_no must be >= 1")
    root = volume_root.rstrip("/")
    return (
        f"{root}/meetings"
        f"/source_account={slug}"
        f"/ingest_date={ingest_date:%Y-%m-%d}"
        f"/run_id={run_id}"
        f"/page-{page_no:04d}.json"
    )


def source_account_from_path(path: str) -> str | None:
    match = _ACCOUNT_PATTERN.search(path)
    return match.group(1) if match else None
