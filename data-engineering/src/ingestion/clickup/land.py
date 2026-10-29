"""Landing job: fetch tasks from ClickUp and persist raw JSON files."""
from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime

from src.ingestion.clickup.client import ClickUpClient
from src.ingestion.clickup.paths import build_page_path

Writer = Callable[[str, str], None]


def default_fs_writer(path: str, content: str) -> None:
    import pathlib

    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


def land_team_tasks(
    client: ClickUpClient,
    writer: Writer,
    volume_root: str,
    team_id: str,
    updated_after: datetime | None,
    updated_before: datetime | None,
    run_id: str,
    ingest_date: str,
    max_pages: int = 100,
) -> int:
    """Fetch tasks for one ClickUp team and write raw landing JSON pages.

    Returns the count of pages written.
    """
    page_count = 0
    for page_no, payload in client.iter_team_tasks(
        team_id=team_id,
        updated_after=updated_after,
        updated_before=updated_before,
        max_pages=max_pages,
    ):
        body = json.dumps(payload, ensure_ascii=False)
        path = build_page_path(volume_root, team_id, ingest_date, run_id, page_no)
        writer(path, body)
        page_count += 1

    return page_count
