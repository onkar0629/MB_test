"""Landing path construction for ClickUp payloads."""
from __future__ import annotations

import re

PAGE_PATH_RE = re.compile(
    r".*/tasks/team_id=([^/]+)/ingest_date=(\d{4}-\d{2}-\d{2})/run_id=([^/]+)/page-(\d{4})\.json$"
)


def build_page_path(
    volume_root: str,
    team_id: str,
    ingest_date: str,
    run_id: str,
    page_no: int,
) -> str:
    """Return structured landing volume path for a ClickUp tasks page."""
    root = volume_root.rstrip("/")
    prefix = f"tasks/team_id={team_id}/ingest_date={ingest_date}/run_id={run_id}"
    return f"{root}/{prefix}/page-{page_no:04d}.json"


def team_id_from_path(path: str) -> str | None:
    """Extract team_id from a landing page path."""
    match = PAGE_PATH_RE.match(path)
    return match.group(1) if match else None
