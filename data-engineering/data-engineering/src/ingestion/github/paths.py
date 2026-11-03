"""Volume path construction and parsing for raw GitHub landing files."""
from __future__ import annotations

import re

# Format:
# .../repos/<owner>/<repo>/<entity>/ingest_date=YYYY-MM-DD/run_id=<run_id>/page-0001.json
PATH_PATTERN = re.compile(
    r".*/repos/(?P<owner>[^/]+)/(?P<repo>[^/]+)/(?P<entity>[^/]+)"
    r"/ingest_date=(?P<ingest_date>\d{4}-\d{2}-\d{2})"
    r"/run_id=(?P<run_id>[^/]+)/page-(?P<page>\d{4})\.json$"
)


def build_page_path(
    volume_root: str,
    owner: str,
    repo: str,
    entity: str,
    ingest_date: str,
    run_id: str,
    page_number: int,
) -> str:
    """Build a deterministic Volume file path for one GitHub JSON page."""
    root = volume_root.rstrip("/")
    return (
        f"{root}/repos/{owner}/{repo}/{entity}/"
        f"ingest_date={ingest_date}/run_id={run_id}/page-{page_number:04d}.json"
    )


def parse_page_path(path: str) -> dict[str, str] | None:
    """Extract owner, repo, entity, ingest_date, run_id, and page from a path."""
    match = PATH_PATTERN.match(path.replace("\\", "/"))
    if not match:
        return None
    return match.groupdict()
