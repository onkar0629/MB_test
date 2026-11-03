"""Landing job: fetch PRs, commits, and issues from GitHub and persist raw JSON files."""
from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime

from src.ingestion.github.client import GitHubClient
from src.ingestion.github.paths import build_page_path

Writer = Callable[[str, str], None]


def default_fs_writer(path: str, content: str) -> None:
    import pathlib

    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


def land_repo_entity(
    client: GitHubClient,
    writer: Writer,
    volume_root: str,
    owner: str,
    repo: str,
    entity: str,
    updated_after: datetime | None,
    updated_before: datetime | None,
    run_id: str,
    ingest_date: str,
    max_pages: int = 100,
) -> int:
    """Fetch one entity type (pull_requests, commits, issues) and write landing pages.

    Returns the count of pages written.
    """
    if entity == "pull_requests":
        generator = client.iter_pull_requests(
            owner=owner,
            repo=repo,
            updated_after=updated_after,
            max_pages=max_pages,
        )
    elif entity == "commits":
        generator = client.iter_commits(
            owner=owner,
            repo=repo,
            since=updated_after,
            until=updated_before,
            max_pages=max_pages,
        )
    elif entity == "issues":
        generator = client.iter_issues(
            owner=owner,
            repo=repo,
            since=updated_after,
            max_pages=max_pages,
        )
    else:
        raise ValueError(f"Unknown entity type: {entity}")

    page_count = 0
    for page_no, items in generator:
        # Wrap the list in a canonical page object with metadata
        payload = {
            "owner": owner,
            "repo": repo,
            "entity": entity,
            "items": items,
            "page": page_no,
        }
        body = json.dumps(payload, ensure_ascii=False)
        path = build_page_path(
            volume_root=volume_root,
            owner=owner,
            repo=repo,
            entity=entity,
            ingest_date=ingest_date,
            run_id=run_id,
            page_number=page_no,
        )
        writer(path, body)
        page_count += 1

    return page_count
