"""Landing job: fetch users and channels from Slack and persist raw JSON files."""
from __future__ import annotations

import json
from collections.abc import Callable

from src.ingestion.slack.client import SlackClient
from src.ingestion.slack.paths import (
    build_channels_page_path,
    build_users_page_path,
)

Writer = Callable[[str, str], None]


def default_fs_writer(path: str, content: str) -> None:
    import pathlib

    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


def land_slack_users(
    client: SlackClient,
    writer: Writer,
    volume_root: str,
    ingest_date: str,
    run_id: str,
    max_pages: int = 100,
) -> int:
    """Fetch users from Slack and write raw landing JSON pages."""
    page_count = 0
    for page_no, payload in client.iter_users(max_pages=max_pages):
        body = json.dumps(payload, ensure_ascii=False)
        path = build_users_page_path(volume_root, ingest_date, run_id, page_no)
        writer(path, body)
        page_count += 1
    return page_count


def land_slack_channels(
    client: SlackClient,
    writer: Writer,
    volume_root: str,
    ingest_date: str,
    run_id: str,
    types: str = "public_channel,private_channel",
    max_pages: int = 100,
) -> int:
    """Fetch channels from Slack and write raw landing JSON pages."""
    page_count = 0
    for page_no, payload in client.iter_channels(types=types, max_pages=max_pages):
        body = json.dumps(payload, ensure_ascii=False)
        path = build_channels_page_path(volume_root, ingest_date, run_id, page_no)
        writer(path, body)
        page_count += 1
    return page_count
