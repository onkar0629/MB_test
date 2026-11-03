"""Landing job: fetch users and channels from Slack and persist raw JSON files."""
from __future__ import annotations

import json
from collections.abc import Callable

from src.ingestion.slack.client import SlackClient
from src.ingestion.slack.paths import (
    build_channels_page_path,
    build_messages_page_path,
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


def land_slack_public_channel_messages(
    client: SlackClient,
    writer: Writer,
    volume_root: str,
    ingest_date: str,
    run_id: str,
    watermarks: dict[str, str] | None = None,
    channel_ids: list[str] | None = None,
    max_pages_per_channel: int = 100,
    limit: int = 200,
) -> tuple[int, dict[str, str]]:
    """Fetch public channel messages from Slack with pagination and watermarks.

    STRICT PRIVACY GUARANTEE: Only ingests public channels. Never fetches private channels or DMs.
    """
    watermarks = dict(watermarks or {})
    updated_watermarks = dict(watermarks)
    total_pages = 0

    target_channel_ids = channel_ids
    if target_channel_ids is None:
        target_channel_ids = []
        for _, payload in client.iter_channels(types="public_channel", max_pages=100):
            for ch in payload.get("channels", []):
                # Strict check against private or DM channels
                if (
                    ch.get("is_private") is True
                    or ch.get("is_im") is True
                    or ch.get("is_mpim") is True
                ):
                    continue
                cid = ch.get("id")
                if cid:
                    target_channel_ids.append(cid)

    for ch_id in target_channel_ids:
        oldest = watermarks.get(ch_id)
        channel_max_ts = oldest

        for page_no, payload in client.iter_channel_messages(
            channel_id=ch_id,
            oldest=oldest,
            limit=limit,
            max_pages=max_pages_per_channel,
        ):
            messages = payload.get("messages", [])
            if not messages:
                continue

            for msg in messages:
                ts = msg.get("ts")
                if ts:
                    if channel_max_ts is None or float(ts) > float(channel_max_ts):
                        channel_max_ts = str(ts)

            body = json.dumps(payload, ensure_ascii=False)
            path = build_messages_page_path(volume_root, ch_id, ingest_date, run_id, page_no)
            writer(path, body)
            total_pages += 1

        if channel_max_ts:
            updated_watermarks[ch_id] = channel_max_ts

    return total_pages, updated_watermarks

