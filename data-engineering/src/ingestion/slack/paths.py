"""Landing path construction for Slack payloads."""
from __future__ import annotations


def build_users_page_path(
    volume_root: str,
    ingest_date: str,
    run_id: str,
    page_no: int,
) -> str:
    """Return landing volume path for a Slack users page."""
    root = volume_root.rstrip("/")
    prefix = f"users/ingest_date={ingest_date}/run_id={run_id}"
    return f"{root}/{prefix}/page-{page_no:04d}.json"


def build_channels_page_path(
    volume_root: str,
    ingest_date: str,
    run_id: str,
    page_no: int,
) -> str:
    """Return landing volume path for a Slack channels page."""
    root = volume_root.rstrip("/")
    prefix = f"channels/ingest_date={ingest_date}/run_id={run_id}"
    return f"{root}/{prefix}/page-{page_no:04d}.json"


def build_messages_page_path(
    volume_root: str,
    channel_id: str,
    ingest_date: str,
    run_id: str,
    page_no: int,
) -> str:
    """Return landing volume path for a Slack channel messages page."""
    root = volume_root.rstrip("/")
    prefix = (
        f"messages/channel_id={channel_id}/ingest_date={ingest_date}/run_id={run_id}"
    )
    return f"{root}/{prefix}/page-{page_no:04d}.json"
