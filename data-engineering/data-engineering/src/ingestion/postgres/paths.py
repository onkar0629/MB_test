"""Landing path construction for PostgreSQL Action OS Twenty CRM payloads."""
from __future__ import annotations


def build_page_path(
    volume_root: str,
    entity: str,
    ingest_date: str,
    run_id: str,
    page_no: int,
) -> str:
    """Return structured landing volume path for a PostgreSQL entity page."""
    root = volume_root.rstrip("/")
    prefix = f"{entity}/ingest_date={ingest_date}/run_id={run_id}"
    return f"{root}/{prefix}/page-{page_no:04d}.json"
