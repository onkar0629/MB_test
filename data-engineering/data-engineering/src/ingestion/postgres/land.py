"""Landing logic for PostgreSQL Action OS Twenty CRM tables."""
from __future__ import annotations

import json
from collections.abc import Callable

from src.ingestion.postgres.client import PostgresClient
from src.ingestion.postgres.paths import build_page_path

Writer = Callable[[str, str], None]


def default_fs_writer(path: str, content: str) -> None:
    import pathlib

    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


def land_postgres_entity(
    client: PostgresClient,
    writer: Writer,
    volume_root: str,
    entity: str,
    ingest_date: str,
    run_id: str,
    page_size: int = 500,
) -> int:
    """Fetch entity records from PostgreSQL and write raw landing JSON pages."""
    page_count = 0
    for page_no, payload in client.iter_entity(entity=entity, page_size=page_size):
        body = json.dumps(payload, ensure_ascii=False)
        path = build_page_path(
            volume_root=volume_root,
            entity=entity,
            ingest_date=ingest_date,
            run_id=run_id,
            page_no=page_no,
        )
        writer(path, body)
        page_count += 1
    return page_count
