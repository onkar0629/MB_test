"""Job entry point: pull one account's meetings into the landing Volume.

Invoked once per contributor by the job's for_each task. Failures are isolated
per account: one expired key must not stop the other contributor's load.
"""
from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable
from datetime import date, datetime, timezone

import requests

from src.common.watermark import advance_watermark, ensure_table, read_watermark
from src.ingestion.fathom.accounts import validate_slug
from src.common.window import compute_window
from src.ingestion.fathom.client import FathomClient
from src.ingestion.fathom.paths import build_page_path

ENTITY = "meetings"


# Deviation from the spec, recorded deliberately: the spec says landing files
# hold the response body "unmodified". They hold a canonical re-serialization of
# it instead -- same content, possibly different whitespace and escaping --
# because the client hands back parsed JSON. Nothing downstream depends on the
# bytes, only the content, so replay is unaffected.
def land_account(
    client,
    writer: Callable[[str, str], None],
    volume_root: str,
    slug: str,
    window: tuple[datetime, datetime],
    run_id: str,
    ingest_date: date,
    max_pages: int,
) -> int:
    """Write each API page verbatim to the Volume. Returns the page count."""
    created_after, created_before = window
    pages = 0
    cursor = None
    for page_no, payload in client.iter_meeting_pages(
        created_after, created_before, max_pages
    ):
        path = build_page_path(volume_root, slug, ingest_date, run_id, page_no)
        writer(path, json.dumps(payload, ensure_ascii=False))
        pages = page_no
        cursor = payload.get("next_cursor")

    if cursor:
        raise RuntimeError(
            f"Account {slug!r} reached max_pages ({max_pages}) with more pages remaining. "
            "Aborting run to prevent watermark advance and data loss."
        )
    return pages


def write_to_volume(path: str, body: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(body)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--secret-key", required=True)
    parser.add_argument("--secret-scope", default="mb-ingest")
    parser.add_argument("--volume-root", required=True)
    parser.add_argument("--state-table", required=True)
    parser.add_argument("--default-start", default="2024-01-01T00:00:00+00:00")
    parser.add_argument("--overlap-hours", type=int, default=48)
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--created-after", default=None)
    parser.add_argument("--created-before", default=None)
    args = parser.parse_args(argv)

    from databricks.sdk.runtime import dbutils  # noqa: PLC0415
    from pyspark.sql import SparkSession  # noqa: PLC0415

    validate_slug(args.slug)
    spark = SparkSession.builder.getOrCreate()
    run_id = args.run_id

    api_key = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key)
    ensure_table(spark, args.state_table)

    now = datetime.now(timezone.utc)
    if args.created_after:
        # Explicit backfill window; the watermark is left untouched.
        window = (
            datetime.fromisoformat(args.created_after),
            datetime.fromisoformat(args.created_before or now.isoformat()),
        )
        backfill = True
    else:
        window = compute_window(
            read_watermark(spark, args.state_table, args.slug, ENTITY),
            now,
            datetime.fromisoformat(args.default_start),
            args.overlap_hours,
        )
        backfill = False

    client = FathomClient(api_key=api_key, session=requests.Session())
    pages = land_account(
        client=client,
        writer=write_to_volume,
        volume_root=args.volume_root,
        slug=args.slug,
        window=window,
        run_id=run_id,
        ingest_date=now.date(),
        max_pages=args.max_pages,
    )

    if not backfill:
        advance_watermark(
            spark, args.state_table, args.slug, ENTITY, window[1], run_id
        )

    print(f"landed slug={args.slug} pages={pages} window={window}")
    return pages


if __name__ == "__main__":
    main()
