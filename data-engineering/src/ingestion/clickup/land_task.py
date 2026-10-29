"""Databricks task entry point for landing ClickUp tasks."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import requests

from src.common.watermark import advance_watermark, ensure_table, read_watermark
from src.common.window import compute_window
from src.ingestion.clickup.client import ClickUpClient
from src.ingestion.clickup.land import default_fs_writer, land_team_tasks

ENTITY = "tasks"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret-scope", default="mb-ingest")
    parser.add_argument("--secret-key", default="clickup-api-token")
    parser.add_argument("--volume-root", required=True)
    parser.add_argument("--state-table", required=True)
    parser.add_argument("--default-start", default="2026-01-01T00:00:00+00:00")
    parser.add_argument("--overlap-hours", type=int, default=48)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--ingest-date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    parser.add_argument("--updated-after", default=None)
    parser.add_argument("--updated-before", default=None)
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args(argv)

    try:
        from databricks.sdk.runtime import dbutils

        api_token = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key)
    except ImportError:
        import os

        api_token = os.environ.get("CLICKUP_API_TOKEN")
        if not api_token:
            raise RuntimeError(
                "dbutils not available and CLICKUP_API_TOKEN env var not set"
            )

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    ensure_table(spark, args.state_table)

    session = requests.Session()
    client = ClickUpClient(api_token=api_token, session=session)

    teams = client.list_teams()
    print(f"Discovered {len(teams)} ClickUp teams/workspaces")

    now = datetime.now(timezone.utc)
    total_pages = 0
    for team in teams:
        team_id = str(team["id"])
        team_name = team.get("name", "Unknown")
        print(f"Landing tasks for team {team_id} ({team_name})...")

        if args.updated_after:
            window = (
                datetime.fromisoformat(args.updated_after),
                datetime.fromisoformat(args.updated_before or now.isoformat()),
            )
            backfill = True
        else:
            last_watermark = read_watermark(spark, args.state_table, team_id, ENTITY)
            window = compute_window(
                last_watermark=last_watermark,
                now=now,
                default_start=datetime.fromisoformat(args.default_start),
                overlap_hours=args.overlap_hours,
            )
            backfill = False

        pages = land_team_tasks(
            client=client,
            writer=default_fs_writer,
            volume_root=args.volume_root,
            team_id=team_id,
            updated_after=window[0],
            updated_before=window[1],
            run_id=args.run_id,
            ingest_date=args.ingest_date,
            max_pages=args.max_pages,
        )
        print(f"Wrote {pages} pages for team {team_id} in window {window}")

        if not backfill:
            advance_watermark(
                spark=spark,
                table=args.state_table,
                slug=team_id,
                entity=ENTITY,
                value=window[1],
                run_id=args.run_id,
            )

        total_pages += pages

    print(f"Landing complete: {total_pages} total pages written to {args.volume_root}")


if __name__ == "__main__":
    main()
