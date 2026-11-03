"""Databricks task entry point for landing GitHub events."""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
import requests

from src.common.watermark import advance_watermark, ensure_table, read_watermark
from src.common.window import compute_window
from src.ingestion.github.client import GitHubClient
from src.ingestion.github.land import default_fs_writer, land_repo_entity

ENTITIES = ["pull_requests", "commits", "issues"]


def repo_to_slug(owner: str, repo: str) -> str:
    """Format a safe, deterministic slug under 32 characters conforming to SLUG_PATTERN."""
    raw = f"{owner}_{repo}".lower()
    cleaned = re.sub(r"[^a-z0-9_-]", "_", raw)
    # Ensure it starts with alphanumeric and is at most 32 chars
    cleaned = re.sub(r"^[^a-z0-9]+", "", cleaned)
    return cleaned[:32]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret-scope", default="mb-ingest")
    parser.add_argument("--secret-key", default="github-api-token")
    parser.add_argument("--volume-root", required=True)
    parser.add_argument("--state-table", required=True)
    parser.add_argument("--repos", default='["coders-boutique/action-os-twenty"]')
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

        api_token = os.environ.get("GITHUB_API_TOKEN")
        if not api_token:
            raise RuntimeError(
                "dbutils not available and GITHUB_API_TOKEN env var not set"
            )

    try:
        repo_list = json.loads(args.repos)
    except json.JSONDecodeError:
        repo_list = [r.strip() for r in args.repos.split(",") if r.strip()]

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    ensure_table(spark, args.state_table)

    session = requests.Session()
    client = GitHubClient(api_token=api_token, session=session)

    now = datetime.now(timezone.utc)
    total_pages = 0

    for repo_full in repo_list:
        if "/" not in repo_full:
            print(f"Skipping invalid repo format: {repo_full}")
            continue
        owner, repo = repo_full.split("/", 1)
        slug = repo_to_slug(owner, repo)

        for entity in ENTITIES:
            print(f"Landing {entity} for {owner}/{repo} (slug: {slug})...")

            if args.updated_after:
                window = (
                    datetime.fromisoformat(args.updated_after),
                    datetime.fromisoformat(args.updated_before or now.isoformat()),
                )
                backfill = True
            else:
                last_watermark = read_watermark(spark, args.state_table, slug, entity)
                window = compute_window(
                    last_watermark=last_watermark,
                    now=now,
                    default_start=datetime.fromisoformat(args.default_start),
                    overlap_hours=args.overlap_hours,
                )
                backfill = False

            pages = land_repo_entity(
                client=client,
                writer=default_fs_writer,
                volume_root=args.volume_root,
                owner=owner,
                repo=repo,
                entity=entity,
                updated_after=window[0],
                updated_before=window[1],
                run_id=args.run_id,
                ingest_date=args.ingest_date,
                max_pages=args.max_pages,
            )
            print(f"Wrote {pages} pages for {owner}/{repo} ({entity}) in window {window}")

            if not backfill:
                advance_watermark(
                    spark=spark,
                    table=args.state_table,
                    slug=slug,
                    entity=entity,
                    value=window[1],
                    run_id=args.run_id,
                )

            total_pages += pages

    print(f"GitHub landing complete: {total_pages} total pages written to {args.volume_root}")


if __name__ == "__main__":
    main()
