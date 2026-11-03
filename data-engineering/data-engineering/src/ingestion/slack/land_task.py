"""Databricks task entry point for landing Slack users and channels."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import requests

from src.ingestion.slack.client import SlackClient
from src.ingestion.slack.land import (
    default_fs_writer,
    land_slack_channels,
    land_slack_users,
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret-scope", default="mb-ingest")
    parser.add_argument("--secret-key", default="slack-bot-token")
    parser.add_argument("--volume-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--ingest-date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args(argv)

    try:
        from databricks.sdk.runtime import dbutils

        bot_token = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key)
    except ImportError:
        import os

        bot_token = os.environ.get("SLACK_BOT_TOKEN")
        if not bot_token:
            raise RuntimeError(
                "dbutils not available and SLACK_BOT_TOKEN env var not set"
            )

    session = requests.Session()
    client = SlackClient(bot_token=bot_token, session=session)

    # 1. Land Users (Full snapshot)
    print("Landing Slack users...")
    user_pages = land_slack_users(
        client=client,
        writer=default_fs_writer,
        volume_root=args.volume_root,
        ingest_date=args.ingest_date,
        run_id=args.run_id,
        max_pages=args.max_pages,
    )
    print(f"Landed {user_pages} user pages.")

    # 2. Land Channels (Full snapshot)
    print("Landing Slack channels...")
    channel_pages = land_slack_channels(
        client=client,
        writer=default_fs_writer,
        volume_root=args.volume_root,
        ingest_date=args.ingest_date,
        run_id=args.run_id,
        max_pages=args.max_pages,
    )
    print(f"Landed {channel_pages} channel pages.")

    print(f"Slack landing complete to {args.volume_root}")


if __name__ == "__main__":
    main()
