"""Databricks task entry point for loading Slack landing files to Bronze."""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession

from src.transform.slack.bronze import load_bronze_channels, load_bronze_users


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume-root", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--users-table", required=True)
    parser.add_argument("--channels-table", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    print(f"Loading Slack users to Bronze table {args.users_table}...")
    load_bronze_users(
        spark=spark,
        volume_root=args.volume_root,
        checkpoint_path=f"{args.checkpoint_path}/users",
        target_table=args.users_table,
        run_id=args.run_id,
    )

    print(f"Loading Slack channels to Bronze table {args.channels_table}...")
    load_bronze_channels(
        spark=spark,
        volume_root=args.volume_root,
        checkpoint_path=f"{args.checkpoint_path}/channels",
        target_table=args.channels_table,
        run_id=args.run_id,
    )

    print("Slack Bronze load complete.")


if __name__ == "__main__":
    main()
