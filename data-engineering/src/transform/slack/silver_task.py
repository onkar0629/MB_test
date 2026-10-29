"""Databricks task entry point for loading Slack Silver tables."""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession

from src.transform.slack.silver import load_silver


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-users-table", required=True)
    parser.add_argument("--bronze-channels-table", required=True)
    parser.add_argument("--silver-users-table", required=True)
    parser.add_argument("--silver-channels-table", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    print(
        f"Transforming Slack Bronze to Silver:\n"
        f"  Users:    {args.bronze_users_table} -> {args.silver_users_table}\n"
        f"  Channels: {args.bronze_channels_table} -> {args.silver_channels_table}"
    )

    load_silver(
        spark=spark,
        bronze_users_table=args.bronze_users_table,
        bronze_channels_table=args.bronze_channels_table,
        silver_users_table=args.silver_users_table,
        silver_channels_table=args.silver_channels_table,
    )

    print("Slack Silver load complete.")


if __name__ == "__main__":
    main()
