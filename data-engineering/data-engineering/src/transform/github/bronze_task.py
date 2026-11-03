"""Databricks task entry point for GitHub Bronze ingestion."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from src.transform.github.bronze import ingest_bronze_stream


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume-root", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    print(f"Starting GitHub Bronze ingestion into {args.target_table}...")
    ingest_bronze_stream(
        spark=spark,
        volume_root=args.volume_root,
        checkpoint_path=args.checkpoint_path,
        target_table=args.target_table,
        run_id=args.run_id,
    )
    print("GitHub Bronze streaming batch complete.")


if __name__ == "__main__":
    main()
