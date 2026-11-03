"""Databricks task entry point for ClickUp bronze ingestion."""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession
from src.transform.clickup.bronze import load_bronze


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume-root", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    load_bronze(
        spark=spark,
        volume_root=args.volume_root,
        checkpoint_path=args.checkpoint_path,
        target_table=args.target_table,
        run_id=args.run_id,
    )


if __name__ == "__main__":
    main()
