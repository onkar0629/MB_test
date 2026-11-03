"""Databricks task entry point for loading PostgreSQL landing files to Bronze."""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession

from src.transform.postgres.bronze import load_bronze_entity


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume-root", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--people-table", required=True)
    parser.add_argument("--members-table", required=True)
    parser.add_argument("--tasks-table", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    print(f"Loading PostgreSQL people to Bronze table {args.people_table}...")
    load_bronze_entity(
        spark=spark,
        volume_root=args.volume_root,
        entity="people",
        checkpoint_path=f"{args.checkpoint_path}/people",
        target_table=args.people_table,
        run_id=args.run_id,
    )

    print(f"Loading PostgreSQL members to Bronze table {args.members_table}...")
    load_bronze_entity(
        spark=spark,
        volume_root=args.volume_root,
        entity="workspace_members",
        checkpoint_path=f"{args.checkpoint_path}/members",
        target_table=args.members_table,
        run_id=args.run_id,
    )

    print(f"Loading PostgreSQL tasks to Bronze table {args.tasks_table}...")
    load_bronze_entity(
        spark=spark,
        volume_root=args.volume_root,
        entity="tasks",
        checkpoint_path=f"{args.checkpoint_path}/tasks",
        target_table=args.tasks_table,
        run_id=args.run_id,
    )

    print("PostgreSQL Bronze load complete.")


if __name__ == "__main__":
    main()
