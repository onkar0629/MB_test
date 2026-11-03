"""Databricks task entry point for loading PostgreSQL Silver tables."""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession

from src.transform.postgres.silver import load_silver


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-people-table", required=True)
    parser.add_argument("--bronze-members-table", required=True)
    parser.add_argument("--bronze-tasks-table", required=True)
    parser.add_argument("--silver-people-table", required=True)
    parser.add_argument("--silver-members-table", required=True)
    parser.add_argument("--silver-tasks-table", required=True)
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    print(
        f"Transforming PostgreSQL Bronze to Silver:\n"
        f"  People:  {args.bronze_people_table} -> {args.silver_people_table}\n"
        f"  Members: {args.bronze_members_table} -> {args.silver_members_table}\n"
        f"  Tasks:   {args.bronze_tasks_table} -> {args.silver_tasks_table}"
    )

    load_silver(
        spark=spark,
        bronze_people_table=args.bronze_people_table,
        bronze_members_table=args.bronze_members_table,
        bronze_tasks_table=args.bronze_tasks_table,
        silver_people_table=args.silver_people_table,
        silver_members_table=args.silver_members_table,
        silver_tasks_table=args.silver_tasks_table,
    )

    print("PostgreSQL Silver load complete.")


if __name__ == "__main__":
    main()
