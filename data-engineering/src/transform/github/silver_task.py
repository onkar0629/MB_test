"""Databricks task entry point for GitHub Silver transformations."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from src.transform.github.silver import (
    transform_commits,
    transform_issues,
    transform_pull_requests,
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--pulls-table", required=True)
    parser.add_argument("--commits-table", required=True)
    parser.add_argument("--issues-table", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    print(f"Reading GitHub Bronze table: {args.bronze_table}...")
    bronze_df = spark.table(args.bronze_table)

    pulls_df = transform_pull_requests(bronze_df)
    commits_df = transform_commits(bronze_df)
    issues_df = transform_issues(bronze_df)

    print(f"Writing Silver pull requests to {args.pulls_table}...")
    pulls_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(args.pulls_table)

    print(f"Writing Silver commits to {args.commits_table}...")
    commits_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(args.commits_table)

    print(f"Writing Silver issues to {args.issues_table}...")
    issues_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(args.issues_table)

    print("GitHub Silver transformations complete.")


if __name__ == "__main__":
    main()
