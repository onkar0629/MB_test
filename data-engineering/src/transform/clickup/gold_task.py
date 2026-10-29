"""Job entry point for computing ClickUp Gold analytics tables."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from src.transform.clickup.gold import (
    build_clickup_daily_summary,
    build_clickup_team_productivity,
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--productivity-table", required=True)
    parser.add_argument("--daily-summary-table", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    tasks = spark.table(args.silver_table)

    productivity = build_clickup_team_productivity(tasks)
    daily_summary = build_clickup_daily_summary(tasks)

    productivity.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(args.productivity_table)

    daily_summary.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(args.daily_summary_table)

    print(f"ClickUp Gold tables written: {args.productivity_table}, {args.daily_summary_table}")


if __name__ == "__main__":
    main()
