"""Job entry point for computing Gold analytics tables."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from src.transform.fathom.gold import (
    build_daily_meeting_summary,
    build_team_productivity,
)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--meetings-table", required=True)
    parser.add_argument("--transcripts-table", required=True)
    parser.add_argument("--action-items-table", required=True)
    parser.add_argument("--productivity-table", required=True)
    parser.add_argument("--daily-summary-table", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    meetings = spark.table(args.meetings_table)
    transcripts = spark.table(args.transcripts_table)
    action_items = spark.table(args.action_items_table)

    productivity = build_team_productivity(transcripts, meetings)
    daily_summary = build_daily_meeting_summary(meetings, action_items)

    productivity.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(args.productivity_table)
    daily_summary.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(args.daily_summary_table)


if __name__ == "__main__":
    main()
