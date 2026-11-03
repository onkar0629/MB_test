"""Databricks task entry point for Gold cross-source identity and activity models."""
from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

# Prefer the deployed source tree in this bundle over the wheel artifact, so
# code edits in files/src take effect without rebuilding the wheel.
# NOTE: job-executed Python does NOT define __file__, so the path must be explicit.
sys.path.insert(
    0,
    "/Workspace/Users/nikhilkalra.mb@gmail.com/.bundle/mb-data-engineering/personal/files",
)

from src.transform.gold.dim_person import build_dim_person
from src.transform.gold.person_activity import build_gold_person_activity


def safe_table(spark: SparkSession, table_name: str | None):
    """Load table if it exists in the catalog, else return None."""
    if not table_name:
        return None
    try:
        if spark.catalog.tableExists(table_name):
            return spark.table(table_name)
    except Exception as exc:
        print(f"Notice: Table '{table_name}' check threw: {exc}. Skipping.")
    return None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--people-table", default=None)
    parser.add_argument("--members-table", default=None)
    parser.add_argument("--slack-users-table", default=None)
    parser.add_argument("--github-prs-table", default=None)
    parser.add_argument("--github-commits-table", default=None)
    parser.add_argument("--clickup-tasks-table", default=None)
    parser.add_argument("--fathom-meetings-table", default=None)
    parser.add_argument("--team-mapping-table", default=None)
    parser.add_argument("--dim-person-table", required=True)
    parser.add_argument("--gold-activity-table", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    # 1. Load inputs safely from all available platforms
    people_df = safe_table(spark, args.people_table)
    members_df = safe_table(spark, args.members_table)
    slack_users_df = safe_table(spark, args.slack_users_table)
    github_prs_df = safe_table(spark, args.github_prs_table)
    github_commits_df = safe_table(spark, args.github_commits_table)
    clickup_tasks_df = safe_table(spark, args.clickup_tasks_table)
    fathom_meetings_df = safe_table(spark, args.fathom_meetings_table)
    team_mapping_df = safe_table(spark, args.team_mapping_table)

    sources = [
        people_df,
        members_df,
        slack_users_df,
        github_prs_df,
        github_commits_df,
        clickup_tasks_df,
        fathom_meetings_df,
    ]
    if not any(s is not None for s in sources):
        raise RuntimeError("No input tables exist in catalog to build Gold analytics.")

    # 2. Build multi-platform dim_person
    print("Building multi-platform dim_person table...")
    dim_person = build_dim_person(
        people_df=people_df,
        members_df=members_df,
        slack_users_df=slack_users_df,
        github_prs_df=github_prs_df,
        github_commits_df=github_commits_df,
        clickup_tasks_df=clickup_tasks_df,
        fathom_meetings_df=fathom_meetings_df,
        team_mapping_df=team_mapping_df,
    )
    (
        dim_person.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(args.dim_person_table)
    )
    print(f"Saved dim_person to {args.dim_person_table}")

    # 3. Build gold_person_activity
    print("Building gold_person_activity table...")
    gold_activity = build_gold_person_activity(
        dim_person_df=dim_person,
        github_prs_df=github_prs_df,
        github_commits_df=github_commits_df,
        clickup_tasks_df=clickup_tasks_df,
        fathom_meetings_df=fathom_meetings_df,
    )
    (
        gold_activity.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(args.gold_activity_table)
    )
    print(f"Saved gold_person_activity to {args.gold_activity_table}")


if __name__ == "__main__":
    main()
