"""Transform Silver ClickUp tables into Gold analytics tables."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_clickup_team_productivity(tasks_df: DataFrame) -> DataFrame:
    """Aggregate ClickUp task activity per contributor per day.

    Attributes tasks to assignees (or falls back to the creator when unassigned).
    Resolves canonical contributor identity grouping by email when available, else name.
    """
    # Explode assignees; fall back to creator if task is unassigned
    exploded = (
        tasks_df.withColumn("assignee", F.explode_outer("assignees"))
        .withColumn(
            "contributor_id",
            F.coalesce(F.col("assignee.id"), F.col("creator_id")),
        )
        .withColumn(
            "contributor_name",
            F.coalesce(F.col("assignee.username"), F.col("creator_username")),
        )
        .withColumn(
            "contributor_email",
            F.coalesce(F.col("assignee.email"), F.col("creator_email")),
        )
    )

    # Resolve contributor key: email preferred, else username, else string ID
    with_key = (
        exploded.withColumn(
            "contributor_key",
            F.coalesce(
                F.col("contributor_email"),
                F.col("contributor_name"),
                F.col("contributor_id").cast("string"),
            ),
        )
        .withColumn(
            "activity_date",
            F.coalesce(F.to_date("updated_at"), F.to_date("created_at")),
        )
        .filter(F.col("activity_date").isNotNull() & F.col("contributor_key").isNotNull())
    )

    return (
        with_key.groupBy("activity_date", "contributor_key")
        .agg(
            F.max("contributor_name").alias("contributor_name"),
            F.max("contributor_email").alias("contributor_email"),
            F.countDistinct("task_id").alias("tasks_worked"),
            F.countDistinct(
                F.when(F.col("status_type").isin("closed", "done"), F.col("task_id"))
            ).alias("tasks_completed"),
            F.countDistinct(
                F.when(F.col("status_type").isin("open", "custom"), F.col("task_id"))
            ).alias("tasks_in_progress"),
            F.countDistinct(
                F.when(
                    F.col("parent_id").isNotNull()
                    & F.col("status_type").isin("closed", "done"),
                    F.col("task_id"),
                )
            ).alias("subtasks_completed"),
            F.round(
                F.coalesce(F.sum("time_estimate_minutes"), F.lit(0.0)), 1
            ).alias("total_time_estimate_minutes"),
            F.round(
                F.coalesce(F.sum("time_spent_minutes"), F.lit(0.0)), 1
            ).alias("total_time_spent_minutes"),
            F.min("updated_at").alias("first_activity_at"),
            F.max("updated_at").alias("last_activity_at"),
        )
        .select(
            "activity_date",
            "contributor_name",
            "contributor_email",
            "tasks_worked",
            "tasks_completed",
            "tasks_in_progress",
            "subtasks_completed",
            "total_time_estimate_minutes",
            "total_time_spent_minutes",
            "first_activity_at",
            "last_activity_at",
        )
    )


def build_clickup_daily_summary(tasks_df: DataFrame) -> DataFrame:
    """Aggregate workspace-wide task activity and completion metrics by date."""
    with_date = tasks_df.withColumn(
        "summary_date",
        F.coalesce(F.to_date("updated_at"), F.to_date("created_at")),
    ).filter(F.col("summary_date").isNotNull())

    return (
        with_date.groupBy("summary_date")
        .agg(
            F.countDistinct("task_id").alias("total_tasks_active"),
            F.countDistinct(
                F.when(F.col("status_type").isin("closed", "done"), F.col("task_id"))
            ).alias("tasks_completed"),
            F.countDistinct(
                F.when(F.col("status_type").isin("open", "custom"), F.col("task_id"))
            ).alias("tasks_in_progress"),
            F.countDistinct(
                F.when(F.col("parent_id").isNotNull(), F.col("task_id"))
            ).alias("total_subtasks"),
            F.round(
                F.coalesce(F.sum("time_estimate_minutes"), F.lit(0.0)), 1
            ).alias("total_time_estimate_minutes"),
            F.round(
                F.coalesce(F.sum("time_spent_minutes"), F.lit(0.0)), 1
            ).alias("total_time_spent_minutes"),
            F.countDistinct("creator_id").alias("unique_creators"),
        )
        .select(
            "summary_date",
            "total_tasks_active",
            "tasks_completed",
            "tasks_in_progress",
            "total_subtasks",
            "total_time_estimate_minutes",
            "total_time_spent_minutes",
            "unique_creators",
        )
    )
