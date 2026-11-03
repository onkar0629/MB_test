"""Transform Silver ClickUp tables into Gold analytics tables."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F



def build_clickup_team_productivity(tasks_df: DataFrame) -> DataFrame:
    """Aggregate ClickUp task activity per contributor per day.

    Attributes tasks to assignees, falling back to the creator
    when a task has no assignees.

    Task credit is given to every assignee.
    Time metrics are equally divided among assignees.
    """

    # Replace null arrays with empty arrays before zipping.
    assignee_records = F.arrays_zip(
        F.coalesce(
            F.col("assignee_ids"),
            F.array().cast("array<bigint>")
        ),
        F.coalesce(
            F.col("assignee_usernames"),
            F.array().cast("array<string>")
        ),
        F.coalesce(
            F.col("assignee_emails"),
            F.array().cast("array<string>")
        )
    )

    # Count assignees for equal time attribution.
    prepared = (
        tasks_df
        .withColumn("_assignee_records", assignee_records)
        .withColumn(
            "_assignee_count",
            F.when(
                F.size("_assignee_records") > 0,
                F.size("_assignee_records")
            ).otherwise(1)
        )
    )

    # Explode zipped records to preserve ID/name/email alignment.
    # Note: F.arrays_zip assigns positional struct field names: 0 (id), 1 (username), 2 (email).
    exploded = (
        prepared
        .withColumn(
            "assignee",
            F.explode_outer("_assignee_records")
        )
        .withColumn(
            "contributor_id",
            F.coalesce(
                F.col("assignee.0"),
                F.col("creator_id")
            )
        )
        .withColumn(
            "contributor_name",
            F.coalesce(
                F.col("assignee.1"),
                F.col("creator_username")
            )
        )
        .withColumn(
            "contributor_email",
            F.coalesce(
                F.col("assignee.2"),
                F.col("creator_email")
            )
        )
    )

    # Resolve contributor identity.
    with_key = (
        exploded
        .withColumn(
            "contributor_key",
            F.coalesce(
                F.col("contributor_email"),
                F.col("contributor_name"),
                F.col("contributor_id").cast("string")
            )
        )
        .withColumn(
            "activity_date",
            F.coalesce(
                F.to_date("updated_at"),
                F.to_date("created_at")
            )
        )
        .filter(
            F.col("activity_date").isNotNull()
            & F.col("contributor_key").isNotNull()
        )
    )

    # Aggregate contributor productivity.
    return (
        with_key
        .groupBy("activity_date", "contributor_key")
        .agg(
            F.max("contributor_name").alias("contributor_name"),
            F.max("contributor_email").alias("contributor_email"),

            F.countDistinct("task_id").alias("tasks_worked"),

            F.countDistinct(
                F.when(
                    F.col("status_type").isin("closed", "done"),
                    F.col("task_id")
                )
            ).alias("tasks_completed"),

            F.countDistinct(
                F.when(
                    F.col("status_type").isin("open", "custom"),
                    F.col("task_id")
                )
            ).alias("tasks_in_progress"),

            F.countDistinct(
                F.when(
                    F.col("parent_id").isNotNull()
                    & F.col("status_type").isin("closed", "done"),
                    F.col("task_id")
                )
            ).alias("subtasks_completed"),

            F.round(
                F.coalesce(
                    F.sum(
                        F.col("time_estimate_minutes")
                        / F.col("_assignee_count")
                    ),
                    F.lit(0.0)
                ),
                1
            ).alias("total_time_estimate_minutes"),

            F.round(
                F.coalesce(
                    F.sum(
                        F.col("time_spent_minutes")
                        / F.col("_assignee_count")
                    ),
                    F.lit(0.0)
                ),
                1
            ).alias("total_time_spent_minutes"),

            F.min("updated_at").alias("first_activity_at"),
            F.max("updated_at").alias("last_activity_at")
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
            "last_activity_at"
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
