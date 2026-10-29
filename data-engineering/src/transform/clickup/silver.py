"""Transform raw ClickUp Bronze records into structured, deduplicated Silver tables."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

USER_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("username", StringType()),
    StructField("email", StringType()),
])

STATUS_SCHEMA = StructType([
    StructField("status", StringType()),
    StructField("type", StringType()),
])

TASK_PAYLOAD_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("text_content", StringType()),
    StructField("status", STATUS_SCHEMA),
    StructField("orderindex", StringType()),
    StructField("date_created", StringType()),
    StructField("date_updated", StringType()),
    StructField("date_closed", StringType()),
    StructField("date_done", StringType()),
    StructField("due_date", StringType()),
    StructField("start_date", StringType()),
    StructField("time_estimate", LongType()),
    StructField("time_spent", LongType()),
    StructField("creator", USER_SCHEMA),
    StructField("assignees", ArrayType(USER_SCHEMA)),
    StructField("priority", StructType([StructField("priority", StringType())])),
    StructField("url", StringType()),
    StructField("parent", StringType()),
    StructField(
        "list", StructType([StructField("id", StringType()), StructField("name", StringType())])
    ),
    StructField(
        "project", StructType([StructField("id", StringType()), StructField("name", StringType())])
    ),
    StructField(
        "folder", StructType([StructField("id", StringType()), StructField("name", StringType())])
    ),
])


def transform_bronze_tasks(df: DataFrame) -> DataFrame:
    """Parse Bronze JSON payloads and deduplicate tasks by task_id."""
    parsed = (
        df.filter(F.col("_payload").isNotNull())
        .withColumn("payload", F.from_json("_payload", TASK_PAYLOAD_SCHEMA))
        .filter(F.col("payload").isNotNull())
        .withColumn(
            "task_id",
            F.coalesce(F.col("task_id"), F.col("payload.id")),
        )
        .filter(F.col("task_id").isNotNull())
    )

    # Pick the newest record per task_id ordering by date_updated cast to long
    window = Window.partitionBy("task_id").orderBy(
        F.col("payload.date_updated").cast("long").desc_nulls_last(),
        F.col("_fetched_at").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )
    latest = (
        parsed.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Convert ClickUp epoch millisecond strings/numbers to timestamps
    def to_ts_from_ms(col_name: str):
        return F.timestamp_millis(F.col(f"payload.{col_name}").cast("long"))

    return latest.select(
        "task_id",
        F.col("payload.parent").alias("parent_id"),
        F.col("payload.name").alias("title"),
        F.col("payload.status.status").alias("status"),
        F.col("payload.status.type").alias("status_type"),
        to_ts_from_ms("date_created").alias("created_at"),
        to_ts_from_ms("date_updated").alias("updated_at"),
        to_ts_from_ms("date_closed").alias("closed_at"),
        to_ts_from_ms("date_done").alias("done_at"),
        to_ts_from_ms("due_date").alias("due_date"),
        to_ts_from_ms("start_date").alias("start_date"),
        F.round(F.col("payload.time_estimate") / 60000.0, 1).alias("time_estimate_minutes"),
        F.round(F.col("payload.time_spent") / 60000.0, 1).alias("time_spent_minutes"),
        F.col("payload.creator.id").alias("creator_id"),
        F.col("payload.creator.username").alias("creator_username"),
        F.col("payload.creator.email").alias("creator_email"),
        F.col("payload.assignees.id").alias("assignee_ids"),
        F.col("payload.assignees.username").alias("assignee_usernames"),
        F.col("payload.assignees.email").alias("assignee_emails"),
        F.col("payload.priority.priority").alias("priority"),
        F.col("payload.list.id").alias("list_id"),
        F.col("payload.list.name").alias("list_name"),
        F.col("payload.folder.name").alias("folder_name"),
        F.col("payload.url").alias("url"),
        F.col("_source_team"),
        F.col("_ingested_at"),
    )
