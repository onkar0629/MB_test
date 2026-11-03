"""Silver transformations for PostgreSQL Twenty CRM Bronze data.

Parses JSON payloads, casts types, deduplicates across ingest runs,
and filters out soft-deleted records AFTER picking the latest record.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

PERSON_PAYLOAD_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("nameFirstName", StringType()),
    StructField("nameLastName", StringType()),
    StructField("emailsPrimaryEmail", StringType()),
    StructField("phonesPrimaryPhone", StringType()),
    StructField("jobTitle", StringType()),
    StructField("city", StringType()),
    StructField("avatarUrl", StringType()),
    StructField("createdAt", StringType()),
    StructField("updatedAt", StringType()),
    StructField("deletedAt", StringType(), True),
])

MEMBER_PAYLOAD_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("nameFirstName", StringType()),
    StructField("nameLastName", StringType()),
    StructField("userEmail", StringType()),
    StructField("colorScheme", StringType()),
    StructField("locale", StringType()),
    StructField("timeZone", StringType()),
    StructField("createdAt", StringType()),
    StructField("updatedAt", StringType()),
    StructField("deletedAt", StringType(), True),
])

TASK_PAYLOAD_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("title", StringType()),
    StructField("body", StringType()),
    StructField("status", StringType()),
    StructField("dueAt", StringType()),
    StructField("assigneeId", StringType()),
    StructField("createdById", StringType()),
    StructField("createdAt", StringType()),
    StructField("updatedAt", StringType()),
    StructField("deletedAt", StringType(), True),
])


def transform_bronze_people(df: DataFrame) -> DataFrame:
    """Parse Bronze JSON payloads, deduplicate first, then drop if latest is soft-deleted."""
    parsed = (
        df.filter(F.col("_payload").isNotNull())
        .withColumn("payload", F.from_json("_payload", PERSON_PAYLOAD_SCHEMA))
        .filter(F.col("payload").isNotNull())
        .withColumn("person_id", F.coalesce(F.col("record_id"), F.col("payload.id")))
        .filter(F.col("person_id").isNotNull())
    )

    window = Window.partitionBy("person_id").orderBy(
        F.coalesce(F.to_timestamp("payload.updatedAt"), F.col("_fetched_at")).desc(),
        F.col("_fetched_at").desc(),
    )

    deduped = (
        parsed.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Drop record if its latest version has deletedAt set
    active = deduped.filter(F.col("payload.deletedAt").isNull())

    return (
        active.withColumn("first_name", F.col("payload.nameFirstName"))
        .withColumn("last_name", F.col("payload.nameLastName"))
        .withColumn(
            "full_name",
            F.trim(F.concat_ws(" ", F.col("payload.nameFirstName"), F.col("payload.nameLastName"))),
        )
        .withColumn("primary_email", F.lower(F.trim(F.col("payload.emailsPrimaryEmail"))))
        .withColumn("primary_phone", F.col("payload.phonesPrimaryPhone"))
        .withColumn("job_title", F.col("payload.jobTitle"))
        .withColumn("city", F.col("payload.city"))
        .withColumn("avatar_url", F.col("payload.avatarUrl"))
        .withColumn(
            "created_at",
            F.coalesce(F.to_timestamp("payload.createdAt"), F.col("_fetched_at")),
        )
        .withColumn(
            "updated_at",
            F.coalesce(F.to_timestamp("payload.updatedAt"), F.col("_fetched_at")),
        )
        .withColumn("_ingest_run_id", F.col("_run_id"))
        .withColumn("_ingest_timestamp", F.col("_ingested_at"))
        .select(
            "person_id",
            "first_name",
            "last_name",
            "full_name",
            "primary_email",
            "primary_phone",
            "job_title",
            "city",
            "avatar_url",
            "created_at",
            "updated_at",
            "_ingest_run_id",
            "_ingest_timestamp",
        )
    )


def transform_bronze_members(df: DataFrame) -> DataFrame:
    """Parse Bronze JSON payloads, deduplicate first, then drop if latest is soft-deleted."""
    parsed = (
        df.filter(F.col("_payload").isNotNull())
        .withColumn("payload", F.from_json("_payload", MEMBER_PAYLOAD_SCHEMA))
        .filter(F.col("payload").isNotNull())
        .withColumn("member_id", F.coalesce(F.col("record_id"), F.col("payload.id")))
        .filter(F.col("member_id").isNotNull())
    )

    window = Window.partitionBy("member_id").orderBy(
        F.coalesce(F.to_timestamp("payload.updatedAt"), F.col("_fetched_at")).desc(),
        F.col("_fetched_at").desc(),
    )

    deduped = (
        parsed.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Drop record if its latest version has deletedAt set
    active = deduped.filter(F.col("payload.deletedAt").isNull())

    return (
        active.withColumn("first_name", F.col("payload.nameFirstName"))
        .withColumn("last_name", F.col("payload.nameLastName"))
        .withColumn(
            "full_name",
            F.trim(F.concat_ws(" ", F.col("payload.nameFirstName"), F.col("payload.nameLastName"))),
        )
        .withColumn("user_email", F.lower(F.trim(F.col("payload.userEmail"))))
        .withColumn("color_scheme", F.col("payload.colorScheme"))
        .withColumn("locale", F.col("payload.locale"))
        .withColumn("timezone", F.col("payload.timeZone"))
        .withColumn(
            "created_at",
            F.coalesce(F.to_timestamp("payload.createdAt"), F.col("_fetched_at")),
        )
        .withColumn(
            "updated_at",
            F.coalesce(F.to_timestamp("payload.updatedAt"), F.col("_fetched_at")),
        )
        .withColumn("_ingest_run_id", F.col("_run_id"))
        .withColumn("_ingest_timestamp", F.col("_ingested_at"))
        .select(
            "member_id",
            "first_name",
            "last_name",
            "full_name",
            "user_email",
            "color_scheme",
            "locale",
            "timezone",
            "created_at",
            "updated_at",
            "_ingest_run_id",
            "_ingest_timestamp",
        )
    )


def transform_bronze_tasks(df: DataFrame) -> DataFrame:
    """Parse Bronze JSON payloads, deduplicate first, then drop if latest is soft-deleted."""
    parsed = (
        df.filter(F.col("_payload").isNotNull())
        .withColumn("payload", F.from_json("_payload", TASK_PAYLOAD_SCHEMA))
        .filter(F.col("payload").isNotNull())
        .withColumn("task_id", F.coalesce(F.col("record_id"), F.col("payload.id")))
        .filter(F.col("task_id").isNotNull())
    )

    window = Window.partitionBy("task_id").orderBy(
        F.coalesce(F.to_timestamp("payload.updatedAt"), F.col("_fetched_at")).desc(),
        F.col("_fetched_at").desc(),
    )

    deduped = (
        parsed.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    # Drop record if its latest version has deletedAt set
    active = deduped.filter(F.col("payload.deletedAt").isNull())

    return (
        active.withColumn("title", F.col("payload.title"))
        .withColumn("body", F.col("payload.body"))
        .withColumn("status", F.col("payload.status"))
        .withColumn("due_at", F.to_timestamp("payload.dueAt"))
        .withColumn("assignee_id", F.col("payload.assigneeId"))
        .withColumn("created_by_id", F.col("payload.createdById"))
        .withColumn(
            "created_at",
            F.coalesce(F.to_timestamp("payload.createdAt"), F.col("_fetched_at")),
        )
        .withColumn(
            "updated_at",
            F.coalesce(F.to_timestamp("payload.updatedAt"), F.col("_fetched_at")),
        )
        .withColumn("_ingest_run_id", F.col("_run_id"))
        .withColumn("_ingest_timestamp", F.col("_ingested_at"))
        .select(
            "task_id",
            "title",
            "body",
            "status",
            "due_at",
            "assignee_id",
            "created_by_id",
            "created_at",
            "updated_at",
            "_ingest_run_id",
            "_ingest_timestamp",
        )
    )


def load_silver(
    spark: SparkSession,
    bronze_people_table: str,
    bronze_members_table: str,
    bronze_tasks_table: str,
    silver_people_table: str,
    silver_members_table: str,
    silver_tasks_table: str,
) -> None:
    """Read Bronze tables, deduplicate, and overwrite Silver tables atomically."""
    # 1. People
    people_df = spark.table(bronze_people_table)
    silver_people = transform_bronze_people(people_df)
    (
        silver_people.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_people_table)
    )

    # 2. Workspace Members
    members_df = spark.table(bronze_members_table)
    silver_members = transform_bronze_members(members_df)
    (
        silver_members.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_members_table)
    )

    # 3. Tasks
    tasks_df = spark.table(bronze_tasks_table)
    silver_tasks = transform_bronze_tasks(tasks_df)
    (
        silver_tasks.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_tasks_table)
    )
