"""Transform raw Slack Bronze records into structured, deduplicated Silver tables."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

USER_PROFILE_SCHEMA = StructType([
    StructField("email", StringType()),
    StructField("display_name", StringType()),
    StructField("real_name", StringType()),
])

USER_PAYLOAD_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("team_id", StringType()),
    StructField("name", StringType()),
    StructField("deleted", BooleanType()),
    StructField("real_name", StringType()),
    StructField("tz", StringType()),
    StructField("profile", USER_PROFILE_SCHEMA),
    StructField("is_admin", BooleanType()),
    StructField("is_owner", BooleanType()),
    StructField("is_bot", BooleanType()),
    StructField("updated", LongType()),
])

TEXT_VAL_SCHEMA = StructType([
    StructField("value", StringType()),
])

CHANNEL_PAYLOAD_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("is_channel", BooleanType()),
    StructField("is_group", BooleanType()),
    StructField("is_private", BooleanType()),
    StructField("created", LongType()),
    StructField("is_archived", BooleanType()),
    StructField("is_member", BooleanType()),
    StructField("num_members", LongType()),
    StructField("topic", TEXT_VAL_SCHEMA),
    StructField("purpose", TEXT_VAL_SCHEMA),
])


def transform_bronze_users(df: DataFrame) -> DataFrame:
    """Parse Bronze JSON payloads and deduplicate users by user_id."""
    parsed = (
        df.filter(F.col("_payload").isNotNull())
        .withColumn("payload", F.from_json("_payload", USER_PAYLOAD_SCHEMA))
        .filter(F.col("payload").isNotNull())
        .withColumn("user_id", F.coalesce(F.col("user_id"), F.col("payload.id")))
        .filter(F.col("user_id").isNotNull())
    )

    window = Window.partitionBy("user_id").orderBy(
        F.coalesce(F.col("payload.updated"), F.lit(0)).desc(),
        F.col("_fetched_at").desc(),
    )

    deduped = (
        parsed.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    return (
        deduped.withColumn("team_id", F.col("payload.team_id"))
        .withColumn("username", F.col("payload.name"))
        .withColumn(
            "real_name",
            F.coalesce(F.col("payload.real_name"), F.col("payload.profile.real_name")),
        )
        .withColumn("display_name", F.col("payload.profile.display_name"))
        .withColumn("email", F.col("payload.profile.email"))
        .withColumn("timezone", F.col("payload.tz"))
        .withColumn("is_bot", F.coalesce(F.col("payload.is_bot"), F.lit(False)))
        .withColumn("is_admin", F.coalesce(F.col("payload.is_admin"), F.lit(False)))
        .withColumn("is_deleted", F.coalesce(F.col("payload.deleted"), F.lit(False)))
        .withColumn(
            "updated_at",
            F.when(
                F.col("payload.updated").isNotNull(),
                F.timestamp_seconds(F.col("payload.updated")),
            ).otherwise(F.col("_fetched_at")),
        )
        .withColumn("_ingest_run_id", F.col("_run_id"))
        .withColumn("_ingest_timestamp", F.col("_ingested_at"))
        .select(
            "user_id",
            "team_id",
            "username",
            "real_name",
            "display_name",
            "email",
            "timezone",
            "is_bot",
            "is_admin",
            "is_deleted",
            "updated_at",
            "_ingest_run_id",
            "_ingest_timestamp",
        )
    )


def transform_bronze_channels(df: DataFrame) -> DataFrame:
    """Parse Bronze JSON payloads and deduplicate channels by channel_id."""
    parsed = (
        df.filter(F.col("_payload").isNotNull())
        .withColumn("payload", F.from_json("_payload", CHANNEL_PAYLOAD_SCHEMA))
        .filter(F.col("payload").isNotNull())
        .withColumn("channel_id", F.coalesce(F.col("channel_id"), F.col("payload.id")))
        .filter(F.col("channel_id").isNotNull())
    )

    window = Window.partitionBy("channel_id").orderBy(
        F.col("_fetched_at").desc(),
    )

    deduped = (
        parsed.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    return (
        deduped.withColumn("channel_name", F.col("payload.name"))
        .withColumn("is_private", F.coalesce(F.col("payload.is_private"), F.lit(False)))
        .withColumn("is_archived", F.coalesce(F.col("payload.is_archived"), F.lit(False)))
        .withColumn("is_member", F.coalesce(F.col("payload.is_member"), F.lit(False)))
        .withColumn("num_members", F.coalesce(F.col("payload.num_members"), F.lit(0)))
        .withColumn("topic", F.col("payload.topic.value"))
        .withColumn("purpose", F.col("payload.purpose.value"))
        .withColumn(
            "created_at",
            F.when(
                F.col("payload.created").isNotNull(),
                F.timestamp_seconds(F.col("payload.created")),
            ).otherwise(F.col("_fetched_at")),
        )
        .withColumn("_ingest_run_id", F.col("_run_id"))
        .withColumn("_ingest_timestamp", F.col("_ingested_at"))
        .select(
            "channel_id",
            "channel_name",
            "is_private",
            "is_archived",
            "is_member",
            "num_members",
            "topic",
            "purpose",
            "created_at",
            "_ingest_run_id",
            "_ingest_timestamp",
        )
    )


def load_silver(
    spark: SparkSession,
    bronze_users_table: str,
    bronze_channels_table: str,
    silver_users_table: str,
    silver_channels_table: str,
) -> None:
    """Read Bronze tables, deduplicate, and overwrite Silver tables atomically."""
    # 1. Users
    bronze_users_df = spark.table(bronze_users_table)
    silver_users_df = transform_bronze_users(bronze_users_df)
    (
        silver_users_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_users_table)
    )

    # 2. Channels
    bronze_channels_df = spark.table(bronze_channels_table)
    silver_channels_df = transform_bronze_channels(bronze_channels_df)
    (
        silver_channels_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_channels_table)
    )
