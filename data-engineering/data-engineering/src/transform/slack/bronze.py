"""Landing Volume -> raw_slack Bronze tables.

Stores users and channels as raw JSON text to ensure schema evolution resilience.
Bronze is append-only; deduplication happens in Silver.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

USERS_LANDING_SCHEMA = "members array<string>, ok boolean"
CHANNELS_LANDING_SCHEMA = "channels array<string>, ok boolean"
MESSAGES_LANDING_SCHEMA = "messages array<string>, ok boolean"

USER_BRONZE_COLUMNS = [
    "user_id",
    "_payload",
    "_source_file",
    "_run_id",
    "_fetched_at",
    "_ingested_at",
]

CHANNEL_BRONZE_COLUMNS = [
    "channel_id",
    "_payload",
    "_source_file",
    "_run_id",
    "_fetched_at",
    "_ingested_at",
]

MESSAGE_BRONZE_COLUMNS = [
    "channel_id",
    "message_ts",
    "_payload",
    "_source_file",
    "_run_id",
    "_fetched_at",
    "_ingested_at",
]



def to_bronze_user_rows(df: DataFrame, run_id: str) -> DataFrame:
    """Explode landing users JSON into bronze rows."""
    return (
        df.withColumn("_payload", F.explode("members"))
        .withColumn("user_id", F.get_json_object("_payload", "$.id"))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(*USER_BRONZE_COLUMNS)
    )


def to_bronze_channel_rows(df: DataFrame, run_id: str) -> DataFrame:
    """Explode landing channels JSON into bronze rows."""
    return (
        df.withColumn("_payload", F.explode("channels"))
        .withColumn("channel_id", F.get_json_object("_payload", "$.id"))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(*CHANNEL_BRONZE_COLUMNS)
    )


def load_bronze_users(
    spark: SparkSession,
    volume_root: str,
    checkpoint_path: str,
    target_table: str,
    run_id: str,
) -> None:
    """Read landing users incrementally and append to Bronze."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            user_id         STRING,
            _payload        STRING,
            _source_file    STRING,
            _run_id         STRING,
            _fetched_at     TIMESTAMP,
            _ingested_at    TIMESTAMP
        ) USING DELTA
        CLUSTER BY (user_id)
        """
    )

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .schema(USERS_LANDING_SCHEMA)
        .load(f"{volume_root}/users/*/*/*.json")
        .select(
            "members",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_fetched_at"),
        )
    )

    (
        to_bronze_user_rows(stream, run_id)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/cp")
        .trigger(availableNow=True)
        .toTable(target_table)
    ).awaitTermination()


def load_bronze_channels(
    spark: SparkSession,
    volume_root: str,
    checkpoint_path: str,
    target_table: str,
    run_id: str,
) -> None:
    """Read landing channels incrementally and append to Bronze."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            channel_id      STRING,
            _payload        STRING,
            _source_file    STRING,
            _run_id         STRING,
            _fetched_at     TIMESTAMP,
            _ingested_at    TIMESTAMP
        ) USING DELTA
        CLUSTER BY (channel_id)
        """
    )

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .schema(CHANNELS_LANDING_SCHEMA)
        .load(f"{volume_root}/channels/*/*/*.json")
        .select(
            "channels",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_fetched_at"),
        )
    )

    (
        to_bronze_channel_rows(stream, run_id)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/cp")
        .trigger(availableNow=True)
        .toTable(target_table)
    ).awaitTermination()


def to_bronze_message_rows(df: DataFrame, run_id: str) -> DataFrame:
    """Explode landing messages JSON into bronze rows."""
    extracted_channel = (
        F.col("channel_id")
        if "channel_id" in df.columns
        else F.regexp_extract("_source_file", r"channel_id=([^/]+)", 1)
    )
    return (
        df.withColumn("_payload", F.explode("messages"))
        .withColumn("channel_id", extracted_channel)
        .withColumn("message_ts", F.get_json_object("_payload", "$.ts"))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(*MESSAGE_BRONZE_COLUMNS)
    )


def load_bronze_messages(
    spark: SparkSession,
    volume_root: str,
    checkpoint_path: str,
    target_table: str,
    run_id: str,
) -> None:
    """Read landing messages incrementally and append to Bronze."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            channel_id      STRING,
            message_ts      STRING,
            _payload        STRING,
            _source_file    STRING,
            _run_id         STRING,
            _fetched_at     TIMESTAMP,
            _ingested_at    TIMESTAMP
        ) USING DELTA
        CLUSTER BY (channel_id, message_ts)
        """
    )

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .schema(MESSAGES_LANDING_SCHEMA)
        .load(f"{volume_root}/messages/*/*/*/*.json")
        .select(
            "messages",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_fetched_at"),
        )
    )

    (
        to_bronze_message_rows(stream, run_id)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/cp")
        .trigger(availableNow=True)
        .toTable(target_table)
    ).awaitTermination()

