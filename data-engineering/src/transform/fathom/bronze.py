"""Landing Volume -> raw_fathom.meetings_raw.

Bronze parses nothing beyond `recording_id`. Declaring `items` as
array<string> makes Spark hand back each meeting object as its original JSON
text, so a Fathom schema change can never break this layer -- only the silver
parse, which is reparsed from the same files.

Bronze is append-only and intentionally holds duplicate meetings: the same
recording arrives under both contributors' keys, and the 48-hour overlap
re-fetches it. Deduplication happens in silver.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LANDING_SCHEMA = "items array<string>, next_cursor string, limit int"

BRONZE_COLUMNS = [
    "recording_id",
    "_payload",
    "_source_account",
    "_source_file",
    "_run_id",
    "_fetched_at",
    "_ingested_at",
]


def to_bronze_rows(df: DataFrame, run_id: str) -> DataFrame:
    """Explode a landing DataFrame into bronze rows.

    `df` must already carry `items`, `_source_file` and `_fetched_at`.
    """
    return (
        df.withColumn("_payload", F.explode("items"))
        .withColumn(
            "recording_id", F.get_json_object("_payload", "$.recording_id")
        )
        .withColumn(
            "_source_account",
            F.regexp_extract("_source_file", r"source_account=([^/]+)", 1),
        )
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(*BRONZE_COLUMNS)
    )


def load_bronze(
    spark: SparkSession,
    volume_root: str,
    checkpoint_path: str,
    target_table: str,
    run_id: str,
) -> None:
    """Read new landing files exactly once and append them to bronze."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            recording_id    STRING,
            _payload        STRING,
            _source_account STRING,
            _source_file    STRING,
            _run_id         STRING,
            _fetched_at     TIMESTAMP,
            _ingested_at    TIMESTAMP
        ) USING DELTA
        CLUSTER BY (recording_id, _source_account)
        """
    )

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .schema(LANDING_SCHEMA)
        .load(f"{volume_root.rstrip('/')}/meetings")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_fetched_at", F.col("_metadata.file_modification_time"))
    )

    (
        to_bronze_rows(stream, run_id)
        .writeStream.format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/commits")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(target_table)
        .awaitTermination()
    )
