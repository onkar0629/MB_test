"""Landing Volume -> raw_clickup.tasks_raw.

Bronze parses nothing beyond `task_id`. Storing tasks as raw JSON text ensures
that schema evolutions in ClickUp never break ingestion. Bronze is append-only;
deduplication by task_id happens in Silver.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LANDING_SCHEMA = "tasks array<string>, last_page boolean"

BRONZE_COLUMNS = [
    "task_id",
    "_payload",
    "_source_team",
    "_source_file",
    "_run_id",
    "_fetched_at",
    "_ingested_at",
]


def to_bronze_rows(df: DataFrame, run_id: str) -> DataFrame:
    """Explode landing JSON into bronze rows."""
    return (
        df.withColumn("_payload", F.explode("tasks"))
        .withColumn("task_id", F.get_json_object("_payload", "$.id"))
        .withColumn(
            "_source_team",
            F.regexp_extract("_source_file", r"team_id=([^/]+)", 1),
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
    """Read landing files incrementally and append to Bronze."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            task_id         STRING,
            _payload        STRING,
            _source_team    STRING,
            _source_file    STRING,
            _run_id         STRING,
            _fetched_at     TIMESTAMP,
            _ingested_at    TIMESTAMP
        ) USING DELTA
        CLUSTER BY (task_id, _source_team)
        """
    )

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .schema(LANDING_SCHEMA)
        .load(f"{volume_root}/tasks/*/*/*/*.json")
        .select(
            "tasks",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_fetched_at"),
        )
    )

    (
        to_bronze_rows(stream, run_id)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_path}/cp")
        .trigger(availableNow=True)
        .toTable(target_table)
    ).awaitTermination()
