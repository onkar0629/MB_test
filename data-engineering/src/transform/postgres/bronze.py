"""Landing Volume -> raw_postgres Bronze tables.

Stores PostgreSQL records as raw JSON text to ensure schema evolution resilience.
Bronze is append-only; deduplication happens in Silver.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LANDING_SCHEMA = "records array<string>, entity string, total long"

BRONZE_COLUMNS = [
    "record_id",
    "_payload",
    "_source_file",
    "_run_id",
    "_fetched_at",
    "_ingested_at",
]


def to_bronze_rows(df: DataFrame, run_id: str) -> DataFrame:
    """Explode landing JSON records into bronze rows."""
    return (
        df.withColumn("_payload", F.explode("records"))
        .withColumn("record_id", F.get_json_object("_payload", "$.id"))
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .select(*BRONZE_COLUMNS)
    )


def load_bronze_entity(
    spark: SparkSession,
    volume_root: str,
    entity: str,
    checkpoint_path: str,
    target_table: str,
    run_id: str,
) -> None:
    """Read landing files for an entity incrementally and append to Bronze."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            record_id       STRING,
            _payload        STRING,
            _source_file    STRING,
            _run_id         STRING,
            _fetched_at     TIMESTAMP,
            _ingested_at    TIMESTAMP
        ) USING DELTA
        CLUSTER BY (record_id)
        """
    )

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .schema(LANDING_SCHEMA)
        .load(f"{volume_root}/{entity}/*/*/*.json")
        .select(
            "records",
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
