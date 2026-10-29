"""Bronze streaming ingestion for raw GitHub landing files using Databricks Auto Loader."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def ingest_bronze_stream(
    spark: SparkSession,
    volume_root: str,
    checkpoint_path: str,
    target_table: str,
    run_id: str,
) -> None:
    """Read raw JSON landing files incrementally using Auto Loader and append to Delta."""
    raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .load(f"{volume_root.rstrip('/')}/repos/*/*/*/*/*/*.json")
    )

    bronze_df = transform_raw_to_bronze(raw_stream, run_id)

    query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()


def transform_raw_to_bronze(df: DataFrame, run_id: str) -> DataFrame:
    """Add file origin, parsed entity, and audit timestamps to raw JSON payloads."""
    path_col = F.col("_metadata.file_path")

    # Regex matches: .../repos/<owner>/<repo>/<entity>/ingest_date=...
    repo_owner = F.regexp_extract(path_col, r"/repos/([^/]+)/([^/]+)/", 1)
    repo_name = F.regexp_extract(path_col, r"/repos/([^/]+)/([^/]+)/", 2)
    repo_extract = F.concat_ws("/", repo_owner, repo_name)
    entity_extract = F.regexp_extract(
        path_col, r"/repos/[^/]+/[^/]+/([^/]+)/", 1
    )

    return df.select(
        F.col("value").alias("_payload"),
        path_col.alias("_source_file"),
        repo_extract.alias("_source_repo"),
        entity_extract.alias("_source_entity"),
        F.lit(run_id).alias("_run_id"),
        F.current_timestamp().alias("_ingested_at"),
    )
