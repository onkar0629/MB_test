"""Read and advance the ingest watermark.

One row per (source_account, entity). The watermark is advanced only after the
run's pages are durably on the Volume, so a crash re-fetches rather than skips.
"""
from __future__ import annotations

from datetime import datetime, timezone
from src.ingestion.fathom.accounts import validate_slug


def ensure_table(spark, table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            source_account STRING,
            entity         STRING,
            watermark      TIMESTAMP,
            last_run_id    STRING,
            updated_at     TIMESTAMP
        ) USING DELTA
        """
    )


def read_watermark(spark, table: str, slug: str, entity: str) -> datetime | None:
    validate_slug(slug)
    rows = spark.sql(
        f"SELECT watermark FROM {table} "
        f"WHERE source_account = '{slug}' AND entity = '{entity}'"
    ).collect()
    if not rows or rows[0]["watermark"] is None:
        return None
    return rows[0]["watermark"].replace(tzinfo=timezone.utc)


def advance_watermark(
    spark, table: str, slug: str, entity: str, value: datetime, run_id: str
) -> None:
    validate_slug(slug)
    spark.sql(
        f"""
        MERGE INTO {table} AS t
        USING (SELECT '{slug}' AS source_account, '{entity}' AS entity) AS s
          ON t.source_account = s.source_account AND t.entity = s.entity
        WHEN MATCHED THEN UPDATE SET
            t.watermark   = TIMESTAMP '{value.isoformat()}',
            t.last_run_id = '{run_id}',
            t.updated_at  = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
            (source_account, entity, watermark, last_run_id, updated_at)
        VALUES
            ('{slug}', '{entity}', TIMESTAMP '{value.isoformat()}',
             '{run_id}', current_timestamp())
        """
    )
