"""Databricks task entry point for landing PostgreSQL Action OS tables."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from src.common.watermark import advance_watermark, ensure_table
from src.ingestion.postgres.client import PostgresClient
from src.ingestion.postgres.land import default_fs_writer, land_postgres_entity

ENTITIES = ["people", "workspace_members", "tasks"]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--volume-root", required=True)
    parser.add_argument("--state-table", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--ingest-date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    )
    parser.add_argument("--host", default=None, help="PostgreSQL server host/IP")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--database", default="default")
    parser.add_argument(
        "--workspace-schema",
        default=None,
        help="Explicit Twenty CRM workspace schema name (e.g. workspace_xxx)",
    )
    parser.add_argument("--secret-scope", default="mb-ingest")
    parser.add_argument("--secret-key", default="postgres-password")
    args = parser.parse_args(argv)

    # 1. Resolve PostgreSQL password strictly (no default fallback)
    password = None
    try:
        from databricks.sdk.runtime import dbutils

        password = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key)
    except Exception:
        password = os.environ.get("PG_DATABASE_PASSWORD")

    if not password:
        raise ValueError(
            f"PostgreSQL password secret '{args.secret_key}' in scope '{args.secret_scope}' "
            "or PG_DATABASE_PASSWORD environment variable is required and was not found."
        )

    # 2. Resolve PostgreSQL host
    host = args.host
    if not host:
        try:
            from databricks.sdk.runtime import dbutils

            host = dbutils.secrets.get(scope=args.secret_scope, key="postgres-host")
        except Exception:
            host = os.environ.get("PG_DATABASE_HOST")

    if not host:
        raise ValueError(
            "PostgreSQL host must be provided via --host parameter, secret 'postgres-host', "
            "or PG_DATABASE_HOST environment variable."
        )

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    ensure_table(spark, args.state_table)

    client = PostgresClient(
        host=host,
        port=args.port,
        user=args.user,
        password=password,
        database=args.database,
        workspace_schema=args.workspace_schema,
    )

    now = datetime.now(timezone.utc)
    for entity in ENTITIES:
        print(f"Landing PostgreSQL entity '{entity}' from {host}:{args.port}...")
        pages = land_postgres_entity(
            client=client,
            writer=default_fs_writer,
            volume_root=args.volume_root,
            entity=entity,
            ingest_date=args.ingest_date,
            run_id=args.run_id,
        )
        print(f"Landed {pages} pages for '{entity}'.")
        advance_watermark(
            spark=spark,
            table=args.state_table,
            slug="twenty",
            entity=f"postgres_{entity}",
            value=now,
            run_id=args.run_id,
        )

    print(f"PostgreSQL landing complete to {args.volume_root}")


if __name__ == "__main__":
    main()
