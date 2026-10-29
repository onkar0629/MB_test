"""Databricks task entry point for ClickUp silver transformation."""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession
from src.transform.clickup.silver import transform_bronze_tasks


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-table", required=True)
    parser.add_argument("--silver-table", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    bronze_df = spark.table(args.bronze_table)
    silver_df = transform_bronze_tasks(bronze_df)

    silver_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(args.silver_table)


if __name__ == "__main__":
    main()
