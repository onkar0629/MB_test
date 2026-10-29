"""Job entry point for transforming Bronze Fathom data into Silver tables."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from src.transform.fathom.silver import transform_bronze


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--meetings-table", required=True)
    parser.add_argument("--transcripts-table", required=True)
    parser.add_argument("--action-items-table", required=True)
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    bronze = spark.table(args.source_table)

    meetings, transcripts, action_items = transform_bronze(bronze)

    for table_name, df in (
        (args.meetings_table, meetings),
        (args.transcripts_table, transcripts),
        (args.action_items_table, action_items),
    ):
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)


if __name__ == "__main__":
    main()
