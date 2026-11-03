from __future__ import annotations

import json
import os
import sys
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from src.transform.fathom.bronze import to_bronze_rows  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    # Workers otherwise pick up whatever `python3` is on PATH, which may be a
    # different minor version than the venv and fails with PYTHON_VERSION_MISMATCH.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("bronze-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def _landing_df(spark, items, path, fetched_at):
    return spark.createDataFrame(
        [(items, path, fetched_at)],
        "items array<string>, _source_file string, _fetched_at timestamp",
    )


def test_explodes_one_row_per_meeting_with_provenance(spark):
    items = [
        json.dumps({"recording_id": "rec-1", "title": "Acme sync"}),
        json.dumps({"recording_id": "rec-2", "title": "Standup"}),
    ]
    path = (
        "/Volumes/workspace/raw_fathom/landing/meetings"
        "/source_account=fenil/ingest_date=2026-09-18/run_id=7/page-0001.json"
    )
    df = _landing_df(spark, items, path, datetime(2026, 9, 18, 12, 0))

    rows = to_bronze_rows(df, run_id="7").orderBy("recording_id").collect()

    assert [r["recording_id"] for r in rows] == ["rec-1", "rec-2"]
    assert {r["_source_account"] for r in rows} == {"fenil"}
    assert {r["_run_id"] for r in rows} == {"7"}
    assert rows[0]["_source_file"] == path


def test_keeps_the_payload_verbatim(spark):
    meeting = {"recording_id": "rec-1", "title": "Acme sync", "nested": {"a": 1}}
    df = _landing_df(
        spark,
        [json.dumps(meeting)],
        "/x/source_account=usman/y/page-0001.json",
        datetime(2026, 9, 18, 12, 0),
    )

    row = to_bronze_rows(df, run_id="7").collect()[0]
    assert json.loads(row["_payload"]) == meeting


def test_a_page_with_no_items_produces_no_rows(spark):
    df = _landing_df(
        spark, [], "/x/source_account=usman/y/page-0001.json",
        datetime(2026, 9, 18, 12, 0),
    )
    assert to_bronze_rows(df, run_id="7").count() == 0
