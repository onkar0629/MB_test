from __future__ import annotations

import json
import os
import sys
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from src.transform.clickup.silver import transform_bronze_tasks  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-clickup-silver")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_transform_bronze_tasks_dedup_and_fields(spark):
    task_v1 = {
        "id": "task_1",
        "name": "Design DB schema",
        "parent": "parent_999",
        "status": {"status": "in progress", "type": "open"},
        "date_created": "1729600000000",
        "date_updated": "1729600100000",
        "date_done": "1729600050000",
        "time_estimate": 3600000,
        "time_spent": 1800000,
        "creator": {"id": 101, "username": "nikhil", "email": "nikhil@example.com"},
        "assignees": [{"id": 101, "username": "nikhil", "email": "nikhil@example.com"}],
        "priority": {"priority": "high"},
        "list": {"id": "list_11", "name": "Sprint 1"},
        "folder": {"id": "folder_22", "name": "Backend"},
    }
    task_v2 = {
        "id": "task_1",
        "name": "Design DB schema",
        "parent": "parent_999",
        "status": {"status": "complete", "type": "closed"},
        "date_created": "1729600000000",
        "date_updated": "1729600900000",
        "date_closed": "1729600900000",
        "time_estimate": 3600000,
        "time_spent": 3600000,
        "creator": {"id": 101, "username": "nikhil", "email": "nikhil@example.com"},
        "assignees": [{"id": 101, "username": "nikhil", "email": "nikhil@example.com"}],
        "priority": {"priority": "high"},
        "list": {"id": "list_11", "name": "Sprint 1"},
        "folder": {"id": "folder_22", "name": "Backend"},
    }

    t1_fetch = datetime(2026, 10, 22, 10, 0)
    t1_ingest = datetime(2026, 10, 22, 10, 1)
    t2_fetch = datetime(2026, 10, 22, 11, 0)
    t2_ingest = datetime(2026, 10, 22, 11, 1)

    bronze_data = [
        ("task_1", json.dumps(task_v1), "team_99", "/landing/p1.json", "r1", t1_fetch, t1_ingest),
        ("task_1", json.dumps(task_v2), "team_99", "/landing/p2.json", "r2", t2_fetch, t2_ingest),
    ]
    cols = [
        "task_id", "_payload", "_source_team", "_source_file",
        "_run_id", "_fetched_at", "_ingested_at"
    ]
    bronze_df = spark.createDataFrame(bronze_data, cols)

    silver_df = transform_bronze_tasks(bronze_df)
    rows = silver_df.collect()

    assert len(rows) == 1
    task = rows[0]
    assert task["task_id"] == "task_1"
    assert task["parent_id"] == "parent_999"
    assert task["title"] == "Design DB schema"
    assert task["status"] == "complete"
    assert task["status_type"] == "closed"
    assert task["time_estimate_minutes"] == 60.0
    assert task["time_spent_minutes"] == 60.0
    assert task["creator_id"] == 101
    assert task["creator_username"] == "nikhil"
    assert task["assignee_ids"] == [101]
    assert task["list_id"] == "list_11"
    assert task["list_name"] == "Sprint 1"
    assert task["folder_name"] == "Backend"
    assert task["created_at"] is not None
    assert task["updated_at"] is not None
