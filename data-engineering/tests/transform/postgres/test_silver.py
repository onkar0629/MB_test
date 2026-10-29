"""PySpark tests for PostgreSQL Action OS Silver transformations."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from src.transform.postgres.silver import (  # noqa: E402
    transform_bronze_members,
    transform_bronze_people,
    transform_bronze_tasks,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-postgres-silver")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_transform_bronze_people_dedup_and_fields(spark) -> None:
    p1 = {
        "id": "person_101",
        "nameFirstName": "Brian",
        "nameLastName": "Chesky",
        "emailsPrimaryEmail": "chesky@airbnb.com",
        "phonesPrimaryPhone": "+1-555-1234",
        "jobTitle": "CEO",
        "city": "San Francisco",
        "avatarUrl": "https://example.com/brian.jpg",
        "createdAt": "2026-01-01T00:00:00Z",
        "updatedAt": "2026-01-02T00:00:00Z",
    }
    p2 = dict(p1)
    p2["jobTitle"] = "Co-founder & CEO"
    p2["updatedAt"] = "2026-01-05T00:00:00Z"

    rows = [
        {
            "record_id": "person_101",
            "_payload": json.dumps(p1),
            "_source_file": "landing/people/page-0001.json",
            "_run_id": "run-1",
            "_fetched_at": datetime(2026, 1, 2, 12, 0, 0),
            "_ingested_at": datetime(2026, 1, 2, 12, 5, 0),
        },
        {
            "record_id": "person_101",
            "_payload": json.dumps(p2),
            "_source_file": "landing/people/page-0002.json",
            "_run_id": "run-2",
            "_fetched_at": datetime(2026, 1, 5, 12, 0, 0),
            "_ingested_at": datetime(2026, 1, 5, 12, 5, 0),
        },
    ]

    bronze_df = spark.createDataFrame(rows)
    silver_df = transform_bronze_people(bronze_df)

    assert silver_df.count() == 1
    row = silver_df.first()
    assert row["person_id"] == "person_101"
    assert row["first_name"] == "Brian"
    assert row["last_name"] == "Chesky"
    assert row["full_name"] == "Brian Chesky"
    assert row["primary_email"] == "chesky@airbnb.com"
    assert row["job_title"] == "Co-founder & CEO"


def test_transform_bronze_members_dedup_and_fields(spark) -> None:
    m1 = {
        "id": "member_202",
        "nameFirstName": "nikhil",
        "nameLastName": "kalra",
        "userEmail": "admin@example.com",
        "colorScheme": "Dark",
        "locale": "en",
        "timeZone": "Asia/Kolkata",
        "createdAt": "2026-02-01T00:00:00Z",
        "updatedAt": "2026-02-01T00:00:00Z",
    }

    rows = [
        {
            "record_id": "member_202",
            "_payload": json.dumps(m1),
            "_source_file": "landing/members/page-0001.json",
            "_run_id": "run-1",
            "_fetched_at": datetime(2026, 2, 1, 10, 0, 0),
            "_ingested_at": datetime(2026, 2, 1, 10, 5, 0),
        }
    ]

    bronze_df = spark.createDataFrame(rows)
    silver_df = transform_bronze_members(bronze_df)

    assert silver_df.count() == 1
    row = silver_df.first()
    assert row["member_id"] == "member_202"
    assert row["user_email"] == "admin@example.com"
    assert row["timezone"] == "Asia/Kolkata"


def test_transform_bronze_tasks_dedup_and_fields(spark) -> None:
    t1 = {
        "id": "task_303",
        "title": "Complete Databricks ingestion",
        "body": "Ingest Twenty CRM Postgres data",
        "status": "IN_PROGRESS",
        "dueAt": "2026-10-30T18:00:00Z",
        "assigneeId": "member_202",
        "createdById": "member_202",
        "createdAt": "2026-10-25T00:00:00Z",
        "updatedAt": "2026-10-25T12:00:00Z",
    }

    rows = [
        {
            "record_id": "task_303",
            "_payload": json.dumps(t1),
            "_source_file": "landing/tasks/page-0001.json",
            "_run_id": "run-1",
            "_fetched_at": datetime(2026, 10, 25, 12, 0, 0),
            "_ingested_at": datetime(2026, 10, 25, 12, 5, 0),
        }
    ]

    bronze_df = spark.createDataFrame(rows)
    silver_df = transform_bronze_tasks(bronze_df)

    assert silver_df.count() == 1
    row = silver_df.first()
    assert row["task_id"] == "task_303"
    assert row["title"] == "Complete Databricks ingestion"
    assert row["status"] == "IN_PROGRESS"
    assert row["assignee_id"] == "member_202"
