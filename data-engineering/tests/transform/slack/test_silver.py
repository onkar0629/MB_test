"""PySpark tests for Slack Silver transformation."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from src.transform.slack.silver import (  # noqa: E402
    transform_bronze_channels,
    transform_bronze_users,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-slack-silver")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_transform_bronze_users_dedup_and_fields(spark) -> None:
    user_v1 = {
        "id": "U12345",
        "team_id": "T001",
        "name": "nikhil",
        "deleted": False,
        "real_name": "Nikhil Kalra",
        "tz": "Asia/Kolkata",
        "profile": {
            "email": "nikhil@example.com",
            "display_name": "Nikhil",
            "real_name": "Nikhil Kalra",
        },
        "is_admin": True,
        "is_bot": False,
        "updated": 1729800000,
    }
    user_v2 = dict(user_v1)
    user_v2["profile"] = {
        "email": "nikhil.new@example.com",
        "display_name": "Nikhil Kalra",
        "real_name": "Nikhil Kalra",
    }
    user_v2["updated"] = 1729850000

    rows = [
        {
            "user_id": "U12345",
            "_payload": json.dumps(user_v1),
            "_source_file": "landing/users/page-0001.json",
            "_run_id": "run-1",
            "_fetched_at": datetime(2026, 10, 24, 10, 0, 0),
            "_ingested_at": datetime(2026, 10, 24, 10, 5, 0),
        },
        {
            "user_id": "U12345",
            "_payload": json.dumps(user_v2),
            "_source_file": "landing/users/page-0002.json",
            "_run_id": "run-2",
            "_fetched_at": datetime(2026, 10, 25, 10, 0, 0),
            "_ingested_at": datetime(2026, 10, 25, 10, 5, 0),
        },
    ]

    bronze_df = spark.createDataFrame(rows)
    silver_df = transform_bronze_users(bronze_df)

    assert silver_df.count() == 1
    row = silver_df.first()
    assert row["user_id"] == "U12345"
    assert row["email"] == "nikhil.new@example.com"
    assert row["username"] == "nikhil"
    assert row["is_admin"] is True
    assert row["is_bot"] is False
    assert row["timezone"] == "Asia/Kolkata"


def test_transform_bronze_channels_dedup_and_fields(spark) -> None:
    ch_v1 = {
        "id": "C999",
        "name": "general",
        "is_channel": True,
        "is_private": False,
        "created": 1720000000,
        "is_archived": False,
        "is_member": True,
        "num_members": 15,
        "topic": {"value": "Company updates"},
        "purpose": {"value": "General chat"},
    }
    ch_v2 = dict(ch_v1)
    ch_v2["num_members"] = 18

    rows = [
        {
            "channel_id": "C999",
            "_payload": json.dumps(ch_v1),
            "_source_file": "landing/channels/page-0001.json",
            "_run_id": "run-1",
            "_fetched_at": datetime(2026, 10, 24, 10, 0, 0),
            "_ingested_at": datetime(2026, 10, 24, 10, 5, 0),
        },
        {
            "channel_id": "C999",
            "_payload": json.dumps(ch_v2),
            "_source_file": "landing/channels/page-0002.json",
            "_run_id": "run-2",
            "_fetched_at": datetime(2026, 10, 25, 10, 0, 0),
            "_ingested_at": datetime(2026, 10, 25, 10, 5, 0),
        },
    ]

    bronze_df = spark.createDataFrame(rows)
    silver_df = transform_bronze_channels(bronze_df)

    assert silver_df.count() == 1
    row = silver_df.first()
    assert row["channel_id"] == "C999"
    assert row["channel_name"] == "general"
    assert row["is_private"] is False
    assert row["is_member"] is True
    assert row["num_members"] == 18
    assert row["topic"] == "Company updates"
