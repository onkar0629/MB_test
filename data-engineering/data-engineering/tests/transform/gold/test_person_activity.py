"""Unit tests for Gold weekly person activity scorecard."""
from __future__ import annotations

import os
import sys

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    StringType,
    StructField,
    StructType,
)
from src.transform.gold.person_activity import build_gold_person_activity  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-person-activity")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_weekly_partitioning_not_all_time(spark):
    """Ensure activity is grouped by calendar week, not summed into a single all-time row."""
    dim_schema = StructType([
        StructField("person_id", StringType()),
        StructField("full_name", StringType()),
        StructField("primary_email", StringType()),
        StructField("github_username", StringType()),
        StructField("job_title", StringType()),
    ])
    dim_df = spark.createDataFrame(
        [("p1", "Usman", "usman@codersboutique.com", "go2usman", "Engineer")],
        dim_schema,
    )

    prs_schema = StructType([
        StructField("author_login", StringType()),
        StructField("author_email", StringType()),
        StructField("state", StringType()),
        StructField("merged_at", StringType()),
    ])
    prs_df = spark.createDataFrame(
        [
            ("go2usman", "usman@codersboutique.com", "closed", "2026-10-14T10:00:00Z"),
            ("go2usman", "usman@codersboutique.com", "closed", "2026-10-21T15:00:00Z"),
        ],
        prs_schema,
    )

    result = build_gold_person_activity(dim_person_df=dim_df, github_prs_df=prs_df)
    rows = result.collect()

    assert len(rows) == 2
    weeks = sorted([str(r["activity_week"]) for r in rows])
    assert "2026-10-12" in weeks
    assert "2026-10-19" in weeks

    for r in rows:
        assert r["prs_merged"] == 1
        assert r["productivity_score"] == 25


def test_no_duplicate_rows_from_github(spark):
    """Ensure person is not duplicated even if matched by handle and email."""
    dim_schema = StructType([
        StructField("person_id", StringType()),
        StructField("full_name", StringType()),
        StructField("primary_email", StringType()),
        StructField("github_username", StringType()),
        StructField("job_title", StringType()),
    ])
    dim_df = spark.createDataFrame(
        [("p1", "Usman", "usman@codersboutique.com", "go2usman", "Engineer")],
        dim_schema,
    )

    prs_schema = StructType([
        StructField("author_login", StringType()),
        StructField("author_email", StringType()),
        StructField("state", StringType()),
        StructField("merged_at", StringType()),
    ])
    prs_df = spark.createDataFrame(
        [("go2usman", "usman@codersboutique.com", "closed", "2026-10-21T15:00:00Z")],
        prs_schema,
    )

    result = build_gold_person_activity(dim_person_df=dim_df, github_prs_df=prs_df)
    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["primary_email"] == "usman@codersboutique.com"


def test_gold_person_activity_with_slack(spark):
    """Ensure Slack public channel activity aggregates correctly into gold_person_activity."""
    dim_schema = StructType([
        StructField("person_id", StringType()),
        StructField("full_name", StringType()),
        StructField("primary_email", StringType()),
        StructField("github_username", StringType()),
        StructField("job_title", StringType()),
    ])
    dim_df = spark.createDataFrame(
        [("p1", "Usman", "usman@codersboutique.com", "go2usman", "Engineer")],
        dim_schema,
    )

    slack_users_schema = StructType([
        StructField("user_id", StringType()),
        StructField("email", StringType()),
    ])
    slack_users_df = spark.createDataFrame(
        [("U_USMAN", "usman@codersboutique.com")],
        slack_users_schema,
    )

    slack_msgs_schema = StructType([
        StructField("channel_id", StringType()),
        StructField("message_ts", StringType()),
        StructField("user_id", StringType()),
        StructField("text", StringType()),
        StructField("subtype", StringType()),
        StructField("thread_ts", StringType()),
        StructField("is_thread_reply", BooleanType()),
        StructField("created_at", StringType()),
    ])
    slack_msgs_df = spark.createDataFrame(
        [
            (
                "C1", "1729500000.000100", "U_USMAN", "First msg",
                None, None, False, "2026-10-21T10:00:00Z",
            ),
            (
                "C1", "1729503600.000200", "U_USMAN", "Thread reply",
                None, "1729500000.000100", True, "2026-10-21T11:00:00Z",
            ),
            (
                "C1", "1729590000.000300", "U_USMAN", "Next day msg",
                None, None, False, "2026-10-22T10:00:00Z",
            ),
        ],
        slack_msgs_schema,
    )

    result = build_gold_person_activity(
        dim_person_df=dim_df,
        slack_messages_df=slack_msgs_df,
        slack_users_df=slack_users_df,
    )
    rows = result.collect()

    assert len(rows) == 1
    r = rows[0]
    assert r["primary_email"] == "usman@codersboutique.com"
    assert r["slack_messages_count"] == 3
    assert r["slack_thread_replies_count"] == 1
    assert r["slack_active_days"] == 2

