"""Unit tests for Gold dim_person identity harvesting and deduplication."""
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
from src.transform.gold.dim_person import build_dim_person  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-gold-dim-person")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_harvest_workspace_members_not_crm_people(spark):
    """Ensure dim_person only harvests workspace members, ignoring external CRM contacts."""
    people_schema = StructType([
        StructField("person_id", StringType()),
        StructField("full_name", StringType()),
        StructField("primary_email", StringType()),
    ])
    people_df = spark.createDataFrame(
        [("crm-lead-1", "External Client", "client@external.com")],
        people_schema,
    )

    members_schema = StructType([
        StructField("member_id", StringType()),
        StructField("full_name", StringType()),
        StructField("user_email", StringType()),
    ])
    members_df = spark.createDataFrame(
        [("mem-1", "Usman Internal", "usman@codersboutique.com")],
        members_schema,
    )

    result = build_dim_person(people_df=people_df, members_df=members_df)
    rows = result.collect()

    emails = [r["primary_email"] for r in rows]
    assert "usman@codersboutique.com" in emails
    assert "client@external.com" not in emails


def test_github_handle_mapping_no_fake_emails(spark):
    """Ensure known GitHub handles are mapped to real emails without fake @github.team emails."""
    prs_schema = StructType([
        StructField("author_login", StringType()),
        StructField("author_email", StringType()),
        StructField("author_name", StringType()),
    ])
    prs_df = spark.createDataFrame(
        [("go2usman", "noreply@users.noreply.github.com", "Usman")],
        prs_schema,
    )

    slack_schema = StructType([
        StructField("user_id", StringType()),
        StructField("user_email", StringType()),
        StructField("real_name", StringType()),
        StructField("is_bot", BooleanType()),
    ])
    slack_df = spark.createDataFrame(
        [("U123", "usman@codersboutique.com", "Usman", False)],
        slack_schema,
    )

    result = build_dim_person(slack_users_df=slack_df, github_prs_df=prs_df)
    rows = result.collect()

    for r in rows:
        assert "@github.team" not in r["primary_email"]

    usman_row = [r for r in rows if r["primary_email"] == "usman@codersboutique.com"][0]
    assert usman_row["github_username"] == "go2usman"


def test_deterministic_person_id(spark):
    """Ensure person_id is deterministic sha1 hash of email."""
    slack_schema = StructType([
        StructField("user_id", StringType()),
        StructField("user_email", StringType()),
        StructField("real_name", StringType()),
        StructField("is_bot", BooleanType()),
    ])
    slack_df = spark.createDataFrame(
        [("U456", "deepak@codersboutique.com", "Deepak", False)],
        slack_schema,
    )

    res1 = build_dim_person(slack_users_df=slack_df).collect()[0]
    res2 = build_dim_person(slack_users_df=slack_df).collect()[0]

    assert res1["person_id"] == res2["person_id"]
    assert len(res1["person_id"]) == 40
