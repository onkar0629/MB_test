from __future__ import annotations

import json
import os
import sys
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from src.transform.github.silver import (  # noqa: E402
    transform_commits,
    transform_issues,
    transform_pull_requests,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-github-silver")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_transform_pull_requests(spark):
    pr_data = {
        "owner": "coders-boutique",
        "repo": "action-os-twenty",
        "items": [
            {
                "id": 1,
                "number": 42,
                "title": "Add feature X",
                "state": "closed",
                "draft": False,
                "created_at": "2026-10-20T10:00:00Z",
                "updated_at": "2026-10-20T12:00:00Z",
                "closed_at": "2026-10-20T12:00:00Z",
                "merged_at": "2026-10-20T12:00:00Z",
                "merge_commit_sha": "abc456",
                "user": {"id": 100, "login": "usman"},
                "assignees": [{"id": 100, "login": "usman"}],
                "head": {"sha": "head1", "ref": "feat/x"},
                "base": {"sha": "base1", "ref": "main"},
                "html_url": "https://github.com/coders-boutique/action-os-twenty/pull/42",
            }
        ],
    }

    bronze_records = [
        (
            json.dumps(pr_data),
            "/volumes/.../page-0001.json",
            "coders-boutique/action-os-twenty",
            "pull_requests",
            "run_1",
            datetime(2026, 10, 20, 12, 5),
        )
    ]
    cols = [
        "_payload",
        "_source_file",
        "_source_repo",
        "_source_entity",
        "_run_id",
        "_ingested_at",
    ]
    df = spark.createDataFrame(bronze_records, cols)

    silver_df = transform_pull_requests(df)
    rows = silver_df.collect()

    assert len(rows) == 1
    pr = rows[0]
    assert pr["owner"] == "coders-boutique"
    assert pr["repo"] == "action-os-twenty"
    assert pr["pr_number"] == 42
    assert pr["title"] == "Add feature X"
    assert pr["state"] == "closed"
    assert pr["is_draft"] is False
    assert pr["author_id"] == 100
    assert pr["author_login"] == "usman"
    assert pr["assignee_ids"] == [100]
    assert pr["assignee_logins"] == ["usman"]
    assert pr["base_branch"] == "main"
    assert pr["merged_at"] is not None


def test_transform_commits(spark):
    commit_data = {
        "owner": "coders-boutique",
        "repo": "action-os-twenty",
        "items": [
            {
                "sha": "c1234567890",
                "commit": {
                    "message": "fix: resolve bug",
                    "author": {
                        "name": "Alice",
                        "email": "alice@example.com",
                        "date": "2026-10-21T09:00:00Z",
                    },
                    "committer": {
                        "name": "Alice",
                        "email": "alice@example.com",
                        "date": "2026-10-21T09:00:00Z",
                    },
                },
                "author": {"id": 200, "login": "alice"},
                "committer": {"id": 200, "login": "alice"},
                "html_url": "https://github.com/...",
            }
        ],
    }

    bronze_records = [
        (
            json.dumps(commit_data),
            "/volumes/.../page-0001.json",
            "coders-boutique/action-os-twenty",
            "commits",
            "run_1",
            datetime(2026, 10, 21, 9, 5),
        )
    ]
    cols = [
        "_payload",
        "_source_file",
        "_source_repo",
        "_source_entity",
        "_run_id",
        "_ingested_at",
    ]
    df = spark.createDataFrame(bronze_records, cols)

    silver_df = transform_commits(df)
    rows = silver_df.collect()

    assert len(rows) == 1
    commit = rows[0]
    assert commit["commit_sha"] == "c1234567890"
    assert commit["message"] == "fix: resolve bug"
    assert commit["author_name"] == "Alice"
    assert commit["author_login"] == "alice"
    assert commit["committer_login"] == "alice"
    assert commit["author_date"] is not None


def test_transform_issues_filters_pull_requests(spark):
    issue_data = {
        "owner": "coders-boutique",
        "repo": "action-os-twenty",
        "items": [
            # Real issue
            {
                "id": 10,
                "number": 5,
                "title": "Bug in API",
                "state": "open",
                "created_at": "2026-10-21T10:00:00Z",
                "updated_at": "2026-10-21T10:30:00Z",
                "closed_at": None,
                "user": {"id": 300, "login": "bob"},
                "assignees": [{"id": 300, "login": "bob"}],
                "pull_request": None,
                "html_url": "https://github.com/...",
            },
            # Pull request inside issues response -> MUST be filtered out
            {
                "id": 11,
                "number": 6,
                "title": "A Pull Request mistakenly in issues",
                "state": "open",
                "created_at": "2026-10-21T11:00:00Z",
                "updated_at": "2026-10-21T11:30:00Z",
                "closed_at": None,
                "user": {"id": 400, "login": "charlie"},
                "assignees": [],
                "pull_request": {"url": "https://api.github.com/repos/.../pulls/6"},
                "html_url": "https://github.com/...",
            },
        ],
    }

    bronze_records = [
        (
            json.dumps(issue_data),
            "/volumes/.../page-0001.json",
            "coders-boutique/action-os-twenty",
            "issues",
            "run_1",
            datetime(2026, 10, 21, 10, 35),
        )
    ]
    cols = [
        "_payload",
        "_source_file",
        "_source_repo",
        "_source_entity",
        "_run_id",
        "_ingested_at",
    ]
    df = spark.createDataFrame(bronze_records, cols)

    silver_df = transform_issues(df)
    rows = silver_df.collect()

    # Must only contain the real issue, NOT the PR
    assert len(rows) == 1
    issue = rows[0]
    assert issue["issue_number"] == 5
    assert issue["title"] == "Bug in API"
    assert issue["author_id"] == 300
    assert issue["author_login"] == "bob"
    assert issue["assignee_ids"] == [300]
    assert issue["assignee_logins"] == ["bob"]
