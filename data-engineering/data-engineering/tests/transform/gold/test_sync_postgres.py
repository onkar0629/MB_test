"""Unit tests for syncing Gold metrics back to ActionOS PostgreSQL."""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from scripts.sync_gold_to_postgres_local import (
    DEFAULT_WORKSPACE_SCHEMA,
    is_verified_identity,
    update_local_postgres,
    update_via_docker,
)


def test_is_verified_identity():
    """Verify email filtering filters dummy placeholders and invalid formats."""
    assert is_verified_identity("nikhil@codersboutique.com") is True
    assert is_verified_identity("sahil@marketing-boutique.com") is True
    assert is_verified_identity("admin@example.com") is False
    assert is_verified_identity("yourgithubemail@gmail.com") is False
    assert is_verified_identity("") is False
    assert is_verified_identity(None) is False
    assert is_verified_identity("invalid-email-format") is False


def test_update_local_postgres_executes_parameterized_updates():
    """Verify update_local_postgres writes Gold metrics using COALESCE parameterization."""
    sample_metrics = [
        {
            "primary_email": "nikhil@codersboutique.com",
            "full_name": "Nikhil",
            "github_username": "nik-767",
            "slack_user_id": "U12345",
            "prs_opened": 5,
            "prs_merged": 3,
            "commits_authored": 12,
            "tasks_done": 8,
            "tasks_in_progress": 2,
            "time_spent_minutes": 720.0,
            "meetings_hosted": 4,
            "productivity_score": 85,
            "team_rank": 1,
        }
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.rowcount = 1

    with patch("pg8000.dbapi.connect", return_value=mock_conn):
        updated = update_local_postgres(
            metrics=sample_metrics,
            host="localhost",
            port=5432,
            user="postgres",
            password="fake-password",
            database="default",
            workspace_schema="workspace_483zkq9rh8a9jmm19zk3oxfys",
            dry_run=False,
        )

    assert updated == 1
    mock_conn.commit.assert_called_once()

    update_call = mock_cursor.execute.call_args_list[0]
    sql, params = update_call[0]

    assert '"workspace_483zkq9rh8a9jmm19zk3oxfys"."person"' in sql
    assert 'SET "githubPrsOpenedThisMonth" = COALESCE(%s, "githubPrsOpenedThisMonth")' in sql
    assert params[0] == 5  # prs_opened
    assert params[1] == 3  # prs_merged
    assert params[2] == 12  # commits_authored
    assert params[3] == 8  # tasks_done_this_week
    assert params[4] == 8  # tasks_done_this_month
    assert params[5] == 12.0  # hours (720 min / 60)
    assert params[6] == 2  # tasks_in_progress
    assert params[7] == 4  # meetings_this_week
    assert params[8] == 4  # meetings_this_month
    assert params[9] == 85  # peopleScore
    assert params[10] == 1  # teamRank
    assert params[11] == "nik-767"
    assert params[12] == "U12345"
    assert params[13] == "nikhil@codersboutique.com"


def test_dry_run_does_not_modify_database():
    """Verify dry_run=True returns count without touching database connections or executing SQL."""
    sample_metrics = [
        {
            "primary_email": "nikhil@codersboutique.com",
            "productivity_score": 90,
            "team_rank": 1,
        }
    ]

    with patch("pg8000.dbapi.connect") as mock_connect, patch("subprocess.run") as mock_run:
        updated = update_local_postgres(
            metrics=sample_metrics,
            workspace_schema=DEFAULT_WORKSPACE_SCHEMA,
            dry_run=True,
        )
        assert updated == 1
        mock_connect.assert_not_called()
        mock_run.assert_not_called()


def test_unverified_emails_are_skipped():
    """Verify dummy and invalid emails are filtered out prior to database execution."""
    sample_metrics = [
        {"primary_email": "admin@example.com", "productivity_score": 10},
        {"primary_email": "yourgithubemail@gmail.com", "productivity_score": 10},
        {"primary_email": "invalid_no_domain", "productivity_score": 10},
    ]

    with patch("pg8000.dbapi.connect") as mock_connect:
        updated = update_local_postgres(
            metrics=sample_metrics,
            workspace_schema=DEFAULT_WORKSPACE_SCHEMA,
            dry_run=False,
        )
        assert updated == 0
        mock_connect.assert_not_called()


def test_update_via_docker_uses_parameterized_json():
    """Verify update_via_docker passes JSON parameters safely via -v payload."""
    sample_metrics = [
        {
            "primary_email": "nikhil@codersboutique.com",
            "productivity_score": 95,
            "team_rank": 1,
            "prs_opened": 2,
        }
    ]

    mock_res = MagicMock()
    mock_res.returncode = 0
    mock_res.stdout = "1,0\n"

    with patch("subprocess.run", return_value=mock_res) as mock_run:
        count = update_via_docker(
            metrics=sample_metrics,
            workspace_schema="workspace_483zkq9rh8a9jmm19zk3oxfys",
            dry_run=False,
        )
        assert count == 1
        assert mock_run.called
        cmd = mock_run.call_args[0][0]
        assert "docker" in cmd
        assert "twenty-db-1" in cmd

        # Check that -v payload parameter contains serialized JSON
        payload_arg = next(arg for arg in cmd if arg.startswith("payload="))
        payload_data = json.loads(payload_arg.replace("payload=", ""))
        assert len(payload_data) == 1
        assert payload_data[0]["primary_email"] == "nikhil@codersboutique.com"
        assert payload_data[0]["people_score"] == 95

        # Check that the SQL query uses jsonb_to_recordset and COALESCE
        input_sql = mock_run.call_args[1]["input"]
        assert "jsonb_to_recordset(:'payload'::jsonb)" in input_sql
        assert 'COALESCE(d.people_score, p."peopleScore")' in input_sql
