from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")

from src.transform.fathom.silver import transform_bronze  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-fathom-silver")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_bronze_row(
    recording_id,
    title,
    ingested_at,
    transcript=None,
    action_items=None,
):
    payload = {
        "recording_id": recording_id,
        "title": title,
        "meeting_title": title,
        "transcript": transcript,
        "action_items": action_items,
        "recorded_by": {"name": "Nikhil", "email": "nikhil@example.com"},
        "default_summary": {"markdown_formatted": "Meeting summary"},
    }

    return (
        str(recording_id),
        json.dumps(payload),
        "nikhil",
        f"source_account=nikhil/meeting-{recording_id}.json",
        "run-1",
        ingested_at,
        ingested_at,
    )


def test_deduplicates_meetings_and_keeps_latest(spark):
    older = datetime(2026, 10, 15, tzinfo=timezone.utc)
    newer = datetime(2026, 10, 16, tzinfo=timezone.utc)

    schema = """
        recording_id string,
        _payload string,
        _source_account string,
        _source_file string,
        _run_id string,
        _fetched_at timestamp,
        _ingested_at timestamp
    """
    df = spark.createDataFrame(
        [
            make_bronze_row(101, "Old title", older),
            make_bronze_row(101, "New title", newer),
        ],
        schema=schema,
    )

    meetings, transcripts, action_items = transform_bronze(df)

    rows = meetings.collect()
    assert len(rows) == 1
    assert rows[0]["title"] == "New title"
    assert transcripts.count() == 0
    assert action_items.count() == 0


def test_explodes_transcripts_and_action_items(spark):
    timestamp = datetime(2026, 10, 16, tzinfo=timezone.utc)
    transcript = [
        {
            "speaker": {"display_name": "Nikhil"},
            "text": "I'll fix the bug tomorrow.",
            "timestamp": "00:00:10",
        },
        {
            "speaker": {"display_name": "Usman"},
            "text": "Please create a pull request.",
            "timestamp": "00:00:20",
        },
    ]

    df = spark.createDataFrame(
        [
            make_bronze_row(
                102,
                "Daily standup",
                timestamp,
                transcript,
                [
                    "Fix the bug",
                    "Create a pull request",
                ],
            )
        ],
        schema="""
            recording_id string,
            _payload string,
            _source_account string,
            _source_file string,
            _run_id string,
            _fetched_at timestamp,
            _ingested_at timestamp
        """,
    )

    meetings, transcripts, action_items = transform_bronze(df)

    assert meetings.count() == 1
    transcript_rows = transcripts.orderBy("sequence_no").collect()
    assert len(transcript_rows) == 2
    assert transcript_rows[0]["speaker_name"] == "Nikhil"
    assert transcript_rows[0]["text"] == "I'll fix the bug tomorrow."
    assert transcript_rows[1]["sequence_no"] == 1

    tasks = action_items.orderBy("task_index").collect()
    assert len(tasks) == 2
    assert tasks[0]["task_text"] == "Fix the bug"
    assert tasks[1]["task_text"] == "Create a pull request"


def test_missing_arrays_preserve_meeting(spark):
    timestamp = datetime(2026, 10, 16, tzinfo=timezone.utc)
    df = spark.createDataFrame(
        [make_bronze_row(103, "No transcript", timestamp)],
        schema="""
            recording_id string,
            _payload string,
            _source_account string,
            _source_file string,
            _run_id string,
            _fetched_at timestamp,
            _ingested_at timestamp
        """,
    )

    meetings, transcripts, action_items = transform_bronze(df)

    assert meetings.count() == 1
    assert transcripts.count() == 0
    assert action_items.count() == 0
