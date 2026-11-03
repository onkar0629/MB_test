from __future__ import annotations

import os
import sys
from datetime import date, datetime, timezone

import pytest

pyspark = pytest.importorskip("pyspark")

from src.transform.fathom.gold import (  # noqa: E402
    build_daily_meeting_summary,
    build_team_productivity,
)

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test-fathom-gold")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_team_productivity(spark):
    meetings_data = [
        ("101", datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc)),
        ("102", datetime(2026, 10, 15, 14, 0, tzinfo=timezone.utc)),
        ("103", datetime(2026, 10, 16, 11, 0, tzinfo=timezone.utc)),
    ]
    meetings = spark.createDataFrame(
        meetings_data, ["recording_id", "recording_start_time"]
    )

    # Usman has email=None in meeting 101, but has email in meeting 102 on same day.
    # Also includes a dialogue line with text=None to verify null safety.
    transcripts_data = [
        ("101", 0, "Nikhil", "nikhil@example.com", "Hello team"),
        ("101", 1, "Usman", None, "Hi Nikhil"),
        ("101", 2, "Nikhil", "nikhil@example.com", None),
        ("102", 0, "Usman", "usman@example.com", "Let us review the plan"),
        ("103", 0, "Nikhil", "nikhil@example.com", "Good morning Friday"),
    ]
    transcripts = spark.createDataFrame(
        transcripts_data,
        ["recording_id", "sequence_no", "speaker_name", "speaker_email", "text"],
    )

    productivity = build_team_productivity(transcripts, meetings)
    rows = productivity.collect()

    # Day 1 (10-15): Nikhil (1 row), Usman (1 consolidated row with email resolved)
    # Day 2 (10-16): Nikhil (1 row)
    assert len(rows) == 3

    usman_day1 = next(
        r for r in rows
        if r["speaker_name"] == "Usman" and r["meeting_date"] == date(2026, 10, 15)
    )
    # Email should be resolved and both meetings counted in single row for Day 1
    assert usman_day1["speaker_email"] == "usman@example.com"
    assert usman_day1["total_meetings_spoken"] == 2
    assert usman_day1["total_dialogue_lines"] == 2
    assert usman_day1["total_words_spoken"] == 7  # "Hi Nikhil" (2) + "Let us review the plan" (5)

    nikhil_day1 = next(
        r for r in rows
        if r["speaker_name"] == "Nikhil" and r["meeting_date"] == date(2026, 10, 15)
    )
    assert nikhil_day1["total_dialogue_lines"] == 2
    # "Hello team" (2 words) + None (0 words) = 2 words
    assert nikhil_day1["total_words_spoken"] == 2

    nikhil_day2 = next(
        r for r in rows
        if r["speaker_name"] == "Nikhil" and r["meeting_date"] == date(2026, 10, 16)
    )
    assert nikhil_day2["total_meetings_spoken"] == 1
    assert nikhil_day2["total_words_spoken"] == 3


def test_build_daily_meeting_summary(spark):
    # Day 1: One meeting of 30 mins, one meeting with missing end time (None)
    start1 = datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc)
    end1 = datetime(2026, 10, 15, 10, 30, tzinfo=timezone.utc)  # 30 mins

    start2 = datetime(2026, 10, 15, 14, 0, tzinfo=timezone.utc)
    end2 = None  # Missing end time

    # Day 2: One meeting of 60 mins
    start3 = datetime(2026, 10, 16, 9, 0, tzinfo=timezone.utc)
    end3 = datetime(2026, 10, 16, 10, 0, tzinfo=timezone.utc)  # 60 mins

    meetings_data = [
        ("101", start1, end1, "nikhil@example.com"),
        ("102", start2, end2, "usman@example.com"),
        ("103", start3, end3, "nikhil@example.com"),
    ]
    meetings = spark.createDataFrame(
        meetings_data,
        ["recording_id", "recording_start_time", "recording_end_time", "host_email"],
    )

    action_items_data = [
        ("101", 0, "Fix bug"),
        ("101", 1, "Deploy PR"),
        ("103", 0, "Write tests"),
    ]
    action_items = spark.createDataFrame(
        action_items_data, ["recording_id", "task_index", "task_text"]
    )

    daily = build_daily_meeting_summary(meetings, action_items)
    rows = daily.collect()

    assert len(rows) == 2

    day1 = next(r for r in rows if r["meeting_date"] == date(2026, 10, 15))
    assert day1["total_meetings"] == 2
    # Missing end time must NOT pull avg down to 15.0; it should be 30.0
    assert day1["total_meeting_minutes"] == 30.0
    assert day1["avg_meeting_minutes"] == 30.0
    assert day1["total_action_items"] == 2
    assert day1["unique_hosts"] == 2

    day2 = next(r for r in rows if r["meeting_date"] == date(2026, 10, 16))
    assert day2["total_meetings"] == 1
    assert day2["total_meeting_minutes"] == 60.0
    assert day2["avg_meeting_minutes"] == 60.0
    assert day2["total_action_items"] == 1
    assert day2["unique_hosts"] == 1
