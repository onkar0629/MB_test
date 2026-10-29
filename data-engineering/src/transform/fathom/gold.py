"""Transform Silver Fathom tables into Gold analytics tables."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_team_productivity(
    transcripts_df: DataFrame, meetings_df: DataFrame
) -> DataFrame:
    """Aggregate speaker dialogue metrics per speaker per day.

    Resolves canonical speaker identity (using email where available, else name)
    and partitions metrics by meeting_date.
    """
    safe_text = F.trim(F.coalesce(F.col("text"), F.lit("")))
    cleaned_transcripts = (
        transcripts_df.filter(F.col("speaker_name").isNotNull())
        .withColumn(
            "word_count",
            F.when(safe_text == "", F.lit(0)).otherwise(
                F.size(F.split(safe_text, r"\s+"))
            ),
        )
    )

    speaker_window = Window.partitionBy("speaker_name")
    resolved_email = F.coalesce(
        F.col("speaker_email"),
        F.max("speaker_email").over(speaker_window),
    )
    enriched_transcripts = cleaned_transcripts.withColumn(
        "resolved_email", resolved_email
    ).withColumn(
        "speaker_key",
        F.coalesce(F.col("resolved_email"), F.col("speaker_name")),
    )

    joined = enriched_transcripts.join(
        meetings_df.select("recording_id", "recording_start_time"),
        on="recording_id",
        how="inner",
    ).withColumn("meeting_date", F.to_date("recording_start_time"))

    return (
        joined.groupBy("meeting_date", "speaker_key")
        .agg(
            F.max("speaker_name").alias("speaker_name"),
            F.max("resolved_email").alias("speaker_email"),
            F.countDistinct("recording_id").alias("total_meetings_spoken"),
            F.count("word_count").alias("total_dialogue_lines"),
            F.coalesce(F.sum("word_count"), F.lit(0)).alias("total_words_spoken"),
            F.round(F.avg("word_count"), 1).alias("avg_words_per_line"),
            F.min("recording_start_time").alias("first_spoke_at"),
            F.max("recording_start_time").alias("last_spoke_at"),
        )
        .select(
            "meeting_date",
            "speaker_name",
            "speaker_email",
            "total_meetings_spoken",
            "total_dialogue_lines",
            "total_words_spoken",
            "avg_words_per_line",
            "first_spoke_at",
            "last_spoke_at",
        )
    )


def build_daily_meeting_summary(
    meetings_df: DataFrame, action_items_df: DataFrame
) -> DataFrame:
    """Aggregate company meeting cadence and duration by date."""
    raw_duration = (
        F.unix_timestamp("recording_end_time")
        - F.unix_timestamp("recording_start_time")
    ) / 60.0

    duration_minutes = F.when(
        (F.col("recording_end_time").isNotNull())
        & (F.col("recording_start_time").isNotNull())
        & (raw_duration >= 0),
        raw_duration,
    ).otherwise(None)

    enriched_meetings = (
        meetings_df.filter(F.col("recording_start_time").isNotNull())
        .withColumn("meeting_date", F.to_date("recording_start_time"))
        .withColumn("duration_minutes", duration_minutes)
    )

    actions_per_meeting = (
        action_items_df.groupBy("recording_id").agg(
            F.count("task_index").alias("action_item_count")
        )
    )

    meetings_with_actions = enriched_meetings.join(
        actions_per_meeting, on="recording_id", how="left"
    ).withColumn(
        "action_item_count", F.coalesce(F.col("action_item_count"), F.lit(0))
    )

    return (
        meetings_with_actions.groupBy("meeting_date")
        .agg(
            F.countDistinct("recording_id").alias("total_meetings"),
            F.round(
                F.coalesce(F.sum("duration_minutes"), F.lit(0.0)), 1
            ).alias("total_meeting_minutes"),
            F.round(F.avg("duration_minutes"), 1).alias("avg_meeting_minutes"),
            F.sum("action_item_count").alias("total_action_items"),
            F.countDistinct("host_email").alias("unique_hosts"),
        )
        .select(
            "meeting_date",
            "total_meetings",
            "total_meeting_minutes",
            "avg_meeting_minutes",
            "total_action_items",
            "unique_hosts",
        )
    )
