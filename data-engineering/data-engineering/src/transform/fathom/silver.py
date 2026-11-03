"""Transform raw Fathom Bronze records into structured Silver DataFrames."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

INVITEE_SCHEMA = StructType([
    StructField("email", StringType()),
    StructField("email_domain", StringType()),
    StructField("is_external", BooleanType()),
    StructField("matched_speaker_display_name", StringType()),
    StructField("name", StringType()),
])

SPEAKER_SCHEMA = StructType([
    StructField("display_name", StringType()),
    StructField("matched_calendar_invitee_email", StringType()),
])

TRANSCRIPT_SCHEMA = StructType([
    StructField("speaker", SPEAKER_SCHEMA),
    StructField("text", StringType()),
    StructField("timestamp", StringType()),
])

PAYLOAD_SCHEMA = StructType([
    StructField("action_items", ArrayType(StringType())),
    StructField("calendar_invitees", ArrayType(INVITEE_SCHEMA)),
    StructField("created_at", StringType()),
    StructField(
        "default_summary",
        StructType([
            StructField("markdown_formatted", StringType()),
            StructField("template_name", StringType()),
        ]),
    ),
    StructField("highlights", StringType()),
    StructField("meeting_title", StringType()),
    StructField("meeting_type", StringType()),
    StructField("meeting_url", StringType()),
    StructField(
        "recorded_by",
        StructType([
            StructField("email", StringType()),
            StructField("email_domain", StringType()),
            StructField("name", StringType()),
            StructField("team", StringType()),
        ]),
    ),
    StructField("recording_end_time", StringType()),
    StructField("recording_id", LongType()),
    StructField("recording_start_time", StringType()),
    StructField("scheduled_end_time", StringType()),
    StructField("scheduled_start_time", StringType()),
    StructField("share_url", StringType()),
    StructField("shared_with", StringType()),
    StructField("title", StringType()),
    StructField("transcript", ArrayType(TRANSCRIPT_SCHEMA)),
    StructField("transcript_language", StringType()),
    StructField("url", StringType()),
])


def transform_bronze(df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Return meeting, transcript, and action-item DataFrames."""
    parsed = (
        df.filter(F.col("_payload").isNotNull())
        .withColumn("payload", F.from_json("_payload", PAYLOAD_SCHEMA))
        .filter(F.col("payload").isNotNull())
        .withColumn(
            "recording_id",
            F.coalesce(
                F.col("recording_id"),
                F.col("payload.recording_id").cast("string"),
            ),
        )
        .filter(F.col("recording_id").isNotNull())
    )

    # Keep the newest Bronze copy of each meeting.
    window = Window.partitionBy("recording_id").orderBy(
        F.col("_ingested_at").desc_nulls_last(),
        F.col("_fetched_at").desc_nulls_last(),
        F.col("_source_file").desc_nulls_last(),
    )
    latest = (
        parsed.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    meetings = latest.select(
        "recording_id",
        F.col("payload.title").alias("title"),
        F.col("payload.meeting_title").alias("meeting_title"),
        F.col("payload.meeting_type").alias("meeting_type"),
        F.to_timestamp(
            F.col("payload.recording_start_time")
        ).alias("recording_start_time"),
        F.to_timestamp(
            F.col("payload.recording_end_time")
        ).alias("recording_end_time"),
        F.to_timestamp(
            F.col("payload.scheduled_start_time")
        ).alias("scheduled_start_time"),
        F.to_timestamp(
            F.col("payload.scheduled_end_time")
        ).alias("scheduled_end_time"),
        F.to_timestamp(
            F.col("payload.created_at")
        ).alias("created_at"),
        F.col("payload.recorded_by.name").alias("host_name"),
        F.col("payload.recorded_by.email").alias("host_email"),
        F.col("payload.default_summary.markdown_formatted").alias("summary"),
        F.col("payload.highlights").alias("highlights"),
        F.col("payload.meeting_url").alias("meeting_url"),
        F.col("payload.share_url").alias("share_url"),
        F.col("payload.url").alias("url"),
        F.col("payload.transcript_language").alias("transcript_language"),
        F.col("_source_account"),
        F.col("_source_file"),
        F.col("_run_id"),
        F.col("_ingested_at"),
    )

    transcripts = (
        latest.select(
            "recording_id",
            F.posexplode_outer("payload.transcript").alias(
                "sequence_no", "dialogue"
            ),
        )
        .filter(F.col("dialogue").isNotNull())
        .select(
            "recording_id",
            "sequence_no",
            F.col("dialogue.speaker.display_name").alias("speaker_name"),
            F.col(
                "dialogue.speaker.matched_calendar_invitee_email"
            ).alias("speaker_email"),
            F.col("dialogue.text").alias("text"),
            F.col("dialogue.timestamp").alias("timestamp"),
        )
    )

    action_items = (
        latest.select(
            "recording_id",
            F.posexplode_outer("payload.action_items").alias(
                "task_index", "task_text"
            ),
        )
        .filter(F.col("task_text").isNotNull())
        .select("recording_id", "task_index", "task_text")
    )

    return meetings, transcripts, action_items
