"""Databricks task to write Gold layer analytics back into ActionOS PostgreSQL.

Syncs all 3 Gold groups into the ActionOS 'person' table:
1. Cross-Source Gold: dim_person (handles) & gold_person_activity_weekly (peopleScore, rank)
2. ClickUp Gold: clickup_team_productivity (tasks done, hours tracked this month)
3. Fathom Gold: fathom_team_productivity (meetings this week/month, talk time)
"""
from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def safe_table(spark: SparkSession, table_name: str | None) -> DataFrame | None:
    """Safely load table if it exists in Unity Catalog, else return None."""
    if not table_name:
        return None
    try:
        if spark.catalog.tableExists(table_name):
            return spark.table(table_name)
    except Exception as exc:
        print(f"Notice: Table '{table_name}' check threw: {exc}. Skipping.")
    return None


def prepare_person_metrics(
    dim_person_df: DataFrame,
    gold_activity_df: DataFrame | None = None,
    clickup_gold_df: DataFrame | None = None,
    fathom_gold_df: DataFrame | None = None,
) -> DataFrame:
    """Combine all 3 Gold groups into a single row per person matching ActionOS schema."""
    # 1. Base from dim_person (guarantees every team member has a record)
    base = dim_person_df.select(
        F.lower(F.trim(F.col("primary_email"))).alias("primary_email"),
        F.col("full_name"),
        F.col("github_username"),
        F.col("slack_user_id"),
    )

    # 2. Group 1: Cross-Source Activity Scorecard
    if gold_activity_df is not None and "activity_week" in gold_activity_df.columns:
        # Window across all weeks to get the latest week's score and recent 4-week totals
        activity_clean = gold_activity_df.withColumn(
            "email_clean", F.lower(F.trim(F.col("primary_email")))
        )
        
        # Latest week metrics
        latest_week_val = activity_clean.select(F.max("activity_week")).collect()[0][0]
        latest_week_df = activity_clean.filter(F.col("activity_week") == latest_week_val).select(
            F.col("email_clean").alias("primary_email"),
            F.coalesce(F.col("productivity_score"), F.lit(0)).alias("people_score"),
            F.coalesce(F.col("tasks_done"), F.lit(0)).alias("clickup_tasks_done_this_week"),
            F.coalesce(F.col("meetings_hosted"), F.lit(0)).alias("meetings_this_week"),
        )

        # Monthly totals (sum of all weeks in current month-to-date)
        mtd_df = activity_clean.filter(
            F.col("activity_week") >= F.date_trunc("month", F.lit(latest_week_val))
        )
        monthly_df = mtd_df.groupBy("email_clean").agg(
            F.sum("prs_opened").alias("github_prs_opened_this_month"),
            F.sum("prs_merged").alias("github_prs_merged_this_month"),
            F.sum("commits_authored").alias("github_commits_this_month"),
            F.sum("tasks_done").alias("clickup_tasks_done_this_month"),
            F.sum("time_spent_minutes").alias("clickup_minutes_this_month"),
            F.sum("meetings_hosted").alias("meetings_this_month"),
        ).withColumnRenamed("email_clean", "primary_email")

        combined = base.join(latest_week_df, on="primary_email", how="left").join(
            monthly_df, on="primary_email", how="left"
        )
    else:
        combined = (
            base.withColumn("people_score", F.lit(0))
            .withColumn("clickup_tasks_done_this_week", F.lit(0))
            .withColumn("meetings_this_week", F.lit(0))
            .withColumn("github_prs_opened_this_month", F.lit(0))
            .withColumn("github_prs_merged_this_month", F.lit(0))
            .withColumn("github_commits_this_month", F.lit(0))
            .withColumn("clickup_tasks_done_this_month", F.lit(0))
            .withColumn("clickup_minutes_this_month", F.lit(0.0))
            .withColumn("meetings_this_month", F.lit(0))
        )

    # 3. Group 2: ClickUp Domain Gold (if standalone table exists)
    if clickup_gold_df is not None and "contributor_email" in clickup_gold_df.columns:
        spent_col = (
            "total_time_spent_minutes"
            if "total_time_spent_minutes" in clickup_gold_df.columns
            else "time_spent_minutes"
        )
        done_col = (
            "tasks_completed"
            if "tasks_completed" in clickup_gold_df.columns
            else "tasks_worked"
        )
        cu_clean = (
            clickup_gold_df.filter(F.col("contributor_email").isNotNull())
            .groupBy(F.lower(F.trim(F.col("contributor_email"))).alias("primary_email"))
            .agg(
                F.sum(done_col).alias("cu_gold_tasks_done"),
                F.sum("tasks_in_progress").alias("cu_gold_tasks_in_progress"),
                F.round(F.sum(spent_col) / 60.0, 1).alias("cu_gold_hours_tracked"),
            )
        )
        combined = combined.join(cu_clean, on="primary_email", how="left")
        combined = (
            combined.withColumn(
                "clickup_tasks_done_this_month",
                F.coalesce(F.col("cu_gold_tasks_done"), F.col("clickup_tasks_done_this_month")),
            )
            .withColumn(
                "clickup_tasks_in_progress",
                F.coalesce(F.col("cu_gold_tasks_in_progress"), F.lit(0)),
            )
            .withColumn(
                "clickup_hours_tracked_this_month",
                F.coalesce(
                    F.col("cu_gold_hours_tracked"),
                    F.round(F.coalesce(F.col("clickup_minutes_this_month") / 60.0, F.lit(0.0)), 1),
                ),
            )
        )
    else:
        combined = combined.withColumn(
            "clickup_tasks_in_progress", F.lit(0)
        ).withColumn(
            "clickup_hours_tracked_this_month",
            F.round(F.coalesce(F.col("clickup_minutes_this_month") / 60.0, F.lit(0.0)), 1),
        )

    # 4. Group 3: Fathom Domain Gold (if standalone table exists)
    if fathom_gold_df is not None and "speaker_email" in fathom_gold_df.columns:
        meetings_col = (
            "total_meetings_spoken"
            if "total_meetings_spoken" in fathom_gold_df.columns
            else "meetings_spoken"
        )
        fm_clean = (
            fathom_gold_df.filter(F.col("speaker_email").isNotNull())
            .groupBy(F.lower(F.trim(F.col("speaker_email"))).alias("primary_email"))
            .agg(F.sum(meetings_col).alias("fm_gold_meetings"))
        )
        combined = combined.join(fm_clean, on="primary_email", how="left").withColumn(
            "meetings_this_month",
            F.coalesce(F.col("fm_gold_meetings"), F.col("meetings_this_month")),
        )

    # 5. Calculate Team Rank (dense_rank ordered by people_score descending)
    rank_window = Window.orderBy(F.col("people_score").desc_nulls_last())
    ranked = combined.withColumn("team_rank", F.dense_rank().over(rank_window))

    return ranked.select(
        "primary_email",
        "full_name",
        "github_username",
        "slack_user_id",
        F.coalesce(F.col("people_score"), F.lit(0)).alias("people_score"),
        F.coalesce(F.col("team_rank"), F.lit(99)).alias("team_rank"),
        F.coalesce(F.col("github_prs_opened_this_month"), F.lit(0)).alias(
            "github_prs_opened_this_month"
        ),
        F.coalesce(F.col("github_prs_merged_this_month"), F.lit(0)).alias(
            "github_prs_merged_this_month"
        ),
        F.coalesce(F.col("github_commits_this_month"), F.lit(0)).alias(
            "github_commits_this_month"
        ),
        F.coalesce(F.col("clickup_tasks_done_this_week"), F.lit(0)).alias(
            "clickup_tasks_done_this_week"
        ),
        F.coalesce(F.col("clickup_tasks_done_this_month"), F.lit(0)).alias(
            "clickup_tasks_done_this_month"
        ),
        F.coalesce(F.col("clickup_hours_tracked_this_month"), F.lit(0.0)).alias(
            "clickup_hours_tracked_this_month"
        ),
        F.coalesce(F.col("clickup_tasks_in_progress"), F.lit(0)).alias(
            "clickup_tasks_in_progress"
        ),
        F.coalesce(F.col("meetings_this_week"), F.lit(0)).alias("meetings_this_week"),
        F.coalesce(F.col("meetings_this_month"), F.lit(0)).alias("meetings_this_month"),
    )


DEFAULT_WORKSPACE_SCHEMA = "workspace_483zkq9rh8a9jmm19zk3oxfys"
DUMMY_EMAILS = {"admin@example.com", "yourgithubemail@gmail.com"}


def is_verified_identity(email: str | None) -> bool:
    """Verify that an email is structurally valid and not a placeholder/dummy account."""
    if not email:
        return False
    clean = email.strip().lower()
    if clean in DUMMY_EMAILS:
        return False
    if "@" not in clean or "." not in clean.split("@")[-1]:
        return False
    return True


def write_fathom_meetings(
    cursor,
    schema: str,
    meetings_df: DataFrame | None,
    dry_run: bool = False,
) -> int:
    """Insert or update Fathom call recordings into ActionOS 'callRecording' table."""
    if meetings_df is None:
        return 0
    try:
        rows = meetings_df.collect()
    except Exception as exc:
        print(f"Notice: Could not collect fathom_meetings: {exc}")
        return 0

    if not rows:
        return 0

    if dry_run:
        print(f"[DRY RUN] Would sync {len(rows)} meetings to '{schema}'.'callRecording'")
        return len(rows)

    insert_sql = f"""
        INSERT INTO "{schema}"."callRecording" (
            "id", "createdAt", "updatedAt", "title", "startedAt", "endedAt",
            "meetingUrl", "summaryMarkdown", "recordedByEmail", "fathomRecordingId",
            "status", "pipelineStatus", "createdBySource", "updatedBySource",
            "createdByName", "updatedByName", "position"
        ) VALUES (
            md5('fathom_meeting_' || %s)::uuid,
            COALESCE(%s, NOW()),
            NOW(),
            COALESCE(%s, %s, 'Fathom Meeting'),
            %s,
            %s,
            COALESCE(%s, %s),
            %s,
            %s,
            %s,
            'COMPLETED',
            'COMPLETED',
            'IMPORT',
            'IMPORT',
            'Fathom',
            'Fathom',
            1000.0
        ) ON CONFLICT ("id") DO UPDATE
        SET "summaryMarkdown" = EXCLUDED."summaryMarkdown",
            "meetingUrl" = EXCLUDED."meetingUrl",
            "title" = EXCLUDED."title",
            "updatedAt" = NOW();
    """

    count = 0
    for r in rows:
        rec_id = str(r["recording_id"]) if r["recording_id"] is not None else None
        if not rec_id:
            continue
        cursor.execute(
            insert_sql,
            (
                rec_id,
                r["recording_start_time"],
                r["title"],
                r["meeting_title"] if "meeting_title" in r else None,
                r["recording_start_time"],
                r["recording_end_time"],
                r["meeting_url"],
                r["share_url"] if "share_url" in r else None,
                r["summary"],
                r["host_email"],
                rec_id,
            ),
        )
        count += cursor.rowcount
    print(f"Successfully synced {count} meetings to ActionOS 'callRecording'!")
    return count


def write_fathom_action_items(
    cursor,
    schema: str,
    action_items_df: DataFrame | None,
    dry_run: bool = False,
) -> int:
    """Insert or update Fathom action items into ActionOS 'task' table."""
    if action_items_df is None:
        return 0
    try:
        rows = action_items_df.collect()
    except Exception as exc:
        print(f"Notice: Could not collect fathom_action_items: {exc}")
        return 0

    if not rows:
        return 0

    if dry_run:
        print(f"[DRY RUN] Would sync {len(rows)} action items to '{schema}'.'task'")
        return len(rows)

    insert_sql = f"""
        INSERT INTO "{schema}"."task" (
            "id", "createdAt", "updatedAt", "title", "bodyV2Markdown",
            "assigneeName", "assigneeEmail", "callRecordingId", "dedupeKey",
            "status", "createdBySource", "updatedBySource", "createdByName",
            "updatedByName", "position"
        ) VALUES (
            md5('fathom_task_' || %s || '_' || %s)::uuid,
            NOW(),
            NOW(),
            SUBSTRING(%s FROM 1 FOR 255),
            %s,
            %s,
            %s,
            md5('fathom_meeting_' || %s)::uuid,
            %s,
            CASE WHEN %s THEN 'DONE'::"{schema}"."task_status_enum"
                 ELSE 'TODO'::"{schema}"."task_status_enum" END,
            'IMPORT'::"{schema}"."task_createdBySource_enum",
            'IMPORT'::"{schema}"."task_updatedBySource_enum",
            'Fathom AI',
            'Fathom AI',
            %s
        ) ON CONFLICT ("id") DO UPDATE
        SET "title" = EXCLUDED."title",
            "bodyV2Markdown" = EXCLUDED."bodyV2Markdown",
            "status" = EXCLUDED."status",
            "updatedAt" = NOW();
    """

    import json

    count = 0
    for r in rows:
        rec_id = str(r["recording_id"]) if r["recording_id"] is not None else None
        if not rec_id:
            continue
        t_idx = int(r["task_index"]) if r["task_index"] is not None else 0
        raw_text = r["task_text"] or ""

        title = raw_text
        body = raw_text
        assignee_name = None
        assignee_email = None
        completed = False

        if raw_text.strip().startswith("{") and raw_text.strip().endswith("}"):
            try:
                obj = json.loads(raw_text)
                title = obj.get("description") or title
                playback = obj.get("recording_playback_url")
                body = f"{title}\n\nPlayback: {playback}" if playback else title
                completed = bool(obj.get("completed", False))
                assignee = obj.get("assignee") or {}
                assignee_name = assignee.get("name")
                assignee_email = assignee.get("email")
            except Exception:
                pass

        dedupe_key = f"fathom_{rec_id}_{t_idx}"
        pos = float(t_idx * 1000 + 1000)

        cursor.execute(
            insert_sql,
            (
                rec_id,
                t_idx,
                title,
                body,
                assignee_name,
                assignee_email,
                rec_id,
                dedupe_key,
                completed,
                pos,
            ),
        )
        count += cursor.rowcount
    print(f"Successfully synced {count} action items to ActionOS 'task'!")
    return count


def write_to_postgres(
    metrics_df: DataFrame,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    workspace_schema: str | None = None,
    dry_run: bool = False,
    meetings_df: DataFrame | None = None,
    action_items_df: DataFrame | None = None,
) -> int:
    """Connect to PostgreSQL via pg8000 and update ActionOS tables in batches."""
    import pg8000.dbapi

    rows = metrics_df.collect()
    valid_rows = [r for r in rows if is_verified_identity(r["primary_email"])]
    if not valid_rows:
        print("No verified person metrics to sync back to PostgreSQL.")
        return 0

    schema = workspace_schema or DEFAULT_WORKSPACE_SCHEMA

    if dry_run:
        print(f"\n[DRY RUN] Would update {len(valid_rows)} records in schema \"{schema}\":")
        for r in valid_rows:
            print(
                f"  - {r['primary_email']}: score={r['people_score']}, rank={r['team_rank']}, "
                f"prs_opened={r['github_prs_opened_this_month']}, "
                f"prs_merged={r['github_prs_merged_this_month']}, "
                f"tasks_done={r['clickup_tasks_done_this_month']}, "
                f"github={r['github_username']}, slack={r['slack_user_id']}"
            )
        write_fathom_meetings(None, schema, meetings_df, dry_run=True)
        write_fathom_action_items(None, schema, action_items_df, dry_run=True)
        return len(valid_rows)

    conn = pg8000.dbapi.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        timeout=15,
    )

    try:
        cursor = conn.cursor()
        print(f"Writing Gold metrics back into PostgreSQL table: \"{schema}\".\"person\"...")

        update_sql = f"""
            UPDATE "{schema}"."person"
            SET "githubPrsOpenedThisMonth" = COALESCE(%s, "githubPrsOpenedThisMonth"),
                "githubPrsMergedThisMonth" = COALESCE(%s, "githubPrsMergedThisMonth"),
                "githubCommitsThisMonth" = COALESCE(%s, "githubCommitsThisMonth"),
                "clickupTasksDoneThisWeek" = COALESCE(%s, "clickupTasksDoneThisWeek"),
                "clickupTasksDoneThisMonth" = COALESCE(%s, "clickupTasksDoneThisMonth"),
                "clickupHoursTrackedThisMonth" = COALESCE(
                    %s, "clickupHoursTrackedThisMonth"
                ),
                "clickupTasksInProgress" = COALESCE(%s, "clickupTasksInProgress"),
                "meetingsThisWeek" = COALESCE(%s, "meetingsThisWeek"),
                "meetingsThisMonth" = COALESCE(%s, "meetingsThisMonth"),
                "peopleScore" = COALESCE(%s, "peopleScore"),
                "teamRank" = COALESCE(%s, "teamRank"),
                "githubUsername" = COALESCE(%s, "githubUsername"),
                "slackUserId" = COALESCE(%s, "slackUserId")
            WHERE lower(trim("emailsPrimaryEmail")) = lower(%s)
               OR lower(trim("workEmail")) = lower(%s);
        """

        updated_count = 0
        for r in valid_rows:
            cursor.execute(
                update_sql,
                (
                    r["github_prs_opened_this_month"],
                    r["github_prs_merged_this_month"],
                    r["github_commits_this_month"],
                    r["clickup_tasks_done_this_week"],
                    r["clickup_tasks_done_this_month"],
                    r["clickup_hours_tracked_this_month"],
                    r["clickup_tasks_in_progress"],
                    r["meetings_this_week"],
                    r["meetings_this_month"],
                    r["people_score"],
                    r["team_rank"],
                    r["github_username"],
                    r["slack_user_id"],
                    r["primary_email"],
                    r["primary_email"],
                ),
            )
            updated_count += cursor.rowcount

        # Write meetings and action items
        write_fathom_meetings(cursor, schema, meetings_df, dry_run=False)
        write_fathom_action_items(cursor, schema, action_items_df, dry_run=False)

        conn.commit()
        print(f"Successfully updated {updated_count} person records in ActionOS PostgreSQL!")
        return updated_count
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Sync Gold metrics to ActionOS PostgreSQL")
    parser.add_argument("--dim-person-table", required=True)
    parser.add_argument("--gold-activity-table", default=None)
    parser.add_argument("--clickup-gold-table", default=None)
    parser.add_argument("--fathom-gold-table", default=None)
    parser.add_argument("--fathom-meetings-table", default=None)
    parser.add_argument("--fathom-action-items-table", default=None)
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--database", default="default")
    parser.add_argument(
        "--workspace-schema",
        default=DEFAULT_WORKSPACE_SCHEMA,
        help="Target ActionOS workspace schema in PostgreSQL",
    )
    parser.add_argument("--secret-scope", default="mb-ingest")
    parser.add_argument("--secret-key", default="postgres-password")
    parser.add_argument(
        "--enable-direct-write-back",
        action="store_true",
        default=False,
        help=(
            "Explicitly enable direct PostgreSQL write-back (disabled by default to "
            "prevent competing writers with ActionOS Twenty Logic Function worker)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview updates without modifying the database",
    )
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    # 1. Safely load Gold tables
    dim_df = safe_table(spark, args.dim_person_table)
    if dim_df is None:
        print(f"dim_person table '{args.dim_person_table}' does not exist. Skipping write-back.")
        return

    activity_df = safe_table(spark, args.gold_activity_table)
    cu_gold_df = safe_table(spark, args.clickup_gold_table)
    fm_gold_df = safe_table(spark, args.fathom_gold_table)
    fm_meetings_df = safe_table(spark, args.fathom_meetings_table)
    fm_action_items_df = safe_table(spark, args.fathom_action_items_table)

    # 2. Combine all 3 groups of Gold metrics
    metrics_df = prepare_person_metrics(
        dim_person_df=dim_df,
        gold_activity_df=activity_df,
        clickup_gold_df=cu_gold_df,
        fathom_gold_df=fm_gold_df,
    )

    if args.dry_run:
        write_to_postgres(
            metrics_df=metrics_df,
            host="localhost",
            port=args.port,
            user=args.user,
            password="dry-run-password",
            database=args.database,
            workspace_schema=args.workspace_schema,
            dry_run=True,
            meetings_df=fm_meetings_df,
            action_items_df=fm_action_items_df,
        )
        return

    # Prevent competing writers: ActionOS uses the Twenty Logic Function poller worker
    # as the authoritative, deduplicating writer into ActionOS.
    direct_write_enabled = (
        args.enable_direct_write_back
        or os.environ.get("ENABLE_DIRECT_POSTGRES_WRITE_BACK", "").lower() in ("true", "1", "yes")
    )
    if not direct_write_enabled:
        print(
            "Notice: Direct PostgreSQL write-back is disabled by default to prevent "
            "competing writers with ActionOS Twenty Logic Function worker (databricks-gold-poll). "
            "ActionOS Logic Function safely syncs Gold metrics into ActionOS via GraphQL. "
            "To force direct write-back, pass --enable-direct-write-back or set "
            "ENABLE_DIRECT_POSTGRES_WRITE_BACK=true."
        )
        return

    # 3. Resolve host and password
    host = args.host
    if not host:
        try:
            from databricks.sdk.runtime import dbutils
            host = dbutils.secrets.get(scope=args.secret_scope, key="postgres-host")
        except Exception:
            host = os.environ.get("PG_DATABASE_HOST")

    password = None
    try:
        from databricks.sdk.runtime import dbutils
        password = dbutils.secrets.get(scope=args.secret_scope, key=args.secret_key)
    except Exception:
        password = os.environ.get("PG_DATABASE_PASSWORD")

    if not host or not password:
        raise ValueError(
            "Missing PostgreSQL credentials: host or password is not provided in arguments, "
            "secrets, or environment variables. Cannot perform direct write-back without "
            "credentials."
        )

    try:
        write_to_postgres(
            metrics_df=metrics_df,
            host=host,
            port=args.port,
            user=args.user,
            password=password,
            database=args.database,
            workspace_schema=args.workspace_schema,
            dry_run=False,
            meetings_df=fm_meetings_df,
            action_items_df=fm_action_items_df,
        )
    except Exception as exc:
        print(f"Warning: Could not write back to PostgreSQL at {host}:{args.port}: {exc}")
        if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
            # On cloud production runs, raise so alert triggers
            if host not in ("localhost", "127.0.0.1"):
                raise exc


if __name__ == "__main__":
    main()

