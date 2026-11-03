"""Gold weekly team activity scorecard model partitioned by calendar week."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.transform.gold.dim_person import (
    GITHUB_TEAM_MAPPING,
    STATIC_EMAIL_ALIASES,
    _build_dynamic_github_mapping,
    _normalize_email_expr,
)


def _map_github_author_to_email_expr(
    author_login_col,
    author_email_col,
    mapping: dict[str, str] | None = None,
):
    """Resolve a GitHub commit/PR to a team primary_email using dynamic mapping and email."""
    active_mapping = mapping if mapping is not None else GITHUB_TEAM_MAPPING
    mapping_kvs = []
    for gh_user, email in active_mapping.items():
        mapping_kvs.extend([F.lit(gh_user.lower()), F.lit(email.lower())])
    mapping_map = F.create_map(*mapping_kvs) if mapping_kvs else None

    clean_email = F.when(
        author_email_col.isNotNull()
        & ~author_email_col.like("%users.noreply.github.com")
        & (F.trim(author_email_col) != ""),
        F.lower(F.trim(author_email_col)),
    ).otherwise(F.lit(None))

    if mapping_map is not None:
        mapped_login = F.when(
            author_login_col.isNotNull(),
            mapping_map.getItem(F.lower(F.trim(author_login_col))),
        ).otherwise(F.lit(None))
        resolved = F.coalesce(mapped_login, clean_email)
    else:
        resolved = clean_email
    return _normalize_email_expr(resolved, STATIC_EMAIL_ALIASES)


def aggregate_github_metrics(
    prs_df: DataFrame | None,
    commits_df: DataFrame | None,
    mapping: dict[str, str] | None = None,
) -> DataFrame | None:
    """Aggregate PRs and commits authored per (primary_email, activity_week)."""
    if prs_df is None and commits_df is None:
        return None

    # Always discover login->email mappings from history; explicit mapping wins on conflicts.
    # This keeps attribution working even when dim_person.github_username holds emails,
    # because dynamic login discovery is merged in rather than bypassed.
    mapping = {**_build_dynamic_github_mapping(commits_df, prs_df), **(mapping or {})}

    # Aggregated PRs per week
    prs_agg = None
    if prs_df is not None:
        login_col = F.col("author_login") if "author_login" in prs_df.columns else F.lit(None)
        email_col = F.col("author_email") if "author_email" in prs_df.columns else F.lit(None)
        date_col = F.coalesce(
            F.col("merged_at") if "merged_at" in prs_df.columns else F.lit(None),
            F.col("closed_at") if "closed_at" in prs_df.columns else F.lit(None),
            F.col("created_at") if "created_at" in prs_df.columns else F.lit(None),
        )
        resolved_email = _map_github_author_to_email_expr(login_col, email_col, mapping=mapping)
        state_col = F.col("state") if "state" in prs_df.columns else F.lit("")

        valid_prs = prs_df.filter(resolved_email.isNotNull() & date_col.isNotNull())
        prs_agg = valid_prs.groupBy(
            resolved_email.alias("primary_email"),
            F.to_date(F.date_trunc("week", F.to_timestamp(date_col))).alias("activity_week"),
        ).agg(
            F.count(F.lit(1)).alias("prs_opened"),
            F.sum(
                F.when(
                    (state_col == "closed")
                    & (
                        F.col("merged_at").isNotNull()
                        if "merged_at" in prs_df.columns
                        else F.lit(True)
                    ),
                    1,
                ).otherwise(0)
            ).alias("prs_merged"),
        )

    # Aggregated Commits per week
    commits_agg = None
    if commits_df is not None:
        login_col = F.col("author_login") if "author_login" in commits_df.columns else F.lit(None)
        email_col = F.col("author_email") if "author_email" in commits_df.columns else F.lit(None)
        date_candidates = [
            F.col(c)
            for c in ("committed_at", "authored_at", "author_date", "commit_date")
            if c in commits_df.columns
        ]
        date_col = F.coalesce(*date_candidates) if date_candidates else F.lit(None)
        resolved_email = _map_github_author_to_email_expr(login_col, email_col, mapping=mapping)

        valid_commits = commits_df.filter(resolved_email.isNotNull() & date_col.isNotNull())
        commits_agg = valid_commits.groupBy(
            resolved_email.alias("primary_email"),
            F.to_date(F.date_trunc("week", F.to_timestamp(date_col))).alias("activity_week"),
        ).agg(
            F.count(F.lit(1)).alias("commits_authored"),
        )

    if prs_agg is None and commits_agg is None:
        return None
    if prs_agg is not None and commits_agg is None:
        return prs_agg.withColumn("commits_authored", F.lit(0))
    if prs_agg is None and commits_agg is not None:
        return (
            commits_agg.withColumn("prs_opened", F.lit(0))
            .withColumn("prs_merged", F.lit(0))
        )

    return (
        prs_agg.join(
            commits_agg,
            on=["primary_email", "activity_week"],
            how="outer",
        )
        .withColumn("prs_opened", F.coalesce(F.col("prs_opened"), F.lit(0)))
        .withColumn("prs_merged", F.coalesce(F.col("prs_merged"), F.lit(0)))
        .withColumn("commits_authored", F.coalesce(F.col("commits_authored"), F.lit(0)))
    )


def aggregate_clickup_metrics(tasks_df: DataFrame | None) -> DataFrame | None:
    """Aggregate tasks done, in progress, and time spent per (primary_email, activity_week)."""
    if tasks_df is None:
        return None

    date_col = F.coalesce(
        F.col("done_at") if "done_at" in tasks_df.columns else F.lit(None),
        F.col("updated_at") if "updated_at" in tasks_df.columns else F.lit(None),
        F.col("created_at") if "created_at" in tasks_df.columns else F.lit(None),
        F.col("date_done") if "date_done" in tasks_df.columns else F.lit(None),
        F.col("date_updated") if "date_updated" in tasks_df.columns else F.lit(None),
        F.col("date_created") if "date_created" in tasks_df.columns else F.lit(None),
    )

    if "assignee_emails" in tasks_df.columns:
        exploded = tasks_df.select(
            F.explode_outer("assignee_emails").alias("email_val"),
            F.col("status_type") if "status_type" in tasks_df.columns else F.lit(""),
            F.col("status") if "status" in tasks_df.columns else F.lit(""),
            (
                F.col("time_spent_minutes")
                if "time_spent_minutes" in tasks_df.columns
                else F.lit(0)
            ).alias("time_spent_m"),
            date_col.alias("task_date"),
        )
    elif "assignees" in tasks_df.columns:
        exploded = tasks_df.select(
            F.explode_outer("assignees.email").alias("email_val"),
            (
                F.col("status.type")
                if "status" in tasks_df.columns
                else F.lit("")
            ).alias("status_type"),
            (
                F.col("status.status")
                if "status" in tasks_df.columns
                else F.lit("")
            ).alias("status"),
            (
                F.coalesce(F.col("time_spent") / 60000.0, F.lit(0.0))
                if "time_spent" in tasks_df.columns
                else F.lit(0.0)
            ).alias("time_spent_m"),
            date_col.alias("task_date"),
        )
    else:
        return None

    valid = exploded.filter(F.col("email_val").isNotNull() & F.col("task_date").isNotNull())
    norm_cu_email = _normalize_email_expr(F.col("email_val"), STATIC_EMAIL_ALIASES)
    return valid.groupBy(
        norm_cu_email.alias("primary_email"),
        F.to_date(F.date_trunc("week", F.to_timestamp(F.col("task_date")))).alias("activity_week"),
    ).agg(
        F.sum(
            F.when(
                (F.col("status_type") == "closed") | (F.col("status") == "complete"),
                1,
            ).otherwise(0)
        ).alias("tasks_done"),
        F.sum(
            F.when(
                (F.col("status_type") != "closed") & (F.col("status") != "complete"),
                1,
            ).otherwise(0)
        ).alias("tasks_in_progress"),
        F.coalesce(F.round(F.sum("time_spent_m"), 1), F.lit(0.0)).alias(
            "time_spent_minutes"
        ),
    )


def aggregate_fathom_metrics(meetings_df: DataFrame | None) -> DataFrame | None:
    """Aggregate meetings hosted per (primary_email, activity_week)."""
    if meetings_df is None or "host_email" not in meetings_df.columns:
        return None

    rec_start = (
        F.col("recording_start_time")
        if "recording_start_time" in meetings_df.columns
        else F.lit(None)
    )
    sched_start = (
        F.col("scheduled_start_time")
        if "scheduled_start_time" in meetings_df.columns
        else F.lit(None)
    )
    date_col = F.coalesce(
        rec_start,
        sched_start,
        F.col("created_at") if "created_at" in meetings_df.columns else F.lit(None),
        F.col("start_time") if "start_time" in meetings_df.columns else F.lit(None),
    )
    if (
        "recording_start_time" in meetings_df.columns
        and "recording_end_time" in meetings_df.columns
    ):
        dur_col = (
            F.unix_timestamp(F.to_timestamp(F.col("recording_end_time")))
            - F.unix_timestamp(F.to_timestamp(F.col("recording_start_time")))
        )
    elif "duration_seconds" in meetings_df.columns:
        dur_col = F.col("duration_seconds")
    else:
        dur_col = F.lit(0.0)

    valid = meetings_df.filter(F.col("host_email").isNotNull() & date_col.isNotNull())
    norm_fm_email = _normalize_email_expr(F.col("host_email"), STATIC_EMAIL_ALIASES)
    return valid.groupBy(
        norm_fm_email.alias("primary_email"),
        F.to_date(F.date_trunc("week", F.to_timestamp(date_col))).alias("activity_week"),
    ).agg(
        F.count(F.lit(1)).alias("meetings_hosted"),
        F.coalesce(F.round(F.sum(dur_col) / 60.0, 1), F.lit(0.0)).alias(
            "meeting_duration_minutes"
        ),
    )


def aggregate_slack_metrics(
    messages_df: DataFrame | None,
    users_df: DataFrame | None = None,
) -> DataFrame | None:
    """Aggregate public channel Slack activity per (primary_email, activity_week)."""
    if messages_df is None:
        return None

    # Resolve author to email
    if "email" in messages_df.columns:
        with_email = messages_df
    elif users_df is not None and "user_id" in users_df.columns and "email" in users_df.columns:
        users_lookup = users_df.select(
            F.col("user_id"),
            F.col("email").alias("user_email"),
        ).dropDuplicates(["user_id"])
        with_email = messages_df.join(users_lookup, on="user_id", how="left").withColumn(
            "email", F.col("user_email")
        )
    else:
        return None

    date_col = (
        F.col("created_at")
        if "created_at" in with_email.columns
        else F.timestamp_seconds(F.col("message_ts").cast("double"))
    )

    valid = with_email.filter(F.col("email").isNotNull() & date_col.isNotNull())
    if "subtype" in with_email.columns:
        valid = valid.filter(
            ~F.coalesce(F.col("subtype"), F.lit("")).isin("channel_join", "channel_leave")
        )

    norm_email = _normalize_email_expr(F.col("email"), STATIC_EMAIL_ALIASES)
    is_reply = (
        F.col("is_thread_reply")
        if "is_thread_reply" in valid.columns
        else F.when(
            F.col("thread_ts").isNotNull() & (F.col("thread_ts") != F.col("message_ts")),
            True,
        ).otherwise(False)
    )

    return valid.groupBy(
        norm_email.alias("primary_email"),
        F.to_date(F.date_trunc("week", F.to_timestamp(date_col))).alias("activity_week"),
    ).agg(
        F.count(F.lit(1)).alias("slack_messages_count"),
        F.sum(F.when(is_reply, 1).otherwise(0)).alias("slack_thread_replies_count"),
        F.countDistinct(F.to_date(F.to_timestamp(date_col))).alias("slack_active_days"),
    )


def build_gold_person_activity(
    dim_person_df: DataFrame,
    github_prs_df: DataFrame | None = None,
    github_commits_df: DataFrame | None = None,
    clickup_tasks_df: DataFrame | None = None,
    fathom_meetings_df: DataFrame | None = None,
    slack_messages_df: DataFrame | None = None,
    slack_users_df: DataFrame | None = None,
) -> DataFrame:
    """Join dim_person with weekly GitHub, ClickUp, Fathom, and Slack metrics using exact keys."""
    # Build dynamic mapping from dim_person (the gold single source of truth!)
    dim_mapping = {}
    if "github_username" in dim_person_df.columns and "primary_email" in dim_person_df.columns:
        rows = (
            dim_person_df.filter(
                F.col("github_username").isNotNull() & F.col("primary_email").isNotNull()
            )
            .select("github_username", "primary_email")
            .collect()
        )
        for r in rows:
            if r["github_username"] and r["primary_email"]:
                dim_mapping[r["github_username"].lower()] = r["primary_email"].lower()

    # 1. Aggregate each source by (primary_email, activity_week)
    gh_agg = aggregate_github_metrics(
        github_prs_df,
        github_commits_df,
        mapping=dim_mapping or None,
    )
    cu_agg = aggregate_clickup_metrics(clickup_tasks_df)
    fm_agg = aggregate_fathom_metrics(fathom_meetings_df)
    slack_agg = aggregate_slack_metrics(slack_messages_df, slack_users_df)

    # 2. Collect all active (primary_email, activity_week) pairs
    weeks_dfs = []
    for agg in [gh_agg, cu_agg, fm_agg, slack_agg]:
        if agg is not None:
            weeks_dfs.append(agg.select("primary_email", "activity_week"))

    if not weeks_dfs:
        # If no activity in any source, return an empty schema-compatible dataframe
        empty_df = dim_person_df.limit(0).withColumn(
            "activity_week", F.to_date(F.lit("2026-01-01"))
        )
        return (
            empty_df.withColumn("prs_opened", F.lit(0))
            .withColumn("prs_merged", F.lit(0))
            .withColumn("commits_authored", F.lit(0))
            .withColumn("tasks_done", F.lit(0))
            .withColumn("tasks_in_progress", F.lit(0))
            .withColumn("time_spent_minutes", F.lit(0.0))
            .withColumn("meetings_hosted", F.lit(0))
            .withColumn("meeting_duration_minutes", F.lit(0.0))
            .withColumn("slack_messages_count", F.lit(0))
            .withColumn("slack_thread_replies_count", F.lit(0))
            .withColumn("slack_active_days", F.lit(0))
            .withColumn("productivity_score", F.lit(0))
        )

    active_weeks = weeks_dfs[0]
    for other in weeks_dfs[1:]:
        active_weeks = active_weeks.unionByName(other)
    active_weeks = active_weeks.dropDuplicates(["primary_email", "activity_week"])

    # 3. Join with dim_person (single-key equality join on primary_email)
    activity_grid = active_weeks.join(
        dim_person_df,
        on="primary_email",
        how="inner",
    )

    # 4. Join GitHub metrics (exact equality join on [primary_email, activity_week])
    if gh_agg is not None:
        activity_grid = (
            activity_grid.join(gh_agg, on=["primary_email", "activity_week"], how="left")
            .withColumn("prs_opened", F.coalesce(F.col("prs_opened"), F.lit(0)))
            .withColumn("prs_merged", F.coalesce(F.col("prs_merged"), F.lit(0)))
            .withColumn("commits_authored", F.coalesce(F.col("commits_authored"), F.lit(0)))
        )
    else:
        activity_grid = (
            activity_grid.withColumn("prs_opened", F.lit(0))
            .withColumn("prs_merged", F.lit(0))
            .withColumn("commits_authored", F.lit(0))
        )

    # 5. Join ClickUp metrics (exact equality join on [primary_email, activity_week])
    if cu_agg is not None:
        activity_grid = (
            activity_grid.join(cu_agg, on=["primary_email", "activity_week"], how="left")
            .withColumn("tasks_done", F.coalesce(F.col("tasks_done"), F.lit(0)))
            .withColumn("tasks_in_progress", F.coalesce(F.col("tasks_in_progress"), F.lit(0)))
            .withColumn(
                "time_spent_minutes", F.coalesce(F.col("time_spent_minutes"), F.lit(0.0))
            )
        )
    else:
        activity_grid = (
            activity_grid.withColumn("tasks_done", F.lit(0))
            .withColumn("tasks_in_progress", F.lit(0))
            .withColumn("time_spent_minutes", F.lit(0.0))
        )

    # 6. Join Fathom metrics (exact equality join on [primary_email, activity_week])
    if fm_agg is not None:
        activity_grid = (
            activity_grid.join(fm_agg, on=["primary_email", "activity_week"], how="left")
            .withColumn("meetings_hosted", F.coalesce(F.col("meetings_hosted"), F.lit(0)))
            .withColumn(
                "meeting_duration_minutes",
                F.coalesce(F.col("meeting_duration_minutes"), F.lit(0.0)),
            )
        )
    else:
        activity_grid = activity_grid.withColumn("meetings_hosted", F.lit(0)).withColumn(
            "meeting_duration_minutes", F.lit(0.0)
        )

    # 7. Join Slack metrics (exact equality join on [primary_email, activity_week])
    if slack_agg is not None:
        activity_grid = (
            activity_grid.join(slack_agg, on=["primary_email", "activity_week"], how="left")
            .withColumn("slack_messages_count", F.coalesce(F.col("slack_messages_count"), F.lit(0)))
            .withColumn(
                "slack_thread_replies_count",
                F.coalesce(F.col("slack_thread_replies_count"), F.lit(0)),
            )
            .withColumn("slack_active_days", F.coalesce(F.col("slack_active_days"), F.lit(0)))
        )
    else:
        activity_grid = (
            activity_grid.withColumn("slack_messages_count", F.lit(0))
            .withColumn("slack_thread_replies_count", F.lit(0))
            .withColumn("slack_active_days", F.lit(0))
        )

    # 8. Weekly Balanced Productivity Score (0 - 100) per calendar week
    score_expr = (
        F.col("prs_merged") * 25
        + F.col("commits_authored") * 5
        + F.col("tasks_done") * 15
        + F.col("meetings_hosted") * 5
    )
    result_df = activity_grid.withColumn(
        "productivity_score",
        F.when(score_expr > 100, 100).otherwise(score_expr).cast("int"),
    )

    return result_df.select(
        "person_id",
        "full_name",
        "primary_email",
        "job_title",
        "activity_week",
        "prs_opened",
        "prs_merged",
        "commits_authored",
        "tasks_done",
        "tasks_in_progress",
        "time_spent_minutes",
        "meetings_hosted",
        "meeting_duration_minutes",
        "slack_messages_count",
        "slack_thread_replies_count",
        "slack_active_days",
        "productivity_score",
    )

