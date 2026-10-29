"""Canonical dim_person dimension table harvesting cross-platform team identities."""
from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def _load_team_mapping(
    team_mapping_df: DataFrame | None = None,
    config_path: str | Path | None = None,
) -> dict[str, str]:
    """Load team handle-to-email mapping from Delta table, config file, or fallback.

    Ensures adding a new team member does not require Python code changes.
    """
    mapping: dict[str, str] = {}

    # 1. Delta table source (highest priority for production)
    if team_mapping_df is not None:
        cols = set(team_mapping_df.columns)
        gh_col = "github_username" if "github_username" in cols else (
            "login" if "login" in cols else None
        )
        email_col = "primary_email" if "primary_email" in cols else (
            "email" if "email" in cols else None
        )
        if gh_col and email_col:
            rows = (
                team_mapping_df.filter(
                    F.col(gh_col).isNotNull() & F.col(email_col).isNotNull()
                )
                .select(
                    F.lower(F.trim(F.col(gh_col))).alias("gh"),
                    F.lower(F.trim(F.col(email_col))).alias("em"),
                )
                .collect()
            )
            for r in rows:
                if r["gh"] and r["em"]:
                    mapping[r["gh"]] = r["em"]
            if mapping:
                return mapping

    # 2. External JSON configuration file
    cfg_file = (
        Path(config_path)
        if config_path
        else Path(__file__).resolve().parent / "team_mapping.json"
    )
    if cfg_file.exists():
        with open(cfg_file, encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return {k.lower(): v.lower() for k, v in data.items()}

    # 3. Fallback defaults if no table or config file is available
    return {
        "go2usman": "usman@codersboutique.com",
        "nik-767": "nikhil@codersboutique.com",
        "sahilgidwani-26": "sahil@codersboutique.com",
        "fenil": "fenil@codersboutique.com",
        "dhameliya-fenil": "fenil@codersboutique.com",
        "kavinj98": "kavin@codersboutique.com",
    }


_STATIC_GITHUB_FALLBACK = _load_team_mapping()
GITHUB_TEAM_MAPPING = _STATIC_GITHUB_FALLBACK


def _build_dynamic_github_mapping(
    commits_df: DataFrame | None = None,
    prs_df: DataFrame | None = None,
    static_mapping: dict[str, str] | None = None,
) -> dict[str, str]:
    """Dynamically discover GitHub login -> corporate email from commit history and PRs.

    STATIC / CONFIGURED MAPPING ALWAYS WINS:
    If a user has a designated corporate email in the mapping table or config,
    commits made with personal emails (e.g. gmail.com) will NEVER overwrite it.
    """
    configured = dict(static_mapping if static_mapping is not None else _load_team_mapping())
    dynamic: dict[str, str] = {}

    for src in [commits_df, prs_df]:
        if src is None:
            continue
        needed_cols = {"author_login", "author_email"}
        if not needed_cols.issubset(set(src.columns)):
            continue

        rows = (
            src.filter(
                F.col("author_login").isNotNull()
                & F.col("author_email").isNotNull()
                & ~F.col("author_email").like("%users.noreply.github.com")
                & (F.trim(F.col("author_email")) != "")
            )
            .groupBy(F.lower(F.trim(F.col("author_login"))).alias("login"))
            .agg(F.max(F.lower(F.trim(F.col("author_email")))).alias("email"))
            .collect()
        )
        for row in rows:
            login = row["login"]
            email = row["email"]
            if login and email:
                dynamic[login] = email

    # Static/configured mapping WINS: overwrite any discovered dynamic emails
    final_mapping = dict(dynamic)
    final_mapping.update(configured)
    return final_mapping


def _harvest_twenty_workspace_members(members_df: DataFrame | None) -> DataFrame | None:
    """Harvest internal team members from Twenty CRM workspaceMember table (NOT CRM contacts)."""
    if members_df is None or "user_email" not in members_df.columns:
        return None

    valid = members_df.filter(
        F.col("user_email").isNotNull() & (F.trim(F.col("user_email")) != "")
    )

    full_name_col = F.coalesce(
        F.col("full_name") if "full_name" in members_df.columns else F.lit(None),
        F.trim(F.concat_ws(" ", F.col("first_name"), F.col("last_name"))),
        F.lit("Team Member"),
    )

    return valid.select(
        F.lower(F.trim(F.col("user_email"))).alias("primary_email"),
        full_name_col.alias("full_name"),
        F.lit(None).cast("string").alias("github_username"),
        F.lit(None).cast("string").alias("clickup_user_id"),
        F.lit(None).cast("string").alias("slack_user_id"),
        F.lit("Workspace Member").alias("job_title"),
        F.col("member_id").alias("person_id"),
        F.lit(1).alias("priority"),
    )


def _harvest_slack(slack_users_df: DataFrame | None) -> DataFrame | None:
    """Harvest team identities from Slack workspace users."""
    if slack_users_df is None:
        return None

    email_col_name = (
        "email"
        if "email" in slack_users_df.columns
        else ("user_email" if "user_email" in slack_users_df.columns else None)
    )
    if not email_col_name:
        return None

    is_bot_col = (
        F.col("is_bot") if "is_bot" in slack_users_df.columns else F.lit(False)
    )
    valid = (
        slack_users_df.filter(~is_bot_col)
        .filter(F.col(email_col_name).isNotNull() & (F.trim(F.col(email_col_name)) != ""))
        .dropDuplicates([email_col_name])
    )

    full_name_col = F.coalesce(
        F.col("real_name") if "real_name" in slack_users_df.columns else F.lit(None),
        F.col("display_name") if "display_name" in slack_users_df.columns else F.lit(None),
        F.lit("Team Member"),
    )

    title_col = (
        F.col("title")
        if "title" in slack_users_df.columns
        else F.lit("Team Member")
    )

    return valid.select(
        F.lower(F.trim(F.col(email_col_name))).alias("primary_email"),
        full_name_col.alias("full_name"),
        F.lit(None).cast("string").alias("github_username"),
        F.lit(None).cast("string").alias("clickup_user_id"),
        F.col("user_id").alias("slack_user_id"),
        F.coalesce(title_col, F.lit("Team Member")).alias("job_title"),
        F.lit(None).cast("string").alias("person_id"),
        F.lit(2).alias("priority"),
    )


def _harvest_clickup(clickup_tasks_df: DataFrame | None) -> DataFrame | None:
    """Harvest assignees from ClickUp tasks."""
    if clickup_tasks_df is None:
        return None

    email_dfs = []
    if "assignee_emails" in clickup_tasks_df.columns:
        email_dfs.append(
            clickup_tasks_df.filter(F.col("assignee_emails").isNotNull()).select(
                F.explode_outer(F.col("assignee_emails")).alias("email_raw")
            )
        )
    elif "assignees" in clickup_tasks_df.columns:
        email_dfs.append(
            clickup_tasks_df.filter(F.col("assignees").isNotNull()).select(
                F.explode_outer(F.col("assignees.email")).alias("email_raw")
            )
        )

    if not email_dfs:
        return None

    combined = email_dfs[0]
    for other in email_dfs[1:]:
        combined = combined.unionByName(other)

    valid = combined.filter(
        F.col("email_raw").isNotNull() & (F.trim(F.col("email_raw")) != "")
    ).dropDuplicates(["email_raw"])

    email_clean = F.lower(F.trim(F.col("email_raw")))
    prefix = F.split(email_clean, "@").getItem(0)
    username_guess = F.initcap(F.regexp_replace(prefix, "\\.", " "))

    return valid.select(
        email_clean.alias("primary_email"),
        username_guess.alias("full_name"),
        F.lit(None).cast("string").alias("github_username"),
        F.lit(None).cast("string").alias("clickup_user_id"),
        F.lit(None).cast("string").alias("slack_user_id"),
        F.lit("Team Member").alias("job_title"),
        F.lit(None).cast("string").alias("person_id"),
        F.lit(3).alias("priority"),
    )


def _harvest_fathom(fathom_meetings_df: DataFrame | None) -> DataFrame | None:
    """Harvest meeting hosts from Fathom."""
    if fathom_meetings_df is None or "host_email" not in fathom_meetings_df.columns:
        return None

    valid = fathom_meetings_df.filter(
        F.col("host_email").isNotNull() & (F.trim(F.col("host_email")) != "")
    ).dropDuplicates(["host_email"])

    email_clean = F.lower(F.trim(F.col("host_email")))
    prefix = F.split(email_clean, "@").getItem(0)
    name_col = F.coalesce(
        F.col("host_name") if "host_name" in fathom_meetings_df.columns else F.lit(None),
        F.initcap(F.regexp_replace(prefix, "\\.", " ")),
    )

    return valid.select(
        email_clean.alias("primary_email"),
        name_col.alias("full_name"),
        F.lit(None).cast("string").alias("github_username"),
        F.lit(None).cast("string").alias("clickup_user_id"),
        F.lit(None).cast("string").alias("slack_user_id"),
        F.lit("Team Member").alias("job_title"),
        F.lit(None).cast("string").alias("person_id"),
        F.lit(4).alias("priority"),
    )


def _harvest_github(
    prs_df: DataFrame | None, commits_df: DataFrame | None
) -> DataFrame | None:
    """Harvest contributors who have a verified author email (NO fake dummy emails)."""
    dfs = []
    for src_df in [prs_df, commits_df]:
        if src_df is None or "author_email" not in src_df.columns:
            continue

        author_login = F.col("author_login") if "author_login" in src_df.columns else F.lit(None)
        author_email = F.col("author_email")
        author_name = F.col("author_name") if "author_name" in src_df.columns else F.lit(None)

        with_email = (
            src_df.filter(author_email.isNotNull() & (F.trim(author_email) != ""))
            .filter(~author_email.like("%users.noreply.github.com"))
            .select(
                F.lower(F.trim(author_email)).alias("primary_email"),
                F.coalesce(author_name, author_login, F.lit("Contributor")).alias("full_name"),
                author_login.alias("github_username"),
                F.lit(None).cast("string").alias("clickup_user_id"),
                F.lit(None).cast("string").alias("slack_user_id"),
                F.lit("Contributor").alias("job_title"),
                F.lit(None).cast("string").alias("person_id"),
                F.lit(5).alias("priority"),
            )
        )
        dfs.append(with_email)

    if not dfs:
        return None
    res = dfs[0]
    for other in dfs[1:]:
        res = res.unionByName(other)
    return res.dropDuplicates(["primary_email"])


def build_dim_person(
    people_df: DataFrame | None = None,
    members_df: DataFrame | None = None,
    slack_users_df: DataFrame | None = None,
    github_prs_df: DataFrame | None = None,
    github_commits_df: DataFrame | None = None,
    clickup_tasks_df: DataFrame | None = None,
    fathom_meetings_df: DataFrame | None = None,
    team_mapping_df: DataFrame | None = None,
) -> DataFrame:
    """Build unified dim_person table by harvesting internal identities across platforms.

    Excludes external CRM leads/contacts (people_df) and avoids fake dummy emails.
    Uses dynamic commit history + config mapping where configured mappings always win.
    """
    candidates = []

    # 1. Harvest Twenty CRM internal workspace members (NOT external CRM contacts)
    c_twenty = _harvest_twenty_workspace_members(members_df)
    if c_twenty is not None:
        candidates.append(c_twenty)

    # 2. Harvest Slack members
    c_slack = _harvest_slack(slack_users_df)
    if c_slack is not None:
        candidates.append(c_slack)

    # 3. Harvest ClickUp assignees
    c_clickup = _harvest_clickup(clickup_tasks_df)
    if c_clickup is not None:
        candidates.append(c_clickup)

    # 4. Harvest Fathom hosts
    c_fathom = _harvest_fathom(fathom_meetings_df)
    if c_fathom is not None:
        candidates.append(c_fathom)

    # 5. Harvest GitHub contributors with valid emails
    c_github = _harvest_github(github_prs_df, github_commits_df)
    if c_github is not None:
        candidates.append(c_github)

    if not candidates:
        raise ValueError("No identity data available from any input source.")

    pool = candidates[0]
    for other in candidates[1:]:
        pool = pool.unionByName(other)

    # Deduplicate by primary_email, prioritizing highest-fidelity sources
    window = Window.partitionBy("primary_email").orderBy("priority")
    deduped = (
        pool.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Load mapping from Delta table or config file (static always wins)
    configured_mapping = _load_team_mapping(team_mapping_df)
    gh_mapping = _build_dynamic_github_mapping(
        commits_df=github_commits_df,
        prs_df=github_prs_df,
        static_mapping=configured_mapping,
    )

    # Reverse mapping: email -> github_username
    email_to_gh = {email.lower(): gh for gh, email in gh_mapping.items()}
    mapping_expr = None
    if email_to_gh:
        mapping_kvs = []
        for em, gh in email_to_gh.items():
            mapping_kvs.extend([F.lit(em), F.lit(gh)])
        mapping_expr = F.create_map(*mapping_kvs)

    # Assign deterministic person_id and accurate github_username
    resolved = deduped.withColumn(
        "person_id",
        F.coalesce(F.col("person_id"), F.sha1(F.col("primary_email"))),
    )

    if mapping_expr is not None:
        resolved = resolved.withColumn(
            "github_username",
            F.coalesce(
                mapping_expr.getItem(F.col("primary_email")),
                F.col("github_username"),
            ),
        )

    final_df = (
        resolved.withColumn("is_active", F.lit(True))
        .withColumn("updated_at", F.current_timestamp())
    )

    return final_df.select(
        "person_id",
        "full_name",
        "primary_email",
        "github_username",
        "clickup_user_id",
        "slack_user_id",
        "job_title",
        "is_active",
        "updated_at",
    )
