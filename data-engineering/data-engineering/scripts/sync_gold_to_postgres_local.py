"""Local developer script to sync Databricks Gold metrics to local ActionOS PostgreSQL.

Usage:
    python scripts/sync_gold_to_postgres_local.py
    python scripts/sync_gold_to_postgres_local.py --profile personal --prefix nikhil_
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


def run_databricks_query(
    statement: str, profile: str, warehouse_id: str | None = None
) -> list[dict]:
    """Execute SQL query against Databricks using the CLI and return list of dicts."""
    payload = {"statement": statement, "wait_timeout": "30s"}
    if warehouse_id:
        payload["warehouse_id"] = warehouse_id

    cmd = [
        "databricks", "api", "post", "/api/2.0/sql/statements",
        "-p", profile,
        "--json", json.dumps(payload),
    ]

    res = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    if res.returncode != 0:
        print(f"Databricks query error: {res.stderr}")
        return []

    try:
        data = json.loads(res.stdout)
        manifest = data.get("manifest", {})
        schema_cols = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
        rows = data.get("result", {}).get("data_array", [])
        return [dict(zip(schema_cols, r)) for r in rows]
    except Exception as exc:
        print(f"Error parsing Databricks response: {exc}")
        return []


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


def parse_names(full_name: str | None, email: str) -> tuple[str, str]:
    """Parse first and last name from full_name or fall back to email username."""
    if full_name and full_name.strip():
        parts = full_name.strip().split()
        first_name = parts[0]
        last_name = " ".join(parts[1:]) if len(parts) > 1 else ""
        return first_name, last_name

    username = email.split("@")[0].replace(".", " ").replace("-", " ").replace("_", " ")
    parts = username.split()
    first_name = parts[0].capitalize() if parts else "Team"
    last_name = " ".join(p.capitalize() for p in parts[1:]) if len(parts) > 1 else "Member"
    return first_name, last_name


def ensure_column_exists(
    schema: str,
    table: str,
    column: str,
    data_type: str,
    cursor=None,
    docker_container: str = "twenty-db-1",
) -> None:
    """Check if column exists; if not, automatically CREATE it based on target data type."""
    check_sql = (
        f"SELECT 1 FROM information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' "
        f"AND column_name = '{column}';"
    )
    add_sql = f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{column}" {data_type};'

    if cursor:
        cursor.execute(check_sql)
        if not cursor.fetchone():
            print(
                f"Schema Evolution: Adding column \"{column}\" ({data_type}) to "
                f"\"{schema}\".\"{table}\"..."
            )
            cursor.execute(add_sql)
    else:
        cmd = [
            "docker", "exec", "-i", docker_container, "psql", "-U", "postgres", "-d", "default",
            "-t", "-A", "-c", check_sql,
        ]
        res = subprocess.run(cmd, capture_output=True, text=True)
        if not res.stdout.strip():
            print(f"Schema Evolution (Docker): Adding column \"{column}\" ({data_type})...")
            alter_cmd = [
                "docker", "exec", "-i", docker_container, "psql", "-U", "postgres", "-d", "default",
                "-c", add_sql,
            ]
            subprocess.run(alter_cmd, capture_output=True, text=True)


def format_metric_row(m: dict) -> dict:
    """Extract and type metrics safely, keeping None for absent fields to avoid zero-clobber."""
    email = m.get("primary_email", "").strip().lower()
    first_name, last_name = parse_names(m.get("full_name"), email)
    prs_o = int(m["prs_opened"]) if m.get("prs_opened") is not None else None
    prs_m = int(m["prs_merged"]) if m.get("prs_merged") is not None else None
    commits = int(m["commits_authored"]) if m.get("commits_authored") is not None else None
    tasks_done = int(m["tasks_done"]) if m.get("tasks_done") is not None else None
    hours = (
        round(float(m["time_spent_minutes"]) / 60.0, 1)
        if m.get("time_spent_minutes") is not None
        else None
    )
    tasks_ip = int(m["tasks_in_progress"]) if m.get("tasks_in_progress") is not None else None
    meetings = int(m["meetings_hosted"]) if m.get("meetings_hosted") is not None else None
    score = int(m["productivity_score"]) if m.get("productivity_score") is not None else None
    rank = int(m["team_rank"]) if m.get("team_rank") is not None else None
    gh = str(m["github_username"]).strip() if m.get("github_username") else None
    slack = str(m["slack_user_id"]).strip() if m.get("slack_user_id") else None

    return {
        "primary_email": email,
        "first_name": first_name,
        "last_name": last_name,
        "prs_opened": prs_o,
        "prs_merged": prs_m,
        "commits_authored": commits,
        "tasks_done": tasks_done,
        "hours_tracked": hours,
        "tasks_in_progress": tasks_ip,
        "meetings": meetings,
        "people_score": score,
        "team_rank": rank,
        "github_username": gh,
        "slack_user_id": slack,
    }


def update_via_docker(
    metrics: list[dict],
    workspace_schema: str | None = None,
    dry_run: bool = False,
) -> int:
    """Upsert ActionOS PostgreSQL directly inside Docker container twenty-db-1 safely."""
    schema = workspace_schema or DEFAULT_WORKSPACE_SCHEMA

    # Schema evolution: ensure required columns exist
    ensure_column_exists(schema, "person", "peopleScore", "double precision")
    ensure_column_exists(schema, "person", "teamRank", "double precision")
    ensure_column_exists(schema, "person", "clickupTasksDoneThisMonth", "double precision")
    ensure_column_exists(schema, "person", "clickupTasksInProgress", "double precision")
    ensure_column_exists(schema, "person", "clickupHoursTrackedThisMonth", "double precision")
    ensure_column_exists(schema, "person", "meetingsThisMonth", "double precision")
    ensure_column_exists(schema, "person", "slackUserId", "text")
    ensure_column_exists(schema, "person", "githubUsername", "text")

    valid_records = [
        format_metric_row(m) for m in metrics if is_verified_identity(m.get("primary_email"))
    ]
    if not valid_records:
        print("No verified identities to update.")
        return 0

    if dry_run:
        print(f"\n[DRY RUN] Would upsert {len(valid_records)} records in schema \"{schema}\":")
        for r in valid_records:
            print(
                f"  - {r['first_name']} {r['last_name']} ({r['primary_email']}): "
                f"score={r['people_score']}, rank={r['team_rank']}, "
                f"tasks={r['tasks_done']}, hours={r['hours_tracked']}, "
                f"github={r['github_username']}, slack={r['slack_user_id']}"
            )
        return len(valid_records)

    print(
        f"Executing parameterized upserts in Docker container twenty-db-1 "
        f"(schema: \"{schema}\")..."
    )

    payload_json = json.dumps(valid_records)
    sql = f"""
    WITH data AS (
        SELECT * FROM jsonb_to_recordset(:'payload'::jsonb) AS x(
            primary_email text,
            first_name text,
            last_name text,
            prs_opened int,
            prs_merged int,
            commits_authored int,
            tasks_done int,
            hours_tracked numeric,
            tasks_in_progress int,
            meetings int,
            people_score int,
            team_rank int,
            github_username text,
            slack_user_id text
        )
    ),
    updated AS (
        UPDATE "{schema}"."person" p
        SET "githubPrsOpenedThisMonth" = COALESCE(d.prs_opened, p."githubPrsOpenedThisMonth"),
            "githubPrsMergedThisMonth" = COALESCE(d.prs_merged, p."githubPrsMergedThisMonth"),
            "githubCommitsThisMonth" = COALESCE(d.commits_authored, p."githubCommitsThisMonth"),
            "clickupTasksDoneThisWeek" = COALESCE(d.tasks_done, p."clickupTasksDoneThisWeek"),
            "clickupTasksDoneThisMonth" = COALESCE(d.tasks_done, p."clickupTasksDoneThisMonth"),
            "clickupHoursTrackedThisMonth" = COALESCE(
                d.hours_tracked, p."clickupHoursTrackedThisMonth"
            ),
            "clickupTasksInProgress" = COALESCE(d.tasks_in_progress, p."clickupTasksInProgress"),
            "meetingsThisWeek" = COALESCE(d.meetings, p."meetingsThisWeek"),
            "meetingsThisMonth" = COALESCE(d.meetings, p."meetingsThisMonth"),
            "peopleScore" = COALESCE(d.people_score, p."peopleScore"),
            "teamRank" = COALESCE(d.team_rank, p."teamRank"),
            "githubUsername" = COALESCE(d.github_username, p."githubUsername"),
            "slackUserId" = COALESCE(d.slack_user_id, p."slackUserId")
        FROM data d
        WHERE lower(trim(p."emailsPrimaryEmail")) = lower(trim(d.primary_email))
           OR lower(trim(p."workEmail")) = lower(trim(d.primary_email))
        RETURNING lower(trim(p."emailsPrimaryEmail")) as updated_email
    ),
    inserted AS (
        INSERT INTO "{schema}"."person" (
            "id",
            "createdAt",
            "updatedAt",
            "nameFirstName",
            "nameLastName",
            "emailsPrimaryEmail",
            "workEmail",
            "peopleScore",
            "teamRank",
            "githubPrsOpenedThisMonth",
            "githubPrsMergedThisMonth",
            "githubCommitsThisMonth",
            "clickupTasksDoneThisWeek",
            "clickupTasksDoneThisMonth",
            "clickupHoursTrackedThisMonth",
            "clickupTasksInProgress",
            "meetingsThisWeek",
            "meetingsThisMonth",
            "githubUsername",
            "slackUserId"
        )
        SELECT 
            uuid_generate_v4(),
            NOW(),
            NOW(),
            d.first_name,
            d.last_name,
            d.primary_email,
            d.primary_email,
            d.people_score,
            d.team_rank,
            d.prs_opened,
            d.prs_merged,
            d.commits_authored,
            d.tasks_done,
            d.tasks_done,
            d.hours_tracked,
            d.tasks_in_progress,
            d.meetings,
            d.meetings,
            d.github_username,
            d.slack_user_id
        FROM data d
        WHERE NOT EXISTS (
            SELECT 1 FROM "{schema}"."person" p
            WHERE lower(trim(p."emailsPrimaryEmail")) = lower(trim(d.primary_email))
               OR lower(trim(p."workEmail")) = lower(trim(d.primary_email))
        )
        RETURNING 1
    )
    SELECT 
        (SELECT count(*) FROM updated) as updated_count,
        (SELECT count(*) FROM inserted) as inserted_count;
    """

    proc = subprocess.run(
        [
            "docker", "exec", "-i", "twenty-db-1",
            "psql", "-U", "postgres", "-d", "default",
            "-t", "-A", "-F", ",",
            "-v", f"payload={payload_json}",
        ],
        input=sql,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        print(f"Docker SQL notice: {proc.stderr}")
        return 0

    up_count, in_count = 0, 0
    for line in proc.stdout.strip().splitlines():
        if "," in line:
            parts = line.split(",")
            try:
                up_count = int(parts[0])
                in_count = int(parts[1])
            except (ValueError, IndexError):
                pass

    print(
        f"Upserted records in ActionOS PostgreSQL via Docker: "
        f"{up_count} updated, {in_count} newly inserted!"
    )
    return up_count + in_count


def update_local_postgres(
    metrics: list[dict],
    host: str = "localhost",
    port: int = 5432,
    user: str = "postgres",
    password: str = "postgres",
    database: str = "default",
    workspace_schema: str | None = None,
    dry_run: bool = False,
) -> int:
    """Update local ActionOS PostgreSQL 'person' table using parameterized queries."""
    schema = workspace_schema or DEFAULT_WORKSPACE_SCHEMA

    valid_records = [
        format_metric_row(m) for m in metrics if is_verified_identity(m.get("primary_email"))
    ]
    if not valid_records:
        print("No verified identities to update.")
        return 0

    if dry_run:
        print(f"\n[DRY RUN] Would update {len(valid_records)} records in schema \"{schema}\":")
        for r in valid_records:
            print(
                f"  - {r['primary_email']}: score={r['people_score']}, rank={r['team_rank']}, "
                f"prs_opened={r['prs_opened']}, prs_merged={r['prs_merged']}, "
                f"tasks_done={r['tasks_done']}, hours={r['hours_tracked']}, "
                f"github={r['github_username']}, slack={r['slack_user_id']}"
            )
        return len(valid_records)

    try:
        import pg8000.dbapi
    except ImportError:
        print("pg8000 not installed. Please run: pip install pg8000")
        return 0

    try:
        conn = pg8000.dbapi.connect(
            host=host, port=port, user=user, password=password, database=database, timeout=5
        )
    except Exception as exc:
        print(f"Direct TCP connect to PostgreSQL at {host}:{port} threw: {exc}")
        print("Falling back to Docker container 'twenty-db-1'...")
        return update_via_docker(metrics, workspace_schema=schema, dry_run=dry_run)

    try:
        cursor = conn.cursor()
        print(f"Connecting to ActionOS schema: \"{schema}\"...")

        update_sql = f"""
            UPDATE "{schema}"."person"
            SET "githubPrsOpenedThisMonth" = COALESCE(%s, "githubPrsOpenedThisMonth"),
                "githubPrsMergedThisMonth" = COALESCE(%s, "githubPrsMergedThisMonth"),
                "githubCommitsThisMonth" = COALESCE(%s, "githubCommitsThisMonth"),
                "clickupTasksDoneThisWeek" = COALESCE(%s, "clickupTasksDoneThisWeek"),
                "clickupTasksDoneThisMonth" = COALESCE(%s, "clickupTasksDoneThisMonth"),
                "clickupHoursTrackedThisMonth" = COALESCE(%s, "clickupHoursTrackedThisMonth"),
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

        updated = 0
        for r in valid_records:
            email = r["primary_email"]
            cursor.execute(
                update_sql,
                (
                    r["prs_opened"],
                    r["prs_merged"],
                    r["commits_authored"],
                    r["tasks_done"],
                    r["tasks_done"],
                    r["hours_tracked"],
                    r["tasks_in_progress"],
                    r["meetings"],
                    r["meetings"],
                    r["people_score"],
                    r["team_rank"],
                    r["github_username"],
                    r["slack_user_id"],
                    email,
                    email,
                ),
            )
            updated += cursor.rowcount

        conn.commit()
        print(f"Updated {updated} records in local ActionOS PostgreSQL!")
        return updated
    finally:
        conn.close()

def sync_fathom_meetings_local(
    profile: str,
    warehouse_id: str | None,
    prefix: str,
    workspace_schema: str = DEFAULT_WORKSPACE_SCHEMA,
    dry_run: bool = False,
    docker_container: str = "twenty-db-1",
) -> int:
    """Fetch fathom_meetings from Databricks and insert into ActionOS callRecording table."""
    table = f"workspace.dev.{prefix}fathom_meetings"
    query = f"""
        SELECT 
            recording_id,
            COALESCE(title, meeting_title, 'Fathom Meeting') as title,
            recording_start_time,
            recording_end_time,
            COALESCE(meeting_url, share_url) as meeting_url,
            summary,
            host_email
        FROM {table}
        WHERE recording_id IS NOT NULL;
    """
    print(f"\nFetching Fathom meetings from Databricks ({table})...")
    rows = run_databricks_query(query, profile=profile, warehouse_id=warehouse_id)
    print(f"Retrieved {len(rows)} meetings.")
    if not rows:
        return 0

    if dry_run:
        print(f"[DRY RUN] Would sync {len(rows)} meetings to '{workspace_schema}'.'callRecording'")
        return len(rows)

    clean_rows = []
    for r in rows:
        clean_rows.append({
            "recording_id": str(r["recording_id"]),
            "title": str(r.get("title") or "Fathom Meeting"),
            "started_at": str(r.get("recording_start_time") or ""),
            "ended_at": str(r.get("recording_end_time") or ""),
            "meeting_url": str(r.get("meeting_url") or ""),
            "summary": str(r.get("summary") or ""),
            "host_email": str(r.get("host_email") or ""),
        })

    payload_json = json.dumps(clean_rows)
    sql = f"""
    WITH raw_data AS (
        SELECT $payload${payload_json}$payload$::jsonb AS js
    ),
    data AS (
        SELECT * FROM raw_data, jsonb_to_recordset(js) AS x(
            recording_id text,
            title text,
            started_at text,
            ended_at text,
            meeting_url text,
            summary text,
            host_email text
        )
    ),
    inserted AS (
        INSERT INTO "{workspace_schema}"."callRecording" (
            "id", "createdAt", "updatedAt", "title", "startedAt", "endedAt",
            "meetingUrl", "summaryMarkdown", "recordedByEmail", "fathomRecordingId",
            "status", "pipelineStatus", "createdBySource", "updatedBySource",
            "createdByName", "updatedByName", "position"
        )
        SELECT
            md5('fathom_meeting_' || d.recording_id)::uuid,
            CASE WHEN d.started_at != '' THEN d.started_at::timestamptz ELSE NOW() END,
            NOW(),
            d.title,
            CASE WHEN d.started_at != '' THEN d.started_at::timestamptz ELSE NULL END,
            CASE WHEN d.ended_at != '' THEN d.ended_at::timestamptz ELSE NULL END,
            d.meeting_url,
            d.summary,
            d.host_email,
            d.recording_id,
            'COMPLETED',
            'COMPLETED',
            'IMPORT',
            'IMPORT',
            'Fathom',
            'Fathom',
            1000.0
        FROM data d
        WHERE NOT EXISTS (
            SELECT 1 FROM "{workspace_schema}"."callRecording" c
            WHERE c."fathomRecordingId" = d.recording_id
        )
        RETURNING 1
    )
    SELECT count(*) as inserted_count FROM inserted;
    """

    proc = subprocess.run(
        [
            "docker", "exec", "-i", docker_container,
            "psql", "-U", "postgres", "-d", "default",
            "-t", "-A",
        ],
        input=sql,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        print(f"Docker SQL notice for meetings: {proc.stderr}")
        return 0

    count = 0
    for line in proc.stdout.strip().splitlines():
        try:
            count = int(line.strip())
        except ValueError:
            pass

    print(f"Synced {count} new meetings into ActionOS 'callRecording' via Docker!")
    return count


def sync_fathom_action_items_local(
    profile: str,
    warehouse_id: str | None,
    prefix: str,
    workspace_schema: str = DEFAULT_WORKSPACE_SCHEMA,
    dry_run: bool = False,
    docker_container: str = "twenty-db-1",
) -> int:
    """Fetch fathom_action_items from Databricks and insert into ActionOS task table."""
    table = f"workspace.dev.{prefix}fathom_action_items"
    query = f"""
        SELECT recording_id, task_index, task_text
        FROM {table}
        WHERE recording_id IS NOT NULL;
    """
    print(f"\nFetching Fathom action items from Databricks ({table})...")
    rows = run_databricks_query(query, profile=profile, warehouse_id=warehouse_id)
    print(f"Retrieved {len(rows)} action items.")
    if not rows:
        return 0

    if dry_run:
        print(f"[DRY RUN] Would sync {len(rows)} action items to '{workspace_schema}'.'task'")
        return len(rows)

    clean_items = []
    for r in rows:
        rec_id = str(r["recording_id"])
        t_idx = int(r["task_index"]) if r.get("task_index") is not None else 0
        raw_text = r.get("task_text") or ""

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

        clean_items.append({
            "recording_id": rec_id,
            "task_index": t_idx,
            "title": title[:255],
            "body": body,
            "assignee_name": assignee_name or "",
            "assignee_email": assignee_email or "",
            "dedupe_key": f"fathom_{rec_id}_{t_idx}",
            "completed": completed,
        })

    payload_json = json.dumps(clean_items)
    sql = f"""
    WITH raw_data AS (
        SELECT $payload${payload_json}$payload$::jsonb AS js
    ),
    data AS (
        SELECT * FROM raw_data, jsonb_to_recordset(js) AS x(
            recording_id text,
            task_index int,
            title text,
            body text,
            assignee_name text,
            assignee_email text,
            dedupe_key text,
            completed boolean
        )
    ),
    inserted AS (
        INSERT INTO "{workspace_schema}"."task" (
            "id", "createdAt", "updatedAt", "title", "bodyV2Markdown",
            "assigneeName", "assigneeEmail", "callRecordingId", "dedupeKey",
            "status", "createdBySource", "updatedBySource", "createdByName",
            "updatedByName", "position"
        )
        SELECT
            md5('fathom_task_' || d.recording_id || '_' || d.task_index)::uuid,
            NOW(),
            NOW(),
            d.title,
            d.body,
            NULLIF(d.assignee_name, ''),
            NULLIF(d.assignee_email, ''),
            md5('fathom_meeting_' || d.recording_id)::uuid,
            d.dedupe_key,
            CASE WHEN d.completed THEN 'DONE'::"{workspace_schema}"."task_status_enum"
                 ELSE 'TODO'::"{workspace_schema}"."task_status_enum" END,
            'IMPORT',
            'IMPORT',
            'Fathom AI',
            'Fathom AI',
            d.task_index * 1000.0 + 1000.0
        FROM data d
        WHERE NOT EXISTS (
            SELECT 1 FROM "{workspace_schema}"."task" t
            WHERE t."dedupeKey" = d.dedupe_key
        )
        RETURNING 1
    )
    SELECT count(*) as inserted_count FROM inserted;
    """

    proc = subprocess.run(
        [
            "docker", "exec", "-i", docker_container,
            "psql", "-U", "postgres", "-d", "default",
            "-t", "-A",
        ],
        input=sql,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        print(f"Docker SQL notice for tasks: {proc.stderr}")
        return 0

    count = 0
    for line in proc.stdout.strip().splitlines():
        try:
            count = int(line.strip())
        except ValueError:
            pass

    print(f"Synced {count} new action items into ActionOS 'task' via Docker!")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Gold metrics to ActionOS PostgreSQL")
    parser.add_argument("--profile", default="personal")
    parser.add_argument("--warehouse-id", default="9256e96517ef9151")
    parser.add_argument("--prefix", default="nikhil_")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="postgres")
    parser.add_argument("--database", default="default")
    parser.add_argument(
        "--workspace-schema",
        default=DEFAULT_WORKSPACE_SCHEMA,
        help="Target ActionOS workspace schema in PostgreSQL",
    )
    parser.add_argument(
        "--enable-direct-write-back",
        action="store_true",
        default=False,
        help="Explicitly enable direct PostgreSQL write-back (disabled by default to prevent competing writers with ActionOS Twenty Logic Function worker)",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    direct_write_enabled = (
        args.enable_direct_write_back
        or os.environ.get("ENABLE_DIRECT_POSTGRES_WRITE_BACK", "").lower() in ("true", "1", "yes")
        or args.dry_run
    )
    if not direct_write_enabled:
        print(
            "Notice: Direct local PostgreSQL write-back is disabled by default to prevent competing writers "
            "with ActionOS Twenty Logic Function worker (databricks-gold-poll). "
            "ActionOS Logic Function safely syncs Gold metrics into ActionOS via GraphQL. "
            "To force direct write-back, pass --enable-direct-write-back or set ENABLE_DIRECT_POSTGRES_WRITE_BACK=true."
        )
        return

    if not args.dry_run and (not args.host or not args.password or not args.user or not args.database):
        raise ValueError(
            "Missing PostgreSQL credentials: host, user, password, or database not configured. "
            "Cannot perform write-back without credentials."
        )

    dim_table = f"workspace.dev.{args.prefix}dim_person"
    act_table = f"workspace.dev.{args.prefix}gold_person_activity_weekly"
    cu_table = f"workspace.dev.{args.prefix}clickup_team_productivity"
    fm_table = f"workspace.dev.{args.prefix}fathom_team_productivity"

    query = f"""
        SELECT 
            d.primary_email,
            d.full_name,
            d.github_username,
            d.slack_user_id,
            COALESCE(gh_m.prs_opened, a.prs_opened, 0) as prs_opened,
            COALESCE(gh_m.prs_merged, a.prs_merged, 0) as prs_merged,
            COALESCE(gh_m.commits_authored, a.commits_authored, 0) as commits_authored,
            COALESCE(cu.tasks_completed, a.tasks_done, 0) as tasks_done,
            COALESCE(cu.tasks_in_progress, a.tasks_in_progress, 0) as tasks_in_progress,
            COALESCE(cu.total_time_spent_minutes, a.time_spent_minutes, 0) as time_spent_minutes,
            COALESCE(fm.total_meetings_spoken, a.meetings_hosted, 0) as meetings_hosted,
            COALESCE(a.productivity_score, 0) as productivity_score,
            DENSE_RANK() OVER (ORDER BY COALESCE(a.productivity_score, 0) DESC) as team_rank
        FROM {dim_table} d
        LEFT JOIN (
            SELECT * FROM {act_table}
            WHERE activity_week = (SELECT MAX(activity_week) FROM {act_table})
        ) a ON lower(trim(d.primary_email)) = lower(trim(a.primary_email))
        LEFT JOIN (
            SELECT 
                lower(trim(primary_email)) as primary_email,
                SUM(prs_opened) as prs_opened,
                SUM(prs_merged) as prs_merged,
                SUM(commits_authored) as commits_authored,
                SUM(tasks_done) as tasks_done,
                SUM(meetings_hosted) as meetings_hosted
            FROM {act_table}
            WHERE activity_week >= date_trunc('month', (SELECT MAX(activity_week) FROM {act_table}))
            GROUP BY lower(trim(primary_email))
        ) gh_m ON lower(trim(d.primary_email)) = lower(trim(gh_m.primary_email))
        LEFT JOIN (
            SELECT 
                lower(trim(contributor_email)) as contributor_email,
                SUM(tasks_completed) as tasks_completed,
                SUM(tasks_in_progress) as tasks_in_progress,
                SUM(total_time_spent_minutes) as total_time_spent_minutes
            FROM {cu_table}
            GROUP BY lower(trim(contributor_email))
        ) cu ON lower(trim(d.primary_email)) = lower(trim(cu.contributor_email))
        LEFT JOIN (
            SELECT 
                lower(trim(speaker_email)) as speaker_email,
                SUM(total_meetings_spoken) as total_meetings_spoken
            FROM {fm_table}
            GROUP BY lower(trim(speaker_email))
        ) fm ON lower(trim(d.primary_email)) = lower(trim(fm.speaker_email))
        ORDER BY team_rank ASC;
    """


    print(f"Fetching Gold metrics across all layers from Databricks ({dim_table})...")
    metrics = run_databricks_query(query, profile=args.profile, warehouse_id=args.warehouse_id)
    print(f"Retrieved {len(metrics)} person metrics across all Gold layers.")

    if metrics:
        update_local_postgres(
            metrics=metrics,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            workspace_schema=args.workspace_schema,
            dry_run=args.dry_run,
        )

    # Sync meetings and action items to ActionOS
    sync_fathom_meetings_local(
        profile=args.profile,
        warehouse_id=args.warehouse_id,
        prefix=args.prefix,
        workspace_schema=args.workspace_schema,
        dry_run=args.dry_run,
    )
    sync_fathom_action_items_local(
        profile=args.profile,
        warehouse_id=args.warehouse_id,
        prefix=args.prefix,
        workspace_schema=args.workspace_schema,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
