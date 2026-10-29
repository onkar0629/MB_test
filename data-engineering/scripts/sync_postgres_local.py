"""Extract local Twenty CRM PostgreSQL records and upload to Databricks Landing Volume.

Usage:
    python scripts/sync_postgres_local.py
    python scripts/sync_postgres_local.py --user your_name
    python scripts/sync_postgres_local.py --target-volume dbfs:/Volumes/workspace/...
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.ingestion.postgres.client import PostgresClient  # noqa: E402
from src.ingestion.postgres.land import default_fs_writer, land_postgres_entity  # noqa: E402

ENTITIES = ["people", "workspace_members", "tasks"]


def _discover_databricks_user(profile: str) -> str | None:
    """Attempt to discover the current Databricks username via CLI."""
    try:
        res = subprocess.run(
            ["databricks", "current-user", "me", "-p", profile],
            capture_output=True,
            text=True,
        )
        if res.returncode == 0 and res.stdout.strip():
            user_info = json.loads(res.stdout)
            full_user = user_info.get("userName") or user_info.get("user_name")
            if full_user:
                return full_user.split("@")[0].split(".")[0]
    except Exception:
        pass

    try:
        res = subprocess.run(
            ["databricks", "auth", "describe", "-p", profile],
            capture_output=True,
            text=True,
        )
        for line in res.stdout.splitlines():
            if "Username:" in line:
                return line.split()[-1].split("@")[0].split(".")[0]
    except Exception:
        pass

    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync local Twenty CRM to Databricks Landing Volume"
    )
    parser.add_argument(
        "--profile", default="personal", help="Databricks CLI profile (default: personal)"
    )
    parser.add_argument(
        "--user",
        default=None,
        help="Databricks developer short username for volume partition (e.g. jsmith)",
    )
    parser.add_argument(
        "--target-volume",
        default=None,
        help="Explicit Databricks target volume path (e.g. dbfs:/Volumes/workspace/...)",
    )
    args = parser.parse_args()

    target_volume = args.target_volume
    if not target_volume:
        user_name = args.user or _discover_databricks_user(args.profile)
        if not user_name:
            print("Error: Could not determine Databricks username automatically.")
            print("Please specify --user <username> or --target-volume <volume_path>.")
            sys.exit(1)
        target_volume = f"dbfs:/Volumes/workspace/raw_postgres/landing/dev/{user_name}"

    local_temp = BASE_DIR / ".temp_postgres_landing"
    if local_temp.exists():
        shutil.rmtree(local_temp)
    local_temp.mkdir(parents=True, exist_ok=True)

    run_id = f"local-sync-{int(datetime.now(timezone.utc).timestamp())}"
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print("Connecting to local Twenty CRM (host: localhost)...")
    try:
        client = PostgresClient()
        schema = client.get_workspace_schema()
        print(f"Connected to Twenty CRM workspace schema: {schema}")
    except Exception as exc:
        print(f"Failed to connect to local Twenty CRM: {exc}")
        print("Please ensure your Twenty CRM Docker containers are running.")
        sys.exit(1)

    print(f"Extracting entities: {', '.join(ENTITIES)}...")
    for entity in ENTITIES:
        pages = land_postgres_entity(
            client=client,
            writer=default_fs_writer,
            volume_root=str(local_temp),
            entity=entity,
            ingest_date=ingest_date,
            run_id=run_id,
        )
        print(f"  - Extracted {pages} page(s) for '{entity}'")

    print(f"\nUploading to Databricks Volume: {target_volume} (profile: {args.profile})...")
    cmd = [
        "databricks", "fs", "cp", "-r", str(local_temp), target_volume,
        "-p", args.profile, "--overwrite",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(f"Databricks upload failed:\n{res.stderr}")
        sys.exit(res.returncode)

    print("Upload successful!")
    print("Local sync complete. You can now run 'postgres_ingest' in Databricks.")

    if local_temp.exists():
        shutil.rmtree(local_temp)


if __name__ == "__main__":
    main()
