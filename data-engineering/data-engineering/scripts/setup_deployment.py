#!/usr/bin/env python3
"""
Automated Setup & Deployment Script for Databricks Lakehouse Pipelines
Usage:
    python scripts/setup_deployment.py [--target dev|prod|personal]
"""
import argparse
import shutil
import subprocess
import sys

REQUIRED_SECRETS = [
    ("github-api-token", "GitHub PAT (with repo, read:org scopes)"),
    ("clickup-api-token", "ClickUp API Personal / Workspace Token"),
    ("slack-bot-token", "Slack Bot User OAuth Token (xoxb-...)"),
    ("fathom-api-key-fenil", "Fathom API Key for Fenil"),
    ("fathom-api-key-usman", "Fathom API Key for Usman"),
]

SCOPE_NAME = "mb-ingest"


def run_cmd(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a system command and return output."""
    try:
        return subprocess.run(cmd, check=check, text=True, capture_output=True)
    except FileNotFoundError:
        print(f"❌ Error: Command '{cmd[0]}' not found.")
        sys.exit(1)


def check_prerequisites():
    """Verify that databricks CLI is installed and configured."""
    print("🔍 Checking prerequisites...")
    if not shutil.which("databricks"):
        print("❌ Databricks CLI is not installed or not in PATH.")
        print("   Install it from: https://docs.databricks.com/en/dev-tools/cli/install.html")
        sys.exit(1)

    res = run_cmd(["databricks", "--version"], check=False)
    print(f"✅ Databricks CLI found: {res.stdout.strip()}")

    # Check authentication
    auth_check = run_cmd(["databricks", "auth", "describe"], check=False)
    if auth_check.returncode != 0:
        print("⚠️ Not logged in to Databricks CLI.")
        print("👉 Running: databricks auth login --host ...")
        host_url = "https://dbc-44ca0102-48c3.cloud.databricks.com"
        subprocess.run(["databricks", "auth", "login", "--host", host_url])
    else:
        print("✅ Databricks CLI is authenticated.")


def setup_secret_scope():
    """Ensure the mb-ingest secret scope exists."""
    print(f"\n🔐 Checking secret scope '{SCOPE_NAME}'...")
    list_scopes = run_cmd(["databricks", "secrets", "list-scopes"], check=False)
    if SCOPE_NAME not in list_scopes.stdout:
        print(f"👉 Creating secret scope '{SCOPE_NAME}'...")
        res = run_cmd(["databricks", "secrets", "create-scope", SCOPE_NAME], check=False)
        if res.returncode == 0:
            print(f"✅ Secret scope '{SCOPE_NAME}' created.")
        else:
            print(f"⚠️ Note on secret scope: {res.stderr.strip()}")
    else:
        print(f"✅ Secret scope '{SCOPE_NAME}' already exists.")

    # Check existing secrets in scope
    secrets_cmd = ["databricks", "secrets", "list-secrets", SCOPE_NAME]
    existing_secrets_res = run_cmd(secrets_cmd, check=False)
    existing_output = existing_secrets_res.stdout

    for secret_key, description in REQUIRED_SECRETS:
        if secret_key in existing_output:
            print(f"  ✓ Secret '{secret_key}' is present.")
        else:
            print(f"  ⚠️ Secret '{secret_key}' is MISSING ({description}).")
            val = input(f"    Enter value for {secret_key} (or press Enter to skip): ").strip()
            if val:
                put_cmd = [
                    "databricks",
                    "secrets",
                    "put-secret",
                    SCOPE_NAME,
                    secret_key,
                    "--string-value",
                    val,
                ]
                subprocess.run(put_cmd, check=True)
                print(f"    ✅ Secret '{secret_key}' stored successfully.")


def deploy_bundle(target: str):
    """Validate and deploy the Databricks Asset Bundle."""
    print(f"\n📦 Validating Databricks Asset Bundle for target '{target}'...")
    val_res = subprocess.run(["databricks", "bundle", "validate", "-t", target])
    if val_res.returncode != 0:
        print(f"❌ Bundle validation failed for target '{target}'. Please check databricks.yml.")
        sys.exit(1)
    print("✅ Bundle validation passed.")

    print(f"\n🚀 Deploying Databricks Asset Bundle to target '{target}'...")
    dep_res = subprocess.run(["databricks", "bundle", "deploy", "-t", target])
    if dep_res.returncode != 0:
        print("❌ Bundle deployment failed.")
        sys.exit(1)
    print(f"🎉 Deployment completed successfully to target '{target}'!")


def main():
    parser = argparse.ArgumentParser(description="Setup and deploy Databricks pipelines")
    parser.add_argument(
        "--target",
        "-t",
        default="dev",
        choices=["dev", "prod", "personal"],
        help="Deployment target",
    )
    args = parser.parse_args()

    check_prerequisites()
    setup_secret_scope()
    deploy_bundle(args.target)


if __name__ == "__main__":
    main()
