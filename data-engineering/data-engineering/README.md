# data-engineering

Ingestion and modelling for the Marketing Boutique Databricks workspace.

| | |
|---|---|
| Workspace | https://dbc-44ca0102-48c3.cloud.databricks.com |
| Catalog | `workspace` |
| Schemas | `workspace.dev` (sandbox), `raw_clickup`, `raw_slack`, `raw_fathom` |
| Warehouse | Serverless Starter Warehouse (`bc5e6ab173f2002c`) |
| Secrets | scope `mb-ingest` |

## Role in Action OS

Serves as the Databricks Lakehouse data engineering pipeline (Landing -> Bronze -> Silver -> Gold) for analytics, operating separately from Twenty CRM's operational database.

- **Deployment Model:** Deployed via Databricks Asset Bundles (DABs), not via `docker-compose`.
- **Fathom Ingestion:** Ingests raw meeting data into Unity Catalog volumes and bronze Delta tables. (Note: syncing account lists dynamically from Twenty CRM is tracked as a follow-up task).

## Getting access

You need to be in the `developers` group. That grants workspace access, Databricks SQL
access, `USE CATALOG` on `workspace`, and full table rights on the schemas above.
You cannot create schemas - ask an admin.

```bash
databricks auth login \
  --host https://dbc-44ca0102-48c3.cloud.databricks.com \
  --profile marketing-boutique
```

## Architecture note: no bridge required

This workspace is `PREMIUM` tier with `compute_mode: SERVERLESS`, and the account's serverless
egress policy is `restriction_mode: FULL_ACCESS`. Serverless compute here can call external
APIs directly, so ingestion runs **inside** Databricks - there is no n8n or other bridge.

This differs from `Marketing-Boutique/slack-to-databricks`, which targets a Free Edition
workspace where outbound access is restricted to trusted domains and a bridge is mandatory.
Read that repo for its Slack paging and watermark logic, not for its topology.

Being serverless-only also means there are no all-purpose clusters. Everything runs as
serverless jobs or SQL.

## Layout

```
databricks.yml          Asset Bundle: targets dev and prod (includes *.local.yml)
resources/              Job definitions (fathom_job.yml)
src/ingestion/          One module per source platform
src/transform/          Bronze/Silver/Gold transformations
tests/                  Unit and transformation tests
```

## Working here

Commands must be executed from `services/data-engineering`:

```bash
cd services/data-engineering
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Raw tables keep the source payload intact plus an `_ingested_at` timestamp. Reshaping belongs
in a modelled layer, not in ingestion.

API tokens live in the `mb-ingest` secret scope, never in source:

```python
dbutils.secrets.get(scope="mb-ingest", key="clickup-token")
```
