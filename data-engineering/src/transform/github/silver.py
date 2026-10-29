"""Transform raw GitHub Bronze records into structured, deduplicated Silver tables."""
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

USER_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("login", StringType()),
])

REF_SCHEMA = StructType([
    StructField("sha", StringType()),
    StructField("ref", StringType()),
])

COMMIT_SUB_SCHEMA = StructType([
    StructField("message", StringType()),
    StructField(
        "author",
        StructType([
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("date", StringType()),
        ]),
    ),
    StructField(
        "committer",
        StructType([
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("date", StringType()),
        ]),
    ),
])

PR_ITEM_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("number", LongType()),
    StructField("title", StringType()),
    StructField("state", StringType()),
    StructField("draft", BooleanType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("closed_at", StringType()),
    StructField("merged_at", StringType()),
    StructField("merge_commit_sha", StringType()),
    StructField("user", USER_SCHEMA),
    StructField("assignees", ArrayType(USER_SCHEMA)),
    StructField("head", REF_SCHEMA),
    StructField("base", REF_SCHEMA),
    StructField("html_url", StringType()),
])

COMMIT_ITEM_SCHEMA = StructType([
    StructField("sha", StringType()),
    StructField("commit", COMMIT_SUB_SCHEMA),
    StructField("author", USER_SCHEMA),
    StructField("committer", USER_SCHEMA),
    StructField("html_url", StringType()),
])

ISSUE_ITEM_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("number", LongType()),
    StructField("title", StringType()),
    StructField("state", StringType()),
    StructField("created_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("closed_at", StringType()),
    StructField("user", USER_SCHEMA),
    StructField("assignees", ArrayType(USER_SCHEMA)),
    StructField("pull_request", StructType([StructField("url", StringType())])),
    StructField("html_url", StringType()),
])

WRAPPER_PR_SCHEMA = StructType([
    StructField("owner", StringType()),
    StructField("repo", StringType()),
    StructField("items", ArrayType(PR_ITEM_SCHEMA)),
])

WRAPPER_COMMIT_SCHEMA = StructType([
    StructField("owner", StringType()),
    StructField("repo", StringType()),
    StructField("items", ArrayType(COMMIT_ITEM_SCHEMA)),
])

WRAPPER_ISSUE_SCHEMA = StructType([
    StructField("owner", StringType()),
    StructField("repo", StringType()),
    StructField("items", ArrayType(ISSUE_ITEM_SCHEMA)),
])


def transform_pull_requests(bronze_df: DataFrame) -> DataFrame:
    """Parse raw GitHub pull request pages and deduplicate by (owner, repo, number)."""
    parsed = (
        bronze_df.filter(F.col("_source_entity") == "pull_requests")
        .withColumn("data", F.from_json("_payload", WRAPPER_PR_SCHEMA))
        .filter(F.col("data").isNotNull())
        .select(
            F.col("data.owner").alias("owner"),
            F.col("data.repo").alias("repo"),
            F.explode("data.items").alias("pr"),
            F.col("_ingested_at"),
        )
    )

    # Dedup by (owner, repo, pr.number) ordered by updated_at descending
    window = Window.partitionBy("owner", "repo", "pr.number").orderBy(
        F.to_timestamp("pr.updated_at").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )

    return (
        parsed.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("owner"),
            F.col("repo"),
            F.col("pr.number").alias("pr_number"),
            F.col("pr.title").alias("title"),
            F.col("pr.state").alias("state"),
            F.coalesce(F.col("pr.draft"), F.lit(False)).alias("is_draft"),
            F.to_timestamp("pr.created_at").alias("created_at"),
            F.to_timestamp("pr.updated_at").alias("updated_at"),
            F.to_timestamp("pr.closed_at").alias("closed_at"),
            F.to_timestamp("pr.merged_at").alias("merged_at"),
            F.col("pr.merge_commit_sha").alias("merge_commit_sha"),
            F.col("pr.user.id").alias("author_id"),
            F.col("pr.user.login").alias("author_login"),
            F.col("pr.assignees.id").alias("assignee_ids"),
            F.col("pr.assignees.login").alias("assignee_logins"),
            F.col("pr.base.ref").alias("base_branch"),
            F.col("pr.head.sha").alias("head_sha"),
            F.col("pr.html_url").alias("html_url"),
            F.col("_ingested_at"),
        )
    )


def transform_commits(bronze_df: DataFrame) -> DataFrame:
    """Parse raw GitHub commit pages and deduplicate by (owner, repo, sha)."""
    parsed = (
        bronze_df.filter(F.col("_source_entity") == "commits")
        .withColumn("data", F.from_json("_payload", WRAPPER_COMMIT_SCHEMA))
        .filter(F.col("data").isNotNull())
        .select(
            F.col("data.owner").alias("owner"),
            F.col("data.repo").alias("repo"),
            F.explode("data.items").alias("c"),
            F.col("_ingested_at"),
        )
    )

    # Dedup by (owner, repo, c.sha)
    window = Window.partitionBy("owner", "repo", "c.sha").orderBy(
        F.to_timestamp("c.commit.author.date").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )

    return (
        parsed.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("owner"),
            F.col("repo"),
            F.col("c.sha").alias("commit_sha"),
            F.col("c.commit.message").alias("message"),
            F.col("c.commit.author.name").alias("author_name"),
            F.col("c.commit.author.email").alias("author_email"),
            F.to_timestamp("c.commit.author.date").alias("author_date"),
            F.col("c.commit.committer.name").alias("committer_name"),
            F.col("c.commit.committer.email").alias("committer_email"),
            F.to_timestamp("c.commit.committer.date").alias("commit_date"),
            F.col("c.author.id").alias("author_id"),
            F.col("c.author.login").alias("author_login"),
            F.col("c.committer.login").alias("committer_login"),
            F.col("c.html_url").alias("html_url"),
            F.col("_ingested_at"),
        )
    )


def transform_issues(bronze_df: DataFrame) -> DataFrame:
    """Parse raw GitHub issue pages and deduplicate by (owner, repo, number).

    Filters out pull requests since GitHub issues endpoint returns both.
    """
    parsed = (
        bronze_df.filter(F.col("_source_entity") == "issues")
        .withColumn("data", F.from_json("_payload", WRAPPER_ISSUE_SCHEMA))
        .filter(F.col("data").isNotNull())
        .select(
            F.col("data.owner").alias("owner"),
            F.col("data.repo").alias("repo"),
            F.explode("data.items").alias("issue"),
            F.col("_ingested_at"),
        )
        .filter(F.col("issue.pull_request").isNull())  # Exclude pull requests
    )

    # Dedup by (owner, repo, issue.number)
    window = Window.partitionBy("owner", "repo", "issue.number").orderBy(
        F.to_timestamp("issue.updated_at").desc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
    )

    return (
        parsed.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("owner"),
            F.col("repo"),
            F.col("issue.number").alias("issue_number"),
            F.col("issue.title").alias("title"),
            F.col("issue.state").alias("state"),
            F.to_timestamp("issue.created_at").alias("created_at"),
            F.to_timestamp("issue.updated_at").alias("updated_at"),
            F.to_timestamp("issue.closed_at").alias("closed_at"),
            F.col("issue.user.id").alias("author_id"),
            F.col("issue.user.login").alias("author_login"),
            F.col("issue.assignees.id").alias("assignee_ids"),
            F.col("issue.assignees.login").alias("assignee_logins"),
            F.col("issue.html_url").alias("html_url"),
            F.col("_ingested_at"),
        )
    )
