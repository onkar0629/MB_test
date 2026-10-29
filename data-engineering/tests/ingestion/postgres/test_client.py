"""Unit tests for PostgresClient."""
from __future__ import annotations

from unittest.mock import patch

from src.ingestion.postgres.client import PostgresClient


def test_configured_workspace_schema() -> None:
    client = PostgresClient(workspace_schema="workspace_test123")
    assert client.get_workspace_schema() == "workspace_test123"


def test_discover_workspace_schema() -> None:
    client = PostgresClient()
    with patch.object(client, "_execute_query", return_value=[("workspace_auto_detected",)]):
        assert client.get_workspace_schema() == "workspace_auto_detected"


def test_iter_entity_chunks() -> None:
    client = PostgresClient()
    mock_records = [{"id": f"rec_{i}", "name": f"item_{i}"} for i in range(12)]
    with patch.object(client, "fetch_entity_json", return_value=mock_records):
        pages = list(client.iter_entity("people", page_size=5))

    assert len(pages) == 3
    assert pages[0][0] == 1
    assert len(pages[0][1]["records"]) == 5
    assert pages[1][0] == 2
    assert len(pages[1][1]["records"]) == 5
    assert pages[2][0] == 3
    assert len(pages[2][1]["records"]) == 2


def test_iter_entity_empty() -> None:
    client = PostgresClient()
    with patch.object(client, "fetch_entity_json", return_value=[]):
        pages = list(client.iter_entity("tasks", page_size=10))

    assert len(pages) == 1
    assert pages[0][0] == 1
    assert pages[0][1]["records"] == []
