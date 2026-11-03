from __future__ import annotations

import pytest

from src.ingestion.clickup.client import ClickUpAPIError, ClickUpClient


class FakeResponse:
    def __init__(
        self, status_code: int, json_data: dict | None = None, headers: dict | None = None
    ):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.headers = headers or {}
        self.text = str(json_data or "")

    def json(self):
        return self._json_data


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self._responses = list(responses)
        self.calls: list[tuple[str, dict, dict]] = []

    def get(self, url, headers=None, params=None, timeout=None):
        self.calls.append((url, headers or {}, params or {}))
        if not self._responses:
            raise RuntimeError("No more fake responses")
        return self._responses.pop(0)


def test_client_list_teams():
    session = FakeSession([FakeResponse(200, {"teams": [{"id": "123", "name": "Team A"}]})])
    client = ClickUpClient(api_token="pk_secret123", session=session, sleep=lambda s: None)

    teams = client.list_teams()
    assert len(teams) == 1
    assert teams[0]["name"] == "Team A"
    assert session.calls[0][1]["Authorization"] == "pk_secret123"


def test_client_iter_tasks_pagination():
    page1 = FakeResponse(200, {"tasks": [{"id": "t1"}], "last_page": False})
    page2 = FakeResponse(200, {"tasks": [{"id": "t2"}], "last_page": True})
    session = FakeSession([page1, page2])
    client = ClickUpClient(api_token="pk_secret123", session=session, sleep=lambda s: None)

    pages = list(client.iter_team_tasks(team_id="123", max_pages=5))
    assert len(pages) == 2
    assert pages[0][0] == 1
    assert pages[1][0] == 2
    assert session.calls[0][2]["page"] == "0"
    assert session.calls[1][2]["page"] == "1"


def test_client_max_pages_exhaustion_raises():
    page1 = FakeResponse(200, {"tasks": [{"id": "t1"}], "last_page": False})
    page2 = FakeResponse(200, {"tasks": [{"id": "t2"}], "last_page": False})
    session = FakeSession([page1, page2])
    client = ClickUpClient(api_token="pk_secret123", session=session, sleep=lambda s: None)

    with pytest.raises(RuntimeError) as exc_info:
        list(client.iter_team_tasks(team_id="123", max_pages=2))

    assert "reached max_pages (2) with more tasks remaining" in str(exc_info.value)


def test_client_retries_on_500():
    r500 = FakeResponse(500, {"error": "Internal"})
    r200 = FakeResponse(200, {"tasks": [], "last_page": True})
    session = FakeSession([r500, r200])
    client = ClickUpClient(api_token="pk_secret123", session=session, sleep=lambda s: None)

    pages = list(client.iter_team_tasks(team_id="123", max_pages=1))
    assert len(pages) == 1
    assert len(session.calls) == 2


def test_client_masks_token_in_error():
    r401 = FakeResponse(401, {"error": "Invalid token pk_secret123"})
    session = FakeSession([r401])
    client = ClickUpClient(api_token="pk_secret123", session=session, sleep=lambda s: None)

    with pytest.raises(ClickUpAPIError) as exc_info:
        client.list_teams()

    assert "pk_secret123" not in str(exc_info.value)
    assert "[REDACTED]" in str(exc_info.value)
