from __future__ import annotations

import pytest

from src.ingestion.github.client import GitHubAPIError, GitHubClient


class FakeResponse:
    def __init__(
        self, status_code: int, json_data=None, headers: dict | None = None
    ):
        self.status_code = status_code
        self._json_data = json_data if json_data is not None else []
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


def test_client_iter_pull_requests():
    r1 = FakeResponse(200, [{"id": 1, "number": 101, "updated_at": "2026-10-23T10:00:00Z"}] * 100)
    r2 = FakeResponse(200, [{"id": 2, "number": 102, "updated_at": "2026-10-22T10:00:00Z"}])
    session = FakeSession([r1, r2])
    client = GitHubClient(api_token="ghp_test_token_123", session=session, sleep=lambda s: None)

    pages = list(client.iter_pull_requests(owner="owner", repo="repo", max_pages=5))
    assert len(pages) == 2
    assert pages[0][0] == 1
    assert pages[1][0] == 2
    assert "Bearer ghp_test_token_123" in session.calls[0][1]["Authorization"]
    assert session.calls[0][1]["User-Agent"] == "ActionOS-DataEngineering/1.0"


def test_client_iter_commits():
    r1 = FakeResponse(200, [{"sha": "abc1234"}])
    session = FakeSession([r1])
    client = GitHubClient(api_token="ghp_test_token_123", session=session, sleep=lambda s: None)

    pages = list(client.iter_commits(owner="owner", repo="repo", max_pages=2))
    assert len(pages) == 1
    assert pages[0][1][0]["sha"] == "abc1234"


def test_client_max_pages_raises():
    r1 = FakeResponse(200, [{"id": 1}] * 100)
    session = FakeSession([r1])
    client = GitHubClient(api_token="ghp_test_token_123", session=session, sleep=lambda s: None)

    with pytest.raises(RuntimeError) as exc_info:
        list(client.iter_pull_requests(owner="owner", repo="repo", max_pages=1))

    assert "reached max_pages (1) with more items remaining" in str(exc_info.value)


def test_client_masks_token_in_error():
    r401 = FakeResponse(401, {"message": "Bad credentials ghp_test_token_123"})
    session = FakeSession([r401])
    client = GitHubClient(api_token="ghp_test_token_123", session=session, sleep=lambda s: None)

    with pytest.raises(GitHubAPIError) as exc_info:
        list(client.iter_commits(owner="owner", repo="repo"))

    assert "ghp_test_token_123" not in str(exc_info.value)
    assert "[REDACTED]" in str(exc_info.value)
