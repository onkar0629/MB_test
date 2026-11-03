from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.ingestion.fathom.client import FathomAPIError, FathomClient

AFTER = datetime(2026, 9, 1, tzinfo=timezone.utc)
BEFORE = datetime(2026, 9, 18, tzinfo=timezone.utc)


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


class FakeSession:
    """Returns queued responses and records every request made."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return self._responses.pop(0)


def make_client(responses, sleeps=None):
    session = FakeSession(responses)
    recorded = sleeps if sleeps is not None else []
    client = FathomClient(
        api_key="secret-key",
        session=session,
        sleep=recorded.append,
        jitter=lambda: 1.0,
    )
    return client, session, recorded


def test_follows_the_cursor_until_it_runs_out():
    client, session, _ = make_client([
        FakeResponse(200, {"items": [{"recording_id": "a"}], "next_cursor": "c1"}),
        FakeResponse(200, {"items": [{"recording_id": "b"}], "next_cursor": None}),
    ])
    pages = list(client.iter_meeting_pages(AFTER, BEFORE, max_pages=10))

    assert [page_no for page_no, _ in pages] == [1, 2]
    assert pages[1][1]["items"][0]["recording_id"] == "b"
    assert session.calls[0]["params"].get("cursor") is None
    assert session.calls[1]["params"]["cursor"] == "c1"


def test_sends_the_api_key_and_every_include_flag():
    client, session, _ = make_client([
        FakeResponse(200, {"items": [], "next_cursor": None}),
    ])
    list(client.iter_meeting_pages(AFTER, BEFORE, max_pages=1))

    params = session.calls[0]["params"]
    assert session.calls[0]["headers"]["X-Api-Key"] == "secret-key"
    assert params["include_transcript"] == "true"
    assert params["include_summary"] == "true"
    assert params["include_action_items"] == "true"
    assert params["include_crm_matches"] == "true"
    assert params["created_after"] == "2026-09-01T00:00:00+00:00"


def test_stops_at_max_pages_even_when_a_cursor_remains():
    client, _, _ = make_client([
        FakeResponse(200, {"items": [], "next_cursor": "c1"}),
        FakeResponse(200, {"items": [], "next_cursor": "c2"}),
    ])
    pages = list(client.iter_meeting_pages(AFTER, BEFORE, max_pages=1))
    assert len(pages) == 1


def test_honours_retry_after_on_429():
    client, _, sleeps = make_client([
        FakeResponse(429, headers={"Retry-After": "12"}),
        FakeResponse(200, {"items": [], "next_cursor": None}),
    ])
    list(client.iter_meeting_pages(AFTER, BEFORE, max_pages=1))
    assert sleeps == [12.0]


def test_retries_server_errors_with_backoff():
    client, _, sleeps = make_client([
        FakeResponse(503),
        FakeResponse(200, {"items": [], "next_cursor": None}),
    ])
    list(client.iter_meeting_pages(AFTER, BEFORE, max_pages=1))
    assert sleeps == [1.0]  # attempt 1, full jitter


def test_paces_itself_when_the_budget_is_nearly_spent():
    client, _, sleeps = make_client([
        FakeResponse(
            200,
            {"items": [], "next_cursor": None},
            headers={"RateLimit-Remaining": "1", "RateLimit-Reset": "20"},
        ),
    ])
    list(client.iter_meeting_pages(AFTER, BEFORE, max_pages=1))
    assert sleeps == [20.0]


def test_client_errors_raise_without_retrying():
    client, session, _ = make_client([FakeResponse(401, text="bad key")])
    with pytest.raises(FathomAPIError) as excinfo:
        list(client.iter_meeting_pages(AFTER, BEFORE, max_pages=1))
    assert excinfo.value.status_code == 401
    assert len(session.calls) == 1


def test_api_key_never_appears_in_the_error():
    client, _, _ = make_client([FakeResponse(401, text="bad key")])
    with pytest.raises(FathomAPIError) as excinfo:
        list(client.iter_meeting_pages(AFTER, BEFORE, max_pages=1))
    assert "secret-key" not in str(excinfo.value)
