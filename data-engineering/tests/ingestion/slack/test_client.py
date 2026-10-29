"""Unit tests for SlackClient."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.ingestion.slack.client import SlackAPIError, SlackClient


def test_auth_header_and_users_pagination() -> None:
    session = MagicMock()
    page1_resp = MagicMock(status_code=200)
    page1_resp.json.return_value = {
        "ok": True,
        "members": [{"id": "U1", "name": "alice"}],
        "response_metadata": {"next_cursor": "cur_123"},
    }
    page2_resp = MagicMock(status_code=200)
    page2_resp.json.return_value = {
        "ok": True,
        "members": [{"id": "U2", "name": "bob"}],
        "response_metadata": {"next_cursor": ""},
    }
    session.get.side_effect = [page1_resp, page2_resp]

    client = SlackClient(bot_token="xoxb-test-token", session=session)
    pages = list(client.iter_users())

    assert len(pages) == 2
    assert pages[0][0] == 1
    assert pages[0][1]["members"][0]["id"] == "U1"
    assert pages[1][0] == 2
    assert pages[1][1]["members"][0]["id"] == "U2"

    call_args = session.get.call_args_list[0]
    assert call_args.kwargs["headers"]["Authorization"] == "Bearer xoxb-test-token"


def test_iter_channels() -> None:
    session = MagicMock()
    resp = MagicMock(status_code=200)
    resp.json.return_value = {
        "ok": True,
        "channels": [{"id": "C1", "name": "general"}],
        "response_metadata": {},
    }
    session.get.return_value = resp

    client = SlackClient(bot_token="xoxb-test", session=session)
    pages = list(client.iter_channels())

    assert len(pages) == 1
    assert pages[0][1]["channels"][0]["name"] == "general"


def test_rate_limit_retry() -> None:
    session = MagicMock()
    rate_resp = MagicMock(status_code=429)
    rate_resp.headers = {"Retry-After": "2"}
    ok_resp = MagicMock(status_code=200)
    ok_resp.json.return_value = {"ok": True, "channels": []}
    session.get.side_effect = [rate_resp, ok_resp]

    slept: list[float] = []
    client = SlackClient(
        bot_token="xoxb-test",
        session=session,
        sleep=slept.append,
        jitter=lambda: 0.0,
    )
    pages = list(client.iter_channels())

    assert len(pages) == 1
    assert len(slept) == 1
    assert slept[0] == 2.0


def test_api_error_raised() -> None:
    session = MagicMock()
    err_resp = MagicMock(status_code=200)
    err_resp.json.return_value = {"ok": False, "error": "invalid_auth"}
    session.get.return_value = err_resp

    client = SlackClient(bot_token="xoxb-bad", session=session)
    with pytest.raises(SlackAPIError) as exc_info:
        list(client.iter_users())

    assert exc_info.value.error == "invalid_auth"
