"""Unit tests for Slack landing functions."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

from src.ingestion.slack.land import (
    land_slack_public_channel_messages,
)


def test_land_slack_public_channel_messages_strictly_public_only():
    """Verify private channels and DMs are filtered out, and only public channels are ingested."""
    client = MagicMock()
    # Channels API returns 1 public channel, 1 private channel, and 1 DM
    client.iter_channels.return_value = [
        (
            1,
            {
                "ok": True,
                "channels": [
                    {"id": "C_PUB_1", "name": "general", "is_private": False, "is_channel": True},
                    {"id": "C_PRIV_2", "name": "secret-hr", "is_private": True, "is_channel": True},
                    {"id": "D_DM_3", "name": "direct-dm", "is_private": True, "is_im": True},
                ],
            },
        )
    ]

    # Message history for the public channel
    client.iter_channel_messages.return_value = [
        (
            1,
            {
                "ok": True,
                "messages": [
                    {
                        "type": "message",
                        "user": "U1",
                        "text": "Hello world",
                        "ts": "1730000001.000100",
                    },
                    {
                        "type": "message",
                        "user": "U2",
                        "text": "Morning team",
                        "ts": "1730000002.000200",
                    },
                ],
            },
        )
    ]

    written_files = {}

    def mock_writer(path: str, content: str) -> None:
        written_files[path] = json.loads(content)

    pages, watermarks = land_slack_public_channel_messages(
        client=client,
        writer=mock_writer,
        volume_root="/Volumes/lakehouse/raw_slack/landing",
        ingest_date="2026-11-01",
        run_id="run-123",
        watermarks={"C_PUB_1": "1729999999.000000"},
    )

    # iter_channel_messages should have ONLY been called for C_PUB_1, never for C_PRIV_2 or D_DM_3
    client.iter_channel_messages.assert_called_once_with(
        channel_id="C_PUB_1",
        oldest="1729999999.000000",
        limit=200,
        max_pages=100,
    )

    assert pages == 1
    assert "C_PUB_1" in watermarks
    assert watermarks["C_PUB_1"] == "1730000002.000200"

    expected_path = (
        "/Volumes/lakehouse/raw_slack/landing/messages/channel_id=C_PUB_1/ingest_date=2026-11-01/run_id=run-123/page-0001.json"
    )
    assert expected_path in written_files
    assert len(written_files[expected_path]["messages"]) == 2
