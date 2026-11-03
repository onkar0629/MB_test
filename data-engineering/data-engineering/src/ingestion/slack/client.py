"""Slack Web API client.

Handles authentication, cursor pagination for users and conversations,
exponential backoff on 5xx / 429 rate limits, and token masking.
"""
from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterator

from src.ingestion.slack.ratelimit import backoff_seconds, retry_after_seconds

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://slack.com/api"
REQUEST_TIMEOUT_SECONDS = 60


class SlackAPIError(RuntimeError):
    def __init__(self, error: str, payload: dict | None = None) -> None:
        super().__init__(f"Slack API error '{error}': {payload}")
        self.error = error
        self.payload = payload or {}


class SlackClient:
    def __init__(
        self,
        bot_token: str,
        session,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
        base_url: str = DEFAULT_BASE_URL,
        max_attempts: int = 5,
    ) -> None:
        self._bot_token = bot_token
        self._session = session
        self._sleep = sleep
        self._jitter = jitter
        self._base_url = base_url.rstrip("/")
        self._max_attempts = max_attempts

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        url = f"{self._base_url}/{endpoint.lstrip('/')}"
        headers = {
            "Authorization": f"Bearer {self._bot_token}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        query_params = dict(params or {})

        for attempt in range(1, self._max_attempts + 1):
            try:
                resp = self._session.get(
                    url,
                    headers=headers,
                    params=query_params,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                if attempt == self._max_attempts:
                    raise
                wait = backoff_seconds(attempt) + self._jitter() * 0.5
                logger.warning(
                    "Network error calling Slack endpoint %s (attempt %d/%d): %s. "
                    "Backing off %.2fs",
                    endpoint,
                    attempt,
                    self._max_attempts,
                    exc,
                    wait,
                )
                self._sleep(wait)
                continue

            if resp.status_code == 429:
                if attempt == self._max_attempts:
                    raise SlackAPIError("ratelimited_max_retries", {"status": 429})
                wait = retry_after_seconds(resp.headers) + self._jitter() * 0.5
                logger.warning(
                    "Slack HTTP 429 on %s (attempt %d/%d). Sleeping %.2fs",
                    endpoint,
                    attempt,
                    self._max_attempts,
                    wait,
                )
                self._sleep(wait)
                continue

            if resp.status_code >= 500:
                if attempt == self._max_attempts:
                    raise SlackAPIError(f"server_error_{resp.status_code}", {"body": resp.text})
                wait = backoff_seconds(attempt) + self._jitter() * 0.5
                logger.warning(
                    "Slack HTTP %d on %s (attempt %d/%d). Sleeping %.2fs",
                    resp.status_code,
                    endpoint,
                    attempt,
                    self._max_attempts,
                    wait,
                )
                self._sleep(wait)
                continue

            try:
                payload = resp.json()
            except Exception as exc:
                raise SlackAPIError("invalid_json", {"text": resp.text[:200]}) from exc

            if not payload.get("ok"):
                error_code = payload.get("error", "unknown_error")
                if error_code == "ratelimited" and attempt < self._max_attempts:
                    wait = retry_after_seconds(resp.headers) + self._jitter() * 0.5
                    logger.warning(
                        "Slack ratelimited on %s. Sleeping %.2fs", endpoint, wait
                    )
                    self._sleep(wait)
                    continue
                raise SlackAPIError(error_code, payload)

            return payload

        raise SlackAPIError("max_attempts_exceeded")

    def iter_users(self, limit: int = 200, max_pages: int = 100) -> Iterator[tuple[int, dict]]:
        """Yield (page_number, response_payload) for workspace users."""
        cursor: str | None = None
        for page_no in range(1, max_pages + 1):
            params: dict[str, str | int] = {"limit": limit}
            if cursor:
                params["cursor"] = cursor

            payload = self._get("users.list", params=params)
            yield page_no, payload

            meta = payload.get("response_metadata") or {}
            cursor = meta.get("next_cursor")
            if not cursor:
                break

    def iter_channels(
        self,
        types: str = "public_channel,private_channel",
        limit: int = 200,
        max_pages: int = 100,
    ) -> Iterator[tuple[int, dict]]:
        """Yield (page_number, response_payload) for workspace channels."""
        cursor: str | None = None
        for page_no in range(1, max_pages + 1):
            params: dict[str, str | int] = {"types": types, "limit": limit}
            if cursor:
                params["cursor"] = cursor

            payload = self._get("conversations.list", params=params)
            yield page_no, payload

            meta = payload.get("response_metadata") or {}
            cursor = meta.get("next_cursor")
            if not cursor:
                break

    def iter_channel_messages(
        self,
        channel_id: str,
        oldest: str | None = None,
        latest: str | None = None,
        limit: int = 200,
        max_pages: int = 100,
    ) -> Iterator[tuple[int, dict]]:
        """Yield (page_number, response_payload) for channel messages."""
        cursor: str | None = None
        for page_no in range(1, max_pages + 1):
            params: dict[str, str | int] = {"channel": channel_id, "limit": limit}
            if oldest:
                params["oldest"] = oldest
            if latest:
                params["latest"] = latest
            if cursor:
                params["cursor"] = cursor

            try:
                payload = self._get("conversations.history", params=params)
            except SlackAPIError as exc:
                if exc.error in ("missing_scope", "not_in_channel"):
                    logger.warning(
                        "Skipping history for channel %s due to '%s'",
                        channel_id,
                        exc.error,
                    )
                    break
                raise

            yield page_no, payload

            meta = payload.get("response_metadata") or {}
            cursor = meta.get("next_cursor")
            if not cursor:
                break
