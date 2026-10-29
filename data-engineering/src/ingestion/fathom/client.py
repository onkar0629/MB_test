"""Fathom REST client.

Every request sets all four include_* flags so one call returns meeting
headers, transcripts, summaries and action items together. Listing meetings and
then fetching each transcript separately would cost 1+N heavy calls per page
against a ceiling that can fall to 5/min.
"""
from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterator
from datetime import datetime

from src.ingestion.fathom.ratelimit import (
    RateLimitState,
    backoff_seconds,
    pause_seconds,
)

DEFAULT_BASE_URL = "https://api.fathom.ai/external/v1"
REQUEST_TIMEOUT_SECONDS = 60

INCLUDE_FLAGS = {
    "include_transcript": "true",
    "include_summary": "true",
    "include_action_items": "true",
    "include_crm_matches": "true",
}


class FathomAPIError(RuntimeError):
    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"Fathom API returned {status_code}: {body[:500]}")
        self.status_code = status_code
        self.body = body


class FathomClient:
    def __init__(
        self,
        api_key: str,
        session,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
        base_url: str = DEFAULT_BASE_URL,
        max_attempts: int = 5,
    ) -> None:
        self._api_key = api_key
        self._session = session
        self._sleep = sleep
        self._jitter = jitter
        self._base_url = base_url.rstrip("/")
        self._max_attempts = max_attempts

    def iter_meeting_pages(
        self, created_after: datetime, created_before: datetime, max_pages: int
    ) -> Iterator[tuple[int, dict]]:
        """Yield (page_number, response_payload) until the cursor is exhausted."""
        cursor: str | None = None
        for page_no in range(1, max_pages + 1):
            params = {
                **INCLUDE_FLAGS,
                "created_after": created_after.isoformat(),
                "created_before": created_before.isoformat(),
            }
            if cursor:
                params["cursor"] = cursor

            payload = self._get("/meetings", params)
            yield page_no, payload

            cursor = payload.get("next_cursor")
            if not cursor:
                return

    def _get(self, path: str, params: dict) -> dict:
        url = f"{self._base_url}{path}"
        last_status = 0
        for attempt in range(1, self._max_attempts + 1):
            response = self._session.get(
                url,
                params=params,
                headers={"X-Api-Key": self._api_key},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            last_status = response.status_code

            if response.status_code == 429:
                self._sleep(_retry_after(response.headers))
                continue
            if response.status_code >= 500:
                self._sleep(backoff_seconds(attempt, jitter=self._jitter()))
                continue
            if response.status_code >= 400:
                # Never include params or headers here: they carry the API key.
                raise FathomAPIError(response.status_code, response.text)

            wait = pause_seconds(RateLimitState.from_headers(response.headers))
            if wait:
                self._sleep(wait)
            return response.json()

        raise FathomAPIError(last_status, "exhausted retries")


def _retry_after(headers) -> float:
    try:
        return float(headers.get("Retry-After", 1))
    except (TypeError, ValueError):
        return 1.0
