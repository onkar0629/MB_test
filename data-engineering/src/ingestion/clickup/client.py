"""ClickUp REST API client.

Handles authentication, pagination over team tasks, exponential backoff on 5xx,
pacing on rate limit exhaustion, and secret masking.
"""
from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterator
from datetime import datetime

from src.ingestion.clickup.ratelimit import (
    RateLimitState,
    backoff_seconds,
    pause_seconds,
)

DEFAULT_BASE_URL = "https://api.clickup.com/api/v2"
REQUEST_TIMEOUT_SECONDS = 60


class ClickUpAPIError(RuntimeError):
    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"ClickUp API returned {status_code}: {body[:500]}")
        self.status_code = status_code
        self.body = body


class ClickUpClient:
    def __init__(
        self,
        api_token: str,
        session,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[], float] = random.random,
        base_url: str = DEFAULT_BASE_URL,
        max_attempts: int = 5,
    ) -> None:
        self._api_token = api_token
        self._session = session
        self._sleep = sleep
        self._jitter = jitter
        self._base_url = base_url.rstrip("/")
        self._max_attempts = max_attempts

    def list_teams(self) -> list[dict]:
        """Fetch all teams (workspaces) accessible by this API token."""
        payload = self._get("/team", params={})
        return payload.get("teams", [])

    def iter_team_tasks(
        self,
        team_id: str,
        updated_after: datetime | None = None,
        updated_before: datetime | None = None,
        max_pages: int = 100,
    ) -> Iterator[tuple[int, dict]]:
        """Yield (page_number, response_payload) of tasks updated within window.

        Raises RuntimeError if max_pages is exhausted while more tasks remain.
        """
        page = 0
        while page < max_pages:
            params: dict[str, str] = {
                "page": str(page),
                "order_by": "updated",
                "reverse": "true",
                "subtasks": "true",
                "include_closed": "true",
            }
            if updated_after is not None:
                params["date_updated_gt"] = str(int(updated_after.timestamp() * 1000))
            if updated_before is not None:
                params["date_updated_lt"] = str(int(updated_before.timestamp() * 1000))

            payload = self._get(f"/team/{team_id}/task", params=params)
            tasks = payload.get("tasks", [])
            yield page + 1, payload

            # Stop when no more tasks or last_page flag is set
            if not tasks or payload.get("last_page") is True:
                return
            page += 1

        raise RuntimeError(
            f"Team {team_id!r} reached max_pages ({max_pages}) with more tasks remaining. "
            "Aborting run to prevent data loss."
        )

    def _get(self, path: str, params: dict[str, str]) -> dict:
        url = f"{self._base_url}{path}"
        headers = {"Authorization": self._api_token}

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._session.get(
                    url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS
                )
            except Exception as err:
                msg = str(err).replace(self._api_token, "[REDACTED]")
                if attempt == self._max_attempts:
                    raise RuntimeError(f"ClickUp request to {path} failed: {msg}") from None
                self._sleep(backoff_seconds(attempt) * (0.5 + self._jitter()))
                continue

            # Success
            if response.status_code == 200:
                rl_state = RateLimitState.from_headers(dict(response.headers), time.time())
                nap = pause_seconds(rl_state, time.time())
                if nap > 0:
                    self._sleep(nap)
                return response.json()

            # Rate limited
            if response.status_code == 429:
                reset = response.headers.get("X-RateLimit-Reset")
                retry_after = response.headers.get("Retry-After")
                nap = 1.0
                if retry_after:
                    try:
                        nap = float(retry_after)
                    except ValueError:
                        pass
                elif reset:
                    try:
                        r_val = float(reset)
                        nap = max(0.1, r_val - time.time()) if r_val > 1_000_000 else r_val
                    except ValueError:
                        pass
                self._sleep(nap + self._jitter())
                continue

            # Server error -> retry with backoff
            if 500 <= response.status_code < 600:
                if attempt == self._max_attempts:
                    body = response.text.replace(self._api_token, "[REDACTED]")
                    raise ClickUpAPIError(response.status_code, body)
                self._sleep(backoff_seconds(attempt) * (0.5 + self._jitter()))
                continue

            # Client error 4xx (not 429) -> fail immediately
            body = response.text.replace(self._api_token, "[REDACTED]")
            raise ClickUpAPIError(response.status_code, body)

        raise RuntimeError("Unreachable")
