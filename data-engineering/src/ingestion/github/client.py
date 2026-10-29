"""GitHub REST API client.

Handles authentication, pagination over repo PRs, commits, and issues,
exponential backoff on 5xx, rate limit monitoring, and secret masking.
"""
from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterator
from datetime import datetime

from src.ingestion.github.ratelimit import (
    GitHubRateLimitState,
    backoff_seconds,
    pause_seconds,
)

DEFAULT_BASE_URL = "https://api.github.com"
REQUEST_TIMEOUT_SECONDS = 60
USER_AGENT = "ActionOS-DataEngineering/1.0"


class GitHubAPIError(RuntimeError):
    def __init__(self, status_code: int, body: str) -> None:
        super().__init__(f"GitHub API returned {status_code}: {body[:500]}")
        self.status_code = status_code
        self.body = body


class GitHubClient:
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

    def iter_pull_requests(
        self,
        owner: str,
        repo: str,
        updated_after: datetime | None = None,
        max_pages: int = 100,
    ) -> Iterator[tuple[int, list[dict]]]:
        """Yield (page_number, items) for pull requests in the repository.

        Ordered by updated descending. Stops when items are older than updated_after.
        """
        page = 1
        while page <= max_pages:
            params = {
                "state": "all",
                "sort": "updated",
                "direction": "desc",
                "per_page": "100",
                "page": str(page),
            }
            items = self._get(f"/repos/{owner}/{repo}/pulls", params=params)
            if not isinstance(items, list):
                raise GitHubAPIError(500, f"Expected list response, got: {type(items)}")

            if not items:
                return

            yield page, items

            # Check if the oldest item on this page is older than updated_after
            if updated_after is not None and items:
                oldest_str = items[-1].get("updated_at")
                if oldest_str:
                    try:
                        oldest_dt = datetime.fromisoformat(
                            oldest_str.replace("Z", "+00:00")
                        )
                        if oldest_dt < updated_after:
                            return
                    except ValueError:
                        pass

            if len(items) < 100:
                return

            page += 1

        raise RuntimeError(
            f"Repo {owner}/{repo} PRs reached max_pages ({max_pages}) with more items remaining. "
            "Aborting run to prevent data loss."
        )

    def iter_commits(
        self,
        owner: str,
        repo: str,
        since: datetime | None = None,
        until: datetime | None = None,
        max_pages: int = 100,
    ) -> Iterator[tuple[int, list[dict]]]:
        """Yield (page_number, items) for commits in the repository."""
        page = 1
        while page <= max_pages:
            params = {
                "per_page": "100",
                "page": str(page),
            }
            if since is not None:
                params["since"] = since.isoformat()
            if until is not None:
                params["until"] = until.isoformat()

            items = self._get(f"/repos/{owner}/{repo}/commits", params=params)
            if not isinstance(items, list):
                raise GitHubAPIError(500, f"Expected list response, got: {type(items)}")

            if not items:
                return

            yield page, items

            if len(items) < 100:
                return

            page += 1

        raise RuntimeError(
            f"Repo {owner}/{repo} commits reached max_pages ({max_pages}) "
            "with more items remaining. Aborting run to prevent data loss."
        )

    def iter_issues(
        self,
        owner: str,
        repo: str,
        since: datetime | None = None,
        max_pages: int = 100,
    ) -> Iterator[tuple[int, list[dict]]]:
        """Yield (page_number, items) for issues in the repository."""
        page = 1
        while page <= max_pages:
            params = {
                "state": "all",
                "sort": "updated",
                "direction": "desc",
                "per_page": "100",
                "page": str(page),
            }
            if since is not None:
                params["since"] = since.isoformat()

            items = self._get(f"/repos/{owner}/{repo}/issues", params=params)
            if not isinstance(items, list):
                raise GitHubAPIError(500, f"Expected list response, got: {type(items)}")

            if not items:
                return

            yield page, items

            if len(items) < 100:
                return

            page += 1

        raise RuntimeError(
            f"Repo {owner}/{repo} issues reached max_pages ({max_pages}) "
            "with more items remaining. Aborting run to prevent data loss."
        )

    def _get(self, path: str, params: dict[str, str]) -> list | dict:
        url = f"{self._base_url}{path}"
        headers = {
            "Authorization": f"Bearer {self._api_token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": USER_AGENT,
        }

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._session.get(
                    url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS
                )
            except Exception as err:
                msg = str(err).replace(self._api_token, "[REDACTED]")
                if attempt == self._max_attempts:
                    raise RuntimeError(f"GitHub request to {path} failed: {msg}") from None
                self._sleep(backoff_seconds(attempt) * (0.5 + self._jitter()))
                continue

            # Success
            if response.status_code == 200:
                rl_state = GitHubRateLimitState.from_headers(
                    dict(response.headers), time.time()
                )
                nap = pause_seconds(rl_state, time.time())
                if nap > 0:
                    self._sleep(nap)
                return response.json()

            # Rate limited or primary limit hit
            if response.status_code in (403, 429) and "rate limit" in response.text.lower():
                reset = response.headers.get("x-ratelimit-reset")
                retry_after = response.headers.get("Retry-After")
                nap = 5.0
                if retry_after:
                    try:
                        nap = float(retry_after)
                    except ValueError:
                        pass
                elif reset:
                    try:
                        r_val = float(reset)
                        nap = max(1.0, r_val - time.time()) if r_val > 1_000_000 else r_val
                    except ValueError:
                        pass
                self._sleep(nap + self._jitter())
                continue

            # Server error 5xx -> retry with backoff
            if 500 <= response.status_code < 600:
                if attempt == self._max_attempts:
                    body = response.text.replace(self._api_token, "[REDACTED]")
                    raise GitHubAPIError(response.status_code, body)
                self._sleep(backoff_seconds(attempt) * (0.5 + self._jitter()))
                continue

            # Client error 4xx -> fail immediately
            body = response.text.replace(self._api_token, "[REDACTED]")
            raise GitHubAPIError(response.status_code, body)

        raise RuntimeError("Unreachable")
