"""The registry of Fathom accounts this pipeline pulls from.

One entry per contributor. Adding a person is a secret plus one list entry in
the bundle variable `fathom_accounts` -- no code change.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass

# Slugs are interpolated into Volume paths, so keep them to a safe alphabet.
SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")


def validate_slug(slug: str) -> str:
    """Validate that a slug conforms to SLUG_PATTERN.

    Raises ValueError if invalid, otherwise returns the slug.
    """
    if not SLUG_PATTERN.match(slug):
        raise ValueError(f"invalid slug: {slug!r}")
    return slug


@dataclass(frozen=True)
class FathomAccount:
    slug: str
    secret_key: str


def parse_accounts(raw: str) -> list[FathomAccount]:
    """Parse the `accounts` job parameter into validated records.

    Raises ValueError on anything malformed; the job must fail loudly rather
    than silently skip a contributor's meetings.
    """
    try:
        items = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"accounts is not valid JSON: {exc}") from exc

    if not isinstance(items, list) or not items:
        raise ValueError("accounts must be a non-empty JSON array")

    accounts: list[FathomAccount] = []
    seen: set[str] = set()
    for item in items:
        slug = validate_slug(str(item.get("slug", "")))
        secret_key = str(item.get("secret_key", ""))
        if not secret_key:
            raise ValueError(f"account {slug!r} has no secret_key")
        if slug in seen:
            raise ValueError(f"duplicate slug: {slug!r}")
        seen.add(slug)
        accounts.append(FathomAccount(slug=slug, secret_key=secret_key))
    return accounts
