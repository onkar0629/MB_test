from __future__ import annotations

import pytest

from src.ingestion.fathom.accounts import FathomAccount, parse_accounts, validate_slug


def test_parses_a_valid_registry():
    raw = '[{"slug": "fenil", "secret_key": "fathom-api-key-fenil"}]'
    assert parse_accounts(raw) == [
        FathomAccount(slug="fenil", secret_key="fathom-api-key-fenil")
    ]


def test_rejects_duplicate_slugs():
    raw = (
        '[{"slug": "fenil", "secret_key": "a"},'
        ' {"slug": "fenil", "secret_key": "b"}]'
    )
    with pytest.raises(ValueError, match="duplicate slug"):
        parse_accounts(raw)


@pytest.mark.parametrize("slug", ["../escape", "has space", "UPPER", "", "a/b"])
def test_rejects_slugs_that_are_unsafe_in_a_path(slug):
    raw = '[{"slug": "%s", "secret_key": "k"}]' % slug
    with pytest.raises(ValueError, match="invalid slug"):
        parse_accounts(raw)


def test_rejects_missing_secret_key():
    with pytest.raises(ValueError, match="no secret_key"):
        parse_accounts('[{"slug": "fenil"}]')


def test_rejects_empty_or_malformed_input():
    with pytest.raises(ValueError, match="non-empty JSON array"):
        parse_accounts("[]")
    with pytest.raises(ValueError, match="not valid JSON"):
        parse_accounts("{not json")


def test_validate_slug_accepts_valid():
    assert validate_slug("fenil") == "fenil"
    assert validate_slug("user_12-a") == "user_12-a"


@pytest.mark.parametrize("slug", ["../escape", "has space", "UPPER", "", "a/b"])
def test_validate_slug_rejects_invalid(slug):
    with pytest.raises(ValueError, match="invalid slug"):
        validate_slug(slug)
