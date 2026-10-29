"""Proves the package tree is importable and pytest is wired up."""
from __future__ import annotations


def test_src_package_is_importable():
    import src.ingestion.fathom  # noqa: F401
    import src.common  # noqa: F401
    import src.transform.fathom  # noqa: F401
