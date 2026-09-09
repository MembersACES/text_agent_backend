"""Unit tests for distributor Drive helpers (no live Drive calls)."""

from __future__ import annotations

from tools.distributor_folders import DOCUMENTS_FOLDER_NAME, display_distributor_name


def test_documents_folder_name():
    assert DOCUMENTS_FOLDER_NAME == "Distributor Documents"


def test_display_strips_a_prefix():
    assert display_distributor_name("A - Reddrop") == "Reddrop"
    assert display_distributor_name("A - Specialist Energy Management") == (
        "Specialist Energy Management"
    )
    assert display_distributor_name("a - lowercase prefix") == "lowercase prefix"


def test_display_keeps_unprefixed_names():
    assert display_distributor_name("Other") == "Other"
    assert display_distributor_name("  Trimmed  ") == "Trimmed"
    assert display_distributor_name("A -") == "A -"
    assert display_distributor_name("") == ""
