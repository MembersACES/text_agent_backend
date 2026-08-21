"""Unit tests for multi-file contract status cell updates."""
from services.contract_status_update import apply_status_at_index, _col_index_to_a1


def test_col_index_to_a1():
    assert _col_index_to_a1(0) == "A"
    assert _col_index_to_a1(25) == "Z"
    assert _col_index_to_a1(26) == "AA"


def test_apply_status_single_file():
    assert apply_status_at_index("Signed via ACES", file_index=0, new_status="Signed Externally", file_count=1) == "Signed Externally"


def test_apply_status_multi_file_second_only():
    out = apply_status_at_index(
        "Signed via ACES,Existing Contract",
        file_index=1,
        new_status="Signed Externally",
        file_count=2,
    )
    assert out == "Signed via ACES,Signed Externally"


def test_apply_status_pads_when_status_shorter_than_files():
    out = apply_status_at_index(
        "Signed via ACES",
        file_index=1,
        new_status="Signed Externally",
        file_count=2,
    )
    assert out == "Signed via ACES,Signed Externally"


def test_apply_status_clear():
    out = apply_status_at_index(
        "Signed via ACES,Existing Contract",
        file_index=0,
        new_status="",
        file_count=2,
    )
    assert out == ",Existing Contract"
