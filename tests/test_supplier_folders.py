"""Unit tests for supplier Drive helpers (no live Drive calls)."""

from __future__ import annotations

from tools.supplier_folders import (
    SupplierCategory,
    classify_supplier,
    file_type_from_mime,
    mimetype_for_filename,
    resolve_upload_filename,
)


def test_classify_energy_retailers():
    for name in (
        "Alinta",
        "Origin",
        "Momentum",
        "Shell",
        "CovaU",
        "PlusES",
        "Plus ES",
        "Obee",
        "Solar PPA - Goodwe",
        "Enel X",
        "Blue NRG",
        "Powermetric",
    ):
        assert classify_supplier(name) is SupplierCategory.ENERGY, name


def test_classify_waste_suppliers():
    for name in ("Veolia", "Visy", "Vizy", "Cleanaway"):
        assert classify_supplier(name) is SupplierCategory.WASTE, name


def test_classify_other_folders():
    for name in (
        "Pudu Robotics",
        "ERA Robotics Pty Ltd",
        "Other (internal)",
        "Invoices to Supplier",
        "Select Advice Finance",
        "EGB",
    ):
        assert classify_supplier(name) is SupplierCategory.OTHER, name


def test_waste_wins_over_energy_substring():
    assert classify_supplier("Visy") is SupplierCategory.WASTE


def test_mimetype_from_extension():
    assert mimetype_for_filename("form.pdf") == "application/pdf"
    assert mimetype_for_filename("rates.xlsx") == (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert mimetype_for_filename("scan.PDF") == "application/pdf"


def test_mimetype_prefers_content_type():
    assert mimetype_for_filename("file.bin", "application/pdf") == "application/pdf"
    assert mimetype_for_filename("file.pdf", "application/octet-stream") == "application/pdf"


def test_file_type_from_mime():
    assert file_type_from_mime("application/vnd.google-apps.folder", "X") == "folder"
    assert file_type_from_mime("application/pdf", "a.pdf") == "pdf"
    assert file_type_from_mime("application/vnd.google-apps.spreadsheet", "Rates") == "sheet"
    assert file_type_from_mime("application/vnd.google-apps.document", "Form") == "doc"
    assert file_type_from_mime("image/png", "logo.png") == "image"


def test_resolve_upload_filename():
    assert resolve_upload_filename("scan.pdf") == "scan.pdf"
    assert resolve_upload_filename("scan.pdf", "Alinta ESG 2026") == "Alinta ESG 2026.pdf"
    assert resolve_upload_filename("scan.pdf", "Alinta ESG 2026.pdf") == "Alinta ESG 2026.pdf"
    assert resolve_upload_filename(r"C:\tmp\form.docx", "Rates") == "Rates.docx"
    assert resolve_upload_filename("a.pdf", 'bad:name?.pdf') == "bad-name-.pdf"
    assert resolve_upload_filename("a.pdf", "   ") == "a.pdf"
