"""
Update per-utility signed-contract status cells in the FILE_IDS Google Sheet.

Supports multiple comma-separated file IDs / statuses in one cell (index-aligned).
"""
from __future__ import annotations

import logging
import re
from typing import Any, Optional

from googleapiclient.errors import HttpError

from tools.business_info import (
    FILE_IDS_SHEET_ID,
    FILE_IDS_SHEET_NAME,
    _find_sheet_header_row,
    _normalize_business_name_for_match,
    get_sheets_service,
)

logger = logging.getLogger(__name__)

# Sheet header (as shown in FILE_IDS) → app contract label
CONTRACT_STATUS_HEADERS: dict[str, str] = {
    "C&I Electricity": "SC C&I E Status:",
    "SME Electricity": "SC SME E Status:",
    "C&I Gas": "SC C&I G Status:",
    "SME Gas": "SC SME G Status:",
    "Waste": "SC Waste Status:",
    "Oil": "SC Oil Status:",
    "DMA": "SC DMA Status:",
}

CONTRACT_FILE_HEADERS: dict[str, str] = {
    "C&I Electricity": "SC C&I E",
    "SME Electricity": "SC SME E",
    "C&I Gas": "SC C&I G",
    "SME Gas": "SC SME G",
    "Waste": "SC Waste",
    "Oil": "SC Oil",
    "DMA": "SC DMA",
}

ALLOWED_CONTRACT_STATUSES = frozenset(
    {
        "Signed via ACES",
        "Existing Contract",
        "Signed Externally",
    }
)

# Processed-file-ids key used by get-business-info / CRM UI
PROCESSED_STATUS_KEY = {
    label: f"contract_{label}_status" for label in CONTRACT_STATUS_HEADERS
}


def _normalize_header(h: Any) -> str:
    if h is None:
        return ""
    s = str(h).strip().lower()
    return re.sub(r"\s+", " ", s)


def _header_without_parens(h: str) -> str:
    s = re.sub(r"\s*\([^)]*\)", "", h)
    return re.sub(r"\s+", " ", s).strip()


def _col_index_to_a1(col_idx: int) -> str:
    """0-based column index → A1 column letters (0=A, 25=Z, 26=AA)."""
    if col_idx < 0:
        raise ValueError("column index must be >= 0")
    result = ""
    n = col_idx + 1
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def _split_csv_cell(raw: Any) -> list[str]:
    if raw is None or raw == "":
        return []
    return [p.strip() for p in str(raw).split(",")]


def _join_csv_cell(parts: list[str]) -> str:
    # Preserve empty slots so index alignment with file IDs stays stable
    return ",".join(parts)


def _find_header_column(headers: list[str], wanted: str) -> Optional[int]:
    """Find column index for a status/file header, tolerant of trailing colons/parens."""
    wanted_n = _normalize_header(wanted)
    wanted_stripped = _header_without_parens(wanted_n).rstrip(":")
    for i, h in enumerate(headers):
        hn = _normalize_header(h)
        if not hn:
            continue
        if hn == wanted_n:
            return i
        if _header_without_parens(hn).rstrip(":") == wanted_stripped:
            return i
    return None


def apply_status_at_index(
    current_status_cell: str,
    *,
    file_index: int,
    new_status: str,
    file_count: int,
) -> str:
    """
    Update one status slot in a comma-separated cell.

    Pads/truncates status list to align with file_count when possible.
    """
    if file_index < 0:
        raise ValueError("file_index must be >= 0")

    statuses = _split_csv_cell(current_status_cell)
    # Determine target length: prefer file_count, else keep enough room for index
    target_len = max(file_count, file_index + 1, len(statuses), 1)
    while len(statuses) < target_len:
        # Fill missing slots from last known status (legacy sheet behaviour)
        fill = statuses[-1] if statuses else ""
        statuses.append(fill)
    if file_index >= len(statuses):
        raise ValueError(f"file_index {file_index} out of range for {len(statuses)} status slot(s)")

    statuses[file_index] = new_status.strip()
    # If everything empty, return blank cell
    if not any(s.strip() for s in statuses):
        return ""
    return _join_csv_cell(statuses)


def update_contract_status(
    business_name: str,
    contract_key: str,
    status: str,
    *,
    file_index: int = 0,
) -> dict[str, Any]:
    """
    Write FILE_IDS status cell for one utility / file index.

    Returns dict with updated_status_cell, processed_key, sheet range, etc.
    Raises ValueError for validation errors; RuntimeError for sheet/API failures.
    """
    name = (business_name or "").strip()
    key = (contract_key or "").strip()
    new_status = (status or "").strip()

    if not name:
        raise ValueError("business_name is required")
    if key not in CONTRACT_STATUS_HEADERS:
        raise ValueError(
            f"Invalid contract_key. Must be one of: {', '.join(CONTRACT_STATUS_HEADERS)}"
        )
    if new_status and new_status not in ALLOWED_CONTRACT_STATUSES:
        raise ValueError(
            f"Invalid status. Must be one of: {', '.join(sorted(ALLOWED_CONTRACT_STATUSES))} "
            f"(or empty to clear)"
        )
    if file_index < 0:
        raise ValueError("file_index must be >= 0")
    if not FILE_IDS_SHEET_ID:
        raise RuntimeError("FILE_IDS_SHEET_ID is not set")

    service = get_sheets_service()
    if not service:
        raise RuntimeError("Could not create Google Sheets service")

    tab = FILE_IDS_SHEET_NAME or "Data from Airtable"
    range_str = f"'{tab}'!A1:AZ5000"
    try:
        result = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=FILE_IDS_SHEET_ID,
                range=range_str,
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )
    except HttpError as e:
        raise RuntimeError(f"Google Sheets read failed: {e}") from e

    rows = result.get("values", [])
    if not rows:
        raise RuntimeError("FILE_IDS sheet returned no rows")

    header_idx = _find_sheet_header_row(rows)
    headers = [str(h).strip() if h is not None else "" for h in rows[header_idx]]
    status_col = _find_header_column(headers, CONTRACT_STATUS_HEADERS[key])
    if status_col is None:
        raise RuntimeError(
            f"Could not find status column {CONTRACT_STATUS_HEADERS[key]!r} in FILE_IDS sheet"
        )
    file_col = _find_header_column(headers, CONTRACT_FILE_HEADERS[key])

    target = _normalize_business_name_for_match(name)
    matched_row_idx: Optional[int] = None
    matched_row: Optional[list[Any]] = None
    for i, row in enumerate(rows[header_idx + 1 :], start=header_idx + 1):
        row_dict: dict[str, Any] = {}
        for j, header in enumerate(headers):
            if not header:
                continue
            val = row[j] if j < len(row) else ""
            row_dict[header] = val if val is not None else ""
        bn = row_dict.get("Business Name") or row_dict.get("business name") or ""
        if _normalize_business_name_for_match(str(bn)) == target:
            matched_row_idx = i
            matched_row = row
            break

    if matched_row_idx is None or matched_row is None:
        raise ValueError(f"No FILE_IDS row matched business_name={name!r}")

    current_status = (
        matched_row[status_col] if status_col < len(matched_row) else ""
    )
    current_status_str = "" if current_status is None else str(current_status)

    file_cell = ""
    if file_col is not None and file_col < len(matched_row):
        raw_file = matched_row[file_col]
        file_cell = "" if raw_file is None else str(raw_file)
    file_parts = [p for p in _split_csv_cell(file_cell) if p]
    file_count = len(file_parts)

    if file_count > 0 and file_index >= file_count:
        raise ValueError(
            f"file_index {file_index} out of range — {key} has {file_count} file(s)"
        )
    if file_count == 0 and file_index > 0:
        raise ValueError(
            f"file_index {file_index} out of range — {key} has no filed contracts yet"
        )

    updated_cell = apply_status_at_index(
        current_status_str,
        file_index=file_index,
        new_status=new_status,
        file_count=max(file_count, 1),
    )

    # Sheets API is 1-indexed for rows
    sheet_row_number = matched_row_idx + 1
    a1_col = _col_index_to_a1(status_col)
    write_range = f"'{tab}'!{a1_col}{sheet_row_number}"

    try:
        service.spreadsheets().values().update(
            spreadsheetId=FILE_IDS_SHEET_ID,
            range=write_range,
            valueInputOption="RAW",
            body={"values": [[updated_cell]]},
        ).execute()
    except HttpError as e:
        raise RuntimeError(f"Google Sheets write failed: {e}") from e

    # Bust short-TTL contract index so climate / by-business reads see the new status
    try:
        from services.signed_contract_dry_run import load_contract_index

        load_contract_index(force=True)
    except Exception as e:  # pragma: no cover - non-fatal
        logger.info("[contract-status] contract index refresh skipped: %s", e)

    logger.info(
        "[contract-status] business=%r key=%s index=%s status=%r range=%s",
        name,
        key,
        file_index,
        new_status or "(cleared)",
        write_range,
    )

    return {
        "business_name": name,
        "contract_key": key,
        "file_index": file_index,
        "status": new_status,
        "updated_status_cell": updated_cell,
        "processed_key": PROCESSED_STATUS_KEY[key],
        "sheet_range": write_range,
        "file_count": file_count,
    }
