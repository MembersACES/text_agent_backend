"""Orchestrate member/distributor Drive folders, FILE_IDS, and Airtable (n8n fallback)."""

from __future__ import annotations

import logging
import os
from typing import Any, Optional
import requests

from services.airtable_client import (
    AIRTABLE_API_KEY,
    LOA_TABLE_ID,
    _escape_formula_value,
    _headers,
    _url,
    get_loa_record_by_id,
    get_loa_records_by_business_name,
    loa_record_candidate_summary,
)
from tools.business_info import FILE_IDS_SHEET_ID, FILE_IDS_SHEET_NAME, get_sheets_service
from tools.distributor_agreement import (
    append_distributor_row,
    details_to_sheet_row,
    extract_distribution_agreement,
    update_distributor_drive_cells,
)
from tools.member_folder_drive import (
    DISTRIBUTORS_FOLDER_ID,
    MEMBERS_B_FOLDER_ID,
    WIP_TEMPLATE_FILE_ID,
    MemberFolderDriveError,
    copy_file_into_folder,
    create_empty_named_folder,
    create_member_drive_folder,
    find_named_file_in_folder,
    list_child_folders,
    member_wip_spreadsheet_name,
    upload_bytes_to_folder,
)
from tools.share_folder import drive_folder_url

logger = logging.getLogger(__name__)

CLASSIFICATION_TABLE_ID = os.getenv("AIRTABLE_CLASSIFICATION_TABLE_ID", "tblqo5b3yNybQaE3F")
SUBFOLDER_TABLE_ID = os.getenv("AIRTABLE_CLASSIFICATION_SUBFOLDER_TABLE_ID", "tblst4H9zkPILG5Ix")
DRIVE_SHEET_AIRTABLE_TABLE = os.getenv(
    "AIRTABLE_DRIVE_SHEET_TABLE",
    "Google Drive - Sheet & Airtable",
)
LOA_LINK_FIELD = "1st Sheet - LOA Business Details"
N8N_MEMBER_FOLDER_WEBHOOK = os.getenv(
    "N8N_UPDATE_AIRTABLE_CALL_SCRIPT_URL",
    "https://membersaces.app.n8n.cloud/webhook/update_airtable_call_script_function",
)


def list_industry_folders() -> list[dict[str, str]]:
    return list_child_folders(MEMBERS_B_FOLDER_ID, name_prefix="003-")


def list_subfolders(parent_id: str) -> list[dict[str, str]]:
    return list_child_folders(parent_id)


def _search_airtable(table_id: str, formula: str, max_records: int = 5) -> list[dict]:
    if not AIRTABLE_API_KEY:
        return []
    try:
        r = requests.get(
            _url(table_id),
            headers=_headers(),
            params={"filterByFormula": formula, "maxRecords": max(1, max_records)},
            timeout=30,
        )
        r.raise_for_status()
        records = r.json().get("records") or []
        return records if isinstance(records, list) else []
    except requests.RequestException as e:
        logger.warning("[member_folder] Airtable search failed: %s", e)
        return []


def _create_airtable_record(table_id: str, fields: dict[str, Any]) -> Optional[dict]:
    if not AIRTABLE_API_KEY:
        return None
    try:
        r = requests.post(
            _url(table_id),
            headers=_headers(),
            json={"fields": fields, "typecast": True},
            timeout=30,
        )
        if not r.ok:
            logger.warning(
                "[member_folder] Airtable create %s failed %s: %s",
                table_id,
                r.status_code,
                (r.text or "")[:400],
            )
            return None
        return r.json()
    except requests.RequestException as e:
        logger.warning("[member_folder] Airtable create failed: %s", e)
        return None


def _patch_airtable_record(table_id: str, record_id: str, fields: dict[str, Any]) -> bool:
    if not AIRTABLE_API_KEY or not record_id:
        return False
    try:
        r = requests.patch(
            _url(table_id, record_id),
            headers=_headers(),
            json={"fields": fields, "typecast": True},
            timeout=30,
        )
        if not r.ok:
            logger.warning(
                "[member_folder] Airtable patch %s/%s failed %s: %s",
                table_id,
                record_id,
                r.status_code,
                (r.text or "")[:400],
            )
            return False
        return True
    except requests.RequestException as e:
        logger.warning("[member_folder] Airtable patch failed: %s", e)
        return False


def merge_linked_ids(existing: Any, new_id: str) -> list[str]:
    ids: list[str] = []
    if isinstance(existing, list):
        ids = [x for x in existing if isinstance(x, str) and x.startswith("rec")]
    elif isinstance(existing, str) and existing.startswith("rec"):
        ids = [existing]
    if new_id and new_id not in ids:
        ids.append(new_id)
    return ids


def upsert_classification_folder(
    *,
    file_path: str,
    name: str,
    parent_folder: str,
    folder_id: str,
    folder_url: str,
) -> Optional[str]:
    escaped = _escape_formula_value(file_path)
    records = _search_airtable(CLASSIFICATION_TABLE_ID, f"{{File Path}}='{escaped}'")
    fields = {
        "File Path": file_path,
        "Name": name,
        "Parent Folder": parent_folder,
        "File ID": folder_id,
        "Link": folder_url,
        "Open in Google Drive": folder_url,
    }
    if records:
        rid = str(records[0].get("id") or "")
        _patch_airtable_record(CLASSIFICATION_TABLE_ID, rid, fields)
        return rid or None
    created = _create_airtable_record(CLASSIFICATION_TABLE_ID, fields)
    return str(created.get("id") or "") if created else None


def upsert_classification_subfolder(
    *,
    file_path: str,
    name: str,
    parent_folder: str,
    folder_id: str,
    folder_url: str,
) -> Optional[str]:
    escaped = _escape_formula_value(file_path)
    records = _search_airtable(SUBFOLDER_TABLE_ID, f"{{File Path}}='{escaped}'")
    fields = {
        "File Path": file_path,
        "Name": name,
        "Parent Folder": parent_folder,
        "File ID": folder_id,
        "Link": folder_url,
        "Open in Google Drive": folder_url,
    }
    if records:
        rid = str(records[0].get("id") or "")
        _patch_airtable_record(SUBFOLDER_TABLE_ID, rid, fields)
        return rid or None
    created = _create_airtable_record(SUBFOLDER_TABLE_ID, fields)
    return str(created.get("id") or "") if created else None


def _link_loa_to_record(table_id: str, record_id: str, loa_id: str) -> bool:
    rec = None
    try:
        r = requests.get(_url(table_id, record_id), headers=_headers(), timeout=20)
        if r.ok:
            rec = r.json()
    except requests.RequestException:
        rec = None
    existing = (rec or {}).get("fields", {}).get(LOA_LINK_FIELD)
    merged = merge_linked_ids(existing, loa_id)
    return _patch_airtable_record(table_id, record_id, {LOA_LINK_FIELD: merged})


def upsert_drive_sheet_airtable_row(
    *,
    business_name: str,
    loa_file_id: Optional[str],
    loa_file_url: Optional[str],
    folder_id: str,
    folder_url: str,
) -> bool:
    if not AIRTABLE_API_KEY:
        return False
    escaped = _escape_formula_value(business_name)
    records = _search_airtable(DRIVE_SHEET_AIRTABLE_TABLE, f"{{Business Name}}='{escaped}'")
    fields = {
        "Business Name": business_name,
        "LOA File ID": loa_file_id or "",
        "LOA File ID Link": loa_file_url or "",
        "Google Drive Folder ID": folder_id,
        "Google Drive Folder Link": folder_url,
    }
    if records:
        rid = str(records[0].get("id") or "")
        return _patch_airtable_record(DRIVE_SHEET_AIRTABLE_TABLE, rid, fields)
    created = _create_airtable_record(DRIVE_SHEET_AIRTABLE_TABLE, fields)
    return bool(created)


def patch_loa_drive_folder(loa_record_id: str, folder_url: str) -> bool:
    return _patch_airtable_record(
        LOA_TABLE_ID,
        loa_record_id,
        {"File ID Google Drive Client Folder": folder_url},
    )


def update_file_ids_row(
    *,
    business_name: str,
    classification: str,
    state: str,
    loa_file_id: str = "",
    loa_file_url: str = "",
    folder_id: str = "",
    folder_url: str = "",
    wip_file_id: str = "",
) -> bool:
    sheet_id = FILE_IDS_SHEET_ID
    tab = FILE_IDS_SHEET_NAME or "Data from Airtable"
    service = get_sheets_service()
    if not service or not sheet_id:
        return False
    try:
        resp = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=f"'{tab}'!A1:H5000")
            .execute()
        )
        rows = resp.get("values") or []
        row_number = None
        existing_wip = ""
        for i, row in enumerate(rows[1:], start=2):
            cell = str(row[0]).strip() if row else ""
            if cell == business_name:
                row_number = i
                if len(row) > 7:
                    existing_wip = str(row[7]).strip()
        wip_value = (wip_file_id or existing_wip).strip()
        values = [
            business_name,
            classification,
            state,
            loa_file_id,
            loa_file_url,
            folder_id,
            folder_url,
        ]
        end_col = "G"
        if wip_value:
            values.append(wip_value)
            end_col = "H"
        if row_number:
            service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range=f"'{tab}'!A{row_number}:{end_col}{row_number}",
                valueInputOption="USER_ENTERED",
                body={"values": [values]},
            ).execute()
        else:
            service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range=f"'{tab}'!A1",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [values]},
            ).execute()
        return True
    except Exception as e:
        logger.warning("[member_folder] FILE_IDS update failed: %s", e)
        return False


def read_file_ids_wip_id(business_name: str) -> str:
    sheet_id = FILE_IDS_SHEET_ID
    tab = FILE_IDS_SHEET_NAME or "Data from Airtable"
    service = get_sheets_service()
    if not service or not sheet_id or not (business_name or "").strip():
        return ""
    try:
        resp = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=sheet_id, range=f"'{tab}'!A1:H5000")
            .execute()
        )
        for row in (resp.get("values") or [])[1:]:
            cell = str(row[0]).strip() if row else ""
            if cell == business_name and len(row) > 7:
                return str(row[7]).strip()
    except Exception as e:
        logger.warning("[member_folder] FILE_IDS WIP read failed: %s", e)
    return ""


def fill_wip_business_account(
    wip_spreadsheet_id: str,
    *,
    business_name: str,
    loa_file_id: str,
    folder_id: str,
    user_access_token: str = "",
) -> bool:
    if not (wip_spreadsheet_id or "").strip():
        return False
    values = [
        [
            business_name,
            loa_file_id or "",
            folder_id or "No folder assigned",
        ]
    ]

    def _write(service: Any) -> bool:
        if not service:
            return False
        service.spreadsheets().values().update(
            spreadsheetId=wip_spreadsheet_id,
            range="'Business Account'!A2:C2",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()
        return True

    try:
        if _write(get_sheets_service()):
            return True
    except Exception as e:
        logger.warning("[member_folder] WIP Business Account fill via SA failed: %s", e)
    token = (user_access_token or "").strip()
    if not token:
        return False
    try:
        from google.oauth2.credentials import Credentials as UserCredentials
        from googleapiclient.discovery import build

        user_sheets = build(
            "sheets",
            "v4",
            credentials=UserCredentials(token=token),
            cache_discovery=False,
        )
        return _write(user_sheets)
    except Exception as e:
        logger.warning("[member_folder] WIP Business Account fill failed: %s", e)
        return False


def create_member_wip(
    *,
    business_name: str,
    folder_id: str,
    loa_file_id: str = "",
    user_access_token: str = "",
) -> dict[str, Any]:
    """Apps Script createWIPForClients for a single member."""
    name = member_wip_spreadsheet_name(business_name)
    existing = find_named_file_in_folder(folder_id, name)
    if existing:
        filled = fill_wip_business_account(
            existing["id"],
            business_name=business_name,
            loa_file_id=loa_file_id,
            folder_id=folder_id,
            user_access_token=user_access_token,
        )
        return {**existing, "created": False, "business_account_filled": filled}
    copied = copy_file_into_folder(
        WIP_TEMPLATE_FILE_ID,
        folder_id,
        name,
        user_access_token=user_access_token or None,
    )
    filled = fill_wip_business_account(
        copied["id"],
        business_name=business_name,
        loa_file_id=loa_file_id,
        folder_id=folder_id,
        user_access_token=user_access_token,
    )
    return {**copied, "created": True, "business_account_filled": filled}


def _n8n_member_folder_fallback(
    *,
    business_name: str,
    trading_as: str,
    classification: str,
    state: str,
) -> tuple[bool, str]:
    try:
        r = requests.post(
            N8N_MEMBER_FOLDER_WEBHOOK,
            json={
                "Business Name": business_name,
                "Trading As": trading_as or "N/A",
                "Industry Classification Folder": classification,
                "Industry Classification SubFolder": state,
            },
            timeout=90,
        )
        if not r.ok:
            return False, f"n8n fallback HTTP {r.status_code}"
        return True, "n8n webhook accepted"
    except requests.RequestException as e:
        return False, str(e)


def resolve_loa_record(
    business_name: str,
    loa_record_id: Optional[str] = None,
) -> tuple[Optional[dict], list[dict]]:
    if loa_record_id:
        rec = get_loa_record_by_id(loa_record_id)
        return rec, []
    records = get_loa_records_by_business_name(business_name, max_records=10)
    if not records:
        return None, []
    if len(records) == 1:
        return records[0], []
        return None, [loa_record_candidate_summary(r) for r in records]


def create_member_folder(
    *,
    business_name: str,
    trading_as: str,
    classification: str,
    state: str,
    classification_folder_id: Optional[str] = None,
    state_folder_id: Optional[str] = None,
    loa_record_id: Optional[str] = None,
    user_access_token: Optional[str] = None,
) -> dict[str, Any]:
    warnings: list[str] = []
    loa, candidates = resolve_loa_record(business_name, loa_record_id)
    if candidates:
        return {
            "ok": False,
            "error": "Multiple LOA records matched this business name. Select one and retry.",
            "loa_candidates": candidates,
        }
    if not loa:
        warnings.append(
            "No Airtable LOA record matched this business name. Drive folder will still be created."
        )

    drive_result = None
    try:
        drive_result = create_member_drive_folder(
            classification=classification,
            state=state,
            business_name=business_name,
            trading_as=trading_as,
            classification_folder_id=classification_folder_id,
            state_folder_id=state_folder_id,
        )
    except MemberFolderDriveError as e:
        logger.warning("[member_folder] Drive create failed, trying n8n: %s", e.message)
        n8n_ok, n8n_msg = _n8n_member_folder_fallback(
            business_name=business_name,
            trading_as=trading_as,
            classification=classification,
            state=state,
        )
        return {
            "ok": n8n_ok,
            "error": None if n8n_ok else e.message,
            "n8n_fallback_used": True,
            "n8n_fallback_ok": n8n_ok,
            "n8n_fallback_message": n8n_msg,
            "warnings": [e.message],
        }

    loa_file = drive_result.get("loa_file") or {}
    if not loa_file.get("id"):
        warnings.append(
            "LOA was not moved into the member folder. It was either not found in "
            "staging (filenames look like '{business}_LOA_{date}.pdf') or Drive "
            "blocked the move. The folder was still created."
        )
    loa_id = str((loa or {}).get("id") or "").strip()
    airtable: dict[str, Any] = {
        "classification_record_id": None,
        "subfolder_record_id": None,
        "classification_linked": False,
        "subfolder_linked": False,
        "drive_table": False,
        "loa_folder_field": False,
    }

    class_rec = upsert_classification_folder(
        file_path=classification,
        name=classification,
        parent_folder="003-Members-B",
        folder_id=drive_result["classification_folder_id"],
        folder_url=drive_result["classification_folder_url"],
    )
    sub_path = f"{classification}/{state}"
    sub_rec = upsert_classification_subfolder(
        file_path=sub_path,
        name=state,
        parent_folder=classification,
        folder_id=drive_result["state_folder_id"],
        folder_url=drive_result["state_folder_url"],
    )
    airtable["classification_record_id"] = class_rec
    airtable["subfolder_record_id"] = sub_rec
    if loa_id and class_rec:
        airtable["classification_linked"] = _link_loa_to_record(
            CLASSIFICATION_TABLE_ID, class_rec, loa_id
        )
    if loa_id and sub_rec:
        airtable["subfolder_linked"] = _link_loa_to_record(SUBFOLDER_TABLE_ID, sub_rec, loa_id)
    if not class_rec or not sub_rec:
        warnings.append(
            "Airtable classification rows could not be created. "
            "Add 003-Distributors / new A - names to the single-select fields if they are locked."
        )

    airtable["drive_table"] = upsert_drive_sheet_airtable_row(
        business_name=business_name,
        loa_file_id=loa_file.get("id"),
        loa_file_url=loa_file.get("url"),
        folder_id=drive_result["folder_id"],
        folder_url=drive_result["folder_url"],
    )
    if loa_id:
        airtable["loa_folder_field"] = patch_loa_drive_folder(
            loa_id, drive_result["folder_url"]
        )

    wip_file: dict[str, Any] | None = None
    existing_wip_id = read_file_ids_wip_id(business_name)
    if existing_wip_id:
        wip_url = (
            existing_wip_id
            if existing_wip_id.startswith("http")
            else f"https://docs.google.com/spreadsheets/d/{existing_wip_id}"
        )
        wip_file = {"id": existing_wip_id, "url": wip_url, "created": False}
    else:
        try:
            wip_file = create_member_wip(
                business_name=business_name,
                folder_id=drive_result["folder_id"],
                loa_file_id=loa_file.get("id") or "",
                user_access_token=user_access_token or "",
            )
        except MemberFolderDriveError as e:
            warnings.append(e.message)

    file_ids_ok = update_file_ids_row(
        business_name=business_name,
        classification=classification,
        state=state,
        loa_file_id=loa_file.get("id") or "",
        loa_file_url=loa_file.get("url") or "",
        folder_id=drive_result["folder_id"],
        folder_url=drive_result["folder_url"],
        wip_file_id=(wip_file or {}).get("id") or "",
    )
    if not file_ids_ok:
        warnings.append("FILE_IDS sheet (Data from Airtable) was not updated.")
    if wip_file and wip_file.get("business_account_filled") is False:
        warnings.append("WIP was created but Business Account A2:C2 could not be filled.")

    airtable_ok = bool(
        airtable["classification_linked"]
        or airtable["subfolder_linked"]
        or airtable["drive_table"]
        or airtable["loa_folder_field"]
        or not loa_id
    )
    if loa_id and not (
        airtable["classification_linked"] or airtable["subfolder_linked"] or airtable["drive_table"]
    ):
        warnings.append("Airtable writes failed after Drive create; n8n was not called to avoid a duplicate folder.")

    return {
        "ok": True,
        "n8n_fallback_used": False,
        "warnings": warnings,
        "airtable_ok": airtable_ok,
        "file_ids_updated": file_ids_ok,
        "loa_record_id": loa_id or None,
        "airtable": airtable,
        "wip_file": wip_file,
        **drive_result,
    }


def create_distributor_folder(
    *,
    details: dict[str, Any],
    pdf_bytes: bytes,
    pdf_filename: str,
    user_access_token: Optional[str] = None,
) -> dict[str, Any]:
    warnings: list[str] = list(details.get("extraction_warnings") or [])
    folder_name = (details.get("folder_name") or "").strip()
    if not folder_name:
        raise MemberFolderDriveError("Folder Name is required", status_code=400)
    if not folder_name.startswith("A - "):
        folder_name = f"A - {folder_name}"
        details["folder_name"] = folder_name

    # Ensure 003-Distributors exists under Members-B as well as using the known ID.
    from tools.member_folder_drive import find_or_create_folder

    parent_id = (DISTRIBUTORS_FOLDER_ID or "").strip()
    if not parent_id:
        parent_id, _ = find_or_create_folder(MEMBERS_B_FOLDER_ID, "003-Distributors")

    drive_info = create_empty_named_folder(parent_id, folder_name)
    details["drive_folder_id"] = drive_info["folder_id"]
    details["drive_folder_url"] = drive_info["folder_url"]

    safe_name = (pdf_filename or "Distribution Agreement.pdf").strip() or "Distribution Agreement.pdf"
    if not safe_name.lower().endswith(".pdf"):
        safe_name = f"{safe_name}.pdf"
    business = (details.get("distributor_business") or "Distributor").strip()
    upload_name = f"{business} - Distribution Agreement.pdf"
    uploaded: dict[str, str] = {}
    try:
        uploaded = upload_bytes_to_folder(
            pdf_bytes,
            upload_name,
            drive_info["folder_id"],
            user_access_token=user_access_token,
        )
    except MemberFolderDriveError as e:
        warnings.append(e.message)
    details["agreement_file_url"] = uploaded.get("url") or ""

    try:
        sheet_info = append_distributor_row(details, user_access_token=user_access_token)
    except RuntimeError as e:
        warnings.append(str(e))
        sheet_info = {}
    update_distributor_drive_cells(
        folder_name,
        folder_id=drive_info["folder_id"],
        folder_url=drive_info["folder_url"],
        agreement_file_url=details["agreement_file_url"],
        user_access_token=user_access_token,
    )

    class_rec = upsert_classification_folder(
        file_path="003-Distributors",
        name="003-Distributors",
        parent_folder="003-Members-B",
        folder_id=parent_id,
        folder_url=drive_folder_url(parent_id),
    )
    sub_rec = upsert_classification_subfolder(
        file_path=f"003-Distributors/{folder_name}",
        name=folder_name,
        parent_folder="003-Distributors",
        folder_id=drive_info["folder_id"],
        folder_url=drive_info["folder_url"],
    )
    if not class_rec or not sub_rec:
        warnings.append(
            "Airtable classification upsert failed. Unlock the Name / Parent Folder single-selects "
            "or add 003-Distributors and this A - folder by hand."
        )

    return {
        "ok": True,
        "warnings": warnings,
        "sheet": sheet_info,
        "airtable": {
            "classification_record_id": class_rec,
            "subfolder_record_id": sub_rec,
        },
        "agreement_file_id": uploaded.get("id"),
        "agreement_file_url": details["agreement_file_url"],
        "sheet_row": details_to_sheet_row(details),
        **drive_info,
        "details": details,
    }


def extract_distributor_pdf(pdf_bytes: bytes) -> dict[str, Any]:
    return extract_distribution_agreement(pdf_bytes)
