"""Enrich contract-ending rows with member, contact, state, and portal fields."""
from __future__ import annotations

import re
from typing import Any, Iterable, Optional
from urllib.parse import quote

from services.unsigned_pipeline import guess_state

_NMI_LETTER_STATE = {
    "N": "NSW",
    "V": "VIC",
    "Q": "QLD",
    "S": "SA",
    "W": "WA",
    "T": "TAS",
    "A": "ACT",
}
_NMI_DIGIT_STATE = {
    "2": "NSW",
    "3": "QLD",
    "4": "NSW",
    "5": "SA",
    "6": "VIC",
    "7": "TAS",
    "8": "WA",
}
_MIRN_PREFIX_STATE = {
    "52": "NSW",
    "53": "VIC",
    "54": "QLD",
    "55": "SA",
    "56": "WA",
    "57": "TAS",
    "58": "NT",
}


def _norm_name(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def classify_phone(phone: Optional[str]) -> str:
    """Return mobile, landline, unknown, or empty when no number is present."""
    digits = re.sub(r"\D", "", phone or "")
    if not digits:
        return ""
    if digits.startswith("61") and len(digits) >= 11:
        digits = digits[2:]
    if not digits.startswith("0") and len(digits) == 9:
        digits = "0" + digits
    if digits.startswith("04"):
        return "mobile"
    if digits.startswith(("02", "03", "07", "08")):
        return "landline"
    return "unknown"


def state_from_identifier(identifier: Optional[str], utility_type: Optional[str] = None) -> str:
    raw = re.sub(r"[\s,]", "", identifier or "").upper()
    if not raw:
        return ""
    kind = (utility_type or "").strip().lower()
    if "gas" in kind:
        return _MIRN_PREFIX_STATE.get(raw[:2], "")
    first = raw[0]
    if first.isalpha():
        return _NMI_LETTER_STATE.get(first, "")
    return _NMI_DIGIT_STATE.get(first, "")


def resolve_state(
    *,
    explicit_state: Optional[str] = None,
    site_address: Optional[str] = None,
    identifier: Optional[str] = None,
    utility_type: Optional[str] = None,
) -> str:
    guessed = guess_state(site_address or "", explicit_state)
    if guessed and guessed != "Unknown":
        return guessed
    return state_from_identifier(identifier, utility_type)


def _unique_map(pairs: Iterable[tuple[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[Any]] = {}
    for key, value in pairs:
        if not key:
            continue
        bucket = grouped.setdefault(key, [])
        if value not in bucket:
            bucket.append(value)
    return {key: values[0] for key, values in grouped.items() if len(values) == 1}


def index_loa_records(loa_records: list[dict]) -> tuple[dict[str, dict], dict[str, dict]]:
    by_id: dict[str, dict] = {}
    name_pairs: list[tuple[str, dict]] = []
    for loa in loa_records:
        rid = str(loa.get("record_id") or "").strip()
        if rid:
            by_id[rid] = loa
        for name in (loa.get("business_name"), loa.get("trading_as")):
            key = _norm_name(name if isinstance(name, str) else "")
            if key:
                name_pairs.append((key, loa))
    return by_id, _unique_map(name_pairs)


def index_clients(clients: list[Any]) -> tuple[dict[str, int], dict[str, int]]:
    ext_pairs: list[tuple[str, int]] = []
    name_pairs: list[tuple[str, int]] = []
    for client in clients:
        if isinstance(client, dict):
            cid = int(client.get("id") or 0)
            ext_id = str(client.get("external_business_id") or "").strip()
            name = _norm_name(str(client.get("business_name") or ""))
        else:
            mapping = getattr(client, "_mapping", None)
            if mapping is not None:
                cid = int(mapping.get("id") or 0)
                ext_id = str(mapping.get("external_business_id") or "").strip()
                name = _norm_name(str(mapping.get("business_name") or ""))
            else:
                cid = int(getattr(client, "id", 0) or 0)
                ext_id = str(getattr(client, "external_business_id", None) or "").strip()
                name = _norm_name(str(getattr(client, "business_name", None) or ""))
        if not cid:
            continue
        if ext_id:
            ext_pairs.append((ext_id, cid))
        if name:
            name_pairs.append((name, cid))
    return _unique_map(ext_pairs), _unique_map(name_pairs)


def _portal_path(client_id: Optional[int], business_name: str) -> str:
    if client_id:
        return f"/crm-members/{client_id}"
    name = (business_name or "").strip()
    if name:
        return f"/crm-members?businessName={quote(name)}"
    return ""


def enrich_contract_item(
    rec: dict,
    *,
    loa_by_id: dict[str, dict],
    loa_by_name: dict[str, dict],
    client_by_ext_id: dict[str, int],
    client_by_name: dict[str, int],
) -> dict:
    loa_id = str(rec.get("loa_record_id") or "").strip()
    business_name = str(rec.get("business_name") or "").strip()
    loa = loa_by_id.get(loa_id)
    if loa is None:
        loa = loa_by_name.get(_norm_name(business_name))
    if loa is None:
        loa = {}

    if not business_name:
        business_name = str(loa.get("business_name") or loa.get("trading_as") or "").strip()
    if not loa_id:
        loa_id = str(loa.get("record_id") or "").strip()

    contact_name = str(loa.get("contact_name") or "").strip()
    email = str(loa.get("email") or "").strip()
    telephone = str(loa.get("telephone") or "").strip()
    site_address = str(rec.get("site_address") or loa.get("site_address") or loa.get("postal_address") or "").strip()
    state = resolve_state(
        explicit_state=str(rec.get("state") or loa.get("state") or ""),
        site_address=site_address,
        identifier=str(rec.get("identifier") or ""),
        utility_type=str(rec.get("utility_type") or ""),
    )

    client_id = client_by_ext_id.get(loa_id)
    if client_id is None:
        client_id = client_by_name.get(_norm_name(business_name))

    return {
        "identifier": rec.get("identifier") or "",
        "utility_type": rec.get("utility_type") or "",
        "contract_end_date": rec.get("contract_end_date"),
        "retailer": rec.get("retailer") or "",
        "record_id": rec.get("record_id") or "",
        "business_name": business_name,
        "state": state,
        "contact_name": contact_name,
        "email": email,
        "telephone": telephone,
        "phone_type": classify_phone(telephone),
        "client_id": client_id,
        "portal_path": _portal_path(client_id, business_name),
        "loa_record_id": loa_id,
    }


def enrich_contract_records(
    records: list[dict],
    loa_records: list[dict],
    clients: list[Any],
) -> list[dict]:
    loa_by_id, loa_by_name = index_loa_records(loa_records)
    client_by_ext_id, client_by_name = index_clients(clients)
    return [
        enrich_contract_item(
            rec,
            loa_by_id=loa_by_id,
            loa_by_name=loa_by_name,
            client_by_ext_id=client_by_ext_id,
            client_by_name=client_by_name,
        )
        for rec in records
    ]
