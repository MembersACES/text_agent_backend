"""
Airtable utility invoice rows → activity_record.v1 (B4 boundary).

Pre-Tuesday: stages rows in climate_activity_records only — no Prograde POST.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Optional

ADAPTER_VERSION = "0.1.0"

UTILITY_ACTIVITY_MAP: dict[str, dict[str, Any]] = {
    "C&I Electricity": {
        "activity_type": "electricity_grid",
        "scope": 2,
        "unit": "kWh",
        "quantity_fields": [
            "Activity data kWh",
            "Monthly Consumption",
            "kWh",
            "Total kWh",
            "Usage (kWh)",
            "Usage kWh",
            "Energy (kWh)",
            "Total Usage (kWh)",
            "Consumption (kWh)",
            "General Usage Quantity",
        ],
        "tou_sum_field_groups": [
            ["Peak Usage (kWh)", "Shoulder Usage (kWh)", "Off-Peak Usage (kWh)"],
            ["Peak (kWh)", "Shoulder (kWh)", "Off-Peak (kWh)"],
            [
                "Retail Quantity Peak (kWh)",
                "Retail Quantity Shoulder (kWh)",
                "Retail Quantity Off-Peak (kWh)",
            ],
        ],
    },
    "SME Electricity": {
        "activity_type": "electricity_grid",
        "scope": 2,
        "unit": "kWh",
        "quantity_fields": [
            "Activity data kWh",
            "Monthly Consumption",
            "Total Usage",
            "General Usage Quantity",
            "kWh",
            "Total kWh",
            "Usage (kWh)",
            "Usage kWh",
            "Energy (kWh)",
            "Total Usage (kWh)",
            "Consumption (kWh)",
        ],
        "tou_sum_field_groups": [
            ["Peak Consumption (kWh)", "Shoulder Consumption (kWh)", "Off-Peak Consumption (kWh)"],
            ["Peak Usage (kWh)", "Shoulder Usage (kWh)", "Off-Peak Usage (kWh)"],
            ["Peak (kWh)", "Shoulder (kWh)", "Off-Peak (kWh)"],
        ],
    },
    "C&I Gas": {
        "activity_type": "natural_gas",
        "scope": 1,
        "unit": "GJ",
        "quantity_fields": [
            "Energy Charge Quantity in GJ",
            "Energy Charge Quantity",
            "GJ",
            "Total GJ",
            "Usage (GJ)",
            "Gas Usage (GJ)",
            "Gas Usage",
            "Consumption (GJ)",
            "Quantity",
        ],
        "mj_fields": ["General Usage MJ", "Energy Quantity (MJ)", "Invoice Consumption MJ"],
    },
    "SME Gas": {
        "activity_type": "natural_gas",
        "scope": 1,
        "unit": "GJ",
        "quantity_fields": ["GJ", "Total GJ", "Usage (GJ)", "Total Consumption GJ"],
        "mj_fields": [
            "Total Consumption MJ",
            "Total MJ",
            "Total Usage MJ",
            "Consumption (MJ)",
            "Consumption MJ",
            "General Usage MJ",
            "Energy Quantity (MJ)",
            "Invoice Consumption MJ",
            "Total Consumption (MJ)",
            "Total usage (MJ)",
            "Annual consumption MJ",
        ],
        "mj_sum_field_groups": [
            ["General Usage Quantity", "General Usage Next Quantity"],
            ["General Usage Peak Quantity", "General Usage Next Quantity"],
        ],
    },
    "Waste": {
        "activity_type": "waste_generated_operations",
        "scope": 3,
        "unit": "t",
        # Airtable waste invoices track bin pickups, not mass — tonnes fields are rare.
        "quantity_fields": [
            "Tonnes",
            "Tonnage",
            "Weight (t)",
            "Total (t)",
            "Quantity (t)",
            "Total Weight (t)",
            "Waste Weight (t)",
        ],
    },
    "Oil": {
        "activity_type": "diesel",
        "scope": 1,
        "unit": "L",
        "quantity_fields": [
            "Litres",
            "Liters",
            "Volume (L)",
            "Quantity (L)",
            "Total (L)",
            "Total Quantity",
        ],
        "tou_sum_field_groups": [
            ["Quantity 1", "Quantity 2"],
            ["Quantity 1", "Quantity 2", "Quantity 3"],
        ],
    },
}

PERIOD_DATE_FIELDS = [
    "Invoice Review Period",
    "Review Period",
    "Billing Period",
    "Invoice Date",
    "invoice_date",
    "Invoice Date Formatted",
    "Period Start",
    "Period End",
]

EVIDENCE_URI_FIELDS = [
    "Invoice PDF",
    "Link to Invoice",
    "Invoice Link",
    "PDF Link",
    "Google Drive Link",
    "Drive Link",
]


@dataclass
class EtlContext:
    entity_id: str
    client_id: int
    loa_client_id: Optional[str]
    site_id: str
    utility_type: str
    period_start: date
    period_end: date


@dataclass
class EtlRowResult:
    source_row_id: str
    record_id: str
    body: dict
    skipped: bool
    skip_reason: Optional[str] = None


def default_fy_period(fy_label: str = "FY26") -> tuple[date, date]:
    """Australian FY: FY26 → 2025-07-01 .. 2026-06-30."""
    m = re.match(r"^FY(\d{2})$", (fy_label or "").strip().upper())
    if not m:
        return date(2025, 7, 1), date(2026, 6, 30)
    yy = int(m.group(1))
    start_year = 2000 + yy - 1
    return date(start_year, 7, 1), date(start_year + 1, 6, 30)


def _parse_number(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    s = str(raw).strip()
    if not s:
        return None
    s = s.replace(",", "")
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _normalize_field_key(key: str) -> str:
    return re.sub(r"\s+", " ", str(key or "").strip().lower())


def _first_field(row: dict, names: list[str]) -> Any:
    if not row:
        return None
    norm_to_key = {_normalize_field_key(k): k for k in row.keys()}
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
        actual = norm_to_key.get(_normalize_field_key(name))
        if actual is not None and row.get(actual) not in (None, ""):
            return row[actual]
    return None


def _sum_fields(row: dict, names: list[str]) -> Optional[float]:
    total = 0.0
    found = False
    for name in names:
        val = _parse_number(_first_field(row, [name]))
        if val is not None and val > 0:
            total += val
            found = True
    return total if found else None


def _resolve_quantity(row: dict, cfg: dict) -> Optional[float]:
    direct = _parse_number(_first_field(row, cfg.get("quantity_fields", [])))
    if direct is not None and direct > 0:
        return direct

    for group in cfg.get("tou_sum_field_groups") or []:
        summed = _sum_fields(row, group)
        if summed is not None and summed > 0:
            return summed

    for group in cfg.get("mj_sum_field_groups") or []:
        summed_mj = _sum_fields(row, group)
        if summed_mj is not None and summed_mj > 0:
            return summed_mj / 1000.0

    for mj_field in cfg.get("mj_fields") or []:
        mj = _parse_number(_first_field(row, [mj_field]))
        if mj is not None and mj > 0:
            return mj / 1000.0

    return None


def _parse_iso_date(raw: Any) -> Optional[date]:
    if raw is None:
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    s = str(raw).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def _row_period(row: dict, fallback_start: date, fallback_end: date) -> tuple[date, date]:
    start_raw = _first_field(row, ["Period Start", "Billing Period Start", "Invoice Review Period Start"])
    end_raw = _first_field(row, ["Period End", "Billing Period End", "Invoice Review Period End"])
    start = _parse_iso_date(start_raw) or fallback_start
    end = _parse_iso_date(end_raw) or fallback_end
    if end < start:
        start, end = end, start
    return start, end


def _evidence_uri(row: dict) -> Optional[str]:
    val = _first_field(row, EVIDENCE_URI_FIELDS)
    if isinstance(val, list) and val:
        val = val[0]
    if isinstance(val, dict) and val.get("url"):
        return str(val["url"])
    if val:
        return str(val).strip()
    return None


def _make_record_id(entity_id: str, activity_type: str, source_row_id: str) -> str:
    digest = hashlib.sha256(f"{entity_id}:{activity_type}:{source_row_id}".encode()).hexdigest()[:12]
    return f"act_{digest}"


def invoice_row_to_activity_record(row: dict, ctx: EtlContext) -> EtlRowResult:
    cfg = UTILITY_ACTIVITY_MAP.get(ctx.utility_type)
    source_row_id = str(row.get("record_id") or "").strip()
    if not cfg:
        return EtlRowResult(source_row_id, "", {}, True, f"unsupported utility_type: {ctx.utility_type}")
    if not source_row_id:
        return EtlRowResult("", "", {}, True, "missing airtable record_id")

    quantity = _resolve_quantity(row, cfg)
    if quantity is None or quantity <= 0:
        return EtlRowResult(source_row_id, "", {}, True, "missing or zero quantity")

    period_start, period_end = _row_period(row, ctx.period_start, ctx.period_end)
    activity_type = cfg["activity_type"]
    record_id = _make_record_id(ctx.entity_id, activity_type, source_row_id)
    evidence_uri = _evidence_uri(row)

    body: dict[str, Any] = {
        "schema_version": "1.0",
        "record_id": record_id,
        "entity_id": ctx.entity_id,
        "site_id": ctx.site_id or None,
        "client_id": ctx.loa_client_id,
        "reporting_period": {
            "start": period_start.isoformat(),
            "end": period_end.isoformat(),
        },
        "activity_type": activity_type,
        "scope": cfg["scope"],
        "scope_3_category": 5 if activity_type == "waste_generated_operations" else None,
        "quantity": quantity,
        "unit": cfg["unit"],
        "evidence_refs": [],
        "ingest_source": {
            "system": "aces_b4",
            "adapter_version": ADAPTER_VERSION,
            "n8n_webhook_id": None,
            "ocr_run_id": None,
        },
        "ingest_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "data_quality": {
            "estimation_method": "calculated_invoice",
            "uncertainty_pct": 8,
            "completeness_pct": 85,
            "flags": ["etl_from_airtable", f"utility:{ctx.utility_type}"],
            "quantity_source": "airtable_invoice_etl",
        },
        "notes": f"ETL from Airtable invoice row {source_row_id}",
    }

    if evidence_uri:
        body["evidence_refs"].append(
            {
                "evidence_id": f"ev_{source_row_id[-8:]}",
                "evidence_hash": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                "evidence_kind": "utility_bill",
                "evidence_uri": evidence_uri,
            }
        )

    return EtlRowResult(source_row_id, record_id, body, False)


def transform_invoice_rows(
    rows: list[dict],
    ctx: EtlContext,
) -> tuple[list[EtlRowResult], dict]:
    results: list[EtlRowResult] = []
    skipped = 0
    for row in rows:
        if not isinstance(row, dict):
            skipped += 1
            continue
        res = invoice_row_to_activity_record(row, ctx)
        results.append(res)
        if res.skipped:
            skipped += 1
    diagnostics = {
        "input_rows": len(rows),
        "produced": sum(1 for r in results if not r.skipped),
        "skipped": skipped,
        "utility_type": ctx.utility_type,
        "entity_id": ctx.entity_id,
        "site_id": ctx.site_id,
    }
    return results, diagnostics
