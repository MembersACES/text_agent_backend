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
            "kWh",
            "Total kWh",
            "Usage (kWh)",
            "Usage kWh",
            "Energy (kWh)",
            "Total Usage (kWh)",
            "Consumption (kWh)",
        ],
    },
    "SME Electricity": {
        "activity_type": "electricity_grid",
        "scope": 2,
        "unit": "kWh",
        "quantity_fields": [
            "kWh",
            "Total kWh",
            "Usage (kWh)",
            "Usage kWh",
            "Energy (kWh)",
        ],
    },
    "C&I Gas": {
        "activity_type": "natural_gas",
        "scope": 1,
        "unit": "GJ",
        "quantity_fields": ["GJ", "Total GJ", "Usage (GJ)", "Gas Usage (GJ)", "Consumption (GJ)"],
    },
    "SME Gas": {
        "activity_type": "natural_gas",
        "scope": 1,
        "unit": "GJ",
        "quantity_fields": ["GJ", "Total GJ", "Usage (GJ)"],
    },
    "Waste": {
        "activity_type": "waste_generated_operations",
        "scope": 3,
        "unit": "t",
        "quantity_fields": ["Tonnes", "Tonnage", "Weight (t)", "Total (t)", "Quantity (t)"],
    },
    "Oil": {
        "activity_type": "diesel",
        "scope": 1,
        "unit": "L",
        "quantity_fields": ["Litres", "Liters", "Volume (L)", "Quantity (L)", "Total (L)"],
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


def _first_field(row: dict, names: list[str]) -> Any:
    for name in names:
        if name in row and row[name] not in (None, ""):
            return row[name]
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

    quantity = _parse_number(_first_field(row, cfg["quantity_fields"]))
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
