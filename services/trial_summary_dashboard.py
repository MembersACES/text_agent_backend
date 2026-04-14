"""
Dashboard integration: build trial / performance reports from UI selections.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Union

from services.trial_summary_report import DEFAULT_START_DATE, run_trial_summary_report
from services.trial_summary_report_pdf import render_report_pdf_bytes


RobotInput = Union[str, Dict[str, Any]]


@dataclass
class DashboardTrialReportResult:
    ok: bool
    error: Optional[str] = None
    markdown: str = ""
    json_text: str = ""
    payload: Optional[Dict[str, Any]] = None
    report_pdf_bytes: Optional[bytes] = None


def normalize_robots_for_report(robots: Sequence[RobotInput]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for item in robots:
        if isinstance(item, str):
            sn = item.strip()
            if sn:
                out.append({"sn": sn, "model": sn, "label": sn})
            continue
        if not isinstance(item, dict):
            continue
        sn = str(
            item.get("sn")
            or item.get("SN")
            or item.get("serial")
            or item.get("robot_sn")
            or ""
        ).strip()
        if not sn:
            continue
        lab = item.get("label") or item.get("name") or item.get("robot_name") or item.get("robotName")
        label = str(lab).strip() if lab is not None else ""
        if not label:
            label = sn
        out.append({"sn": sn, "model": label, "label": label})
    return out


def _resolve_credentials(app_key: Optional[str], app_secret: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    key = (app_key or os.getenv("PUDU_APP_KEY", "") or os.getenv("PUDU_API_KEY", "")).strip()
    secret = (app_secret or os.getenv("PUDU_SECRET_KEY", "") or os.getenv("PUDU_API_SECRET", "")).strip()
    return key or None, secret or None


def generate_dashboard_trial_report(
    shop_id: str,
    shop_name: str,
    robots: Sequence[RobotInput],
    *,
    start_date: Optional[str] = None,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    labour_rate: Optional[float] = None,
) -> DashboardTrialReportResult:
    sid = str(shop_id).strip()
    sname = str(shop_name).strip() or f"Shop {sid}"
    report_start = str(start_date).strip() if start_date else DEFAULT_START_DATE
    if not sid:
        return DashboardTrialReportResult(ok=False, error="shop_id is required")

    robot_specs = normalize_robots_for_report(robots)
    if not robot_specs:
        return DashboardTrialReportResult(ok=False, error="At least one robot serial is required")

    key, secret = _resolve_credentials(app_key, app_secret)
    if not key or not secret:
        return DashboardTrialReportResult(
            ok=False,
            error="Missing Pudu credentials (set PUDU_APP_KEY and PUDU_SECRET_KEY).",
        )

    try:
        bundle = run_trial_summary_report(
            app_key=key,
            app_secret=secret,
            shop_id=sid,
            shop_display_name=sname,
            robot_specs=robot_specs,
            start_date=report_start,
            labour_rate=float(labour_rate) if labour_rate is not None and float(labour_rate) > 0 else None,
        )
    except ValueError as e:
        return DashboardTrialReportResult(ok=False, error=str(e))
    except Exception as e:
        return DashboardTrialReportResult(ok=False, error=f"Report generation failed: {e!s}")

    payload = bundle.payload
    md = bundle.markdown
    json_text = json.dumps(payload, indent=2)

    try:
        report_pdf = render_report_pdf_bytes(payload)
    except Exception as e:
        return DashboardTrialReportResult(
            ok=False,
            error=f"Report PDF build failed: {e!s}",
            markdown=md,
            json_text=json_text,
            payload=payload,
        )

    return DashboardTrialReportResult(
        ok=True,
        markdown=md,
        json_text=json_text,
        payload=payload,
        report_pdf_bytes=report_pdf,
    )
