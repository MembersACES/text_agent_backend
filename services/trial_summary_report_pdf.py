"""
Report-style PDF (ReportLab Platypus) from trial_summary_report JSON payload.
"""

from __future__ import annotations

import json
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple
from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def _f(x: Any, digits: int = 2) -> str:
    try:
        return f"{float(x or 0):,.{digits}f}"
    except (TypeError, ValueError):
        return "0.00"


def _i(x: Any) -> int:
    try:
        return int(x or 0)
    except (TypeError, ValueError):
        return 0


def _P(text: str, style: ParagraphStyle) -> Paragraph:
    return Paragraph(escape(str(text)).replace("\n", " "), style)


def _combined_from_robots(robots: Dict[str, Any]) -> Tuple[float, float, int, float, float]:
    a = h = p = w = 0.0
    t = 0
    for r in robots.values():
        if not isinstance(r, dict):
            continue
        a += float(r.get("area_m2") or 0)
        h += float(r.get("runtime_h") or 0)
        t += _i(r.get("tasks"))
        p += float(r.get("power_kwh") or 0)
        w += float(r.get("water_l") or 0)
    return a, h, t, p, w


def _consumable_hours_status(runtime_h: float, hours_limit: Any) -> Tuple[str, str]:
    try:
        limit = float(hours_limit)
    except (TypeError, ValueError):
        return "N/A", "No hour target set"
    if limit <= 0:
        return "N/A", "No hour target set"
    remaining = limit - float(runtime_h or 0)
    if remaining <= 0:
        return "Due now", f"Exceeded by {_f(abs(remaining), 1)} h"
    if remaining <= max(25.0, limit * 0.1):
        return "Due soon", f"{_f(remaining, 1)} h remaining"
    return "OK", f"{_f(remaining, 1)} h remaining"


def _consumable_date_status(last_replaced_at: Any, interval_days: Any) -> Tuple[str, str]:
    try:
        interval = int(interval_days)
    except (TypeError, ValueError):
        return "N/A", "No interval set"
    if interval <= 0:
        return "N/A", "No interval set"
    if not last_replaced_at:
        return "Due soon", "Last replaced date missing"
    try:
        replaced = date.fromisoformat(str(last_replaced_at))
    except ValueError:
        return "N/A", "Invalid replacement date"
    due = replaced + timedelta(days=interval)
    today = date.today()
    delta = (due - today).days
    if delta < 0:
        return "Due now", f"Overdue by {abs(delta)} d"
    if delta <= max(7, interval // 10):
        return "Due soon", f"{delta} d remaining"
    return "OK", f"{delta} d remaining"


def _footer_canvas(canvas, _doc, subtitle: str) -> None:
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#64748b"))
    w, _h = A4
    canvas.drawString(22 * mm, 12 * mm, subtitle)
    canvas.drawRightString(w - 22 * mm, 12 * mm, f"Page {canvas.getPageNumber()}")
    canvas.restoreState()


def render_report_pdf(output_path: str, payload: dict) -> None:
    shop_name = str(payload.get("shop_name") or "Site")
    shop_id = str(payload.get("shop_id") or "")
    start_date = str(payload.get("start_date") or "")
    end_date = str(payload.get("end_date") or "")
    generated_at = str(payload.get("generated_at") or "")
    labour_rate = payload.get("labour_rate")
    robots: Dict[str, Any] = dict(payload.get("robots") or {})
    weekly_trend: List[dict] = list(payload.get("weekly_trend") or [])
    sample_week: dict = dict(payload.get("sample_week") or {})
    consumables_by_sn: Dict[str, List[dict]] = dict(payload.get("consumables_by_sn") or {})
    consumables_runtime_h_by_sn: Dict[str, Any] = dict(payload.get("consumables_runtime_h_by_sn") or {})
    consumables_baseline_start_date_by_sn: Dict[str, Any] = dict(payload.get("consumables_baseline_start_date_by_sn") or {})

    styles = getSampleStyleSheet()
    title = ParagraphStyle(
        name="RepTitle",
        parent=styles["Title"],
        fontSize=17,
        leading=21,
        spaceAfter=10,
        textColor=colors.HexColor("#0f172a"),
        alignment=TA_LEFT,
    )
    h2 = ParagraphStyle(
        name="RepH2",
        parent=styles["Heading2"],
        fontSize=12,
        leading=15,
        spaceBefore=12,
        spaceAfter=8,
        textColor=colors.HexColor("#1e3a8a"),
        fontName="Helvetica-Bold",
    )
    h3 = ParagraphStyle(
        name="RepH3",
        parent=styles["Heading3"],
        fontSize=10.5,
        leading=13,
        spaceBefore=10,
        spaceAfter=6,
        fontName="Helvetica-Bold",
    )
    body = ParagraphStyle(
        name="RepBody",
        parent=styles["Normal"],
        fontSize=10,
        leading=13,
        alignment=TA_LEFT,
    )
    meta = ParagraphStyle(
        name="RepMeta",
        parent=body,
        fontSize=9,
        textColor=colors.HexColor("#475569"),
        leading=12,
    )
    th = ParagraphStyle(name="RepTh", parent=body, fontName="Helvetica-Bold", fontSize=8.5, leading=10)
    td = ParagraphStyle(name="RepTd", parent=body, fontSize=8.5, leading=10)
    td_num = ParagraphStyle(name="RepTdNum", parent=td, alignment=TA_RIGHT)
    td_sn = ParagraphStyle(name="RepTdSn", parent=td, fontName="Helvetica", fontSize=7.5)

    story: List[Any] = []
    story.append(_P(f"Trial performance summary - {shop_name}", title))
    story.append(Spacer(1, 2 * mm))
    story.append(
        _P(
            f"Shop ID: {shop_id}  -  Reporting period: {start_date} to {end_date}  -  Generated: {generated_at}",
            meta,
        )
    )
    story.append(Spacer(1, 6 * mm))

    c_area, c_h, c_tasks, c_pow, c_wat = _combined_from_robots(robots)

    story.append(_P("Cumulative trial totals", h2))
    hdr = [
        _P("Robot", th),
        _P("Serial", th),
        _P("Area (m^2)", th),
        _P("Runtime (h)", th),
        _P("Tasks", th),
        _P("Power (kWh)", th),
        _P("Water (L)", th),
    ]
    rows: List[List[Paragraph]] = [hdr]
    for sn, r in robots.items():
        if not isinstance(r, dict):
            continue
        lab = str(r.get("label") or r.get("model") or sn)
        rows.append(
            [
                _P(lab[:48], td),
                _P(str(r.get("sn") or sn), td_sn),
                _P(_f(r.get("area_m2")), td_num),
                _P(_f(r.get("runtime_h")), td_num),
                _P(f"{_i(r.get('tasks')):,}", td_num),
                _P(_f(r.get("power_kwh")), td_num),
                _P(_f(r.get("water_l")), td_num),
            ]
        )
    rows.append(
        [
            _P("Combined", th),
            _P("", td),
            _P(_f(c_area), td_num),
            _P(_f(c_h), td_num),
            _P(f"{c_tasks:,}", td_num),
            _P(_f(c_pow), td_num),
            _P(_f(c_wat), td_num),
        ]
    )
    tw = [44 * mm, 30 * mm, 22 * mm, 22 * mm, 18 * mm, 20 * mm, 18 * mm]
    t = Table(rows, colWidths=tw, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                ("BACKGROUND", (0, -1), (-1, -1), colors.HexColor("#f1f5f9")),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 4 * mm))

    story.append(_P("Performance notes", h2))
    ei = (c_pow / c_area * 1000.0) if c_area else 0.0
    wi = (c_wat / c_area * 1000.0) if c_area else 0.0
    notes = [
        f"Combined output over the period: {_f(c_area)} m^2 cleaned, {_f(c_h)} runtime hours.",
        f"Energy intensity: {_f(ei, 3)} kWh per 1,000 m^2.",
        f"Water intensity: {_f(wi, 3)} L per 1,000 m^2.",
    ]
    if labour_rate is not None and float(labour_rate) > 0:
        lr = float(labour_rate)
        notes.append(
            f"Indicative redeployment value at ${lr:,.2f}/h: about ${c_h * lr:,.2f} over the reporting period."
        )
    notes.append(
        "Ops framing: robot runtime can support shifting staff effort toward detail cleaning "
        "(edges, high-touch surfaces, periodic deep-clean tasks)."
    )
    for n in notes:
        story.append(_P(f"- {n}", body))
        story.append(Spacer(1, 1.5 * mm))

    story.append(_P("Recent week — site totals (analytics)", h2))
    sw_s = sample_week.get("start_date", "")
    sw_e = sample_week.get("end_date", "")
    story.append(_P(f"Window: {sw_s} to {sw_e}", meta))
    story.append(Spacer(1, 2 * mm))
    ar = list(sample_week.get("analytics_rows") or [])
    if ar:
        sh = [
            _P("Robot", th),
            _P("Area (m^2)", th),
            _P("Runtime (h)", th),
            _P("Tasks", th),
            _P("Power (kWh)", th),
            _P("Water (L)", th),
        ]
        sr = [sh]
        for row in ar:
            sr.append(
                [
                    _P(str(row.get("label") or row.get("sn") or ""), td),
                    _P(_f(row.get("area_m2")), td_num),
                    _P(_f(row.get("runtime_h")), td_num),
                    _P(f"{_i(row.get('tasks')):,}", td_num),
                    _P(_f(row.get("power_kwh")), td_num),
                    _P(_f(row.get("water_l")), td_num),
                ]
            )
        tw2 = [58 * mm, 24 * mm, 24 * mm, 20 * mm, 22 * mm, 20 * mm]
        t2 = Table(sr, colWidths=tw2, repeatRows=1)
        t2.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                    ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ]
            )
        )
        story.append(t2)
    else:
        story.append(_P("No sample-week analytics rows.", meta))

    summaries = sample_week.get("task_summaries") or {}
    robot_labels: Dict[str, str] = {}
    for rsn, rd in robots.items():
        if isinstance(rd, dict):
            robot_labels[str(rsn)] = str(rd.get("label") or rd.get("model") or rsn)
        else:
            robot_labels[str(rsn)] = str(rsn)

    for sn, details in summaries.items():
        if not isinstance(details, dict):
            continue
        lab = robot_labels.get(sn, sn)
        story.append(Spacer(1, 4 * mm))
        story.append(_P(f"Tasks by name — {lab} ({sn})", h3))
        story.append(
            _P(
                f"Execution rows in this 7-day window: {_i(details.get('rows'))}. "
                "All distinct task names from Pudu log/clean_task/query_list (same source as the dashboard executions table).",
                meta,
            )
        )
        sc = details.get("status_counts") or {}
        if sc:
            parts = ", ".join(f"{k}: {v}" for k, v in sorted(sc.items(), key=lambda kv: str(kv[0])))
            story.append(_P(f"Status mix: {parts}", body))
            story.append(Spacer(1, 2 * mm))
        top_area: List[Tuple[Any, Any]] = list(details.get("top_tasks_by_area") or [])
        runs_list = details.get("top_tasks_by_runs") or []
        runs_lookup = dict(runs_list) if isinstance(runs_list, list) else {}
        if top_area:
            zh = [_P("Task name", th), _P("Area (m^2)", th), _P("Runs", th)]
            zr = [zh]
            for task_name, area in top_area:
                tn = str(task_name)[:70]
                zr.append(
                    [
                        _P(tn, td),
                        _P(_f(area), td_num),
                        _P(str(runs_lookup.get(task_name, 0)), td_num),
                    ]
                )
            tw3 = [92 * mm, 28 * mm, 18 * mm]
            tz = Table(zr, colWidths=tw3, repeatRows=1)
            tz.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e1")),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ]
                )
            )
            story.append(tz)
        else:
            story.append(_P("No task-level breakdown rows.", meta))

    if consumables_by_sn:
        story.append(Spacer(1, 6 * mm))
        story.append(_P("Consumables tracking", h2))
        story.append(
            _P(
                "Per robot consumables list configured in the dashboard. Status is estimated from lifetime runtime "
                "since each robot baseline date, plus replacement date/interval where provided.",
                meta,
            )
        )
        for sn, items in consumables_by_sn.items():
            robot_rec = robots.get(sn) if isinstance(robots.get(sn), dict) else {}
            runtime_h = float(consumables_runtime_h_by_sn.get(sn) or (robot_rec or {}).get("runtime_h") or 0.0)
            baseline_date = str(consumables_baseline_start_date_by_sn.get(sn) or "").strip()
            label = str((robot_rec or {}).get("label") or (robot_rec or {}).get("model") or sn)
            story.append(Spacer(1, 3 * mm))
            if baseline_date:
                story.append(_P(f"{label} ({sn}) - lifetime runtime {_f(runtime_h)} h since {baseline_date}", h3))
            else:
                story.append(_P(f"{label} ({sn}) - runtime {_f(runtime_h)} h", h3))
            if not isinstance(items, list) or not items:
                story.append(_P("No consumables configured for this robot.", meta))
                continue
            ch = [_P("Mode", th), _P("Item", th), _P("Qty", th), _P("Lifespan", th), _P("Hours", th), _P("Status", th), _P("Notes", th)]
            cr: List[List[Paragraph]] = [ch]
            for item in items:
                if not isinstance(item, dict):
                    continue
                status_hours, status_note_hours = _consumable_hours_status(runtime_h, item.get("hours"))
                status_date, status_note_date = _consumable_date_status(
                    item.get("last_replaced_at"),
                    item.get("replacement_interval_days"),
                )
                if status_date == "Due now" or status_hours == "Due now":
                    status = "Due now"
                elif status_date == "Due soon" or status_hours == "Due soon":
                    status = "Due soon"
                elif status_date == "OK" or status_hours == "OK":
                    status = "OK"
                else:
                    status = "N/A"
                qty_raw = item.get("quantity")
                qty_txt = ""
                if qty_raw is not None:
                    try:
                        qf = float(qty_raw)
                        qty_txt = str(int(qf)) if qf.is_integer() else _f(qf, 2)
                    except (TypeError, ValueError):
                        qty_txt = str(qty_raw)
                unit_txt = str(item.get("unit") or "").strip()
                qty_cell = f"{qty_txt} {unit_txt}".strip() or "-"
                life_txt = str(item.get("lifespan_per_unit") or "").strip() or "-"
                hours_val = item.get("hours")
                hours_txt = "-"
                if hours_val is not None:
                    try:
                        hours_txt = _f(float(hours_val), 1)
                    except (TypeError, ValueError):
                        hours_txt = str(hours_val)
                replacement_date = str(item.get("last_replaced_at") or "").strip() or "-"
                interval_val = item.get("replacement_interval_days")
                interval_txt = str(interval_val) if interval_val is not None else "-"
                notes_txt = str(item.get("notes") or "").strip()
                status_notes: List[str] = []
                if status_note_hours and status_note_hours != "No hour target set":
                    status_notes.append(status_note_hours)
                if status_note_date and status_note_date != "No interval set":
                    status_notes.append(status_note_date)
                if status_notes:
                    notes_txt = f"{' | '.join(status_notes)}. {notes_txt}".strip()
                cr.append(
                    [
                        _P(str(item.get("mode_name") or "-")[:30], td),
                        _P(str(item.get("name") or "-")[:42], td),
                        _P(qty_cell[:16], td),
                        _P(f"{life_txt[:24]} / rep: {replacement_date} / every {interval_txt}d", td),
                        _P(hours_txt, td_num),
                        _P(status, td),
                        _P((notes_txt or "-")[:64], td),
                    ]
                )
            tc = Table(cr, colWidths=[28 * mm, 42 * mm, 16 * mm, 22 * mm, 14 * mm, 16 * mm, 38 * mm], repeatRows=1)
            tc.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e1")),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                        ("TOPPADDING", (0, 0), (-1, -1), 3),
                        ("FONTSIZE", (0, 1), (-1, -1), 7.5),
                    ]
                )
            )
            story.append(tc)

    story.append(Spacer(1, 6 * mm))
    story.append(_P("Weekly trend (combined robots)", h2))
    if weekly_trend:
        wh = [
            _P("Week start", th),
            _P("Week end", th),
            _P("Area (m^2)", th),
            _P("Runtime (h)", th),
            _P("Tasks", th),
            _P("Power (kWh)", th),
            _P("Water (L)", th),
        ]
        wr = [wh]
        for w in weekly_trend:
            wr.append(
                [
                    _P(str(w.get("start_date", "")), td),
                    _P(str(w.get("end_date", "")), td),
                    _P(_f(w.get("area_m2")), td_num),
                    _P(_f(w.get("runtime_h")), td_num),
                    _P(f"{_i(w.get('tasks')):,}", td_num),
                    _P(_f(w.get("power_kwh")), td_num),
                    _P(_f(w.get("water_l")), td_num),
                ]
            )
        tw4 = [22 * mm, 22 * mm, 24 * mm, 22 * mm, 18 * mm, 20 * mm, 20 * mm]
        t4 = Table(wr, colWidths=tw4, repeatRows=1)
        t4.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e2e8f0")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#cbd5e1")),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                    ("FONTSIZE", (0, 1), (-1, -1), 7.5),
                ]
            )
        )
        story.append(t4)
    else:
        story.append(_P("No weekly buckets.", meta))

    story.append(Spacer(1, 8 * mm))
    story.append(
        _P(
            "Data sources: Pudu Open Platform — analysis/clean/paging (cumulative and weekly site totals per robot) "
            "and log/clean_task/query_list (full task-name breakdown and status mix for the recent 7-day window, per robot).",
            meta,
        )
    )

    doc = SimpleDocTemplate(
        output_path,
        pagesize=A4,
        leftMargin=20 * mm,
        rightMargin=20 * mm,
        topMargin=16 * mm,
        bottomMargin=18 * mm,
        title=f"Trial summary - {shop_name}",
        author="Pudu analytics export",
    )
    foot = f"Pudu trial summary - {shop_name}"
    doc.build(
        story,
        onFirstPage=lambda c, d: _footer_canvas(c, d, foot),
        onLaterPages=lambda c, d: _footer_canvas(c, d, foot),
    )


def render_report_pdf_bytes(payload: dict) -> bytes:
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "trial_summary_report.pdf"
        render_report_pdf(str(path), payload)
        return path.read_bytes()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Report-style PDF from trial summary JSON.")
    parser.add_argument("--json", "-j", required=True, help="JSON from trial_summary_report.py --output-json")
    parser.add_argument("--output", "-o", default="trial_summary_report.pdf", help="Output PDF path")
    args = parser.parse_args()
    payload = json.loads(Path(args.json).read_text(encoding="utf-8"))
    render_report_pdf(args.output, payload)
    print(f"Saved report PDF: {args.output}")


if __name__ == "__main__":
    main()
