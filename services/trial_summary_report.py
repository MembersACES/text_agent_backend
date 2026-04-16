#!/usr/bin/env python3
"""
Trial / multi-robot performance summary.

Primary API:
- run_trial_summary_report(...)
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from services.pudu_signed_request import pudu_message_ok, signed_pudu_get


MELBOURNE_TZ = ZoneInfo("Australia/Melbourne")
DEFAULT_START_DATE = "2026-01-20"


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _task_sn(task: dict) -> Optional[str]:
    for key in ("sn", "SN", "robot_sn", "device_sn", "serial_num", "serial_number"):
        value = task.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def melbourne_offset_for_period(start_ts: int, end_ts: int) -> int:
    mid_ts = (int(start_ts) + int(end_ts)) // 2
    dt = datetime.fromtimestamp(mid_ts, tz=timezone.utc).astimezone(MELBOURNE_TZ)
    return int(dt.utcoffset().total_seconds() // 3600)


def parse_local_date_to_ts(date_text: str) -> int:
    dt_local = datetime.strptime(date_text, "%Y-%m-%d").replace(tzinfo=MELBOURNE_TZ)
    return int(dt_local.timestamp())


def ts_to_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=MELBOURNE_TZ).strftime("%Y-%m-%d")


def signed_get(app_key: str, app_secret: str, endpoint: str, params: dict) -> Tuple[Any, Any]:
    return signed_pudu_get(
        app_key=app_key,
        app_secret=app_secret,
        logical_path=endpoint,
        query_params=params,
        timeout=40,
    )


@dataclass
class RobotTotals:
    sn: str
    model: str
    label: str
    area_m2: float = 0.0
    runtime_h: float = 0.0
    tasks: int = 0
    power_kwh: float = 0.0
    water_l: float = 0.0


def fetch_global_robot_day_records(
    app_key: str,
    app_secret: str,
    *,
    shop_id: str,
    robot_sn: str,
    start_ts: int,
    end_ts: int,
) -> List[dict]:
    out: List[dict] = []
    tz_off = melbourne_offset_for_period(start_ts, end_ts)
    shop_id_s = str(shop_id).strip()

    params = {
        "timezone_offset": tz_off,
        "start_time": start_ts,
        "end_time": end_ts,
        "group_by": "robot",
        "time_unit": "day",
        "clean_mode": 0,
        "sub_mode": 0,
    }
    data, _response, _err = signed_get(app_key, app_secret, "/data-board/v1/analysis/clean/paging", params)
    if not data or not pudu_message_ok(data):
        return out
    block = (data.get("data") or {}).get("list") or []
    for row in block:
        row_shop = str(row.get("shop_id") or "").strip()
        row_sn = str(row.get("sn") or "").strip()
        if row_shop == shop_id_s and row_sn == robot_sn:
            out.append(row)
    return out


def totals_from_analytics_rows(rows: Iterable[dict]) -> dict:
    area = runtime_h = power_kwh = water_l = 0.0
    tasks = 0
    for row in rows:
        area += _float(row.get("area"))
        runtime_h += _float(row.get("duration"))
        tasks += _int(row.get("task_count"))
        power_kwh += _float(row.get("power_consumption"))
        water_l += _float(row.get("water_consumption")) / 1000.0
    return {
        "area_m2": area,
        "runtime_h": runtime_h,
        "tasks": tasks,
        "power_kwh": power_kwh,
        "water_l": water_l,
    }


def totals_from_query_list_rows(tasks: Iterable[dict]) -> dict:
    area = runtime_h = power_kwh = water_l = 0.0
    task_count = 0
    for t in tasks:
        task_count += 1
        area += _float(t.get("clean_area") or t.get("area"))
        if t.get("clean_time") is not None:
            runtime_h += _float(t.get("clean_time")) / 3600.0
        else:
            runtime_h += _float(t.get("duration"))
        power_kwh += _float(t.get("power_consumption"))
        water_l += _float(t.get("water_consumption")) / 1000.0
    return {
        "area_m2": area,
        "runtime_h": runtime_h,
        "tasks": task_count,
        "power_kwh": power_kwh,
        "water_l": water_l,
    }


def _fetch_query_list_page_with_limit(
    app_key: str,
    app_secret: str,
    *,
    shop_id: str,
    robot_sn: str,
    start_ts: int,
    end_ts: int,
    timezone_offset: int,
    offset: int,
    limit: int,
) -> Tuple[List[dict], int]:
    params = {
        "shop_id": str(shop_id).strip(),
        "sn": str(robot_sn).strip(),
        "start_time": start_ts,
        "end_time": end_ts,
        "timezone_offset": timezone_offset,
        "offset": max(0, int(offset)),
        "limit": max(1, int(limit)),
    }
    body, _resp, _err = signed_get(
        app_key,
        app_secret,
        "/data-board/v1/log/clean_task/query_list",
        params,
    )
    if not body or not pudu_message_ok(body):
        return [], 0
    inner = body.get("data") or {}
    rows = inner.get("list") or []
    if not isinstance(rows, list):
        return [], 0
    rows = [r for r in rows if isinstance(r, dict)]
    return rows, int(limit)


def fetch_query_list_for_robot(
    app_key: str,
    app_secret: str,
    *,
    shop_id: str,
    robot_sn: str,
    start_ts: int,
    end_ts: int,
) -> List[dict]:
    out: List[dict] = []
    offset = 0
    tz_off = melbourne_offset_for_period(start_ts, end_ts)
    limits = [50, 20, 10, 5]

    for _ in range(250):
        page_rows: List[dict] = []
        used_limit = 0
        for lim in limits:
            rows, used = _fetch_query_list_page_with_limit(
                app_key,
                app_secret,
                shop_id=shop_id,
                robot_sn=robot_sn,
                start_ts=start_ts,
                end_ts=end_ts,
                timezone_offset=tz_off,
                offset=offset,
                limit=lim,
            )
            if rows:
                page_rows = rows
                used_limit = used
                break
            if used == 0:
                continue

        if not page_rows:
            break

        if any(_task_sn(t) for t in page_rows):
            page_rows = [t for t in page_rows if _task_sn(t) == robot_sn]
        out.extend(page_rows)

        step = used_limit if used_limit > 0 else len(page_rows)
        if step <= 0:
            break
        offset += step
        if len(page_rows) < step:
            break

    return out


def get_robot_period_totals(
    app_key: str,
    app_secret: str,
    *,
    shop_id: str,
    robot_sn: str,
    start_ts: int,
    end_ts: int,
) -> dict:
    analytics_rows = fetch_global_robot_day_records(
        app_key,
        app_secret,
        shop_id=shop_id,
        robot_sn=robot_sn,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    if analytics_rows:
        totals = totals_from_analytics_rows(analytics_rows)
        totals["source"] = "analytics_paging_sn"
        totals["rows"] = len(analytics_rows)
        return totals

    tasks = fetch_query_list_for_robot(
        app_key,
        app_secret,
        shop_id=shop_id,
        robot_sn=robot_sn,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    totals = totals_from_query_list_rows(tasks)
    totals["source"] = "query_list_fallback"
    totals["rows"] = len(tasks)
    return totals


def status_text_from_code(code: Any) -> str:
    mapping = {
        0: "Not Started",
        1: "Cleaning",
        2: "Task Suspended",
        3: "Task Interrupted",
        4: "Task Ended",
        5: "Task Abnormal",
        6: "Task Failed",
    }
    if isinstance(code, str) and code.strip():
        return code.strip()
    return mapping.get(code, f"Status {code}")


def summarize_week_tasks(tasks: List[dict]) -> dict:
    status_counter = Counter()
    top_area_by_task = defaultdict(float)
    top_runs_by_task = Counter()
    total_area = 0.0
    total_runtime_h = 0.0

    for t in tasks:
        status = status_text_from_code(t.get("status"))
        status_counter[status] += 1
        name = str(t.get("task_name") or "Cleaning Task").strip() or "Cleaning Task"
        area = _float(t.get("clean_area") or t.get("area"))
        runtime_h = _float(t.get("clean_time")) / 3600.0 if t.get("clean_time") is not None else _float(t.get("duration"))
        top_area_by_task[name] += area
        top_runs_by_task[name] += 1
        total_area += area
        total_runtime_h += runtime_h

    # Every distinct task name in the window (not capped), sorted by cleaned area then runs.
    names_union = set(top_area_by_task.keys()) | set(top_runs_by_task.keys())
    top_tasks_by_area = sorted(
        ((n, top_area_by_task[n]) for n in names_union),
        key=lambda kv: (-kv[1], -top_runs_by_task[kv[0]], kv[0].lower()),
    )

    return {
        "rows": len(tasks),
        "status_counts": dict(status_counter),
        "top_tasks_by_area": top_tasks_by_area,
        "top_tasks_by_runs": top_runs_by_task.most_common(),
        "total_area_from_logs": total_area,
        "total_runtime_h_from_logs": total_runtime_h,
    }


def weekly_ranges(start_ts: int, end_ts: int) -> List[Tuple[int, int]]:
    ranges = []
    cur = start_ts
    while cur < end_ts:
        nxt = min(cur + 7 * 24 * 3600, end_ts)
        ranges.append((cur, nxt))
        cur = nxt
    return ranges


def fmt(num: float, digits: int = 2) -> str:
    return f"{num:,.{digits}f}"


def build_markdown(
    *,
    shop_name: str,
    shop_id: str,
    start_date: str,
    end_date: str,
    generated_at: str,
    robots: Dict[str, RobotTotals],
    weekly_trend: List[dict],
    sample_week: dict,
    labour_rate: Optional[float],
) -> str:
    combined = RobotTotals(sn="COMBINED", model="Combined", label="Combined")
    for r in robots.values():
        combined.area_m2 += r.area_m2
        combined.runtime_h += r.runtime_h
        combined.tasks += r.tasks
        combined.power_kwh += r.power_kwh
        combined.water_l += r.water_l

    lines: List[str] = []
    lines.append(f"# Trial performance summary - {shop_name}")
    lines.append("")
    lines.append(f"- Site: {shop_name} ({shop_id})")
    lines.append(f"- Period: {start_date} to {end_date}")
    lines.append(f"- Generated: {generated_at}")
    lines.append("")
    lines.append("## Cumulative Trial Totals")
    lines.append("")
    lines.append("| Robot | SN | Area (m2) | Runtime (h) | Tasks | Power (kWh) | Water (L) |")
    lines.append("|---|---|---:|---:|---:|---:|---:|")
    for r in robots.values():
        lines.append(
            f"| {r.label} | `{r.sn}` | {fmt(r.area_m2)} | {fmt(r.runtime_h)} | {r.tasks:,} | {fmt(r.power_kwh)} | {fmt(r.water_l)} |"
        )
    lines.append(
        f"| **Combined** |  | **{fmt(combined.area_m2)}** | **{fmt(combined.runtime_h)}** | **{combined.tasks:,}** | **{fmt(combined.power_kwh)}** | **{fmt(combined.water_l)}** |"
    )
    lines.append("")
    lines.append("## Performance Notes")
    lines.append("")
    lines.append(f"- Combined output since {start_date}: {fmt(combined.area_m2)} m2 over {fmt(combined.runtime_h)} runtime hours.")
    lines.append(
        f"- Energy intensity: {fmt((combined.power_kwh / combined.area_m2) * 1000.0 if combined.area_m2 else 0.0, 3)} kWh per 1000m2."
    )
    lines.append(
        f"- Water intensity: {fmt((combined.water_l / combined.area_m2) * 1000.0 if combined.area_m2 else 0.0, 3)} L per 1000m2."
    )
    if labour_rate is not None and labour_rate > 0:
        potential = combined.runtime_h * labour_rate
        lines.append(
            f"- Indicative redeployment value at ${labour_rate:.2f}/h: about ${potential:,.2f} over the trial period."
        )
    lines.append("- Ops framing: robot runtime can be redirected to detail-focused manual cleaning.")
    lines.append("")
    lines.append("## Typical Weekly Snapshot")
    lines.append("")
    lines.append(f"- Week window: {sample_week['start_date']} to {sample_week['end_date']}")
    lines.append("")
    lines.append("| Robot | Area (m2) | Runtime (h) | Tasks | Power (kWh) | Water (L) |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for row in sample_week["analytics_rows"]:
        lines.append(
            f"| {row['label']} | {fmt(row['area_m2'])} | {fmt(row['runtime_h'])} | {row['tasks']:,} | {fmt(row['power_kwh'])} | {fmt(row['water_l'])} |"
        )
    lines.append("")

    for sn, details in sample_week["task_summaries"].items():
        label = robots[sn].label if sn in robots else sn
        lines.append(f"### {label} (`{sn}`) - What was cleaned")
        lines.append("")
        lines.append(f"- Execution rows: {details['rows']}")
        status_counts = details["status_counts"] or {}
        if status_counts:
            statuses = ", ".join(f"{k}: {v}" for k, v in sorted(status_counts.items(), key=lambda kv: kv[0]))
            lines.append(f"- Status mix: {statuses}")
        lines.append("")
        lines.append("Task names by cleaned area (all names in sample window):")
        lines.append("")
        lines.append("| Task name | Area (m2) | Runs |")
        lines.append("|---|---:|---:|")
        runs_lookup = dict(details["top_tasks_by_runs"])
        for task_name, area in details["top_tasks_by_area"]:
            lines.append(f"| {task_name} | {fmt(area)} | {runs_lookup.get(task_name, 0)} |")
        lines.append("")

    lines.append("## Weekly Trend (Trial Period)")
    lines.append("")
    lines.append("| Week Start | Week End | Area (m2) | Runtime (h) | Tasks | Power (kWh) | Water (L) |")
    lines.append("|---|---|---:|---:|---:|---:|---:|")
    for w in weekly_trend:
        lines.append(
            f"| {w['start_date']} | {w['end_date']} | {fmt(w['area_m2'])} | {fmt(w['runtime_h'])} | {w['tasks']:,} | {fmt(w['power_kwh'])} | {fmt(w['water_l'])} |"
        )
    lines.append("")
    lines.append("Data source: Pudu analytics + execution logs.")
    return "\n".join(lines)


@dataclass
class TrialSummaryBundle:
    payload: Dict[str, Any]
    markdown: str
    shop_id: str
    shop_name: str
    start_date: str
    end_date: str
    generated_at: str


def run_trial_summary_report(
    *,
    app_key: str,
    app_secret: str,
    shop_id: str,
    shop_display_name: str,
    robot_specs: List[dict],
    start_date: str,
    labour_rate: Optional[float] = None,
) -> TrialSummaryBundle:
    if not robot_specs:
        raise ValueError("robot_specs must contain at least one robot")

    start_ts = parse_local_date_to_ts(start_date)
    now_local = datetime.now(MELBOURNE_TZ)
    period_end_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    if period_end_local.timestamp() <= start_ts:
        period_end_local = now_local
    now_ts = int(period_end_local.timestamp())
    if start_ts >= now_ts:
        raise ValueError("Start date must be earlier than the report end boundary")

    end_date = (period_end_local - timedelta(days=1)).strftime("%Y-%m-%d")
    robots: Dict[str, RobotTotals] = {
        r["sn"]: RobotTotals(sn=r["sn"], model=r["model"], label=r["label"]) for r in robot_specs
    }

    weekly_trend: List[dict] = []
    cumulative_by_robot: Dict[str, dict] = {
        sn: {"area_m2": 0.0, "runtime_h": 0.0, "tasks": 0, "power_kwh": 0.0, "water_l": 0.0} for sn in robots.keys()
    }
    for ws, we in weekly_ranges(start_ts, now_ts):
        bucket = {"area_m2": 0.0, "runtime_h": 0.0, "tasks": 0, "power_kwh": 0.0, "water_l": 0.0}
        for sn in robots.keys():
            totals = get_robot_period_totals(
                app_key,
                app_secret,
                shop_id=shop_id,
                robot_sn=sn,
                start_ts=ws,
                end_ts=we,
            )
            bucket["area_m2"] += totals["area_m2"]
            bucket["runtime_h"] += totals["runtime_h"]
            bucket["tasks"] += totals["tasks"]
            bucket["power_kwh"] += totals["power_kwh"]
            bucket["water_l"] += totals["water_l"]
            cumulative_by_robot[sn]["area_m2"] += totals["area_m2"]
            cumulative_by_robot[sn]["runtime_h"] += totals["runtime_h"]
            cumulative_by_robot[sn]["tasks"] += totals["tasks"]
            cumulative_by_robot[sn]["power_kwh"] += totals["power_kwh"]
            cumulative_by_robot[sn]["water_l"] += totals["water_l"]
        weekly_trend.append({"start_date": ts_to_date(ws), "end_date": ts_to_date(we), **bucket})

    for sn, robot in robots.items():
        agg = cumulative_by_robot[sn]
        robot.area_m2 = agg["area_m2"]
        robot.runtime_h = agg["runtime_h"]
        robot.tasks = int(agg["tasks"])
        robot.power_kwh = agg["power_kwh"]
        robot.water_l = agg["water_l"]

    sample_end_local = period_end_local
    sample_start_local = sample_end_local - timedelta(days=7)
    sample_start_ts = int(sample_start_local.timestamp())
    sample_end_ts = int(sample_end_local.timestamp())

    sample_analytics_rows = []
    for sn, robot in robots.items():
        totals = get_robot_period_totals(
            app_key,
            app_secret,
            shop_id=shop_id,
            robot_sn=sn,
            start_ts=sample_start_ts,
            end_ts=sample_end_ts,
        )
        sample_analytics_rows.append(
            {
                "sn": sn,
                "label": robot.label,
                "area_m2": totals["area_m2"],
                "runtime_h": totals["runtime_h"],
                "tasks": totals["tasks"],
                "power_kwh": totals["power_kwh"],
                "water_l": totals["water_l"],
                "source": totals.get("source"),
            }
        )

    sample_task_summaries = {}
    for sn in robots.keys():
        tasks = fetch_query_list_for_robot(
            app_key,
            app_secret,
            shop_id=shop_id,
            robot_sn=sn,
            start_ts=sample_start_ts,
            end_ts=sample_end_ts,
        )
        sample_task_summaries[sn] = summarize_week_tasks(tasks)

    generated_at = datetime.now(MELBOURNE_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    sample_week = {
        "start_date": sample_start_local.strftime("%Y-%m-%d"),
        "end_date": sample_end_local.strftime("%Y-%m-%d"),
        "analytics_rows": sample_analytics_rows,
        "task_summaries": sample_task_summaries,
    }

    lr = float(labour_rate) if labour_rate is not None and float(labour_rate) > 0 else None
    md = build_markdown(
        shop_name=shop_display_name,
        shop_id=shop_id,
        start_date=start_date,
        end_date=end_date,
        generated_at=generated_at,
        robots=robots,
        weekly_trend=weekly_trend,
        sample_week=sample_week,
        labour_rate=lr,
    )

    payload: Dict[str, Any] = {
        "shop_id": shop_id,
        "shop_name": shop_display_name,
        "start_date": start_date,
        "end_date": end_date,
        "generated_at": generated_at,
        "robots": {k: vars(v) for k, v in robots.items()},
        "weekly_trend": weekly_trend,
        "sample_week": sample_week,
    }
    if lr is not None:
        payload["labour_rate"] = lr

    return TrialSummaryBundle(
        payload=payload,
        markdown=md,
        shop_id=shop_id,
        shop_name=shop_display_name,
        start_date=start_date,
        end_date=end_date,
        generated_at=generated_at,
    )


def build_email_draft_from_payload(payload: dict) -> Tuple[str, str]:
    shop_name = str(payload.get("shop_name") or "Site")
    shop_id = str(payload.get("shop_id") or "")
    start_date = str(payload.get("start_date") or "")
    end_date = str(payload.get("end_date") or "")
    subject = f"{shop_name} - Robot performance summary ({start_date} to {end_date})"
    body = (
        f"Hi,\n\nConsolidated performance summary for {shop_name} (shop_id {shop_id}), "
        f"trial/reporting window {start_date} to {end_date}.\n\nRegards,\n"
    )
    return subject, body


if __name__ == "__main__":
    raise SystemExit("Use run_trial_summary_report() from services.trial_summary_dashboard")
