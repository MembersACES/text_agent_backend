"""
Pudu cleaning dashboard aggregation: mode, robot-scoped paging, execution list (resilient query_list).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pytz

from services.pudu_signed_request import pudu_message_ok, signed_pudu_get

DM_SYSTEM_ERROR = "DM_SYSTEM_ERROR"

PUDU_LOG = logging.getLogger("aces.pudu")

_PRIO_ROBOT_SN_KEYS = (
    "machine_sn",
    "machineSn",
    "machine_serial_number",
    "machine_serial_no",
    "machineNo",
    "robot_sn",
    "robotSn",
    "robot_serial_number",
    "device_sn",
    "deviceSn",
    "device_serial_number",
    "serial_number",
    "serialNumber",
    "serial_no",
    "equipment_sn",
    "equipment_no",
    "sn",
    "SN",
    "Sn",
    "S/N",
)


def _norm_serial_str(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() in ("null", "none", "undefined", "-"):
        return ""
    return s


def _robot_serial_candidates(row: Dict[str, Any], *, depth: int = 0) -> List[str]:
    """Ordered serial-like strings from an open-platform robot row (sn is sometimes wrong or shared)."""
    out: List[str] = []
    seen: set[str] = set()

    def add(s: str) -> None:
        s = s.strip()
        if len(s) < 4 or s in seen:
            return
        seen.add(s)
        out.append(s)

    for k in _PRIO_ROBOT_SN_KEYS:
        add(_norm_serial_str(row.get(k)))

    if depth < 3:
        for nest_key in ("robot", "device", "machine", "deviceInfo", "robotInfo", "robot_info"):
            inner = row.get(nest_key)
            if isinstance(inner, dict):
                for c in _robot_serial_candidates(inner, depth=depth + 1):
                    add(c)

    for key, v in row.items():
        if isinstance(v, (dict, list)):
            continue
        sv = _norm_serial_str(v)
        if len(sv) < 6:
            continue
        kn = str(key).lower().replace("-", "_")
        if kn in ("shop_id", "shopid", "id", "shop_name", "shopname"):
            continue
        if "serial" in kn or kn.endswith("_sn") or kn == "sn" or "machine_no" in kn:
            add(sv)
    return out


def annotate_robot_list_with_canonical_sn(robots: List[Any]) -> None:
    """
    Mutates each robot dict in-place: sets ``sn_canonical`` to a best-effort unique serial per shop list.
    Open-platform rows occasionally repeat ``sn`` across different machines; prefer machine-specific keys first.
    """
    used: set[str] = set()
    for i, r in enumerate(robots):
        if not isinstance(r, dict):
            continue
        cands = _robot_serial_candidates(r)
        chosen = ""
        for c in cands:
            if c not in used:
                chosen = c
                break
        if not chosen and cands:
            chosen = cands[0]
            PUDU_LOG.warning(
                "robot list: could not pick unused sn_canonical row=%s raw_sn=%r candidates=%s",
                i,
                r.get("sn"),
                cands[:8],
            )
        if chosen:
            r["sn_canonical"] = chosen
            used.add(chosen)
        elif not cands:
            PUDU_LOG.warning(
                "robot list: no serial candidates row=%s keys=%s",
                i,
                list(r.keys())[:24],
            )


def _execution_row_sn(row: Any) -> str:
    """Best-effort serial from a Pudu clean_task/query_list row (keys vary by tenant/version)."""
    if not isinstance(row, dict):
        return ""
    for k in ("sn", "SN", "Sn", "S/N", "s/n", "serial_number", "robot_sn", "machine_sn"):
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    for key, v in row.items():
        nk = str(key).lower().replace("-", "_").replace("/", "_")
        if nk in ("sn", "s_n") or nk.endswith("_sn") or nk == "serial" or nk == "serial_number":
            if v is not None and str(v).strip():
                return str(v).strip()
    return ""


def _execution_sn_histogram(rows: List[Any]) -> Dict[str, int]:
    hist: Dict[str, int] = {}
    for row in rows:
        s = _execution_row_sn(row)
        if s:
            hist[s] = hist.get(s, 0) + 1
    return hist


_cache_lock = threading.Lock()
_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
CACHE_TTL_SEC = 45.0


def _json_stable(obj: Any) -> str:
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        return str(obj)


def get_pudu_credentials() -> Tuple[Optional[str], Optional[str]]:
    key = os.environ.get("PUDU_APP_KEY") or os.environ.get("PUDU_API_KEY")
    secret = os.environ.get("PUDU_SECRET_KEY") or os.environ.get("PUDU_API_SECRET")
    if key:
        key = key.strip()
    if secret:
        secret = secret.strip()
    return key, secret


def melbourne_offset_hours(start_ts: int, end_ts: int) -> int:
    """Integer hours offset from UTC at midpoint of [start_ts, end_ts] in Australia/Melbourne."""
    mid = (start_ts + end_ts) / 2.0
    utc = pytz.UTC
    mel = pytz.timezone("Australia/Melbourne")
    dt = datetime.fromtimestamp(mid, tz=utc)
    loc = dt.astimezone(mel)
    off = loc.utcoffset()
    if off is None:
        return 10
    return int(off.total_seconds() // 3600)


def _body_has_dm(body: Optional[Dict[str, Any]], response_text: str) -> bool:
    if DM_SYSTEM_ERROR in (response_text or ""):
        return True
    if body is None:
        return False
    try:
        blob = json.dumps(body)
    except Exception:
        blob = str(body)
    return DM_SYSTEM_ERROR in blob


def fetch_clean_task_query_list_page(
    app_key: str,
    app_secret: str,
    query: Dict[str, Any],
    *,
    initial_limit: int = 50,
) -> Tuple[Optional[List[Any]], int, Optional[str]]:
    """
    Resilient first page of query_list: limit chain  initial_limit → 20 → 10 → 5, retries on 5xx/429.
    Returns (rows, used_lim, error).
    """
    limits = [initial_limit, 20, 10, 5]
    last_err: Optional[str] = None
    for lim in limits:
        q = {**query, "limit": lim}
        for attempt in range(4):
            body, resp, err = signed_pudu_get(
                app_key,
                app_secret,
                "/data-board/v1/log/clean_task/query_list",
                q,
            )
            text = (resp.text if resp is not None else "") or ""
            if err and resp is None:
                last_err = err
                time.sleep(0.35 * (attempt + 1))
                continue
            if resp is not None and resp.status_code == 429:
                last_err = "HTTP 429"
                time.sleep(0.8 * (attempt + 1))
                continue
            if resp is not None and resp.status_code >= 500:
                last_err = err or f"HTTP {resp.status_code}"
                time.sleep(0.45 * (attempt + 1))
                continue
            if body is not None and _body_has_dm(body, text) and lim > 5:
                last_err = DM_SYSTEM_ERROR
                break
            if body is not None and pudu_message_ok(body):
                data = body.get("data")
                rows: List[Any] = []
                if isinstance(data, dict):
                    raw = data.get("list")
                    if isinstance(raw, list):
                        rows = raw
                elif isinstance(data, list):
                    rows = data
                return rows, lim, None
            if body is not None:
                last_err = str(body.get("message", "unknown"))
            else:
                last_err = err or "empty body"
            break
    return None, 0, last_err or "query_list failed"


def _row_time_candidates_unix_seconds(row: Dict[str, Any]) -> List[int]:
    out: List[int] = []
    for key in ("start_time", "end_time", "create_time"):
        v = row.get(key)
        if isinstance(v, (int, float)):
            iv = int(v)
            if 946684800 <= iv <= 4102444800:  # 2000-01-01 .. 2100-01-01
                out.append(iv)
        elif isinstance(v, str):
            s = v.strip()
            if s.isdigit():
                iv = int(s)
                if 946684800 <= iv <= 4102444800:
                    out.append(iv)
    return out


def detect_robot_first_execution_ts(
    app_key: str,
    app_secret: str,
    *,
    shop_id: str,
    robot_sn: str,
    max_pages: int = 1200,
) -> Tuple[Optional[int], Optional[str]]:
    """
    Best-effort first available execution timestamp for a robot by walking query_list pages.
    query_list is newest-first; this scans pages until exhaustion and returns the oldest ts seen.
    """
    end_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = 946684800  # 2000-01-01 UTC
    tz_off = melbourne_offset_hours(start_ts, end_ts)
    offset = 0
    oldest: Optional[int] = None
    last_err: Optional[str] = None

    for _ in range(max_pages):
        rows, used_lim, err = fetch_clean_task_query_list_page(
            app_key,
            app_secret,
            {
                "shop_id": str(shop_id).strip(),
                "sn": str(robot_sn).strip(),
                "start_time": start_ts,
                "end_time": end_ts,
                "timezone_offset": tz_off,
                "offset": offset,
            },
            initial_limit=50,
        )
        if rows is None:
            last_err = err
            break
        if not rows:
            break

        filtered_rows = rows
        if any(_execution_row_sn(r) for r in rows if isinstance(r, dict)):
            filtered_rows = [
                r
                for r in rows
                if isinstance(r, dict) and _execution_row_sn(r).strip().lower() == robot_sn.strip().lower()
            ]

        for r in filtered_rows:
            if not isinstance(r, dict):
                continue
            for ts in _row_time_candidates_unix_seconds(r):
                oldest = ts if oldest is None else min(oldest, ts)

        step = used_lim if used_lim > 0 else len(rows)
        if step <= 0:
            break
        offset += step
        if len(rows) < step:
            break

    if oldest is not None:
        return oldest, None
    return None, last_err or "no rows found"


def fetch_clean_paging_sn_resilient(
    app_key: str,
    app_secret: str,
    base_params: Dict[str, Any],
    *,
    initial_limit: int = 50,
) -> Tuple[Any, Optional[str]]:
    """
    Robot-scoped clean/paging with limit chain and retries (same idea as query_list).
    Pudu often returns HTTP 500 + DM_SYSTEM_ERROR for larger ``limit`` values.
    Returns (data object from body['data'] or None, error_message).
    """
    base = {k: v for k, v in base_params.items() if k != "limit"}
    limits = [initial_limit, 30, 20, 10, 5]
    last_err: Optional[str] = None
    for lim in limits:
        q = {**base, "limit": lim, "offset": int(base.get("offset", 0) or 0)}
        for attempt in range(4):
            body, resp, err = signed_pudu_get(
                app_key,
                app_secret,
                "/data-board/v1/analysis/clean/paging",
                q,
            )
            text = (resp.text if resp is not None else "") or ""
            if err and resp is None:
                last_err = err
                time.sleep(0.35 * (attempt + 1))
                continue
            if resp is not None and resp.status_code == 429:
                last_err = "HTTP 429"
                time.sleep(0.8 * (attempt + 1))
                continue
            if resp is not None and resp.status_code != 200:
                if DM_SYSTEM_ERROR in text and lim > 5:
                    last_err = f"paging DM_SYSTEM_ERROR (limit={lim})"
                    break
                if DM_SYSTEM_ERROR in text:
                    last_err = f"HTTP {resp.status_code}: {text[:400]}"
                    break
                last_err = err or f"HTTP {resp.status_code}"
                time.sleep(0.45 * (attempt + 1))
                continue
            if body is None:
                last_err = err or "empty response"
                break
            if pudu_message_ok(body):
                return body.get("data"), None
            if _body_has_dm(body, text) and lim > 5:
                last_err = DM_SYSTEM_ERROR
                break
            if _body_has_dm(body, text):
                last_err = f"{DM_SYSTEM_ERROR} (limit={lim})"
                break
            last_err = str(body.get("message", "unknown"))
            break
    return None, last_err or "paging failed"


def _fetch_mode_with_label(
    layer: str, app_key: str, app_secret: str, q: Dict[str, Any]
) -> Tuple[str, Any, Optional[str]]:
    body, _, err = signed_pudu_get(
        app_key, app_secret, "/data-board/v1/analysis/clean/mode", q
    )
    if err:
        return layer, None, err
    if not pudu_message_ok(body):
        return layer, body, body.get("message") if isinstance(body, dict) else "bad message"
    return layer, body.get("data"), None


def _fetch_paging_sn_job(
    app_key: str, app_secret: str, paging_base: Dict[str, Any]
) -> Tuple[str, Any, Optional[str]]:
    data, err = fetch_clean_paging_sn_resilient(app_key, app_secret, paging_base, initial_limit=50)
    if err:
        return "paging", None, err
    return "paging", data, None


def build_dashboard_payload(
    sn: str,
    start_time: int,
    end_time: int,
    shop_id: Optional[str],
    execution_offset: int,
    *,
    only_executions: bool = False,
    skip_cache: bool = False,
) -> Dict[str, Any]:
    key, secret = get_pudu_credentials()
    if not key or not secret:
        raise RuntimeError("PUDU_APP_KEY and PUDU_SECRET_KEY are not configured")

    cache_key = json.dumps(
        {
            "sn": sn.strip(),
            "shop": (shop_id or "").strip(),
            "s": start_time,
            "e": end_time,
            "o": execution_offset,
            "x": only_executions,
        },
        sort_keys=True,
    )
    if not skip_cache and execution_offset == 0 and not only_executions:
        with _cache_lock:
            hit = _cache.get(cache_key)
            if hit:
                ts, payload = hit
                if time.time() - ts < CACHE_TTL_SEC:
                    out = dict(payload)
                    out["cached"] = True
                    PUDU_LOG.info(
                        "Pudu dashboard cache hit shop_id=%s sn=%s range_unix=%s-%s age_sec=%.1f",
                        (shop_id or "").strip() or "(none)",
                        sn.strip(),
                        start_time,
                        end_time,
                        time.time() - ts,
                    )
                    return out

    PUDU_LOG.info(
        "Pudu dashboard build start shop_id=%s request_sn=%s range_unix=%s-%s execution_offset=%s only_executions=%s",
        (shop_id or "").strip() or "(none)",
        sn.strip(),
        start_time,
        end_time,
        execution_offset,
        only_executions,
    )

    tz = melbourne_offset_hours(start_time, end_time)
    base_time: Dict[str, Any] = {
        "timezone_offset": tz,
        "start_time": start_time,
        "end_time": end_time,
    }

    degraded = {"mode": False, "robot_mode": False, "paging": False, "executions": False}
    errors: List[Dict[str, Any]] = []
    mode_data: Any = None
    robot_mode_data: Any = None
    paging_data: Any = None
    paging_list: List[Any] = []

    if not only_executions:
        mode_params = {
            **base_time,
            "time_unit": "day",
            "clean_mode": 0,
            "sub_mode": -1,
        }
        if shop_id:
            mode_params["shop_id"] = str(shop_id).strip()
        robot_mode_params = {**mode_params, "sn": sn.strip()}

        paging_base = {
            **base_time,
            "offset": 0,
            "clean_mode": 0,
            "sub_mode": -1,
            "sn": sn.strip(),
        }

        with ThreadPoolExecutor(max_workers=3) as ex:
            futs = [
                ex.submit(_fetch_mode_with_label, "mode", key, secret, mode_params),
                ex.submit(_fetch_mode_with_label, "robot_mode", key, secret, robot_mode_params),
                ex.submit(_fetch_paging_sn_job, key, secret, paging_base),
            ]
            for fut in as_completed(futs):
                layer, data, err = fut.result()
                if err:
                    degraded[layer] = True  # type: ignore[index]
                    errors.append({"layer": layer, "detail": err})
                if layer == "mode":
                    mode_data = data
                elif layer == "robot_mode":
                    robot_mode_data = data
                else:
                    paging_data = data

            if shop_id and mode_data is not None and robot_mode_data is not None:
                if _json_stable(mode_data) == _json_stable(robot_mode_data):
                    PUDU_LOG.info(
                        "Pudu clean/mode returned identical payload for shop-only vs shop+sn; treating robot_mode as absent sn=%s shop=%s",
                        sn.strip()[:10],
                        str(shop_id).strip(),
                    )
                    robot_mode_data = None

        PUDU_LOG.info(
            "Pudu dashboard vendor layers shop_id=%s request_sn=%s has_site_mode=%s has_robot_mode=%s has_paging=%s degraded_partial=%s",
            (shop_id or "").strip() or "(none)",
            sn.strip(),
            mode_data is not None,
            robot_mode_data is not None,
            paging_data is not None,
            degraded,
        )

    exec_query: Dict[str, Any] = {
        **base_time,
        "sn": sn.strip(),
        "offset": max(0, execution_offset),
    }
    if shop_id:
        exec_query["shop_id"] = str(shop_id).strip()

    rows, used_lim, ex_err = fetch_clean_task_query_list_page(
        key, secret, exec_query, initial_limit=50
    )
    if ex_err:
        degraded["executions"] = True
        errors.append({"layer": "executions", "detail": ex_err})

    exec_rows = rows or []
    exec_hist = _execution_sn_histogram(exec_rows)
    PUDU_LOG.info(
        "Pudu dashboard query_list shop_id=%s request_sn=%s row_count=%s used_lim=%s sn_histogram=%s request_sn_in_page=%s",
        (shop_id or "").strip() or "(none)",
        sn.strip(),
        len(exec_rows),
        used_lim,
        exec_hist,
        sn.strip() in exec_hist,
    )

    if isinstance(paging_data, dict):
        pl = paging_data.get("list")
        if isinstance(pl, list):
            paging_list = pl

    result: Dict[str, Any] = {
        "sn": sn.strip(),
        "shop_id": str(shop_id).strip() if shop_id else None,
        "timezone_offset": tz,
        "start_time": start_time,
        "end_time": end_time,
        "degraded": degraded,
        "errors": errors,
        "mode": mode_data,
        "robot_mode": robot_mode_data,
        "paging": paging_data,
        "paging_list": paging_list,
        "executions": {
            "list": exec_rows,
            "offset": execution_offset,
            "used_lim": used_lim,
        },
        "execution_next_offset": execution_offset + used_lim if used_lim else execution_offset,
        "execution_has_more": bool(exec_rows) and used_lim > 0 and len(exec_rows) >= used_lim,
        "cached": False,
    }

    if not skip_cache and execution_offset == 0 and not only_executions:
        with _cache_lock:
            to_store = {k: v for k, v in result.items() if k != "cached"}
            _cache[cache_key] = (time.time(), to_store)
            if len(_cache) > 200:
                for k in list(_cache.keys())[:50]:
                    _cache.pop(k, None)

    PUDU_LOG.info(
        "Pudu dashboard done shop_id=%s request_sn=%s range_unix=%s-%s execution_rows=%s sn_histogram=%s degraded=%s error_layers=%s",
        (shop_id or "").strip() or "(none)",
        sn.strip(),
        start_time,
        end_time,
        len(exec_rows),
        exec_hist,
        degraded,
        [e.get("layer") for e in errors],
    )
    return result


def fetch_clean_task_detail(
    sn: str,
    report_id: str,
    start_time: int,
    end_time: int,
    shop_id: Optional[str],
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    key, secret = get_pudu_credentials()
    if not key or not secret:
        raise RuntimeError("PUDU_APP_KEY and PUDU_SECRET_KEY are not configured")
    tz = melbourne_offset_hours(start_time, end_time)
    q: Dict[str, Any] = {
        "timezone_offset": tz,
        "start_time": start_time,
        "end_time": end_time,
        "sn": sn.strip(),
        "report_id": str(report_id).strip(),
    }
    if shop_id:
        q["shop_id"] = str(shop_id).strip()
    body, _, err = signed_pudu_get(
        key, secret, "/data-board/v1/log/clean_task/query", q
    )
    if err:
        return None, err
    if not pudu_message_ok(body):
        return body, str(body.get("message", "unexpected message"))
    data = body.get("data")
    if isinstance(data, dict) and "data" in data:
        return data.get("data"), None  # type: ignore[return-value]
    return data if isinstance(data, dict) else {"raw": data}, None


def records_from_open_platform_body(body: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normalize list payloads from Pudu open-platform (shop / robot) responses."""
    data = body.get("data")
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("list", "data", "records", "rows", "machines"):
        inner = data.get(key)
        if isinstance(inner, list):
            return [x for x in inner if isinstance(x, dict)]
        if isinstance(inner, dict):
            nested = inner.get("list")
            if isinstance(nested, list):
                return [x for x in nested if isinstance(x, dict)]
    return []


def paginate_open_platform_list(
    app_key: str,
    app_secret: str,
    logical_path: str,
    extra: Dict[str, Any],
    *,
    page_limit: int = 100,
    max_pages: int = 100,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Walk limit/offset pages until a short page or max_pages."""
    combined: List[Dict[str, Any]] = []
    offset = 0
    for _ in range(max_pages):
        params = {**extra, "limit": page_limit, "offset": offset}
        body, _, err = signed_pudu_get(app_key, app_secret, logical_path, params)
        if err:
            return combined, err
        if not pudu_message_ok(body):
            msg = str(body.get("message")) if isinstance(body, dict) else "bad response"
            return combined, msg
        batch = records_from_open_platform_body(body)
        combined.extend(batch)
        if len(batch) < page_limit:
            break
        offset += page_limit
    return combined, None


def fetch_pudu_shops_list() -> Tuple[List[Dict[str, Any]], Optional[str]]:
    key, secret = get_pudu_credentials()
    if not key or not secret:
        raise RuntimeError("PUDU_APP_KEY and PUDU_SECRET_KEY are not configured")
    return paginate_open_platform_list(
        key,
        secret,
        "/data-open-platform-service/v1/api/shop",
        {},
        page_limit=100,
        max_pages=100,
    )


def fetch_pudu_robots_list(shop_id: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    key, secret = get_pudu_credentials()
    if not key or not secret:
        raise RuntimeError("PUDU_APP_KEY and PUDU_SECRET_KEY are not configured")
    extra: Dict[str, Any] = {}
    if shop_id and str(shop_id).strip():
        extra["shop_id"] = str(shop_id).strip()
    return paginate_open_platform_list(
        key,
        secret,
        "/data-open-platform-service/v1/api/robot",
        extra,
        page_limit=100,
        max_pages=100,
    )


def fetch_cleanbot_task_definitions(
    shop_id: str,
    sn: str,
    offset: int = 0,
    limit: int = 50,
) -> Tuple[Any, Optional[str]]:
    key, secret = get_pudu_credentials()
    if not key or not secret:
        raise RuntimeError("PUDU_APP_KEY and PUDU_SECRET_KEY are not configured")
    params: Dict[str, Any] = {
        "shop_id": str(shop_id).strip(),
        "sn": str(sn).strip(),
        "limit": max(1, min(100, int(limit))),
        "offset": max(0, int(offset)),
    }
    body, _, err = signed_pudu_get(
        key,
        secret,
        "/cleanbot-service/v1/api/open/task/list",
        params,
    )
    if err:
        return None, err
    if not pudu_message_ok(body):
        return body, str(body.get("message", "unexpected message"))
    return body.get("data"), None
