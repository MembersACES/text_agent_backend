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

_cache_lock = threading.Lock()
_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
CACHE_TTL_SEC = 45.0


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


def _fetch_mode(
    app_key: str, app_secret: str, q: Dict[str, Any]
) -> Tuple[str, Any, Optional[str]]:
    body, _, err = signed_pudu_get(
        app_key, app_secret, "/data-board/v1/analysis/clean/mode", q
    )
    if err:
        return "mode", None, err
    if not pudu_message_ok(body):
        return "mode", body, body.get("message") if isinstance(body, dict) else "bad message"
    return "mode", body.get("data"), None


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
                    return out

    tz = melbourne_offset_hours(start_time, end_time)
    base_time: Dict[str, Any] = {
        "timezone_offset": tz,
        "start_time": start_time,
        "end_time": end_time,
    }

    degraded = {"mode": False, "paging": False, "executions": False}
    errors: List[Dict[str, Any]] = []
    mode_data: Any = None
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

        paging_base = {
            **base_time,
            "offset": 0,
            "clean_mode": 0,
            "sub_mode": -1,
            "sn": sn.strip(),
        }

        with ThreadPoolExecutor(max_workers=2) as ex:
            futs = [
                ex.submit(_fetch_mode, key, secret, mode_params),
                ex.submit(_fetch_paging_sn_job, key, secret, paging_base),
            ]
            for fut in as_completed(futs):
                layer, data, err = fut.result()
                if err:
                    degraded[layer] = True  # type: ignore[index]
                    errors.append({"layer": layer, "detail": err})
                if layer == "mode":
                    mode_data = data
                else:
                    paging_data = data

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
        "paging": paging_data,
        "paging_list": paging_list,
        "executions": {
            "list": rows or [],
            "offset": execution_offset,
            "used_lim": used_lim,
        },
        "execution_next_offset": execution_offset + used_lim if used_lim else execution_offset,
        "execution_has_more": bool(rows) and used_lim > 0 and len(rows) >= used_lim,
        "cached": False,
    }

    if not skip_cache and execution_offset == 0 and not only_executions:
        with _cache_lock:
            to_store = {k: v for k, v in result.items() if k != "cached"}
            _cache[cache_key] = (time.time(), to_store)
            if len(_cache) > 200:
                for k in list(_cache.keys())[:50]:
                    _cache.pop(k, None)

    logging.info(
        "Pudu dashboard sn=%s range=%s-%s degraded=%s err_count=%s",
        sn[:8],
        start_time,
        end_time,
        degraded,
        len(errors),
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
