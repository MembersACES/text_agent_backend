"""Pudu cleanbot open task definitions (not executions)."""

from __future__ import annotations

from typing import Any, Optional, Tuple

from services.pudu_dashboard import get_pudu_credentials
from services.pudu_signed_request import pudu_message_ok, signed_pudu_get


def fetch_open_task_list(shop_id: str, sn: str) -> Tuple[Optional[Any], Optional[str]]:
    key, secret = get_pudu_credentials()
    if not key or not secret:
        raise RuntimeError("PUDU_APP_KEY and PUDU_SECRET_KEY are not configured")
    q = {"shop_id": str(shop_id).strip(), "sn": str(sn).strip()}
    body, _, err = signed_pudu_get(
        key,
        secret,
        "/cleanbot-service/v1/api/open/task/list",
        q,
    )
    if err:
        return None, err
    if not pudu_message_ok(body):
        return body, str(body.get("message", "unexpected message")) if isinstance(body, dict) else "bad message"
    data = body.get("data") if isinstance(body, dict) else None
    return data, None
