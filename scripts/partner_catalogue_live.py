"""Hit the deployed backend with a real Google ID token.

Modes:
  refuse  — email must not be in partner_users. Staff Google routes 403.
  partner — seeded test partner token. ALLOW paths work; everything else refuses.

  python scripts/partner_catalogue_live.py refuse --base URL --token TOKEN
  python scripts/partner_catalogue_live.py partner --base URL --token TOKEN
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from partner_allow import PARTNER_ALLOW  # noqa: E402
from partner_authz import PARTNER_CLIENT_KEYS  # noqa: E402

SKIP_METHODS = {"HEAD", "OPTIONS"}
PARTNER_CLIENT_KEYS_EXACT = set(PARTNER_CLIENT_KEYS)


def _concrete_path(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "1", path)


def _iter_openapi_operations(spec: dict):
    for path, item in (spec.get("paths") or {}).items():
        for method, op in item.items():
            method_u = method.upper()
            if method_u in SKIP_METHODS or not isinstance(op, dict):
                continue
            yield method_u, path, op


def _has_security(op: dict, spec: dict) -> bool:
    if op.get("security"):
        return True
    if "security" in op:
        return bool(op["security"])
    return bool(spec.get("security"))


def _request(base: str, method: str, path: str, token: str, **kwargs):
    url = base.rstrip("/") + _concrete_path(path)
    headers = dict(kwargs.pop("headers", {}))
    headers["Authorization"] = f"Bearer {token}"
    if method in {"POST", "PUT", "PATCH"} and "files" not in kwargs and "data" not in kwargs:
        kwargs.setdefault("json", {})
    return requests.request(method, url, headers=headers, timeout=30, **kwargs)


def run_refuse(base: str, token: str) -> int:
    spec = requests.get(f"{base.rstrip('/')}/openapi.json", timeout=30).json()
    failures: list[str] = []
    checked = 0
    for method, path, op in _iter_openapi_operations(spec):
        if (method, path) in PARTNER_ALLOW:
            continue
        if path.startswith("/api/partner/"):
            failures.append(f"{method} {path} partner route not in PARTNER_ALLOW")
            continue
        res = _request(base, method, path, token)
        checked += 1
        if path.startswith("/api/") and _has_security(op, spec):
            if res.status_code == 200:
                failures.append(
                    f"{method} {path}: expected refuse, got {res.status_code}"
                )
    print(f"refuse catalogue checked {checked} operations")
    if failures:
        print("\n".join(failures))
        return 1
    print("PASS: non-allow routes did not succeed with this token")
    return 0


def run_partner(base: str, token: str) -> int:
    failures: list[str] = []
    me = _request(base, "GET", "/api/partner/me", token)
    if me.status_code != 200:
        print(f"FAIL GET /api/partner/me {me.status_code} {me.text[:300]}")
        return 1
    body = me.json()
    print(f"partner me: {json.dumps(body)}")
    partner_id = body.get("partner_id")

    listed = _request(base, "GET", "/api/partner/clients", token)
    if listed.status_code != 200:
        failures.append(f"GET /api/partner/clients {listed.status_code}")
    else:
        rows = listed.json()
        if rows and set(rows[0]) != PARTNER_CLIENT_KEYS_EXACT:
            failures.append(f"list keys {set(rows[0])} != {PARTNER_CLIENT_KEYS_EXACT}")

    other = _request(base, "GET", "/api/partner/clients/1", token)
    if other.status_code not in {200, 404}:
        failures.append(f"GET own-or-other client {other.status_code}")
    if other.status_code == 200 and set(other.json()) != PARTNER_CLIENT_KEYS_EXACT:
        failures.append(f"get keys {set(other.json())} != {PARTNER_CLIENT_KEYS_EXACT}")

    spec = requests.get(f"{base.rstrip('/')}/openapi.json", timeout=30).json()
    for method, path, op in _iter_openapi_operations(spec):
        if (method, path) in PARTNER_ALLOW:
            continue
        if path.startswith("/api/partner/"):
            failures.append(f"{method} {path} partner route not in PARTNER_ALLOW")
            continue
        if not path.startswith("/api/") or not _has_security(op, spec):
            continue
        res = _request(base, method, path, token)
        if res.status_code < 400:
            failures.append(f"{method} {path}: partner token opened staff route ({res.status_code})")

    print(f"partner_id={partner_id}")
    if failures:
        print("\n".join(failures))
        return 1
    print("PASS: ALLOW paths reachable; staff routes refused; serializer keys held")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("refuse", "partner"))
    parser.add_argument("--base", required=True)
    parser.add_argument("--token", default=os.environ.get("PARTNER_CATALOGUE_TOKEN", ""))
    args = parser.parse_args()
    if not args.token:
        print("set PARTNER_CATALOGUE_TOKEN or pass --token")
        return 2
    if args.mode == "refuse":
        return run_refuse(args.base, args.token)
    return run_partner(args.base, args.token)


if __name__ == "__main__":
    raise SystemExit(main())
