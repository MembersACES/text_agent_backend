"""
Prograde DRIFT_EVENT v1 webhook verification (DRIFT_EVENT_v1.md §4.2).

Ported from prograde-contracts PC3 mock_service/signing.py — keep in sync.
"""
from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass
from threading import Lock
from typing import Optional, Union

DEFAULT_TOLERANCE_SECONDS = 300
SCHEME = "v1"
NONCE_TTL_SECONDS = 600


def _to_bytes(body: Union[str, bytes]) -> bytes:
    if isinstance(body, bytes):
        return body
    return body.encode("utf-8")


def sign_payload(
    body: Union[str, bytes],
    secret: str,
    timestamp: Optional[int] = None,
) -> tuple[int, str]:
    t = int(timestamp if timestamp is not None else time.time())
    signed_payload = f"{t}.".encode("utf-8") + _to_bytes(body)
    mac = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256)
    return t, mac.hexdigest()


def build_signature_header(
    body: Union[str, bytes],
    secret: str,
    timestamp: Optional[int] = None,
) -> str:
    t, sig = sign_payload(body, secret, timestamp)
    return f"t={t},{SCHEME}={sig}"


def parse_signature_header(header: str) -> dict[str, str]:
    out: dict[str, str] = {}
    if not header:
        return out
    for part in header.split(","):
        part = part.strip()
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip()
    return out


@dataclass
class VerifyResult:
    ok: bool
    reason: str = ""

    def __bool__(self) -> bool:
        return self.ok


def verify_signature(
    body: Union[str, bytes],
    header: str,
    secret: str,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
    now: Optional[int] = None,
) -> VerifyResult:
    parsed = parse_signature_header(header)
    if "t" not in parsed:
        return VerifyResult(False, "missing timestamp (t) in signature header")
    if SCHEME not in parsed:
        return VerifyResult(False, f"missing {SCHEME} scheme in signature header")

    try:
        t = int(parsed["t"])
    except (TypeError, ValueError):
        return VerifyResult(False, "timestamp (t) is not an integer")

    current = int(now if now is not None else time.time())
    if abs(current - t) > tolerance_seconds:
        return VerifyResult(
            False,
            f"timestamp outside tolerance window "
            f"(|{current} - {t}| = {abs(current - t)}s > {tolerance_seconds}s)",
        )

    _, expected = sign_payload(body, secret, timestamp=t)
    if hmac.compare_digest(expected, parsed[SCHEME]):
        return VerifyResult(True, "ok")
    return VerifyResult(False, "signature mismatch")


class NonceCache:
    """In-memory replay guard for event_id (600s TTL)."""

    def __init__(self, ttl_seconds: int = NONCE_TTL_SECONDS) -> None:
        self._ttl = ttl_seconds
        self._seen: dict[str, float] = {}
        self._lock = Lock()

    def check_and_store(self, event_id: str, now: Optional[float] = None) -> bool:
        """Return True if new; False if duplicate within TTL."""
        if not event_id:
            return True
        ts = now if now is not None else time.time()
        with self._lock:
            self._purge(ts)
            if event_id in self._seen:
                return False
            self._seen[event_id] = ts
            return True

    def _purge(self, now: float) -> None:
        cutoff = now - self._ttl
        expired = [k for k, v in self._seen.items() if v < cutoff]
        for k in expired:
            del self._seen[k]


_nonce_cache = NonceCache()


def get_nonce_cache() -> NonceCache:
    return _nonce_cache
