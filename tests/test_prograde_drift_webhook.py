"""Tests for Prograde DRIFT_EVENT v1 §4.2 HMAC verification."""
import hashlib
import hmac

from services.prograde_drift_webhook import (
    NonceCache,
    build_signature_header,
    sign_payload,
    verify_signature,
)

SECRET = "whsec_test_prograde"


def test_pinned_vector_4_3():
    body = '{"event_id":"drift_demo","event_type":"standards.drift.review_required"}'
    t, v1 = sign_payload(body, SECRET, timestamp=1749000000)
    assert t == 1749000000
    expected = hmac.new(
        b"whsec_test_prograde",
        f"1749000000.{body}".encode(),
        hashlib.sha256,
    ).hexdigest()
    assert v1 == expected


def test_sign_then_verify_ok():
    body = b'{"hello":"world"}'
    header = build_signature_header(body, SECRET)
    assert verify_signature(body, header, SECRET).ok


def test_verify_rejects_tampered_body():
    body = b'{"amount":1}'
    header = build_signature_header(body, SECRET)
    res = verify_signature(b'{"amount":999}', header, SECRET)
    assert not res.ok and res.reason == "signature mismatch"


def test_verify_rejects_expired_timestamp():
    body = b'{"x":1}'
    header = build_signature_header(body, SECRET, timestamp=1)
    res = verify_signature(body, header, SECRET, now=10_000)
    assert not res.ok and "tolerance" in res.reason


def test_nonce_cache_rejects_duplicate():
    cache = NonceCache(ttl_seconds=600)
    assert cache.check_and_store("drift_abc", now=1000.0)
    assert not cache.check_and_store("drift_abc", now=1001.0)
    assert cache.check_and_store("drift_abc", now=1700.0)
