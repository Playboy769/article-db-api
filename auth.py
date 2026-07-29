"""Shared HMAC-signed token + cookie + CSRF + rate-limit helpers for the
cross-domain login system (OutsideFramework central auth + this service).
Stdlib only -- no PyJWT/itsdangerous/python-jose, to avoid adding a new
dependency to a security-critical code path.

Token format: base64url(JSON payload) + "." + base64url(HMAC-SHA256(payload_b64, secret))
Verification HMACs the *received* payload_b64 string directly (never
re-serializes JSON), so this is byte-for-byte compatible with the Node
version of this same file (auth.js in the repo root and globe-invest/)
without any cross-language JSON canonicalization concerns.

IMPORTANT: keep this file identical across every repo that copies it
(structural_holes/, article_db/). AUTH_SIGNING_SECRET must also be
identical (same Railway env var value) across all four deployed services.
"""

import base64
import hashlib
import hmac
import json
import os
import secrets as _secrets
import time
from urllib.parse import urlparse

ALLOWED_ORIGINS = {
    "https://ofw.up.railway.app",
    "https://globe-invest.up.railway.app",
    "https://structural-holes-production.up.railway.app",
    "https://articlebase.up.railway.app",
}

# Only honored when ENV != "production" -- lets return_to/redirect
# validation work during local development without weakening prod checks.
DEV_ORIGINS = {
    "http://localhost:8125",  # outside-framework
    "http://localhost:8124",  # globe
    "http://localhost:8129",  # structural-holes (local dev)
    "http://localhost:8127",  # article-db (local dev)
}


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def sign_token(payload: dict, secret: str) -> str:
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(secret.encode("utf-8"), payload_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url(sig)}"


def verify_token(token, secret: str):
    """Returns the decoded payload dict on success, or None on any failure
    (malformed token, bad signature, expired). Never raises."""
    if not token or not isinstance(token, str) or "." not in token:
        return None
    payload_b64, sig_b64 = token.split(".", 1)
    try:
        provided = _b64url_decode(sig_b64)
    except Exception:
        return None
    expected = hmac.new(secret.encode("utf-8"), payload_b64.encode("ascii"), hashlib.sha256).digest()
    if not hmac.compare_digest(provided, expected):
        return None
    try:
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception:
        return None
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)) or time.time() > exp:
        return None
    return payload


def random_token(nbytes: int = 24) -> str:
    return _b64url(_secrets.token_bytes(nbytes))


def _origin_of(url: str):
    p = urlparse(url)
    if not p.scheme or not p.netloc:
        return None
    return f"{p.scheme}://{p.netloc}"


def is_allowed_return_to(url: str) -> bool:
    origin = _origin_of(url)
    if not origin:
        return False
    if origin in ALLOWED_ORIGINS:
        return True
    if os.getenv("ENV") != "production" and origin in DEV_ORIGINS:
        return True
    return False


def parse_cookies(header: str) -> dict:
    out = {}
    if not header:
        return out
    for part in header.split(";"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        if k:
            out[k] = v
    return out


def cookie_header(name: str, value: str, max_age=None, http_only=True, secure=True,
                   same_site="Lax", path="/") -> str:
    out = f"{name}={value}; Path={path}; SameSite={same_site}"
    if http_only:
        out += "; HttpOnly"
    if secure:
        out += "; Secure"
    if max_age is not None:
        out += f"; Max-Age={max_age}"
    return out


def clear_cookie_header(name: str, path="/") -> str:
    return cookie_header(name, "", max_age=0, path=path)


class RateLimiter:
    """In-memory fixed-window limiter, keyed by an arbitrary string (client IP).

    Resets on process restart -- intentionally not a distributed limiter;
    this is a personal single-instance deployment and the goal is raising
    the cost of naive automated abuse, not building a full defense.
    """

    def __init__(self, window_seconds: int = 600, max_hits: int = 20):
        self.window = window_seconds
        self.max_hits = max_hits
        self._hits = {}

    def check(self, key: str) -> bool:
        now = time.time()
        entry = self._hits.get(key)
        if not entry or now > entry[1]:
            entry = [0, now + self.window]
            self._hits[key] = entry
        entry[0] += 1
        if len(self._hits) > 5000:
            for k in list(self._hits):
                if now > self._hits[k][1]:
                    del self._hits[k]
        return entry[0] <= self.max_hits


def client_ip(request) -> str:
    """Extracts a client IP from a Starlette/FastAPI Request, preferring
    X-Forwarded-For (Railway sits behind a proxy)."""
    xf = request.headers.get("x-forwarded-for")
    if xf:
        return xf.split(",")[0].strip()
    client = getattr(request, "client", None)
    return client.host if client else "unknown"
