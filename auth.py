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


# Token purposes. Every token carries one, and verification only accepts a token whose
# purpose matches what the caller asked for.
#
# Why this exists: a security audit found `aud` was signed into every token but never
# checked on the session path, and the local session cookie each service minted for itself
# carried no `aud` at all -- making that cookie a universal key accepted by every sibling
# service. Purpose and audience are now both enforced.
TYP_SESSION = "session"   # long-lived local login cookie for one service
TYP_HANDOFF = "handoff"   # short-lived cross-service handoff, travels in a URL
TYP_STATE = "state"       # OAuth state parameter, binds one consent-screen round trip


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _key_bytes(secret) -> bytes:
    """Accepts either the master secret (str) or an already-derived key (bytes)."""
    return secret if isinstance(secret, (bytes, bytearray)) else str(secret).encode("utf-8")


def sign_token(payload: dict, secret) -> str:
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(_key_bytes(secret), payload_b64.encode("ascii"), hashlib.sha256).digest()
    return f"{payload_b64}.{_b64url(sig)}"


def verify_token(token, secret, *, aud=None, typ=None):
    """Returns the decoded payload dict on success, or None on any failure
    (malformed token, bad signature, expired, wrong audience, wrong purpose).
    Never raises.

    `aud`/`typ` are optional here only so a token-inspection utility can decode without
    asserting what a token is. Never gate access without passing both -- prefer verify_for.
    """
    if not token or not isinstance(token, str) or "." not in token:
        return None
    payload_b64, sig_b64 = token.split(".", 1)
    try:
        provided = _b64url_decode(sig_b64)
    except Exception:
        return None
    expected = hmac.new(_key_bytes(secret), payload_b64.encode("ascii"), hashlib.sha256).digest()
    if not hmac.compare_digest(provided, expected):
        return None
    try:
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)) or time.time() > exp:
        return None
    # A token minted without a typ is a pre-fix token: reject it rather than grandfathering
    # it in, because "no typ" is exactly what a replayed old token looks like.
    if typ is not None and payload.get("typ") != typ:
        return None
    if aud is not None:
        allowed = {aud} if isinstance(aud, str) else set(aud)
        if payload.get("aud") not in allowed:
            return None
    return payload


# -- Per-audience key derivation -------------------------------------------------------
# Every token is signed with a key derived from the audience it is minted for:
#
#   service_key(aud) = HMAC-SHA256(AUTH_SIGNING_SECRET, "ofw-token-v2|" + aud)
#
# So a token minted for globe-invest cannot verify here -- not because someone remembered
# to compare `aud`, but because the signature is computed under a different key. The audit
# found `aud` signed-but-unchecked in four places; a rule that depends on every call site
# remembering a comparison is a rule that will break again. verify_for() cannot even be
# called without naming the audience, because the audience is what produces the key.
#
# Byte-for-byte identical to deriveKey() in the Node port (auth.js) -- same context string,
# same HMAC, raw 32-byte digest used directly as the token key.
#
# This is deliberately NOT a blast-radius fix: all four services still hold the same master
# secret, so any of them can derive any other's key. Splitting the master out so each
# service holds only its own derived key needs per-service Railway env vars.
KEY_CONTEXT = "ofw-token-v2"


def derive_key(master_secret: str, aud: str) -> bytes:
    return hmac.new(
        str(master_secret).encode("utf-8"),
        (KEY_CONTEXT + "|" + str(aud)).encode("utf-8"),
        hashlib.sha256,
    ).digest()


def sign_for(payload: dict, master_secret: str) -> str:
    """Signs a payload under its own audience's key. Raises rather than silently producing
    a token nobody can verify -- a missing aud/typ is a programming error, not input."""
    if not payload.get("aud") or not isinstance(payload.get("aud"), str):
        raise ValueError("sign_for: payload['aud'] is required")
    if not payload.get("typ") or not isinstance(payload.get("typ"), str):
        raise ValueError("sign_for: payload['typ'] is required")
    return sign_token(payload, derive_key(master_secret, payload["aud"]))


def verify_for(token, master_secret: str, *, aud: str, typ: str):
    """Verifies a token minted for `aud` with purpose `typ`. Both are mandatory."""
    if not aud or not isinstance(aud, str) or not typ or not isinstance(typ, str):
        return None
    return verify_token(token, derive_key(master_secret, aud), aud=aud, typ=typ)


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


# How many reverse proxies sit in front of this service.
#
# X-Forwarded-For is APPENDED to by each proxy: a proxy writes the address it received the
# connection from onto the end. So with one trusted proxy the LAST entry is the only one
# written by infrastructure we control, and everything left of it is attacker text. Reading
# the FIRST entry -- as this did before -- let any client choose its own rate-limit bucket
# just by sending the header, which silently disabled every limiter built on it.
#
# Getting the direction right is only half of it. If NOTHING is in front of the process
# there is no appended entry, so even the last element is still whatever the client typed.
# Hence the header is ignored outright unless we are actually behind a proxy.
def _trusted_proxy_hops() -> int:
    raw = os.getenv("TRUSTED_PROXY_HOPS")
    if raw not in (None, ""):
        try:
            return max(0, int(raw))
        except ValueError:
            return 0
    behind_railway = (
        os.getenv("ENV") == "production"
        or bool(os.getenv("RAILWAY_PROJECT_ID"))
        or bool(os.getenv("RAILWAY_ENVIRONMENT_NAME"))
    )
    return 1 if behind_railway else 0


TRUSTED_PROXY_HOPS = _trusted_proxy_hops()


def client_ip(request) -> str:
    """Extracts a client IP from a Starlette/FastAPI Request. Only consults
    X-Forwarded-For when actually deployed behind a trusted proxy, and then reads the
    proxy-appended entry from the right rather than the client-supplied one."""
    client = getattr(request, "client", None)
    socket_ip = client.host if client else "unknown"
    if TRUSTED_PROXY_HOPS <= 0:
        return socket_ip
    xf = request.headers.get("x-forwarded-for")
    if not xf:
        return socket_ip
    parts = [p.strip() for p in xf.split(",") if p.strip()]
    # A header with fewer entries than there are proxies in front of us cannot have been
    # produced by those proxies -- treat it as forged and fall back to the socket.
    if len(parts) < TRUSTED_PROXY_HOPS:
        return socket_ip
    return parts[len(parts) - TRUSTED_PROXY_HOPS]
