from __future__ import annotations
import json
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlsplit, urlunsplit, parse_qsl, urlencode
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential
from ..config import SETTINGS
from .circuits import CircuitBreaker

ETAG_CACHE = Path(".etag_cache.json")
_circuit = CircuitBreaker()

_headers = {
    "User-Agent": SETTINGS.user_agent,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

if ETAG_CACHE.exists():
    _etags = json.loads(ETAG_CACHE.read_text())
else:
    _etags = {}


def _save_cache():
    ETAG_CACHE.write_text(json.dumps(_etags))


def _domain(url: str) -> str:
    return urlparse(url).netloc


def _add_nocache(url: str) -> str:
    parts = urlsplit(url)
    qs = dict(parse_qsl(parts.query, keep_blank_values=True))
    qs["_ts"] = "nc"  # stable but different from common cache keys
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(qs), parts.fragment))


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
def fetch(url: str, timeout: float = 10.0, nocache: bool = False) -> httpx.Response:
    """
    HTTP GET with polite headers, optional ETag caching, and an opt-out (nocache).
    When nocache=True we:
      - do NOT send If-None-Match,
      - add Cache-Control: no-cache,
      - append a _ts query param to bypass intermediary caches (e.g., CF).
    """
    dom = _domain(url)
    if not _circuit.allow(dom):
        raise RuntimeError(f"Circuit open for {dom}")

    headers = dict(_headers)
    req_url = url
    if nocache:
        headers["Cache-Control"] = "no-cache"
    else:
        etag = _etags.get(url)
        if etag:
            headers["If-None-Match"] = etag

    if nocache:
        req_url = _add_nocache(url)

    try:
        with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as c:
            r = c.get(req_url)
    except Exception:
        _circuit.record_failure(dom)
        raise

    if r.status_code in (429, 500, 502, 503, 504):
        _circuit.record_failure(dom)

    # Only learn/store etags when nocache is False.
    if not nocache and r.status_code == 304:
        # Synthesize an empty body; caller must handle "no update".
        r._content = b""  # type: ignore[attr-defined]
        return r

    if not nocache:
        et = r.headers.get("ETag")
        if et:
            _etags[url] = et
            _save_cache()
    return r
