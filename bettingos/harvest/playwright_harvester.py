from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

import yaml
from playwright.sync_api import sync_playwright

from ..config import SETTINGS
from ..db.mongo import get_db


@dataclass
class HarvestCfg:
    start_urls: list[str]
    xhr_allow: list[str]
    wait_for: list[str]
    headless: bool
    max_time_s: int


def _load_book_cfg(book_key: str) -> HarvestCfg:
    with open("books.yaml", "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    book = next((b for b in data.get("books", []) if b.get("key") == book_key), None)
    if not book:
        raise SystemExit(f"book '{book_key}' not found in books.yaml")

    h = (book.get("harvest") or {})
    start_urls = h.get("start_urls") or []
    if not start_urls:
        raise SystemExit(f"book '{book_key}' has no harvest.start_urls configured")

    xhr_allow = h.get("xhr_allow") or []
    wait_for = h.get("wait_for") or ["body"]
    headless = bool(h.get("headless", True))
    max_time_s = int(h.get("max_time_s", 8))

    return HarvestCfg(
        start_urls=start_urls,
        xhr_allow=xhr_allow,
        wait_for=wait_for,
        headless=headless,
        max_time_s=max_time_s,
    )


def _compile_patterns(patterns: Iterable[str]) -> list[re.Pattern]:
    out = []
    for p in patterns:
        try:
            out.append(re.compile(p))
        except re.error:
            # treat as literal substring if regex fails
            out.append(re.compile(re.escape(p)))
    return out


def run_once(book_key: str, debug: bool = False) -> int:
    """
    Open each configured start_url and capture XHR/Fetch responses whose URLs
    match any `xhr_allow` pattern. Store lightweight records in `harvest_logs`.
    Returns the number of matched XHR responses.
    """
    cfg = _load_book_cfg(book_key)
    allow = _compile_patterns(cfg.xhr_allow)
    db = get_db()
    coll = db.get_collection("harvest_logs")

    hits = []

    def _allowed(url: str) -> bool:
        if not allow:
            return True
        return any(p.search(url) for p in allow)

    now = datetime.now(timezone.utc)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=cfg.headless)
        context = browser.new_context(user_agent=SETTINGS.user_agent)

        # capture responses for the whole context
        def on_response(resp):
            try:
                req = resp.request
                if req.resource_type not in ("xhr", "fetch"):
                    return
                url = resp.url
                if not _allowed(url):
                    return
                status = resp.status
                ct = resp.headers.get("content-type", "")
                length = int(resp.headers.get("content-length") or 0)

                j = None
                if "json" in ct.lower():
                    try:
                        j = resp.json()
                    except Exception:
                        # fall back to text, capped
                        try:
                            txt = resp.text()
                            j = {"_text": txt[:4000]}
                        except Exception:
                            j = {"_note": "unreadable body"}

                hits.append(
                    {
                        "book": book_key,
                        "url": url,
                        "status": status,
                        "content_type": ct,
                        "length": length,
                        "captured_at_utc": now,
                        "json": j,
                    }
                )
                if debug:
                    print(f"[harvest] {status} {url} ct={ct} len={length or '-'}")
            except Exception as e:
                if debug:
                    print(f"[harvest] response error: {e}")

        context.on("response", on_response)

        for u in cfg.start_urls:
            page = context.new_page()
            if debug:
                print(f"[harvest] open {u}")
            try:
                page.goto(u, timeout=45000, wait_until="domcontentloaded")
                for sel in cfg.wait_for:
                    try:
                        page.wait_for_selector(sel, timeout=8000)
                    except Exception:
                        if debug:
                            print(f"[harvest] wait_for_selector timeout: {sel}")
                # let XHR settle
                time.sleep(max(1, cfg.max_time_s))
            finally:
                page.close()

        context.close()
        browser.close()

    # persist lightweight logs (cap json size to keep DB sane)
    docs = []
    for h in hits:
        j = h.get("json")
        # keep it small
        if isinstance(j, (list, dict)):
            try:
                js = json.dumps(j)
                if len(js) > 20000:
                    j = {"_truncated": True}
            except Exception:
                j = {"_note": "non-serializable"}
        h["json"] = j
        docs.append(h)

    if docs:
        coll.insert_many(docs, ordered=False)

    # quick summary
    if debug:
        by_host = {}
        for h in hits:
            host = re.sub(r"^https?://", "", h["url"]).split("/")[0]
            by_host[host] = by_host.get(host, 0) + 1
        print("[harvest] summary by host:", by_host)

    return len(hits)
