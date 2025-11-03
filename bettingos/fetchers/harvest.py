from __future__ import annotations
import json, os, re, time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional

from playwright.sync_api import sync_playwright
from ..config import SETTINGS
from ..db.mongo import get_db

@dataclass
class HarvestSpec:
    start_urls: List[str]
    xhr_allow: List[str]                  # substrings or regex to keep
    wait_for: List[str] = None           # CSS selectors to wait for (optional)
    max_time_s: int = 10                 # extra idle time after onload
    headless: bool = True
    screenshot_on_block: bool = True
    user_agent: Optional[str] = None

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _compile_patterns(pats: Iterable[str]) -> List[re.Pattern]:
    out = []
    for p in pats:
        try:
            out.append(re.compile(p, re.I))
        except re.error:
            # treat as plain substring if not valid regex
            out.append(re.compile(re.escape(p), re.I))
    return out

def _match_any(url: str, regs: List[re.Pattern]) -> bool:
    for r in regs:
        if r.search(url):
            return True
    return False

def run_harvest(book_key: str, spec: HarvestSpec) -> int:
    """
    Visit configured pages, capture matching XHR responses,
    persist to disk (data/harvest/<book>/<ts>/N.json) and Mongo (raw_harvest).
    Returns count of captured documents.
    """
    out_dir = Path("data/harvest") / book_key / _now_iso()
    out_dir.mkdir(parents=True, exist_ok=True)
    db = get_db()
    raw = db.get_collection("raw_harvest")
    raw.create_index([("book", 1), ("captured_at_utc", -1)])

    allow_regs = _compile_patterns(spec.xhr_allow or [])
    wait_for = spec.wait_for or []
    ua = spec.user_agent or SETTINGS.user_agent

    captured: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=spec.headless)
        context = browser.new_context(user_agent=ua, locale="en-US", viewport={"width":1280,"height":800})
        page = context.new_page()

        def on_response(resp):
            try:
                url = resp.url
                if not _match_any(url, allow_regs):
                    return
                status = resp.status
                ct = resp.headers.get("content-type","")
                body: Any
                try:
                    body = resp.json()
                except Exception:
                    try:
                        body = resp.text()
                    except Exception:
                        body = None
                doc = {
                    "book": book_key,
                    "url": url,
                    "status": status,
                    "content_type": ct,
                    "captured_at_utc": datetime.now(timezone.utc),
                    "body": body,
                }
                captured.append(doc)
            except Exception:
                # swallow noisy per-response failures
                pass

        page.on("response", on_response)

        for u in spec.start_urls:
            try:
                page.goto(u, wait_until="load", timeout=30000)
                for sel in wait_for:
                    try:
                        page.wait_for_selector(sel, timeout=10000)
                    except Exception:
                        pass
                page.wait_for_timeout(spec.max_time_s * 1000)
            except Exception as e:
                if spec.screenshot_on_block:
                    shot = out_dir / f"blocked_{int(time.time())}.png"
                    try:
                        page.screenshot(path=str(shot), full_page=True)
                    except Exception:
                        pass

        # teardown
        context.close()
        browser.close()

    # persist
    for i, doc in enumerate(captured):
        p = out_dir / f"{i:04d}.json"
        with p.open("w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2, default=str)
        raw.insert_one(doc)

    print(f"[harvest] book={book_key} captured={len(captured)} saved_to={out_dir}")
    return len(captured)
