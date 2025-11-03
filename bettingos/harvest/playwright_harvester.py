from __future__ import annotations
import json, re, time
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Callable, Iterable
import yaml
from pymongo.errors import PyMongoError
from playwright.sync_api import sync_playwright

from ..db.mongo import get_db
from ..config import SETTINGS

# Where to save local debug captures
DEBUG_DIR = Path("debug/harvest")

def _now_utc():
    return datetime.now(timezone.utc)

def _compile_filters(patterns: Iterable[str]) -> list[Callable[[str], bool]]:
    """Return a list of callables that test if a URL should be kept."""
    out: list[Callable[[str], bool]] = []
    for p in patterns or []:
        p = p.strip()
        if not p:
            continue
        # Treat entries that look like regexes as regex; else substring
        is_regex = any(ch in p for ch in r".*+?[](){}|\^$")
        if is_regex:
            rx = re.compile(p)
            out.append(lambda url, rx=rx: bool(rx.search(url)))
        else:
            out.append(lambda url, sub=p: sub in url)
    if not out:
        # Keep everything if no filter is present
        out.append(lambda _url: True)
    return out

def _matches(url: str, filters: list[Callable[[str], bool]]) -> bool:
    for f in filters:
        if f(url):
            return True
    return False

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def _safe_json(obj: Any) -> Any:
    """Best-effort conversion to JSON-serializable."""
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return {"_note": "non-json-serializable object"}

def harvest_run_once(book_key: str, debug: bool = False) -> int:
    """
    Open each configured page for the given book, capture XHR/JSON responses that
    match 'xhr_allow' filters, save them to Mongo (collection 'harvest_raw') and
    to jsonl in debug/harvest/<book>/.
    Returns the number of JSON payloads inserted into Mongo.
    """
    # --- load harvest config from books.yaml ---
    with open("books.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    book = next((b for b in cfg.get("books", []) if b.get("key") == book_key), None)
    if not book:
        raise RuntimeError(f"book '{book_key}' not found in books.yaml")

    h = book.get("harvest") or {}
    start_urls: list[str] = h.get("start_urls", [])
    xhr_allow: list[str] = h.get("xhr_allow", [])
    wait_for: list[str] = h.get("wait_for", ["body"])
    headless: bool = bool(h.get("headless", True))
    max_time_s: int = int(h.get("max_time_s", 8))

    if not start_urls:
        raise RuntimeError(f"book '{book_key}' has no harvest.start_urls configured")

    filters = _compile_filters(xhr_allow)
    db = get_db()
    raw_coll = db.get_collection("harvest_raw")

    # prepare debug writer
    ts_tag = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = DEBUG_DIR / book_key
    _ensure_dir(out_dir)
    out_path = out_dir / f"{ts_tag}.jsonl"
    fout = out_path.open("a", encoding="utf-8")

    inserted = 0
    total_seen = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=SETTINGS.user_agent,
            ignore_https_errors=True,
            locale="en-US",
        )
        page = context.new_page()

        # capture handler
        def on_response(resp):
            nonlocal inserted, total_seen
            try:
                req = resp.request
                rtype = req.resource_type
                url = resp.url
                if rtype not in ("xhr", "fetch"):
                    return
                if not _matches(url, filters):
                    return
                ctype = resp.headers.get("content-type", "")
                if "json" not in ctype.lower():
                    return

                total_seen += 1
                # May throw if body not JSON
                data = resp.json()
                rec = {
                    "book": book_key,
                    "page_url": page.url,
                    "url": url,
                    "status": resp.status,
                    "content_type": ctype,
                    "captured_at_utc": _now_utc(),
                    "json": data,
                }
                # write debug
                line = json.dumps({
                    "t": rec["captured_at_utc"].isoformat(),
                    "page": rec["page_url"],
                    "url": rec["url"],
                    "status": rec["status"],
                    "ct": rec["content_type"],
                    "json": rec["json"],
                }, ensure_ascii=False)
                fout.write(line + "\n")

                # insert to mongo
                try:
                    raw_coll.insert_one({**rec, "json": _safe_json(data)})
                    inserted += 1
                except PyMongoError:
                    # Still keep debug line; ignore DB error in this prototype
                    pass
            except Exception:
                # swallow per-response errors; this is a best-effort collector
                return

        page.on("response", on_response)

        for url in start_urls:
            page.goto(url, wait_until="networkidle")
            # Optional: wait for DOM hints to stabilize
            for sel in wait_for:
                try:
                    page.wait_for_selector(sel, timeout=5_000)
                except Exception:
                    pass
            # Let background XHRs flow
            time.sleep(max_time_s)

        # Clean up
        page.off("response", on_response)
        context.close()
        browser.close()

    fout.close()
    if debug:
        print(f"[harvest] book={book_key} seen={total_seen} inserted={inserted} file={out_path}")
    return inserted
