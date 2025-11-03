from __future__ import annotations
import yaml
from ...config import SETTINGS
from ...fetchers.harvest import HarvestSpec, run_harvest

def run_once(book_key: str) -> int:
    # Read spec from books.yaml
    data = yaml.safe_load(open("books.yaml","r",encoding="utf-8")) or {}
    book = next((b for b in data.get("books", []) if b.get("key") == book_key and b.get("enabled")), None)
    if not book:
        print(f"[browser-proto] book '{book_key}' not enabled or missing in books.yaml")
        return 0
    harvest = (book.get("harvest") or {})
    start_urls = [str(u) for u in (harvest.get("start_urls") or [])]
    xhr_allow = [str(x) for x in (harvest.get("xhr_allow") or [])]
    wait_for = [str(s) for s in (harvest.get("wait_for") or [])]
    max_time_s = int(harvest.get("max_time_s", 10))
    headless = bool(harvest.get("headless", True))
    spec = HarvestSpec(
        start_urls=start_urls,
        xhr_allow=xhr_allow,
        wait_for=wait_for,
        max_time_s=max_time_s,
        headless=headless,
        user_agent=SETTINGS.user_agent,
    )
    return run_harvest(book_key, spec)
