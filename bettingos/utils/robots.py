from __future__ import annotations
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib import robotparser
import httpx
from pymongo.database import Database

COLL = "robots_cache"

def _host(url: str) -> str:
    p = urlparse(url)
    scheme = p.scheme or "https"
    return f"{scheme}://{p.netloc}"

def fetch_and_store(db: Database, base_url: str) -> dict:
    origin = _host(base_url)
    robots_url = f"{origin}/robots.txt"
    doc = {
        "host": urlparse(base_url).netloc,
        "robots_url": robots_url,
        "status": None,
        "content": "",
        "fetched_at_utc": datetime.now(timezone.utc),
        "error": None,
    }
    try:
        with httpx.Client(timeout=10) as c:
            r = c.get(robots_url)
            doc["status"] = r.status_code
            if r.is_success:
                doc["content"] = r.text
    except Exception as e:
        doc["error"] = str(e)
    db.get_collection(COLL).replace_one({"host": doc["host"]}, doc, upsert=True)
    return doc

def is_allowed(db: Database, user_agent: str, url: str) -> bool:
    host = urlparse(url).netloc
    rp = robotparser.RobotFileParser()
    cached = db.get_collection(COLL).find_one({"host": host})
    if cached and cached.get("content"):
        rp.parse(cached["content"].splitlines())
    else:
        origin = _host(url)
        rp.set_url(f"{origin}/robots.txt")
        rp.read()
    return rp.can_fetch(user_agent, url)
