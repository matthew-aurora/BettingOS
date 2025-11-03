from __future__ import annotations
import json, os, yaml, requests
from ...config import SETTINGS
from ...db.mongo import insert_snapshot

BOOKMAKER = "betonline"

HEADERS = {
    "User-Agent": SETTINGS.user_agent,
    "Accept": "application/json, text/plain, */*",
}

def fetch_json(url: str):
    r = requests.get(url, timeout=15, headers=HEADERS)
    r.raise_for_status()
    try: return r.json()
    except Exception: return json.loads(r.text)

def _iter_events(payload):
    if payload is None: return
    if isinstance(payload, list):
        for x in payload: yield from _iter_events(x); return
    if isinstance(payload, dict):
        evs = payload.get("events") or payload.get("data") or []
        if isinstance(evs, list):
            for e in evs: yield e

def map_event_generic(evt: dict, *, sport: str, league: str, source_url: str):
    """Minimal generic mapper placeholder.
       Scraper team will replace with a proper mapper once payload shape is known.
    """
    from ...models.snapshot import Snapshot   # local import
    snaps = []
    eid = str(evt.get("id") or evt.get("eventId") or evt.get("gameId") or "")
    mkts = evt.get("markets") or evt.get("lines") or []
    # Try to detect common shapes:
    for m in mkts:
        desc = str(m.get("description") or m.get("name") or "").lower()
        oc = m.get("outcomes") or m.get("selections") or []
        # moneyline (2-way)
        if any(k in desc for k in ("moneyline","money line","ml")) and len(oc) >= 2:
            home = oc[0]; away = oc[1]
            for label, out in (("home", home), ("away", away)):
                dec = (out.get("price") or out.get("odds") or {}).get("decimal")
                if not dec: continue
                snaps.append(Snapshot(
                    bookmaker=BOOKMAKER, event_key=eid, sport=sport, league=league,
                    kickoff_utc=Snapshot.model_fields["captured_at_utc"].default_factory(),  # placeholder if no startTime
                    market_uid="FT_ML_2W", selection=label, odds_decimal=float(dec),
                    source_url=source_url, parse_stage="normalized", raw={"event": evt, "market": m},
                ))
    return snaps

def run_once() -> int:
    cfg = yaml.safe_load(open("books.yaml","r",encoding="utf-8")) or {}
    book = next((b for b in cfg.get("books", []) if b.get("key") == BOOKMAKER and b.get("enabled")), None)
    if not book: return 0
    feeds = (book.get("proto") or {}).get("feeds") or []
    total = 0
    for f in feeds:
        url = (f.get("url") or "").strip()
        if not url: continue
        payload = fetch_json(url)
        for evt in _iter_events(payload):
            snaps = map_event_generic(evt, sport=str(f.get("sport") or ""), league=str(f.get("league") or ""), source_url=url)
            for s in snaps:
                insert_snapshot(s.doc()); total += 1
    return total
