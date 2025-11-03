from __future__ import annotations
import json
from typing import Any, Iterable
from datetime import datetime, timezone
import yaml

from ...utils.http import fetch
from ...db.mongo import insert_snapshot
from ...models.snapshot import Snapshot

BOOKMAKER = "bovada"

def _to_decimal(price_obj: dict) -> float | None:
    if not isinstance(price_obj, dict):
        return None
    dec = price_obj.get("decimal")
    if isinstance(dec, (int, float)) and dec > 0:
        return float(dec)
    am = price_obj.get("american")
    if isinstance(am, str) and am.strip():
        try:
            x = int(am)
            if x > 0:
                return 1.0 + x / 100.0
            if x < 0:
                return 1.0 + 100.0 / abs(x)
        except ValueError:
            return None
    return None

def _norm_sel(out_desc: str, out_type: str | None, home_name: str, away_name: str) -> str | None:
    s = (out_desc or "").strip().lower()
    t = (out_type or "").strip().upper()
    # Bovada often sets type: "H", "A", "D" (draw), or "O"/"U" for totals
    if t in ("H", "HOME"): return "home"
    if t in ("A", "AWAY"): return "away"
    if t in ("D", "DRAW", "X"): return "draw"
    if t in ("O", "OVER"): return "over"
    if t in ("U", "UNDER"): return "under"
    # Fallback by comparing strings
    if s in ("over", "o"): return "over"
    if s in ("under", "u"): return "under"
    if home_name and s.lower().startswith(home_name.lower()): return "home"
    if away_name and s.lower().startswith(away_name.lower()): return "away"
    if s == "draw": return "draw"
    return None

def _line_from(out: dict, market: dict) -> float | None:
    # Try common spots: price.handicap / price.hdp, outcome.spread / line
    p = out.get("price") or {}
    for k in ("handicap", "hdp", "line", "points", "spread"):
        v = p.get(k, out.get(k, market.get(k)))
        if isinstance(v, (int, float)):
            return float(v)
        # sometimes as string
        if isinstance(v, str):
            try:
                return float(v.replace("+",""))
            except ValueError:
                pass
    return None

def _is_live(evt: dict) -> bool:
    # Bovada marks live in multiple ways; be permissive.
    if evt.get("live") is True:
        return True
    status = (evt.get("status") or "").lower()
    if "live" in status or "inplay" in status or "in-play" in status:
        return True
    return False

def _kickoff(evt: dict) -> datetime | None:
    # startTime often in ms
    ts = evt.get("startTime")
    if isinstance(ts, (int, float)) and ts > 10_000_000_000:
        ts /= 1000.0
    if isinstance(ts, (int, float)) and ts > 0:
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    return None

def _home_away(evt: dict) -> tuple[str, str]:
    home = away = ""
    for comp in evt.get("competitors") or []:
        if comp.get("home"):
            home = comp.get("name") or comp.get("description") or ""
        else:
            away = comp.get("name") or comp.get("description") or ""
    # Fallback via description like "Team A vs Team B"
    if not (home and away):
        desc = evt.get("description") or ""
        if " vs " in desc.lower():
            a, b = desc.split(" vs ", 1)
            if not home: home = a.strip()
            if not away: away = b.strip()
    return home, away

def _market_uid(mdesc: str, mm: dict) -> str | None:
    mdesc_low = (mdesc or "").lower()
    # Book-specific mapping comes from books.yaml
    # We’ll search keywords to decide UID
    for uid, conf in (mm or {}).items():
        kws = [k.lower() for k in conf.get("description_keywords", [])]
        if any(k in mdesc_low for k in kws):
            return uid
    return None

def _yield_from_event(evt: dict, market_map: dict, src_url: str) -> Iterable[Snapshot]:
    home, away = _home_away(evt)
    in_play = _is_live(evt)
    ko = _kickoff(evt)
    event_key = str(evt.get("id") or evt.get("eventId") or evt.get("link") or evt.get("description"))

    for dg in evt.get("displayGroups") or []:
        for m in dg.get("markets") or []:
            mdesc = m.get("description") or ""
            uid = _market_uid(mdesc, market_map)
            if not uid:
                continue
            for out in m.get("outcomes") or []:
                price = _to_decimal(out.get("price") or {})
                if not price:
                    continue
                sel = _norm_sel(out.get("description",""), out.get("type"), home, away)
                if not sel:
                    continue
                param = _line_from(out, m) if uid in ("FT_SPREAD","FT_TOTAL") else None
                yield Snapshot(
                    bookmaker=BOOKMAKER,
                    event_key=event_key,
                    market_uid=uid,
                    selection=sel,
                    odds_decimal=float(price),
                    param=param,
                    line_status="live" if in_play else "open",
                    spider_version="proto/bovada_v1",
                    source_url=src_url,
                    raw={"evt_id": evt.get("id"), "mdesc": mdesc, "out": out},
                )

def run_once() -> int:
    with open("books.yaml","r",encoding="utf-8") as f:
        books = yaml.safe_load(f) or {}
    bov = next((b for b in books.get("books",[]) if b.get("key")=="bovada"), None)
    if not bov or not bov.get("enabled", False):
        return 0

    feeds = (bov.get("proto") or {}).get("feeds") or []
    market_map = bov.get("market_map") or {}
    inserted = 0

    for feed in feeds:
        url = feed.get("url")
        if not url:
            continue
        try:
            # nocache=True is critical for “live” behavior with CF/ETag
            r = fetch(url, timeout=15, nocache=True)
        except Exception as e:
            print(f"[bovada] ERROR fetching {url}: {e}")
            continue

        if r.status_code != 200:
            print(f"[bovada] WARN {url} -> {r.status_code}")
            continue

        if not r.content:
            # 304 path isn’t expected with nocache=True; just skip
            continue

        try:
            payload = r.json()
        except Exception:
            # Some endpoints wrap JSON in text; try to parse
            try:
                payload = json.loads(r.text)
            except Exception as e:
                print(f"[bovada] ERROR parse json {url}: {e}")
                continue

        # Bovada returns a list of category blocks; each has "events"
        blocks = payload if isinstance(payload, list) else [payload]
        events = []
        for b in blocks:
            evs = b.get("events") if isinstance(b, dict) else None
            if isinstance(evs, list):
                events.extend(evs)

        snaps = 0
        for evt in events:
            for snap in _yield_from_event(evt, market_map, url):
                insert_snapshot(snap.doc())
                inserted += 1
                snaps += 1

        if snaps == 0:
            # Debug visibility without being noisy
            print(f"[bovada] feed sport={feed.get('sport')} league={feed.get('league')} -> events={len(events)}, snaps=0")

    print(f"[bovada] total snapshots inserted: {inserted}")
    return inserted
