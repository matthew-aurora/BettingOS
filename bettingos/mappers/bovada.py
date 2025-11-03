from __future__ import annotations
import os
from typing import Any, List, Optional
from datetime import datetime, timezone

from ..models.snapshot import Snapshot

VERBOSE = os.getenv("BOVADA_DEBUG", "0") not in ("0", "false", "False")

def _dbg(msg: str) -> None:
    if VERBOSE:
        print(f"[bovada][map] {msg}")

def _to_dt(ts: int) -> datetime:
    # Bovada often returns milliseconds
    if ts and ts > 10_000_000_000:
        ts //= 1000
    return datetime.fromtimestamp(int(ts or 0), tz=timezone.utc)

def _lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _find_markets(event: dict[str, Any]) -> list[dict[str, Any]]:
    groups = event.get("displayGroups") or []
    out: list[dict[str, Any]] = []
    for g in groups:
        for m in (g or {}).get("markets") or []:
            out.append(m)
    return out

# --- Relaxed "full-time" detector ---
_ALLOWED_FULLTIME_TOKENS = {
    "m", "match", "ft", "full time",
    "g", "game", "reg", "regular", "regulation",
}
_PARTIAL_TOKENS = {
    "1h", "2h", "q1", "q2", "q3", "q4",
    "1st", "2nd", "3rd",
    "1p", "2p", "3p",
    "set", "map", "inning", "period",
    "first half", "second half", "first period", "second period", "third period",
}

def _is_fulltime(market: dict[str, Any]) -> bool:
    period = (market.get("period") or {})
    abbrev = _lower(period.get("abbreviation")) or _lower(period.get("description")) or ""
    # If period is missing, assume full game
    if not abbrev:
        return True
    if any(tok in abbrev for tok in _ALLOWED_FULLTIME_TOKENS):
        return True
    if any(tok in abbrev for tok in _PARTIAL_TOKENS):
        return False
    # Default to true — we prefer to include rather than drop valid markets
    return True

def _price_decimal(outcome: dict[str, Any]) -> Optional[float]:
    price = outcome.get("price") or {}
    dec = price.get("decimal")
    try:
        return float(dec) if dec is not None else None
    except Exception:
        return None

def _price_handicap(outcome: dict[str, Any]) -> Optional[float]:
    price = outcome.get("price") or {}
    h = price.get("handicap")
    try:
        return float(h) if h is not None else None
    except Exception:
        return None

# --- broader keyword sets ---
KW_3WAY = ("3-way", "3 way", "match result", "result", "regulation", "regular time")
KW_ML = ("moneyline", "money line", "ml")
KW_SPREAD = ("point spread", "puck line", "run line", "spread", "handicap", "line")
KW_TOTAL = ("total", "totals", "over/under", "o/u")

def map_event(event: dict[str, Any], *, bookmaker: str, sport: str, league: str, source_url: str) -> List[Snapshot]:
    """
    Normalize a single Bovada event into Snapshot rows across:
    - FT_1X2 (3-way)
    - FT_ML_2W (2-way moneyline)
    - FT_SPREAD (point/puck/run line) with param
    - FT_TOTAL (over/under) with param
    """
    snaps: List[Snapshot] = []
    eid = str(event.get("id") or event.get("eventId") or "")
    kickoff = _to_dt(event.get("startTime") or 0)

    comps = event.get("competitors") or []
    home_name = away_name = None
    for c in comps:
        n = c.get("name") or c.get("shortName")
        if c.get("home"): home_name = n
        else: away_name = n
    home_name = home_name or (comps[0]["name"] if comps else "Home")
    away_name = away_name or (comps[1]["name"] if len(comps) > 1 else "Away")

    total_markets = 0
    mapped_markets = 0

    for m in _find_markets(event):
        total_markets += 1
        if not _is_fulltime(m):
            continue

        desc = _lower(m.get("description") or m.get("shortName") or "")
        oc = m.get("outcomes") or []
        if not oc:
            continue

        # 3-way (soccer + some hockey "regulation" lines)
        if (any(k in desc for k in KW_3WAY) and len(oc) >= 3):
            by_desc = { _lower(o.get("description")): o for o in oc }
            draw = by_desc.get("draw") or by_desc.get("tie")
            # try match teams by name; fallback by order
            home_o = next((o for o in oc if home_name and home_name.lower() in _lower(o.get("description"))), None) or oc[0]
            away_o = next((o for o in oc if away_name and away_name.lower() in _lower(o.get("description"))), None) or oc[-1]
            ordered = [home_o, draw or (oc[1] if len(oc) > 1 else None), away_o]
            labels = ["home", "draw", "away"]
            for label, outc in zip(labels, ordered):
                if not outc:
                    continue
                dec = _price_decimal(outc)
                if not dec:
                    continue
                snaps.append(Snapshot(
                    bookmaker=bookmaker,
                    event_key=eid,
                    sport=sport, league=league, kickoff_utc=kickoff,
                    market_uid="FT_1X2",
                    selection=label,
                    odds_decimal=dec,
                    source_url=source_url,
                    parse_stage="normalized",
                    raw={"event": {"home": home_name, "away": away_name}, "market": m},
                ))
            mapped_markets += 1
            continue

        # 2-way Moneyline (NBA/NHL/Tennis/etc.)
        if any(k in desc for k in KW_ML) and len(oc) >= 2:
            home_o = next((o for o in oc if home_name and home_name.lower() in _lower(o.get("description"))), None)
            away_o = next((o for o in oc if away_name and away_name.lower() in _lower(o.get("description"))), None)
            # Fallback to first two outcomes if name matching doesn't work
            if not home_o or not away_o:
                if len(oc) >= 2:
                    home_o = home_o or oc[0]
                    away_o = away_o or oc[1]
            for label, outc in (("home", home_o), ("away", away_o)):
                if not outc:
                    continue
                dec = _price_decimal(outc)
                if not dec:
                    continue
                snaps.append(Snapshot(
                    bookmaker=bookmaker,
                    event_key=eid,
                    sport=sport, league=league, kickoff_utc=kickoff,
                    market_uid="FT_ML_2W",
                    selection=label,
                    odds_decimal=dec,
                    source_url=source_url,
                    parse_stage="normalized",
                    raw={"event": {"home": home_name, "away": away_name}, "market": m},
                ))
            mapped_markets += 1
            continue

        # Spreads (Point/Puck/Run line)
        if any(k in desc for k in KW_SPREAD) and len(oc) >= 2:
            for outc in oc:
                d = _lower(outc.get("description"))
                dec = _price_decimal(outc)
                hcap = _price_handicap(outc)
                if not dec or hcap is None:
                    continue
                label = "home" if (home_name and home_name.lower() in d) else "away"
                snaps.append(Snapshot(
                    bookmaker=bookmaker,
                    event_key=eid,
                    sport=sport, league=league, kickoff_utc=kickoff,
                    market_uid="FT_SPREAD",
                    selection=label,
                    param=hcap,
                    odds_decimal=dec,
                    source_url=source_url,
                    parse_stage="normalized",
                    raw={"event": {"home": home_name, "away": away_name}, "market": m},
                ))
            mapped_markets += 1
            continue

        # Totals (Over/Under)
        if any(k in desc for k in KW_TOTAL) and len(oc) >= 2:
            over = next((o for o in oc if "over" in _lower(o.get("description"))), None)
            under = next((o for o in oc if "under" in _lower(o.get("description"))), None)
            # Fallback if labels missing
            if not over and len(oc) >= 1:
                over = oc[0]
            if not under and len(oc) >= 2:
                under = oc[1]
            for label, outc in (("over", over), ("under", under)):
                if not outc:
                    continue
                dec = _price_decimal(outc)
                hcap = _price_handicap(outc)
                if not dec or hcap is None:
                    continue
                snaps.append(Snapshot(
                    bookmaker=bookmaker,
                    event_key=eid,
                    sport=sport, league=league, kickoff_utc=kickoff,
                    market_uid="FT_TOTAL",
                    selection=label,
                    param=hcap,
                    odds_decimal=dec,
                    source_url=source_url,
                    parse_stage="normalized",
                    raw={"event": {"home": home_name, "away": away_name}, "market": m},
                ))
            mapped_markets += 1
            continue

    if VERBOSE:
        try:
            total_m = sum(len((g or {}).get("markets") or []) for g in (event.get("displayGroups") or []))
        except Exception:
            total_m = total_markets
        _dbg(f"event id={eid} markets={total_m} mapped={mapped_markets} snaps={len(snaps)}")

    return snaps
