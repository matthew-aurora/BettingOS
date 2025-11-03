# bettingos/spiders/proto/cloudbet_proto.py
from __future__ import annotations

import os
from typing import Any, Dict, Iterable, List, Optional
from datetime import datetime, timezone

import httpx
import yaml

from ...models.snapshot import Snapshot
from ...db.mongo import insert_snapshot

BOOKMAKER = "cloudbet"

# Map Cloudbet market keys -> our normalized market_uids and required selection labels
MARKET_MAP: dict[str, tuple[str, tuple[str, ...]]] = {
    "soccer.matchOdds": ("FT_1X2", ("home", "draw", "away")),
    "basketball.moneyline": ("FT_ML_2W", ("home", "away")),
    "baseball.moneyline": ("FT_ML_2W", ("home", "away")),
    "ice-hockey.moneyline": ("FT_ML_2W", ("home", "away")),
    "tennis.winner": ("FT_ML_2W", ("home", "away")),
}

# Outcome normalization
_OUTCOME_MAP = {
    "home": "home", "1": "home", "team1": "home", "player1": "home", "hometeam": "home",
    "away": "away", "2": "away", "team2": "away", "player2": "away", "awayteam": "away",
    "draw": "draw", "x": "draw", "tie": "draw",
}

def _get_api_key() -> str:
    key = os.getenv("CLOUDBET_API_KEY", "").strip()
    if not key:
        raise RuntimeError("CLOUDBET_API_KEY is not set. Add it to .env or your shell before running.")
    return key

def _client() -> httpx.Client:
    return httpx.Client(
        headers={
            "User-Agent": os.getenv("USER_AGENT", "BettingOS/0.1"),
            "Accept": "application/json",
            "X-API-Key": _get_api_key(),
        },
        timeout=15.0,
        follow_redirects=True,
    )

def _load_proto_block() -> dict:
    with open("books.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    for b in cfg.get("books", []):
        if b.get("key") == "cloudbet":
            return b.get("proto", {}) or {}
    return {}

# ------------------------------- Kickoff helpers -------------------------------

# Keys we’ll look for anywhere in the event (deep search) to find kickoff
_KICKOFF_KEYS = [
    "startTime", "start_time", "startsAt", "start", "kickoff", "kickOff",
    "kickoffTime", "scheduledStartTime", "startTimestamp", "start_time_unix",
    "startTimeUnix", "startTimeSec", "time", "t",
    # common nested carriers
    "epoch", "seconds", "millis", "ms",
    # sometimes in nested blocks:
    "fixtureStartTime", "eventStart", "startDate",
]

def _coerce_kickoff(value: Any) -> Optional[datetime]:
    """
    Accept ISO8601 str, epoch seconds, epoch millis, or nested dicts.
    Return tz-aware UTC datetime or None if not parseable.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            # sometimes numeric encoded as string
            try:
                f = float(value)
                if f > 10_000_000_000:
                    f = f / 1000.0
                return datetime.fromtimestamp(f, tz=timezone.utc)
            except Exception:
                return None
    if isinstance(value, (int, float)):
        f = float(value)
        if f > 10_000_000_000:
            f = f / 1000.0
        try:
            return datetime.fromtimestamp(f, tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, dict):
        # try common numeric leaves
        for k in ("epoch", "seconds", "millis", "ms"):
            if k in value:
                return _coerce_kickoff(value[k])
        # generic try on all values
        for v in value.values():
            dt = _coerce_kickoff(v)
            if dt:
                return dt
    return None

def _deep_get_kickoff(event: dict, debug: bool = False) -> Optional[datetime]:
    """
    Deep search the event for a plausible kickoff field using a BFS over dict/list.
    """
    seen_ids = set()
    q: list[Any] = [event]
    while q:
        node = q.pop(0)
        if id(node) in seen_ids:
            continue
        seen_ids.add(id(node))

        if isinstance(node, dict):
            # fast path: direct known keys
            for k in _KICKOFF_KEYS:
                if k in node and node[k] is not None:
                    dt = _coerce_kickoff(node[k])
                    if dt:
                        return dt
            # BFS into children
            q.extend(node.values())
        elif isinstance(node, list):
            q.extend(node)
    return None

# ------------------------------ Market helpers --------------------------------

def _price_from_sel(sel: dict) -> Optional[float]:
    """
    Cloudbet selection payloads commonly carry decimal odds in one of:
    - price
    - decimalOdds / oddsDecimal / odds
    If only american odds (+110/-130) are present, try to convert.
    """
    # decimal-ish
    for k in ("price", "decimalOdds", "oddsDecimal", "odds", "d"):
        v = sel.get(k)
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            try:
                return float(v)
            except Exception:
                pass
    # american fallback
    am = sel.get("americanOdds") or sel.get("american")
    if isinstance(am, (int, float, str)):
        try:
            a = float(am)
            if a > 0:
                return 1.0 + (a / 100.0)
            else:
                return 1.0 + (100.0 / abs(a))
        except Exception:
            return None
    return None

def _iter_selections(event: dict, market_key: str) -> Iterable[dict]:
    """
    Support both dict- and list-shaped 'markets'.
    Dict shape (rare in latest API):
      event.markets[market_key].submarkets[*].selections[*]
    List shape (common):
      event.markets = [{ key: market_key, submarkets: [...]}] or selections directly.
    """
    markets = event.get("markets")
    if not markets:
        return []

    # dict-shaped
    if isinstance(markets, dict):
        m = markets.get(market_key)
        if not m:
            return []
        sub = m.get("submarkets") or m.get("subMarkets")
        if isinstance(sub, dict):
            for s in sub.values():
                for sel in s.get("selections", []) or s.get("outcomes", []) or []:
                    yield sel
        elif isinstance(sub, list):
            for s in sub:
                for sel in s.get("selections", []) or s.get("outcomes", []) or []:
                    yield sel
        else:
            # sometimes selections directly
            for sel in m.get("selections", []) or m.get("outcomes", []) or []:
                yield sel
        return []

    # list-shaped
    if isinstance(markets, list):
        for m in markets:
            if not isinstance(m, dict):
                continue
            key = (m.get("key") or m.get("marketKey") or "").lower()
            if key != market_key.lower():
                continue
            sub = m.get("submarkets") or m.get("subMarkets")
            if isinstance(sub, dict):
                for s in sub.values():
                    for sel in s.get("selections", []) or s.get("outcomes", []) or []:
                        yield sel
            elif isinstance(sub, list):
                for s in sub:
                    for sel in s.get("selections", []) or s.get("outcomes", []) or []:
                        yield sel
            else:
                # direct selections on market
                for sel in m.get("selections", []) or m.get("outcomes", []) or []:
                    yield sel
    return []

def _normalize_event(
    event: dict,
    *,
    market_key: str,
    sport: str,
    league: str,
    source_url: str,
    debug: bool = False,
) -> int:
    if market_key not in MARKET_MAP:
        return 0
    market_uid, wanted = MARKET_MAP[market_key]

    event_key = str(event.get("id") or event.get("eventId") or event.get("key") or "")
    kickoff = _deep_get_kickoff(event, debug=debug)
    if kickoff is None:
        if debug:
            print(f"[cloudbet] skip event with no kickoff: id={event_key}")
        return 0

    # Collect best/latest price per canonical outcome
    prices: dict[str, float] = {}
    found_any = False
    for sel in _iter_selections(event, market_key):
        found_any = True
        raw_out = str(sel.get("outcome", sel.get("name", ""))).strip().lower()
        label = _OUTCOME_MAP.get(raw_out)
        price = _price_from_sel(sel)
        if not label or price is None:
            continue
        prices[label] = float(price)

    if not found_any and debug:
        # First few times, dump minimal hints to see the structure
        mkts = event.get("markets")
        shape = type(mkts).__name__
        print(f"[cloudbet] no selections found for event {event_key} market={market_key} (markets shape={shape})")

    if not all(lbl in prices for lbl in wanted):
        if debug:
            have = list(prices.keys())
            print(f"[cloudbet] incomplete market {market_key} for event {event_key}; have={have}, want={wanted}")
        return 0

    snaps = 0
    for lbl in wanted:
        snap = Snapshot(
            bookmaker=BOOKMAKER,
            sport=sport,
            league=league,
            event_key=event_key,
            kickoff_utc=kickoff,
            market_uid=market_uid,
            period="FT",
            selection=lbl,
            odds_decimal=prices[lbl],
            raw={"event_id": event_key, "market_key": market_key},
            source_url=source_url,
            spider_version="proto/cloudbet_v2",
        )
        insert_snapshot(snap.doc())
        snaps += 1
    return snaps

def run_once() -> int:
    proto = _load_proto_block()
    api_base = (proto.get("api_base") or "https://sports-api.cloudbet.com/pub/v2/odds").rstrip("/")
    feeds: List[Dict[str, Any]] = proto.get("feeds") or []

    if not feeds:
        print("[cloudbet] no feeds configured in books.yaml:proto.feeds")
        return 0

    debug = bool(int(os.getenv("CLOUDBET_DEBUG", "0")))
    total = 0

    with _client() as c:
        for feed in feeds:
            sport = str(feed.get("sport") or "").strip() or "unknown"
            comp_key = str(feed.get("competition_key") or "").strip()
            markets: List[str] = [m for m in (feed.get("markets") or []) if m in MARKET_MAP]
            if not comp_key or not markets:
                if debug:
                    print(f"[cloudbet] skip feed (missing comp_key/markets): {feed}")
                continue

            league_alias = comp_key.split("-")[-1] if "-" in comp_key else comp_key

            # Build params with repeated 'markets'
            params: list[tuple[str, str]] = []
            for m in markets:
                params.append(("markets", m))

            url = f"{api_base}/competitions/{comp_key}"
            r = c.get(url, params=params)
            if r.status_code in (401, 403):
                print(f"[cloudbet] unauthorized/forbidden for {url}; check CLOUDBET_API_KEY")
                continue
            r.raise_for_status()
            data = r.json()

            events = data.get("events")
            if not isinstance(events, list):
                # some responses nest under 'data' or use a direct list
                events = data.get("data") if isinstance(data.get("data"), list) else (data if isinstance(data, list) else [])

            ins = 0
            for ev in events:
                for m in markets:
                    ins += _normalize_event(
                        ev,
                        market_key=m,
                        sport=sport,
                        league=league_alias,
                        source_url=str(r.request.url),
                        debug=debug,
                    )

            print(f"[cloudbet] feed comp={comp_key} markets={markets} -> events={len(events)}, snaps={ins}")
            total += ins

    print(f"[cloudbet] total snapshots inserted: {total}")
    return total
