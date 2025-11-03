from __future__ import annotations
import argparse
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone

from .config import SETTINGS
from .db.mongo import ensure_indices, get_db
from .ev.calc import de_vig_three_way, ev_decimal
from .ev.arbit import is_three_way_arb, is_two_way_arb

# Spiders (keep as-is if you have them)
from .spiders.proto import bovada_proto
from .spiders.proto import cloudbet_proto  # if not present, remove this line

# Playwright harvester
from .harvest.playwright_harvester import run_once as harvest_run_once


def _as_aware(dt):
    return dt if getattr(dt, "tzinfo", None) else dt.replace(tzinfo=timezone.utc)


def cmd_init_db(args):
    ensure_indices()
    print("Indices ensured.")


def cmd_proto_scrape(args):
    if args.book == "bovada":
        n = bovada_proto.run_once()
    elif args.book == "cloudbet":
        n = cloudbet_proto.run_once()
    else:
        print(f"Unknown book '{args.book}'. Try: bovada | cloudbet")
        return 2
    print(f"Inserted {n} snapshots.")


def cmd_harvest_once(args):
    count = harvest_run_once(args.book, debug=args.debug)
    print(f"[harvest] captured {count} XHR responses")


def cmd_scheduler(args):
    from .scheduler.run_scheduler import main as scheduler_main
    scheduler_main()


def _ev_scan_print(ev_threshold: float) -> None:
    db = get_db()
    coll = db.get_collection("quotes_snapshots")

    pipeline = [
        {"$match": {"market_uid": {"$in": ["FT_1X2", "FT_ML_2W"]}}},
        {"$sort": {"captured_at_utc": -1}},
        {"$group": {"_id": {"event": "$event_key", "selection": "$selection"}, "doc": {"$first": "$$ROOT"}}},
        {"$group": {"_id": "$_id.event", "rows": {"$push": "$doc"}}},
    ]

    for row in coll.aggregate(pipeline):
        rows = row["rows"]
        labels = {r["selection"] for r in rows}
        if {"home", "draw", "away"}.issubset(labels):
            od = {r["selection"]: r["odds_decimal"] for r in rows if r["selection"] in {"home", "draw", "away"}}
            probs = de_vig_three_way([od["home"], od["draw"], od["away"]])
            for sel, p in zip(["home", "draw", "away"], probs):
                ev = ev_decimal(p, od[sel])
                if ev >= ev_threshold:
                    print(f"EV hit: event={row['_id']} sel={sel} odds={od[sel]:.2f} EV={ev*100:.2f}%")
        elif {"home", "away"}.issubset(labels):
            od = {r["selection"]: r["odds_decimal"] for r in rows if r["selection"] in {"home", "away"}}
            inv = [1.0 / od["home"], 1.0 / od["away"]]
            s = sum(inv)
            probs = [x / s for x in inv]
            for sel, p in zip(["home", "away"], probs):
                ev = ev_decimal(p, od[sel])
                if ev >= ev_threshold:
                    print(f"EV hit: event={row['_id']} sel={sel} odds={od[sel]:.2f} EV={ev*100:.2f}%")


def cmd_ev_scan(args):
    _ev_scan_print(args.edge)


def cmd_ev_scan_xbook(args):
    db = get_db()
    coll = db.get_collection("quotes_snapshots")
    ev_coll = db.get_collection("ev_hits")

    pipeline = [
        {"$match": {"market_uid": {"$in": ["FT_1X2", "FT_ML_2W"]}}},
        {"$sort": {"odds_decimal": -1, "captured_at_utc": -1}},
        {"$group": {"_id": {"event": "$event_key", "selection": "$selection"}, "doc": {"$first": "$$ROOT"}}},
        {"$group": {"_id": "$_id.event", "rows": {"$push": "$doc"}}},
    ]

    hits = 0
    now = datetime.now(timezone.utc)
    for row in coll.aggregate(pipeline):
        rows = row["rows"]
        by_sel = {r["selection"]: r for r in rows}

        if all(k in by_sel for k in ("home", "draw", "away")):
            prices = [by_sel["home"]["odds_decimal"], by_sel["draw"]["odds_decimal"], by_sel["away"]["odds_decimal"]]
            probs = de_vig_three_way(prices)
            if is_three_way_arb(prices):
                print(f"ARB (3-way): event={row['_id']} prices={prices}")
            loop = [("home", probs[0]), ("draw", probs[1]), ("away", probs[2])]
        elif all(k in by_sel for k in ("home", "away")):
            h, a = by_sel["home"]["odds_decimal"], by_sel["away"]["odds_decimal"]
            inv = [1.0 / h, 1.0 / a]
            s = sum(inv)
            probs = [x / s for x in inv]
            if is_two_way_arb(h, a):
                print(f"ARB (2-way): event={row['_id']} prices={[h, a]}")
            loop = [("home", probs[0]), ("away", probs[1])]
        else:
            continue

        for sel, p in loop:
            best = by_sel[sel]
            ev = ev_decimal(p, best["odds_decimal"])
            age_s = max(0.0, (now - _as_aware(best["captured_at_utc"])).total_seconds())
            if ev >= args.edge:
                doc = {
                    "event_key": row["_id"],
                    "market_uid": best.get("market_uid", "FT_1X2"),
                    "selection": sel,
                    "bookmaker": best["bookmaker"],
                    "odds": best["odds_decimal"],
                    "edge": ev,
                    "p_star": p,
                    "captured_at_utc": best["captured_at_utc"],
                    "age_s": age_s,
                    "computed_at_utc": now,
                }
                ev_coll.insert_one(doc)
                print(
                    f"EV+ : event={row['_id']} sel={sel} {best['bookmaker']}@{best['odds_decimal']:.2f} "
                    f"EV={ev*100:.2f}% age={age_s:.0f}s"
                )
                hits += 1

    print(f"Total EV hits stored: {hits}")


def cmd_metrics_odds_age(args):
    db = get_db()
    coll = db.get_collection("quotes_snapshots")
    now = datetime.now(timezone.utc)

    match = {}
    if args.book:
        match["bookmaker"] = args.book

    buckets = {"0-60s": 0, "1-5m": 0, "5-30m": 0, "30m-2h": 0, ">2h": 0}
    for doc in coll.find(match, {"captured_at_utc": 1}).limit(50000):
        age = (now - _as_aware(doc["captured_at_utc"])).total_seconds()
        if age <= 60:
            buckets["0-60s"] += 1
        elif age <= 300:
            buckets["1-5m"] += 1
        elif age <= 1800:
            buckets["5-30m"] += 1
        elif age <= 7200:
            buckets["30m-2h"] += 1
        else:
            buckets[">2h"] += 1
    print("Odds age histogram:", buckets)


def _parse_dur_to_seconds(s: str) -> int:
    s = s.strip().lower()
    if s.endswith("ms"):
        return max(1, int(float(s[:-2]) / 1000))
    if s.endswith("s"):
        return int(float(s[:-1]))
    if s.endswith("m"):
        return int(float(s[:-1]) * 60)
    if s.endswith("h"):
        return int(float(s[:-1]) * 3600)
    return int(s)


def cmd_pulse(args):
    """Hammer one book on an interval to validate 'live' updates."""
    every = _parse_dur_to_seconds(args.every)
    total = _parse_dur_to_seconds(args.for_)
    end_ts = time.time() + total
    runs = 0
    while time.time() < end_ts:
        if args.book == "bovada":
            n = bovada_proto.run_once()
        elif args.book == "cloudbet":
            n = cloudbet_proto.run_once()
        else:
            print(f"Unknown book '{args.book}'. Try: bovada | cloudbet")
            return 2
        runs += 1
        print(f"[pulse] run={runs} inserted={n}")
        time.sleep(every)
    print(f"[pulse] done runs={runs}")


def cmd_tail(args):
    """Print the most recent N snapshots (optionally filter by book/event/market)."""
    db = get_db()
    coll = db.get_collection("quotes_snapshots")
    q = {}
    if args.book:
        q["bookmaker"] = args.book
    if args.event:
        q["event_key"] = args.event
    if args.market:
        q["market_uid"] = args.market
    cur = coll.find(q).sort("captured_at_utc", -1).limit(args.limit)
    for d in cur:
        print(
            {
                "bookmaker": d.get("bookmaker"),
                "event_key": d.get("event_key"),
                "market_uid": d.get("market_uid"),
                "selection": d.get("selection"),
                "odds_decimal": d.get("odds_decimal"),
                "param": d.get("param"),
                "captured_at_utc": d.get("captured_at_utc"),
                "source_url": d.get("source_url"),
            }
        )


def cmd_settings(args):
    print("\n".join(f"{k} = {v}" for k, v in asdict(SETTINGS).items()))


def main(argv=None):
    p = argparse.ArgumentParser(prog="bettingos")
    sub = p.add_subparsers(dest="cmd")

    s = sub.add_parser("init-db")
    s.set_defaults(func=cmd_init_db)

    s = sub.add_parser("proto-scrape")
    s.add_argument("book")
    s.set_defaults(func=cmd_proto_scrape)

    s = sub.add_parser("harvest-once")
    s.add_argument("book")
    s.add_argument("--debug", action="store_true")
    s.set_defaults(func=cmd_harvest_once)

    s = sub.add_parser("scheduler")
    s.set_defaults(func=cmd_scheduler)

    s = sub.add_parser("ev-scan")
    s.add_argument("--edge", type=float, default=0.02)
    s.set_defaults(func=cmd_ev_scan)

    s = sub.add_parser("ev-scan-xbook")
    s.add_argument("--edge", type=float, default=0.02)
    s.set_defaults(func=cmd_ev_scan_xbook)

    s = sub.add_parser("metrics-odds-age")
    s.add_argument("--book")
    s.set_defaults(func=cmd_metrics_odds_age)

    s = sub.add_parser("pulse")
    s.add_argument("--book", required=True)
    s.add_argument("--every", default="12s")
    s.add_argument("--for", dest="for_", default="2m")
    s.set_defaults(func=cmd_pulse)

    s = sub.add_parser("tail")
    s.add_argument("--book")
    s.add_argument("--event")
    s.add_argument("--market")
    s.add_argument("--limit", type=int, default=20)
    s.set_defaults(func=cmd_tail)

    s = sub.add_parser("settings")
    s.set_defaults(func=cmd_settings)

    args = p.parse_args(argv)
    if not hasattr(args, "func"):
        p.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
