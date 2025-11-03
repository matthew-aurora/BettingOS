from __future__ import annotations
import os
from pymongo.database import Database

def ensure_indices(db: Database) -> None:
    coll = db.get_collection("quotes_snapshots")
    coll.create_index([("bookmaker", 1), ("market_uid", 1), ("captured_at_utc", -1)])
    coll.create_index([("event_key", 1), ("captured_at_utc", -1)])
    # TTL for snapshots
    ttl_days = int(os.getenv("QUOTES_TTL_DAYS", "14"))
    coll.create_index("captured_at_utc", expireAfterSeconds=ttl_days*24*3600, name="ttl_snapshots")

    ev = db.get_collection("ev_hits")
    ev.create_index([("event_key", 1), ("market_uid", 1), ("selection", 1), ("computed_at_utc", -1)])
    ev_ttl_days = int(os.getenv("EV_HITS_TTL_DAYS", "14"))
    ev.create_index("computed_at_utc", expireAfterSeconds=ev_ttl_days*24*3600, name="ttl_ev_hits")
