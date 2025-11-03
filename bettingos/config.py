from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db: str = os.getenv("MONGO_DB", "bettingos")
    user_agent: str = os.getenv("USER_AGENT", "BettingOS/0.1")
    kill_file: str = os.getenv("KILL_SWITCH_FILE", ".kill")
    # Tunables
    quotes_ttl_days: int = int(os.getenv("QUOTES_TTL_DAYS", "14"))
    ev_hits_ttl_days: int = int(os.getenv("EV_HITS_TTL_DAYS", "14"))
    stale_s: int = int(os.getenv("STALE_S", "120"))
    min_books_for_consensus: int = int(os.getenv("MIN_BOOKS_FOR_CONSENSUS", "1"))
    slippage_bps: int = int(os.getenv("SLIPPAGE_BPS", "40"))

SETTINGS = Settings()
