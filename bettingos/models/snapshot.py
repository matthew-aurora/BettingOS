from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime, timezone

class Snapshot(BaseModel):
    captured_at_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    bookmaker: str
    event_key: str
    sport: str
    league: str
    kickoff_utc: datetime
    market_uid: str  # FT_1X2, FT_ML_2W, FT_SPREAD, FT_TOTAL
    period: str = "FT"
    selection: str  # home|draw|away|over|under
    odds_decimal: float
    param: Optional[float] = None  # spread/total line
    line_status: str = "open"
    spider_version: str = "proto/v0"
    selector_version: Optional[str] = None
    source_url: Optional[str] = None
    http_status: Optional[int] = None
    parse_stage: Optional[str] = None
    raw: dict[str, Any] = {}

    def doc(self) -> dict:
        d = self.model_dump()
        d["captured_at_utc"] = self.captured_at_utc
        d["kickoff_utc"] = self.kickoff_utc
        return d
