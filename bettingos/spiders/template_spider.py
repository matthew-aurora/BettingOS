"""Cookie-cutter template for new Scrapy spiders.
Fill parse_markets/map_selection based on your proved selectors.
"""
from __future__ import annotations
import scrapy
from ..models.snapshot import Snapshot
from ..db.mongo import insert_snapshot

class BaseBookSpider(scrapy.Spider):
    name = "basebook"
    custom_settings = {
        "USER_AGENT": "BettingOS/0.1",
        "LOG_LEVEL": "INFO",
        "FEED_EXPORT_ENCODING": "utf-8",
        "DOWNLOAD_TIMEOUT": 10,
        "AUTOTHROTTLE_ENABLED": True,
    }

    start_urls: list[str] = []
    bookmaker_key: str = ""

    def parse(self, response: scrapy.http.Response, **kwargs):
        yield from self.parse_markets(response)

    # --- To implement per bookmaker ---
    def parse_markets(self, response):  # yield Snapshot docs
        raise NotImplementedError

    def yield_snapshot(self, **kwargs):
        snap = Snapshot(**kwargs)
        insert_snapshot(snap.doc())
