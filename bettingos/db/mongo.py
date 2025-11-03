from __future__ import annotations
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError, ServerSelectionTimeoutError
from datetime import timezone
from . import init_indices
from ..config import SETTINGS

_client: MongoClient | None = None

def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(
            SETTINGS.mongo_uri,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=5000,
            tz_aware=True,
            tzinfo=timezone.utc,
        )
    return _client

def get_db() -> Database:
    return get_client()[SETTINGS.mongo_db]

def quotes_collection() -> Collection:
    db = get_db()
    # Use a regular collection first; migrate to time-series if needed
    return db.get_collection("quotes_snapshots")

def ensure_indices() -> None:
    try:
        # Force a quick connectivity check
        get_client().admin.command("ping")
    except ServerSelectionTimeoutError as e:
        raise RuntimeError(
            "Cannot connect to MongoDB at MONGO_URI. "
            "Start Mongo first (e.g., `docker compose up -d`) and retry."
        ) from e
    init_indices.ensure_indices(get_db())

def insert_snapshot(doc: dict) -> None:
    try:
        quotes_collection().insert_one(doc)
    except PyMongoError as e:
        raise
