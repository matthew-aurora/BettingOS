from __future__ import annotations
from dataclasses import dataclass
import unicodedata
import yaml
from pathlib import Path
from rapidfuzz import process, fuzz

ROOT = Path(__file__).resolve().parents[2]
TEAMS_PATH = ROOT / "aliases" / "teams.yaml"
COMPS_PATH = ROOT / "aliases" / "competitions.yaml"

def _safe_load(path: Path, key: str) -> dict[str, list[str]]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return data.get(key, {}) or {}

TEAMS = _safe_load(TEAMS_PATH, "teams")
COMPS = _safe_load(COMPS_PATH, "competitions")

def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii")
    return " ".join(s.lower().strip().split())

def _expand_map(d: dict[str, list[str]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for canonical, aliases in d.items():
        out[_norm(canonical)] = canonical
        for a in (aliases or []):
            out[_norm(a)] = canonical
    return out

TEAM_MAP = _expand_map(TEAMS)
COMP_MAP = _expand_map(COMPS)

@dataclass
class MatchResult:
    canonical: str
    confidence: float
    matched_on: str  # "alias" | "fuzzy" | "none"

def match_team(name: str) -> MatchResult:
    key = _norm(name)
    if key in TEAM_MAP:
        return MatchResult(TEAM_MAP[key], 1.0, "alias")
    if not TEAM_MAP:
        return MatchResult(name, 0.0, "none")
    choices = list(TEAM_MAP.keys())
    best = process.extractOne(key, choices, scorer=fuzz.WRatio)
    if not best:
        return MatchResult(name, 0.0, "none")
    matched_key, score, _ = best
    return MatchResult(TEAM_MAP[matched_key], score / 100.0, "fuzzy")

def match_competition(name: str) -> MatchResult:
    key = _norm(name)
    if key in COMP_MAP:
        return MatchResult(COMP_MAP[key], 1.0, "alias")
    if not COMP_MAP:
        return MatchResult(name, 0.0, "none")
    choices = list(COMP_MAP.keys())
    best = process.extractOne(key, choices, scorer=fuzz.WRatio)
    if not best:
        return MatchResult(name, 0.0, "none")
    matched_key, score, _ = best
    return MatchResult(COMP_MAP[matched_key], score / 100.0, "fuzzy")
