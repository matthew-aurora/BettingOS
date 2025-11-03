from __future__ import annotations
from typing import Iterable

def implied_prob(odds_decimal: float) -> float:
    return 1.0 / odds_decimal

def de_vig_three_way(odds: Iterable[float]) -> list[float]:
    inv = [1.0/o for o in odds]
    s = sum(inv)
    return [x/s for x in inv]

def ev_decimal(p_true: float, odds_decimal: float) -> float:
    return p_true * (odds_decimal - 1.0) - (1.0 - p_true)
