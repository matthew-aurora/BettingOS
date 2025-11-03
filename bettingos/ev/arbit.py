from __future__ import annotations
from typing import Iterable

def is_two_way_arb(o1: float, o2: float) -> bool:
    return 1.0/o1 + 1.0/o2 < 1.0

def is_three_way_arb(odds: Iterable[float]) -> bool:
    return sum(1.0/o for o in odds) < 1.0
