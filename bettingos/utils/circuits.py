from __future__ import annotations
import time
from collections import deque, defaultdict

class CircuitBreaker:
    def __init__(self, fail_threshold: int = 5, window_seconds: int = 60, cool_off: int = 120):
        self.fail_threshold = fail_threshold
        self.window = window_seconds
        self.cool_off = cool_off
        self.fail_log = defaultdict(lambda: deque())  # domain -> deque[timestamps]
        self.open_until = {}  # domain -> ts

    def record_failure(self, domain: str) -> None:
        q = self.fail_log[domain]
        now = time.time()
        q.append(now)
        while q and now - q[0] > self.window:
            q.popleft()
        if len(q) >= self.fail_threshold:
            self.open_until[domain] = now + self.cool_off

    def allow(self, domain: str) -> bool:
        ts = self.open_until.get(domain)
        return not ts or time.time() > ts
