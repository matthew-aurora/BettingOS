from __future__ import annotations
import os
import time
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from ..config import SETTINGS
from ..logging_conf import configure_logging
import structlog

log = structlog.get_logger()

def _parse_dur(s: str) -> int:
    s = (s or "").strip().lower()
    if s.endswith("ms"): return int(float(s[:-2]) / 1000)
    if s.endswith("s"):  return int(float(s[:-1]))
    if s.endswith("m"):  return int(float(s[:-1]) * 60)
    if s.endswith("h"):  return int(float(s[:-1]) * 3600)
    return int(s or 0)

def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _job_fn(book_key: str, kind: str):
    # Lazy import so we only pull a module if enabled
    if kind == "proto":
        if book_key == "bookx":
            from ..spiders.proto import bookx_proto
            return bookx_proto.run_once
        if book_key == "bovada":
            from ..spiders.proto import bovada_proto
            return bovada_proto.run_once
    if kind == "scrapy":
        return lambda: os.system(f"python -m scrapy crawl {book_key}")  # placeholder for real spiders
    return lambda: 0

def main():
    configure_logging()
    if os.path.exists(SETTINGS.kill_file):
        log.warning("Kill switch present; exiting", file=SETTINGS.kill_file)
        return

    cadence_cfg = _load_yaml("cadence.yaml")
    books_cfg = _load_yaml("books.yaml")

    cadence_default = _parse_dur((cadence_cfg.get("cadence") or {}).get("default", "600"))
    backoff_cfg = cadence_cfg.get("backoff") or {}
    b_initial = _parse_dur(backoff_cfg.get("initial", "30s")) or 30
    b_factor = float(str(backoff_cfg.get("factor", "2")))
    b_max = _parse_dur(backoff_cfg.get("max", "600")) or 600

    sched = BackgroundScheduler()
    failure_state: dict[str, dict] = {}

    def wrap_run(book_key: str, fn):
        def _inner():
            if os.path.exists(SETTINGS.kill_file):
                log.warning("Kill switch engaged; skipping run", book=book_key)
                return
            job_id = f"{book_key}"
            st = failure_state.setdefault(job_id, {"fails": 0, "interval": None})
            try:
                res = fn()
                st["fails"] = 0
                # restore cadence after backoff
                if st.get("interval"):
                    bk = next((b for b in books_cfg.get("books", []) if b.get("key") == book_key), None)
                    base_every = _parse_dur(str(bk.get("cadence"))) if bk and bk.get("cadence") else cadence_default
                    job = sched.get_job(job_id)
                    if job:
                        sched.reschedule_job(job.id, trigger=IntervalTrigger(seconds=base_every))
                    st["interval"] = None
                log.info("job_ok", book=book_key, result=int(res) if isinstance(res, int) else res)
            except Exception as e:
                st["fails"] += 1
                delay = min(int(b_initial * (b_factor ** (st["fails"] - 1))), b_max)
                job = sched.get_job(job_id)
                if job:
                    sched.reschedule_job(job.id, trigger=IntervalTrigger(seconds=delay))
                    st["interval"] = delay
                log.warning("job_fail_backoff", book=book_key, fails=st["fails"], next_interval_s=delay, error=str(e))
        return _inner

    for b in books_cfg.get("books", []):
        if not b.get("enabled", False):
            log.info("book_disabled", book=b.get("key"))
            continue
        key = str(b.get("key"))
        every = _parse_dur(str(b.get("cadence"))) if b.get("cadence") else cadence_default
        kind = "proto" if b.get("proto") else "scrapy"
        fn = _job_fn(key, kind)
        sched.add_job(wrap_run(key, fn), "interval", seconds=every, id=key, max_instances=1, coalesce=True)
        log.info("scheduled", job=key, every_seconds=every, kind=kind)

    sched.start()
    log.info("scheduler_started")
    try:
        while True:
            time.sleep(1)
            if os.path.exists(SETTINGS.kill_file):
                log.warning("Kill switch engaged; shutting down")
                break
    finally:
        sched.shutdown(wait=False)
        log.info("scheduler_stopped")

if __name__ == "__main__":
    main()
