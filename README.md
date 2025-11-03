# BettingOS — Scraper-First MVP

Every spider is a new revenue sensor. Keep it simple; keep it moving.

### Core ideas
- Prototype → harden: Requests+BS first, then Scrapy.
- Mongo first: fast inserts, time-series wins. Store UTC, display JHB.
- Cadence is the metronome: YAML-driven, step down near kickoff, back off on 4xx/5xx.
- Observability: structured logs, freshness SLOs, error rates.
- Guardrails: circuit breakers, kill switch, clear red lines.

### Quickstart (Windows 11 + PowerShell)
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
mkdir bettingos; cd bettingos
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
docker compose up -d
Copy-Item .env.example .env
python -m bettingos.cli init-db
python -m bettingos.cli proto-scrape bookx    # or set $env:BOOKX_URL
python -m bettingos.cli ev-scan --edge 0.02
python -m bettingos.cli scheduler
```

### Useful CLI
- `python -m bettingos.cli ev-scan-xbook --edge 0.02` — cross-book EV/arb pass, persists EV hits.
- `python -m bettingos.cli book pause <book_key> --reason "…"`, `python -m bettingos.cli book resume <book_key>` — toggle books.
- `python -m bettingos.cli robots-refresh` — fetch/store robots.txt for all configured books.
- `python -m bettingos.cli metrics-odds-age` — print simple odds-age histogram.
