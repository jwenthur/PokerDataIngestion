# PokerStatTracker

Imports GG Spin&Gold tournament summary .txt files into PostgreSQL for P&L tracking.

## Setup

1) Install dependencies:
   pip install -r requirements.txt

2) Create `.env` at project root:
   DB_HOST=localhost
   DB_PORT=5433
   DB_NAME=PokerTracking_db
   DB_USER=postgres
   DB_PASSWORD=...

3) Edit config:
   config/config.yaml

4) Run:
   python main.py

## Folder routing

- Cash results -> DB insert -> move to Processed/
- Non-cash payout (ticket etc.) -> no insert -> Needs Review/
- Parse error -> no insert -> Needs Review/
- Duplicate (hash exists OR unique constraint) -> no insert -> Duplicate/

## Logging

Appends JSON Lines to:
  <input_dir>/logs/import_log.jsonl