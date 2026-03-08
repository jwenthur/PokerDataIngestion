"""
import_hand_decisions.py
=======================
Loads a filled hand_decision_template.xlsx into public.hand_decision in PostgreSQL.

Usage:
    python import_hand_decisions.py path/to/hand_decision_template.xlsx [--dry-run]

What it does:
    1. Reads the 'hand_decision' sheet from the Excel file.
    2. Skips the example row (row 2) and any blank rows.
    3. Looks up site + tournament_id from fact_hand using hand_id.
    4. Denormalises preflop_spot_type, stack_bucket, hero_cards, hero_position
       from fact_hand so the decision row is self-contained for reporting.
    5. Upserts into hand_decision (ON CONFLICT DO UPDATE) so re-running is safe.
    6. Prints a summary of inserted / updated / skipped / errored rows.

Requirements:
    pip install pandas openpyxl sqlalchemy psycopg2-binary python-dotenv

Environment (.env file in project root, or environment variables):
    DB_HOST     localhost
    DB_PORT     5432
    DB_NAME     PokerTracking_db
    DB_USER     your_user
    DB_PASSWORD your_password
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

def _get_engine():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    dbname   = os.getenv("DB_NAME", "PokerTracking_db")
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not user or not password:
        sys.exit(
            "ERROR: DB_USER and DB_PASSWORD must be set in .env or environment variables."
        )

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url)


# ---------------------------------------------------------------------------
# Read Excel
# ---------------------------------------------------------------------------

EXAMPLE_HAND_ID = "SG2595635456"   # The example row — skip it

def _read_excel(path: Path) -> pd.DataFrame:
    df = pd.read_excel(
        path,
        sheet_name="hand_decision",
        dtype=str,           # Read everything as string; we'll coerce below
        header=0,
    )

    # Drop completely empty rows
    df = df.dropna(how="all")

    # Strip whitespace from all string columns
    for col in df.columns:
        df[col] = df[col].str.strip() if df[col].dtype == object else df[col]

    # Drop the example row
    df = df[df["hand_id"] != EXAMPLE_HAND_ID]

    # Drop rows with no hand_id
    df = df[df["hand_id"].notna() & (df["hand_id"] != "")]

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Enrich from fact_hand
# ---------------------------------------------------------------------------

FACT_LOOKUP_SQL = """
SELECT
    hand_id,
    site,
    tournament_id,
    preflop_spot_type,
    stack_bucket,
    hero_cards,
    hero_position
FROM public.fact_hand
WHERE hand_id = ANY(:hand_ids)
"""

def _lookup_fact_hand(engine, hand_ids: list[str]) -> dict[str, dict]:
    with engine.connect() as conn:
        result = conn.execute(
            text(FACT_LOOKUP_SQL),
            {"hand_ids": hand_ids},
        )
        rows = result.mappings().all()

    return {r["hand_id"]: dict(r) for r in rows}


# ---------------------------------------------------------------------------
# Coerce is_correct
# ---------------------------------------------------------------------------

def _parse_is_correct(val) -> bool | None:
    if pd.isna(val) or str(val).strip() == "":
        return None
    s = str(val).strip().upper()
    if s in ("TRUE", "1", "YES"):
        return True
    if s in ("FALSE", "0", "NO"):
        return False
    return None


# ---------------------------------------------------------------------------
# Upsert SQL
# ---------------------------------------------------------------------------

UPSERT_SQL = """
INSERT INTO public.hand_decision (
    site, tournament_id, hand_id,
    street,
    preflop_spot_type, stack_bucket, hero_cards, hero_position,
    hero_action, correct_action, is_correct,
    notes, source, reviewed_at
)
VALUES (
    :site, :tournament_id, :hand_id,
    :street,
    :preflop_spot_type, :stack_bucket, :hero_cards, :hero_position,
    :hero_action, :correct_action, :is_correct,
    :notes, :source, NOW()
)
ON CONFLICT (site, tournament_id, hand_id, street)
DO UPDATE SET
    hero_action         = EXCLUDED.hero_action,
    correct_action      = EXCLUDED.correct_action,
    is_correct          = EXCLUDED.is_correct,
    notes               = EXCLUDED.notes,
    source              = EXCLUDED.source,
    reviewed_at         = NOW();
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(excel_path: Path, dry_run: bool = False) -> None:
    print(f"\nReading: {excel_path}")
    df = _read_excel(excel_path)
    print(f"  Rows after cleaning: {len(df)}")

    if df.empty:
        print("  No rows to process. Exiting.")
        return

    engine = _get_engine()

    # Lookup fact_hand data
    hand_ids = df["hand_id"].unique().tolist()
    print(f"  Looking up {len(hand_ids)} unique hand_ids in fact_hand ...")
    fact_lookup = _lookup_fact_hand(engine, hand_ids)
    print(f"  Found {len(fact_lookup)} matches in fact_hand.")

    # Process rows
    inserted = 0
    skipped  = 0
    errors   = 0
    rows_to_upsert = []

    for _, row in df.iterrows():
        hand_id = row.get("hand_id", "").strip()
        street  = (row.get("street") or "preflop").strip().lower()
        hero_action    = (row.get("hero_action") or "").strip().upper()
        correct_action = (row.get("correct_action") or "").strip().upper() or None
        notes  = row.get("notes") or None
        is_correct = _parse_is_correct(row.get("is_correct"))

        # Validation
        if not hand_id:
            skipped += 1
            continue
        if not hero_action:
            print(f"  SKIP {hand_id}: hero_action is blank")
            skipped += 1
            continue

        # Enrich from fact_hand
        fact = fact_lookup.get(hand_id)
        if not fact:
            print(f"  SKIP {hand_id}: not found in fact_hand — check hand_id is correct")
            skipped += 1
            continue

        rows_to_upsert.append({
            "site":              fact["site"],
            "tournament_id":     int(fact["tournament_id"]),
            "hand_id":           hand_id,
            "street":            street,
            "preflop_spot_type": fact.get("preflop_spot_type"),
            "stack_bucket":      fact.get("stack_bucket"),
            "hero_cards":        fact.get("hero_cards"),
            "hero_position":     fact.get("hero_position"),
            "hero_action":       hero_action,
            "correct_action":    correct_action if correct_action else None,
            "is_correct":        is_correct,
            "notes":             str(notes) if notes else None,
            "source":            "manual",
        })

    if not rows_to_upsert:
        print("  Nothing to upsert after validation.")
        return

    if dry_run:
        print(f"\n  DRY RUN — would upsert {len(rows_to_upsert)} rows.")
        for r in rows_to_upsert[:5]:
            print(f"    {r['hand_id']} | {r['street']} | "
                  f"hero={r['hero_action']} correct={r['correct_action']} "
                  f"is_correct={r['is_correct']}")
        if len(rows_to_upsert) > 5:
            print(f"    ... and {len(rows_to_upsert) - 5} more.")
        return

    # Execute upsert
    with engine.begin() as conn:
        for r in rows_to_upsert:
            try:
                conn.execute(text(UPSERT_SQL), r)
                inserted += 1
            except Exception as e:
                print(f"  ERROR {r['hand_id']}: {e}")
                errors += 1

    print(f"\nDone.")
    print(f"  Upserted : {inserted}")
    print(f"  Skipped  : {skipped}")
    print(f"  Errors   : {errors}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import hand_decision Excel file into PostgreSQL."
    )
    parser.add_argument("excel_path", type=Path, help="Path to filled hand_decision_template.xlsx")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be imported without writing to DB",
    )
    args = parser.parse_args()

    if not args.excel_path.exists():
        sys.exit(f"ERROR: File not found: {args.excel_path}")

    run(args.excel_path, dry_run=args.dry_run)