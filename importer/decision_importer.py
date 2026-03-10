# importer/decision_importer.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


FACT_LOOKUP_SQL = """
SELECT hand_id, site, tournament_id
FROM public.fact_hand
WHERE hand_id = ANY(:hand_ids)
"""

UPSERT_SQL = """
INSERT INTO public.hand_decision (
    site, tournament_id, hand_id,
    street,
    hero_action, correct_action, is_correct,
    notes, source, reviewed_at
)
VALUES (
    :site, :tournament_id, :hand_id,
    :street,
    :hero_action, :correct_action, :is_correct,
    :notes, :source, NOW()
)
ON CONFLICT (site, tournament_id, hand_id, street)
DO UPDATE SET
    hero_action     = EXCLUDED.hero_action,
    correct_action  = EXCLUDED.correct_action,
    is_correct      = EXCLUDED.is_correct,
    notes           = EXCLUDED.notes,
    source          = EXCLUDED.source,
    reviewed_at     = NOW();
"""

EXAMPLE_HAND_ID = "SG2595635456"


def _read_excel(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="hand_decision", dtype=str, header=0)
    df = df.dropna(how="all")
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.strip()
    df = df[df["hand_id"] != EXAMPLE_HAND_ID]
    df = df[df["hand_id"].notna() & (df["hand_id"] != "")]
    return df.reset_index(drop=True)


def _parse_is_correct(val) -> Optional[bool]:
    if pd.isna(val) or str(val).strip() == "":
        return None
    s = str(val).strip().upper()
    if s in ("TRUE", "1", "YES"):
        return True
    if s in ("FALSE", "0", "NO"):
        return False
    return None


class DecisionImporter:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def run(self, excel_path: Path, dry_run: bool = False) -> None:
        print(f"\nReading: {excel_path}")
        df = _read_excel(excel_path)
        print(f"  Rows after cleaning: {len(df)}")

        if df.empty:
            print("  No rows to process.")
            return

        hand_ids = df["hand_id"].unique().tolist()
        print(f"  Looking up {len(hand_ids)} unique hand_ids in fact_hand ...")

        with self.engine.connect() as conn:
            result = conn.execute(text(FACT_LOOKUP_SQL), {"hand_ids": hand_ids})
            fact_lookup = {r["hand_id"]: dict(r) for r in result.mappings()}

        print(f"  Found {len(fact_lookup)} matches in fact_hand.")

        rows_to_upsert = []
        skipped = 0

        for _, row in df.iterrows():
            hand_id     = row.get("hand_id", "").strip()
            street      = (row.get("street") or "preflop").strip().lower()
            hero_action = (row.get("hero_action") or "").strip().upper()
            correct_action = (row.get("correct_action") or "").strip().upper() or None
            notes       = row.get("notes") or None
            is_correct  = _parse_is_correct(row.get("is_correct"))

            if not hero_action:
                print(f"  SKIP {hand_id}: hero_action is blank")
                skipped += 1
                continue

            fact = fact_lookup.get(hand_id)
            if not fact:
                print(f"  SKIP {hand_id}: not found in fact_hand")
                skipped += 1
                continue

            rows_to_upsert.append({
                "site":           fact["site"],
                "tournament_id":  int(fact["tournament_id"]),
                "hand_id":        hand_id,
                "street":         street,
                "hero_action":    hero_action,
                "correct_action": correct_action,
                "is_correct":     is_correct,
                "notes":          str(notes) if notes else None,
                "source":         "manual",
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

        inserted = 0
        errors = 0
        with self.engine.begin() as conn:
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