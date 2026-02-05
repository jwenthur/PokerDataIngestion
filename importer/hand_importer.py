# importer/hand_importer.py
from __future__ import annotations

import os
from typing import Iterable, List

from sqlalchemy import text

from db.engine import get_engine  # adjust if your function name differs
from importer.gg_hand_history_parser import parse_file
from importer.spot_classifier import classify_preflop
from models import ParsedHand


INSERT_FACT_HAND_SQL = """
INSERT INTO public.fact_hand (
    site, tournament_id, hand_id, hand_ts_local,
    table_id, max_players, players_in_hand, is_hu,
    level, sb_amount, bb_amount,
    button_seat, hero_seat,
    hero_position, effective_stack_bb, stack_bucket,
    hero_cards,
    preflop_spot_type, hero_preflop_action, faced_action_type,
    is_all_in_preflop, preflop_action_line,
    hero_stack_start, hero_stack_end, hero_net_chips,
    source_file_name, source_file_hash
)
VALUES (
    :site, :tournament_id, :hand_id, :hand_ts_local,
    :table_id, :max_players, :players_in_hand, :is_hu,
    :level, :sb_amount, :bb_amount,
    :button_seat, :hero_seat,
    :hero_position, :effective_stack_bb, :stack_bucket,
    :hero_cards,
    :preflop_spot_type, :hero_preflop_action, :faced_action_type,
    :is_all_in_preflop, :preflop_action_line,
    :hero_stack_start, :hero_stack_end, :hero_net_chips,
    :source_file_name, :source_file_hash
)
ON CONFLICT (site, tournament_id, hand_id)
DO NOTHING;
"""


def _iter_hh_files(folder: str) -> Iterable[str]:
    for root, _, files in os.walk(folder):
        for fn in files:
            if fn.lower().endswith(".txt") and "tournament #" not in fn.lower():
                # Your naming convention: HH files don't include "Tournament #"
                yield os.path.join(root, fn)


def _to_row(hand: ParsedHand) -> dict:
    return {
        "site": hand.site,
        "tournament_id": hand.tournament_id,
        "hand_id": hand.hand_id,
        "hand_ts_local": hand.hand_ts_local,

        "table_id": hand.table_id,
        "max_players": hand.max_players,
        "players_in_hand": hand.players_in_hand,
        "is_hu": hand.is_hu,

        "level": hand.level,
        "sb_amount": hand.sb_amount,
        "bb_amount": hand.bb_amount,

        "button_seat": hand.button_seat,
        "hero_seat": hand.hero_seat,

        "hero_position": hand.hero_position,
        "effective_stack_bb": hand.effective_stack_bb,
        "stack_bucket": hand.stack_bucket,

        "hero_cards": hand.hero_cards,

        "preflop_spot_type": hand.preflop_spot_type,
        "hero_preflop_action": hand.hero_preflop_action,
        "faced_action_type": hand.faced_action_type,

        "is_all_in_preflop": hand.is_all_in_preflop,
        "preflop_action_line": hand.preflop_action_line,

        "hero_stack_start": hand.hero_stack_start,
        "hero_stack_end": hand.hero_stack_end,
        "hero_net_chips": hand.hero_net_chips,

        "source_file_name": hand.source_file_name,
        "source_file_hash": hand.source_file_hash,
    }


def ingest_hands(site: str, folder: str) -> dict:
    """
    Ingest all GG HH files from a folder into fact_hand.
    Returns counts for logging.
    """
    engine = get_engine()
    files = list(_iter_hh_files(folder))

    total_files = 0
    total_hands_parsed = 0
    total_rows_attempted = 0

    with engine.begin() as conn:
        for path in files:
            total_files += 1
            hands = parse_file(path, site=site)
            total_hands_parsed += len(hands)

            # classify each hand
            hands = [classify_preflop(h) for h in hands]

            rows = [_to_row(h) for h in hands]
            if not rows:
                continue

            conn.execute(text(INSERT_FACT_HAND_SQL), rows)
            total_rows_attempted += len(rows)

    return {
        "files_seen": total_files,
        "hands_parsed": total_hands_parsed,
        "rows_attempted": total_rows_attempted,
    }
