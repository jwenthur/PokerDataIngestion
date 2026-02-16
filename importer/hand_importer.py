# importer/hand_importer.py
from __future__ import annotations

from typing import Any, Dict, List
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

from importer.file_router import ensure_dirs, safe_move_with_suffix, log_jsonl
from importer.gg_hand_history_parser import parse_file
from importer.spot_classifier import classify_preflop
from models import ParsedHand
from utils.hashing import sha256_file


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
    went_to_showdown, allin_street, board_at_allin, 
    pot_at_allin, hero_equity_at_allin, allin_adjusted_chips,
    villain_cards,
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
    :went_to_showdown, :allin_street, :board_at_allin,
    :pot_at_allin, :hero_equity_at_allin, :allin_adjusted_chips,
    :villain_cards,
    :source_file_name, :source_file_hash
)
ON CONFLICT (site, tournament_id, hand_id)
DO NOTHING;
"""

COUNT_EXISTING_HANDS_SQL = """
SELECT COUNT(*) AS existing
FROM public.fact_hand
WHERE site = :site
  AND tournament_id = :tournament_id
  AND hand_id = ANY(:hand_ids);
"""


def _to_row(hand: ParsedHand) -> dict:
    """Convert ParsedHand to database row dict."""
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

        # ChipEV fields
        "went_to_showdown": hand.went_to_showdown,
        "allin_street": hand.allin_street,
        "board_at_allin": hand.board_at_allin,
        "pot_at_allin": hand.pot_at_allin,
        "hero_equity_at_allin": hand.hero_equity_at_allin,
        "allin_adjusted_chips": hand.allin_adjusted_chips,
        "villain_cards": hand.villain_cards,

        "source_file_name": hand.source_file_name,
        "source_file_hash": hand.source_file_hash,
    }


def _chunked(lst: List, size: int) -> List[List]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


class HandImporter:
    def __init__(self, cfg, engine: Engine) -> None:
        self.cfg = cfg
        self.engine = engine

    def _list_input_files(self) -> List[Path]:
        hcfg = self.cfg.hand_histories

        if not hcfg.input_dir.exists():
            raise FileNotFoundError(f"Input directory does not exist: {hcfg.input_dir}")

        ext = (hcfg.file_extension or "").strip().lower()
        if not ext.startswith("."):
            ext = "." + ext

        all_files = [p for p in hcfg.input_dir.iterdir() if p.is_file()]
        filtered = [p for p in all_files if p.suffix.lower() == ext]
        return sorted(filtered, key=lambda p: p.name.lower())

    def run(self, batch_size: int = 100) -> None:
        cfg = self.cfg
        hcfg = cfg.hand_histories
        ensure_dirs(hcfg.folders)

        inserted = 0
        duplicates = 0
        needs_review = 0
        errors = 0
        dry_runs = 0

        files = self._list_input_files()

        for path in files:
            event: Dict[str, Any] = {
                "pipeline": "hands",
                "file_name": path.name,
                "file_hash": None,
                "status": None,
                "reason": None,
                "tournament_id": None,
                "hands_in_file": 0,
                "hands_inserted": 0,
            }

            try:
                file_hash = sha256_file(path)
                event["file_hash"] = file_hash

                # Parse hands
                try:
                    all_hands = parse_file(str(path), site=cfg.site)
                except Exception as e:
                    if not cfg.dry_run:
                        safe_move_with_suffix(path, hcfg.folders.needs_review_dir)
                    needs_review += 1
                    event["status"] = "error"
                    event["reason"] = f"parse_error:{type(e).__name__}"
                    log_jsonl(cfg.log_path, event)
                    continue

                if not all_hands:
                    if not cfg.dry_run:
                        safe_move_with_suffix(path, hcfg.folders.needs_review_dir)
                    needs_review += 1
                    event["status"] = "needs_review"
                    event["reason"] = "no_hands_parsed"
                    log_jsonl(cfg.log_path, event)
                    continue

                event["hands_in_file"] = len(all_hands)
                event["tournament_id"] = all_hands[0].tournament_id if all_hands else None

                # Classify spots
                classified_hands = [classify_preflop(h) for h in all_hands]

                if cfg.dry_run:
                    dry_runs += 1
                    event["status"] = "dry_run"
                    event["reason"] = "no_db_no_move"
                    log_jsonl(cfg.log_path, event)
                    continue

                # Process in batches
                hands_inserted_this_file = 0

                for batch in _chunked(classified_hands, batch_size):
                    with self.engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            # Check for existing hands
                            hand_ids = [h.hand_id for h in batch]
                            existing_count = conn.execute(
                                text(COUNT_EXISTING_HANDS_SQL),
                                {
                                    "site": cfg.site,
                                    "tournament_id": batch[0].tournament_id,
                                    "hand_ids": hand_ids,
                                },
                            ).scalar()

                            if existing_count == len(batch):
                                # All hands already exist
                                trans.rollback()
                                continue

                            # Insert hands
                            rows = [_to_row(h) for h in batch]
                            result = conn.execute(text(INSERT_FACT_HAND_SQL), rows)
                            hands_inserted_this_file += result.rowcount

                            trans.commit()

                        except Exception as e:
                            trans.rollback()
                            try:
                                safe_move_with_suffix(path, hcfg.folders.needs_review_dir)
                            except Exception:
                                pass
                            errors += 1
                            event["status"] = "error"
                            event["reason"] = f"db_error:{type(e).__name__}"
                            log_jsonl(cfg.log_path, event)
                            break
                else:
                    # All batches succeeded
                    safe_move_with_suffix(path, hcfg.folders.processed_dir)
                    inserted += hands_inserted_this_file
                    event["hands_inserted"] = hands_inserted_this_file

                    if hands_inserted_this_file == 0:
                        duplicates += 1
                        event["status"] = "duplicate"
                        event["reason"] = "all_hands_exist"
                    else:
                        event["status"] = "inserted"
                        event["reason"] = "ok"

                    log_jsonl(cfg.log_path, event)

            except Exception as e:
                try:
                    if not cfg.dry_run:
                        safe_move_with_suffix(path, hcfg.folders.needs_review_dir)
                except Exception:
                    pass
                errors += 1
                event["status"] = "error"
                event["reason"] = f"fatal:{type(e).__name__}"
                log_jsonl(cfg.log_path, event)

        print(
            f"Dry Runs: {dry_runs} | Inserted: {inserted} hands | Duplicates: {duplicates} files | "
            f"Needs Review: {needs_review} | Errors: {errors}"
        )