# importer/hand_importer.py
from __future__ import annotations

from typing import Any, Dict, List

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

        "source_file_name": hand.source_file_name,
        "source_file_hash": hand.source_file_hash,
    }


def _chunked(lst: List, size: int) -> List[List]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


class HandImporter:
    def __init__(self, cfg, engine: Engine) -> None:
        self.cfg = cfg
        self.engine = engine

    def _list_input_files(self) -> List:
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

        inserted_files = 0
        duplicates = 0
        needs_review = 0
        errors = 0
        dry_runs = 0

        files = self._list_input_files()

        # Defensive: never allow <= 0
        batch_size = max(1, int(batch_size))

        batches = _chunked(files, batch_size)

        for batch_index, batch in enumerate(batches, start=1):
            # Dry run: no DB writes; still move files? You said dry_run means no moves too.
            if cfg.dry_run:
                for path in batch:
                    event: Dict[str, Any] = {
                        "pipeline": "hand_histories",
                        "file_name": path.name,
                        "file_hash": None,
                        "status": "dry_run",
                        "reason": "no_db_no_move",
                        "tournament_id": None,
                        "hands_in_file": None,
                        "rows_attempted": None,
                        "batch_index": batch_index,
                        "batch_size": batch_size,
                    }
                    try:
                        event["file_hash"] = sha256_file(path)
                        hands = parse_file(str(path), site=cfg.site)
                        event["hands_in_file"] = len(hands)
                        if hands:
                            event["tournament_id"] = hands[0].tournament_id
                        dry_runs += 1
                    except Exception as e:
                        errors += 1
                        event["status"] = "error"
                        event["reason"] = f"dry_run_parse_failed:{type(e).__name__}"
                    log_jsonl(cfg.log_path, event)
                continue

            # Real run: commit every batch
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    for path in batch:
                        event: Dict[str, Any] = {
                            "pipeline": "hand_histories",
                            "file_name": path.name,
                            "file_hash": None,
                            "status": None,
                            "reason": None,
                            "tournament_id": None,
                            "hands_in_file": None,
                            "rows_attempted": None,
                            "batch_index": batch_index,
                            "batch_size": batch_size,
                        }

                        try:
                            file_hash = sha256_file(path)
                            event["file_hash"] = file_hash

                            hands = parse_file(str(path), site=cfg.site)
                            if not hands:
                                safe_move_with_suffix(path, hcfg.folders.needs_review_dir)
                                event["status"] = "needs_review"
                                event["reason"] = "no_hands_parsed"
                                needs_review += 1
                                log_jsonl(cfg.log_path, event)
                                continue

                            hands = [classify_preflop(h) for h in hands]
                            event["tournament_id"] = hands[0].tournament_id
                            event["hands_in_file"] = len(hands)

                            tournament_id = hands[0].tournament_id
                            hand_ids = [h.hand_id for h in hands]

                            existing = conn.execute(
                                text(COUNT_EXISTING_HANDS_SQL),
                                {"site": cfg.site, "tournament_id": tournament_id, "hand_ids": hand_ids},
                            ).scalar_one()

                            if existing == len(hand_ids):
                                safe_move_with_suffix(path, hcfg.folders.duplicate_dir)
                                event["status"] = "duplicate"
                                event["reason"] = "all_hand_ids_exist"
                                duplicates += 1
                                log_jsonl(cfg.log_path, event)
                                continue

                            rows = [_to_row(h) for h in hands]
                            conn.execute(text(INSERT_FACT_HAND_SQL), rows)
                            event["rows_attempted"] = len(rows)

                            safe_move_with_suffix(path, hcfg.folders.processed_dir)
                            inserted_files += 1
                            event["status"] = "inserted"
                            event["reason"] = "ok"
                            log_jsonl(cfg.log_path, event)

                        except Exception as e:
                            # Per-file failure: move to needs review and keep going
                            try:
                                safe_move_with_suffix(path, hcfg.folders.needs_review_dir)
                            except Exception:
                                pass
                            errors += 1
                            event["status"] = "error"
                            event["reason"] = f"fatal:{type(e).__name__}"
                            log_jsonl(cfg.log_path, event)

                    trans.commit()

                except Exception as e:
                    # Batch-level failure: rollback just this batch
                    trans.rollback()
                    # Log batch failure once (do not spam)
                    log_jsonl(cfg.log_path, {
                        "pipeline": "hand_histories",
                        "status": "error",
                        "reason": f"batch_rollback:{type(e).__name__}",
                        "batch_index": batch_index,
                        "batch_size": batch_size,
                        "files_in_batch": len(batch),
                    })
                    # IMPORTANT: don't move files here; we didnâ€™t process them safely.
                    # Let them remain in input so you can retry.
                    continue

        print(
            f"Dry Runs: {dry_runs} | Inserted Files: {inserted_files} | "
            f"Duplicates: {duplicates} | Needs Review: {needs_review} | Errors: {errors}"
        )