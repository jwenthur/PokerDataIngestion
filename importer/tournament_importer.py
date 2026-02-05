from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from db import queries as q
from importer.file_router import FolderConfig, ensure_dirs, safe_move_with_suffix, log_jsonl
from importer.gg_summary_parser import GGSummaryParser
from importer.session_assigner import ensure_session_and_index
from utils.hashing import sha256_file
from utils.text_utils import read_text_with_fallback


@dataclass(frozen=True)
class PipelineConfig:
    input_dir: Path
    file_extension: str
    folders: FolderConfig


@dataclass(frozen=True)
class ImportConfig:
    site: str
    dry_run: bool
    session_gap_minutes: int
    avg_minutes_per_tournament: int

    tournaments: PipelineConfig
    hand_histories: PipelineConfig

    # shared logging
    log_path: Path


def _resolve_folder(base_dir: Path, folder_value: str) -> Path:
    p = Path(folder_value)
    return p if p.is_absolute() else (base_dir / p)


def _build_folder_config(base_input_dir: Path, folders_raw: dict, logs_dir: Path, log_path: Path) -> FolderConfig:
    processed_dir = _resolve_folder(base_input_dir, folders_raw.get("processed", "Processed"))
    needs_review_dir = _resolve_folder(base_input_dir, folders_raw.get("needs_review", "Needs Review"))
    duplicate_dir = _resolve_folder(base_input_dir, folders_raw.get("duplicate", "Duplicate"))

    return FolderConfig(
        processed_dir=processed_dir,
        needs_review_dir=needs_review_dir,
        duplicate_dir=duplicate_dir,
        logs_dir=logs_dir,
        log_path=log_path,
    )


def build_import_config(config_path: Path) -> ImportConfig:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    site = raw.get("site", "GG")
    dry_run = bool(raw.get("dry_run", False))
    session_gap_minutes = int(raw.get("session_gap_minutes", 60))
    avg_minutes_per_tournament = int(raw.get("avg_minutes_per_tournament", 5))

    # Logging (shared)
    logging_raw = raw.get("logging", {})
    logs_dir_value = logging_raw.get("logs_dir", "logs")
    log_file_name = logging_raw.get("log_file_name", "import_log.jsonl")

    # Choose a base directory for logs: tournaments input dir is a sensible default
    tournaments_raw = raw.get("tournaments", {})
    tournaments_input_dir = Path(tournaments_raw["input_dir"]).expanduser()

    logs_dir = _resolve_folder(tournaments_input_dir, logs_dir_value)
    log_path = logs_dir / log_file_name

    # Tournaments pipeline
    t_file_extension = (tournaments_raw.get("file_extension", ".txt") or ".txt")
    t_folders_raw = tournaments_raw.get("folders", {})
    t_folders = _build_folder_config(
        base_input_dir=tournaments_input_dir,
        folders_raw=t_folders_raw,
        logs_dir=logs_dir,
        log_path=log_path,
    )
    tournaments_cfg = PipelineConfig(
        input_dir=tournaments_input_dir,
        file_extension=t_file_extension,
        folders=t_folders,
    )

    # Hand histories pipeline
    hh_raw = raw.get("hand_histories", {})
    hh_input_dir = Path(hh_raw["input_dir"]).expanduser()
    hh_file_extension = (hh_raw.get("file_extension", ".txt") or ".txt")
    hh_folders_raw = hh_raw.get("folders", {})
    hh_folders = _build_folder_config(
        base_input_dir=hh_input_dir,
        folders_raw=hh_folders_raw,
        logs_dir=logs_dir,
        log_path=log_path,
    )
    hand_histories_cfg = PipelineConfig(
        input_dir=hh_input_dir,
        file_extension=hh_file_extension,
        folders=hh_folders,
    )

    return ImportConfig(
        site=site,
        dry_run=dry_run,
        session_gap_minutes=session_gap_minutes,
        avg_minutes_per_tournament=avg_minutes_per_tournament,
        tournaments=tournaments_cfg,
        hand_histories=hand_histories_cfg,
        log_path=log_path,
    )


class TournamentImporter:
    def __init__(self, cfg: ImportConfig, engine) -> None:
        self.cfg = cfg
        self.engine = engine
        self.parser = GGSummaryParser()

    def _hash_exists(self, conn, file_hash: str) -> bool:
        row = conn.execute(text(q.SQL_HASH_EXISTS), {"file_hash": file_hash}).fetchone()
        return row is not None

    def _list_input_files(self) -> List[Path]:
        cfg = self.cfg.tournaments
        if not cfg.input_dir.exists():
            raise FileNotFoundError(f"Input directory does not exist: {cfg.input_dir}")

        ext = (cfg.file_extension or "").strip().lower()
        if not ext.startswith("."):
            ext = "." + ext

        all_files = [p for p in cfg.input_dir.iterdir() if p.is_file()]
        filtered = [p for p in all_files if p.suffix.lower() == ext]
        return sorted(filtered, key=lambda p: p.name.lower())

    def run(self) -> None:
        cfg = self.cfg
        tcfg = cfg.tournaments
        ensure_dirs(tcfg.folders)

        inserted = 0
        duplicates = 0
        needs_review = 0
        errors = 0
        dry_runs = 0

        files = self._list_input_files()

        # Phase 1: pre-parse timestamps for ordering
        file_queue: List[tuple[Optional[Any], Path]] = []

        for path in files:
            try:
                text_content = read_text_with_fallback(path)
                parsed, _ = self.parser.parse(cfg.site, text_content)
                if parsed:
                    file_queue.append((parsed.tournament_start_ts_local, path))
                else:
                    file_queue.append((None, path))
            except Exception:
                file_queue.append((None, path))

        file_queue.sort(key=lambda x: (x[0] is None, x[0]))

        for _, path in file_queue:
            event: Dict[str, Any] = {
                "pipeline": "tournaments",
                "file_name": path.name,
                "file_hash": None,
                "status": None,
                "reason": None,
                "tournament_id": None,
                "start_time": None,
                "buy_in": None,
                "payout": None,
            }

            try:
                file_hash = sha256_file(path)
                event["file_hash"] = file_hash

                with self.engine.connect() as conn:
                    if self._hash_exists(conn, file_hash):
                        if not cfg.dry_run:
                            safe_move_with_suffix(path, tcfg.folders.duplicate_dir)
                        event["status"] = "duplicate"
                        event["reason"] = "hash_exists"
                        duplicates += 1
                        log_jsonl(cfg.log_path, event)
                        continue

                text_content = read_text_with_fallback(path)
                parsed, reason = self.parser.parse(cfg.site, text_content)

                if parsed is None:
                    if not cfg.dry_run:
                        safe_move_with_suffix(path, tcfg.folders.needs_review_dir)

                    event["status"] = "needs_review" if (reason or "").startswith("needs_review") else "error"
                    event["reason"] = reason or "unknown_parse_failure"
                    if event["status"] == "needs_review":
                        needs_review += 1
                    else:
                        errors += 1
                    log_jsonl(cfg.log_path, event)
                    continue

                event["tournament_id"] = parsed.tournament_id
                event["start_time"] = parsed.tournament_start_ts_local.isoformat(sep=" ")
                event["buy_in"] = parsed.buy_in_amount
                event["payout"] = parsed.payout_amount

                if cfg.dry_run:
                    dry_runs += 1
                    event["status"] = "dry_run"
                    event["reason"] = "no_db_no_move"
                    log_jsonl(cfg.log_path, event)
                    continue

                with self.engine.connect() as conn:
                    trans = conn.begin()
                    try:
                        session_id, session_start, session_index = ensure_session_and_index(
                            conn=conn,
                            site=parsed.site,
                            ts_local=parsed.tournament_start_ts_local,
                            gap_minutes=cfg.session_gap_minutes,
                        )

                        conn.execute(
                            text(q.SQL_INSERT_TOURNAMENT),
                            {
                                "site": parsed.site,
                                "tournament_id": parsed.tournament_id,
                                "start_ts": parsed.tournament_start_ts_local,
                                "hero_name": parsed.hero_name,
                                "source_file_name": path.name,
                                "source_file_hash": file_hash,
                                "tournament_name": parsed.tournament_name,
                                "game_type": parsed.game_type,
                                "player_count": parsed.player_count,
                                "currency": parsed.currency,
                                "buy_in_amount": parsed.buy_in_amount,
                                "prize_pool_amount": parsed.prize_pool_amount,
                                "payout_amount": parsed.payout_amount,
                                "profit_amount": parsed.profit_amount,
                                "finish_place": parsed.finish_place,
                                "session_id": session_id,
                                "session_start_ts_local": session_start,
                                "session_tournament_index": session_index,
                            },
                        )

                        safe_move_with_suffix(path, tcfg.folders.processed_dir)

                        trans.commit()
                        inserted += 1
                        event["status"] = "inserted"
                        event["reason"] = "ok"
                        log_jsonl(cfg.log_path, event)

                    except IntegrityError:
                        trans.rollback()
                        safe_move_with_suffix(path, tcfg.folders.duplicate_dir)
                        duplicates += 1
                        event["status"] = "duplicate"
                        event["reason"] = "unique_conflict"
                        log_jsonl(cfg.log_path, event)

                    except Exception as e:
                        trans.rollback()
                        try:
                            safe_move_with_suffix(path, tcfg.folders.needs_review_dir)
                        except Exception:
                            pass
                        errors += 1
                        event["status"] = "error"
                        event["reason"] = f"move_failed_or_db_error:{type(e).__name__}"
                        log_jsonl(cfg.log_path, event)

            except Exception as e:
                try:
                    if not cfg.dry_run:
                        safe_move_with_suffix(path, tcfg.folders.needs_review_dir)
                except Exception:
                    pass
                errors += 1
                event["status"] = "error"
                event["reason"] = f"fatal:{type(e).__name__}"
                log_jsonl(cfg.log_path, event)

        print(
            f"Dry Runs: {dry_runs} | Inserted: {inserted} | Duplicates: {duplicates} | "
            f"Needs Review: {needs_review} | Errors: {errors}"
        )
