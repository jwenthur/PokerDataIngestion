from __future__ import annotations

import argparse
from pathlib import Path

from db.engine import build_engine_from_env
from importer.tournament_importer import TournamentImporter, build_import_config
from importer.hand_importer import HandImporter
from importer.decision_importer import DecisionImporter




def _default_config_path() -> Path:
    return Path(__file__).parent / "config" / "config.yaml"


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="spin-import",
        description="Import GG tournament summaries or hand histories into Postgres.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # Ingest tournaments
    p_t = sub.add_parser("ingest-tournaments", help="Import tournament summary files.")
    p_t.add_argument(
        "--config",
        type=str,
        default=str(_default_config_path()),
        help="Path to config.yaml (defaults to project config/config.yaml)",
    )
    p_t.add_argument(
        "--dry-run",
        action="store_true",
        help="Override config and run without DB writes or file moves.",
    )

    # Ingest hands
    p_h = sub.add_parser("ingest-hands", help="Import hand history files.")
    p_h.add_argument(
        "--config",
        type=str,
        default=str(_default_config_path()),
        help="Path to config.yaml (defaults to project config/config.yaml)",
    )
    p_h.add_argument(
        "--dry-run",
        action="store_true",
        help="Override config and run without DB writes or file moves.",
    )
    p_h.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of hand history files to import per DB transaction (default: 100).",
    )

    # Import decisions
    p_d = sub.add_parser("import-decisions", help="Import hand decision Excel file into hand_decision table.")
    p_d.add_argument(
        "excel_path",
        type=str,
        help="Path to filled hand_decision_template.xlsx (e.g. data/hand_decision_template.xlsx)",
    )
    p_d.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be imported without writing to DB.",
    )

    return parser


def main() -> None:
    parser = build_cli()
    args = parser.parse_args()

    engine = build_engine_from_env()

    if args.command == "ingest-tournaments":
        cfg = build_import_config(Path(args.config).expanduser())
        if getattr(args, "dry_run", False):
            cfg = type(cfg)(**{**cfg.__dict__, "dry_run": True})
        TournamentImporter(cfg=cfg, engine=engine).run()
        return

    if args.command == "ingest-hands":
        cfg = build_import_config(Path(args.config).expanduser())
        if getattr(args, "dry_run", False):
            cfg = type(cfg)(**{**cfg.__dict__, "dry_run": True})
        HandImporter(cfg=cfg, engine=engine).run(batch_size=args.batch_size)
        return

    if args.command == "import-decisions":
        DecisionImporter(engine=engine).run(
            excel_path=Path(args.excel_path).expanduser(),
            dry_run=args.dry_run,
        )
        return

    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    main()