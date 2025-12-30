from pathlib import Path

from db.engine import build_engine_from_env
from importer.tournament_importer import TournamentImporter
from importer.tournament_importer import build_import_config


def main() -> None:
    cfg_path = Path(__file__).parent / "config" / "config.yaml"
    cfg = build_import_config(cfg_path)

    engine = build_engine_from_env()
    importer = TournamentImporter(cfg=cfg, engine=engine)
    importer.run()


if __name__ == "__main__":
    main()