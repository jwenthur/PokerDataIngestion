from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import shutil


@dataclass(frozen=True)
class FolderConfig:
    processed_dir: Path
    needs_review_dir: Path
    duplicate_dir: Path
    logs_dir: Path
    log_path: Path


def ensure_dirs(folders: FolderConfig) -> None:
    folders.processed_dir.mkdir(parents=True, exist_ok=True)
    folders.needs_review_dir.mkdir(parents=True, exist_ok=True)
    folders.duplicate_dir.mkdir(parents=True, exist_ok=True)
    folders.logs_dir.mkdir(parents=True, exist_ok=True)


def safe_move_with_suffix(src: Path, dest_dir: Path) -> Path:
    """
    Move src into dest_dir.
    If a file name already exists, append a timestamp suffix.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name

    if not dest.exists():
        shutil.move(str(src), str(dest))
        return dest

    stem = src.stem
    suffix = src.suffix
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = dest_dir / f"{stem}_{ts}{suffix}"
    counter = 1
    while candidate.exists():
        candidate = dest_dir / f"{stem}_{ts}_{counter}{suffix}"
        counter += 1

    shutil.move(str(src), str(candidate))
    return candidate


def log_jsonl(log_path: Path, payload: Dict[str, Any]) -> None:
    payload = dict(payload)
    payload["timestamp"] = datetime.now().isoformat(timespec="seconds")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
