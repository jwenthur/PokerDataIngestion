from pathlib import Path


def read_text_with_fallback(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        # Windows fallback
        return path.read_text(encoding="cp1252", errors="replace")
