import hashlib
from pathlib import Path
from typing import Union


def sha256_file(path: Union[Path, str]) -> str:
    """Calculate SHA256 hash of a file.

    Args:
        path: File path as either a Path object or string

    Returns:
        Hexadecimal hash string
    """
    # Convert string to Path if needed
    if isinstance(path, str):
        path = Path(path)

    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()