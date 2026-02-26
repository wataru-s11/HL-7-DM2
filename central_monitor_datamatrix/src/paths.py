from __future__ import annotations

from datetime import datetime
from pathlib import Path


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_default_run_dir(base: str | Path = "dataset") -> Path:
    day = datetime.now().strftime("%Y%m%d")
    return Path(base) / day


def resolve_run_dir(run_dir: str | Path | None, base: str | Path = "dataset") -> Path:
    selected = Path(run_dir) if run_dir else get_default_run_dir(base=base)
    return ensure_dir(selected)


def resolve_in_run_dir(path_value: str | Path | None, run_dir: Path) -> Path | None:
    if path_value is None:
        return None
    path = Path(path_value)
    if path.is_absolute():
        return path
    return run_dir / path
