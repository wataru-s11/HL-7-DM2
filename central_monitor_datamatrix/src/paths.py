from __future__ import annotations

from datetime import datetime
from pathlib import Path


DEFAULT_WORK_ROOT = Path(r"C:\Users\sakai\HL7_DM_test")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_work_root(work_root: str | Path | None) -> Path:
    selected = Path(work_root) if work_root else DEFAULT_WORK_ROOT
    return ensure_dir(selected)


def get_default_run_dir(base: str | Path = "dataset") -> Path:
    day = datetime.now().strftime("%Y%m%d")
    return Path(base) / day


def resolve_work_path(path_value: str | Path | None, work_root: Path, default_rel: str | Path | None = None) -> Path:
    selected = Path(path_value) if path_value else (Path(default_rel) if default_rel is not None else Path())
    if selected.is_absolute():
        return selected
    return work_root / selected


def resolve_run_dir(run_dir: str | Path | None, base: str | Path = "dataset", work_root: str | Path | None = None) -> Path:
    root = resolve_work_root(work_root)
    if run_dir:
        selected = Path(run_dir)
        if not selected.is_absolute():
            selected = root / selected
    else:
        selected = root / get_default_run_dir(base=base)
    return ensure_dir(selected)


def resolve_in_run_dir(path_value: str | Path | None, run_dir: Path) -> Path | None:
    if path_value is None:
        return None
    path = Path(path_value)
    if path.is_absolute():
        return path
    return run_dir / path
