from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    path = Path(path)
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def safe_git_info(repo_root: str | Path) -> dict[str, Any]:
    repo_root = Path(repo_root)
    if not (repo_root / ".git").exists():
        return {"present": False}

    def _run(args: list[str]) -> Optional[str]:
        try:
            out = subprocess.check_output(args, cwd=repo_root, stderr=subprocess.STDOUT, text=True)
            return out.strip()
        except Exception:
            return None

    return {
        "present": True,
        "head": _run(["git", "rev-parse", "HEAD"]),
        "describe": _run(["git", "describe", "--always", "--dirty"]),
        "status_porcelain": _run(["git", "status", "--porcelain=v1"]),
    }


def collect_environment(packages: Optional[list[str]] = None) -> dict[str, Any]:
    if packages is None:
        packages = [
            "numpy",
            "pandas",
            "scipy",
            "scikit-learn",
            "statsmodels",
            "matplotlib",
            "pyarrow",
            "pyyaml",
        ]

    versions: dict[str, Optional[str]] = {}
    for p in packages:
        try:
            versions[p] = metadata.version(p)
        except metadata.PackageNotFoundError:
            versions[p] = None

    return {
        "python": sys.version.replace("\n", " "),
        "executable": sys.executable,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cwd": os.getcwd(),
        "packages": versions,
    }


@dataclass(frozen=True)
class FileRecord:
    path: str
    size_bytes: int
    sha256: str


def record_file(path: str | Path) -> FileRecord:
    path = Path(path)
    return FileRecord(path=str(path), size_bytes=path.stat().st_size, sha256=sha256_file(path))


def write_json(obj: Any, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

