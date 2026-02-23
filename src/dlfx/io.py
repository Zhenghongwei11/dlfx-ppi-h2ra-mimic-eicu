from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import pandas as pd


@dataclass(frozen=True)
class TableSpec:
    path: Path
    format: Literal["csv", "parquet"]


def _infer_format(path: Path) -> Literal["csv", "parquet"]:
    suffix = path.suffix.lower()
    if suffix in {".csv"}:
        return "csv"
    if suffix in {".parquet"}:
        return "parquet"
    raise ValueError(f"Unsupported table format for path: {path}")


def read_table(path: str | Path, *, columns: Optional[list[str]] = None) -> pd.DataFrame:
    path = Path(path)
    fmt = _infer_format(path)
    if fmt == "csv":
        return pd.read_csv(path, usecols=columns)
    return pd.read_parquet(path, columns=columns)


def write_table(df: pd.DataFrame, path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = _infer_format(path)
    if fmt == "csv":
        df.to_csv(path, index=False)
        return
    df.to_parquet(path, index=False)

