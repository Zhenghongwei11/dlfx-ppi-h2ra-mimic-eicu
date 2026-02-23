from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class TreatmentEncoding:
    treated_label: str = "ppi"
    control_label: str = "h2ra"


def encode_treatment(
    df: pd.DataFrame,
    *,
    treatment_col: str = "treatment",
    encoding: TreatmentEncoding = TreatmentEncoding(),
) -> pd.Series:
    if treatment_col not in df.columns:
        raise KeyError(f"Missing treatment column: {treatment_col}")
    treatment = df[treatment_col].astype("string")
    is_treated = treatment == encoding.treated_label
    is_control = treatment == encoding.control_label
    if not (is_treated | is_control).all():
        bad = treatment[~(is_treated | is_control)].dropna().unique().tolist()
        raise ValueError(
            f"Unexpected treatment labels in {treatment_col}: {bad}. "
            f"Expected: {encoding.treated_label!r} or {encoding.control_label!r}"
        )
    return is_treated.astype(int)


def split_covariates(
    df: pd.DataFrame, covariates: Sequence[str]
) -> tuple[list[str], list[str]]:
    missing = [c for c in covariates if c not in df.columns]
    if missing:
        raise KeyError(f"Missing covariate columns: {missing}")

    categorical: list[str] = []
    continuous: list[str] = []
    for c in covariates:
        if pd.api.types.is_numeric_dtype(df[c]):
            continuous.append(c)
        else:
            categorical.append(c)
    return categorical, continuous


def impute_for_balance(
    df: pd.DataFrame, *, categorical: Iterable[str], continuous: Iterable[str]
) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for c in continuous:
        series = pd.to_numeric(df[c], errors="coerce")
        med = float(np.nanmedian(series.to_numpy(dtype=float)))
        out[c] = series.fillna(med)
    for c in categorical:
        out[c] = df[c].astype("string").fillna("missing")
    return out


def one_hot_balance_frame(
    df: pd.DataFrame,
    *,
    categorical: Sequence[str],
    continuous: Sequence[str],
) -> pd.DataFrame:
    base = impute_for_balance(df, categorical=categorical, continuous=continuous)
    if not categorical:
        return base[continuous].copy()
    dummies = pd.get_dummies(base[categorical], prefix=categorical, dummy_na=False)
    return pd.concat([base[continuous], dummies], axis=1)

