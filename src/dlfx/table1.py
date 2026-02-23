from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from .balance import standardized_mean_difference


def _wmean(x: np.ndarray, w: np.ndarray) -> float:
    return float(np.sum(w * x) / np.sum(w))


def _wvar(x: np.ndarray, w: np.ndarray) -> float:
    mu = _wmean(x, w)
    return float(np.sum(w * (x - mu) ** 2) / np.sum(w))


def _fmt_mean_sd(mu: float, sd: float) -> str:
    return f"{mu:.2f} ({sd:.2f})"


def _fmt_prop(p: float) -> str:
    return f"{100*p:.1f}%"


@dataclass(frozen=True)
class Table1Config:
    max_levels: int = 20  # hard stop to avoid huge tables


def make_table1(
    df: pd.DataFrame,
    *,
    covariates: Sequence[str],
    treatment_indicator_col: str,
    weight_col: str,
    cfg: Table1Config = Table1Config(),
) -> pd.DataFrame:
    """
    Build a pragmatic Table 1:
      - unweighted and IPTW-weighted summaries by treatment group
      - SMD unweighted and weighted

    For categorical variables, each level becomes a row.
    """
    if treatment_indicator_col not in df.columns:
        raise KeyError(f"Missing {treatment_indicator_col}")
    if weight_col not in df.columns:
        raise KeyError(f"Missing {weight_col}")

    t = df[treatment_indicator_col].to_numpy(dtype=int)
    w = pd.to_numeric(df[weight_col], errors="coerce").to_numpy(dtype=float)

    mask1 = t == 1
    mask0 = t == 0

    rows = []
    for var in covariates:
        if var not in df.columns:
            continue
        s = df[var]

        if pd.api.types.is_numeric_dtype(s):
            x = pd.to_numeric(s, errors="coerce").to_numpy(dtype=float)
            miss = int(np.sum(~np.isfinite(x)))
            x_imp = x.copy()
            med = float(np.nanmedian(x_imp))
            x_imp[~np.isfinite(x_imp)] = med

            # Unweighted summaries should ignore missingness; otherwise any NaN yields NaN.
            mu1 = float(np.nanmean(x[mask1])) if np.isfinite(x[mask1]).any() else float("nan")
            mu0 = float(np.nanmean(x[mask0])) if np.isfinite(x[mask0]).any() else float("nan")
            sd1 = float(np.nanstd(x[mask1], ddof=0)) if np.isfinite(x[mask1]).any() else float("nan")
            sd0 = float(np.nanstd(x[mask0], ddof=0)) if np.isfinite(x[mask0]).any() else float("nan")

            wmu1 = _wmean(x_imp[mask1], w[mask1])
            wmu0 = _wmean(x_imp[mask0], w[mask0])
            wsd1 = float(np.sqrt(_wvar(x_imp[mask1], w[mask1])))
            wsd0 = float(np.sqrt(_wvar(x_imp[mask0], w[mask0])))

            smd_unw = standardized_mean_difference(x_imp, t, w=None)
            smd_w = standardized_mean_difference(x_imp, t, w=w)

            rows.append(
                {
                    "variable": var,
                    "level": "",
                    "type": "continuous",
                    "missing_n": miss,
                    "unweighted_ppi": _fmt_mean_sd(mu1, sd1),
                    "unweighted_h2ra": _fmt_mean_sd(mu0, sd0),
                    "weighted_ppi": _fmt_mean_sd(wmu1, wsd1),
                    "weighted_h2ra": _fmt_mean_sd(wmu0, wsd0),
                    "smd_unweighted": smd_unw,
                    "smd_weighted": smd_w,
                }
            )
            continue

        # Categorical
        x = s.astype("string").fillna("missing")
        levels = x.value_counts(dropna=False).index.tolist()
        if len(levels) > cfg.max_levels:
            # Keep top levels and collapse the rest.
            top = levels[: cfg.max_levels - 1]
            x = x.where(x.isin(top), other="other")
            levels = x.value_counts(dropna=False).index.tolist()

        for lvl in levels:
            ind = (x == lvl).to_numpy(dtype=float)

            p1 = float(np.mean(ind[mask1])) if ind[mask1].size else float("nan")
            p0 = float(np.mean(ind[mask0])) if ind[mask0].size else float("nan")
            wp1 = _wmean(ind[mask1], w[mask1])
            wp0 = _wmean(ind[mask0], w[mask0])

            smd_unw = standardized_mean_difference(ind, t, w=None)
            smd_w = standardized_mean_difference(ind, t, w=w)
            rows.append(
                {
                    "variable": var,
                    "level": str(lvl),
                    "type": "categorical",
                    "missing_n": int(np.sum(x == "missing")) if lvl == "missing" else "",
                    "unweighted_ppi": _fmt_prop(p1),
                    "unweighted_h2ra": _fmt_prop(p0),
                    "weighted_ppi": _fmt_prop(wp1),
                    "weighted_h2ra": _fmt_prop(wp0),
                    "smd_unweighted": smd_unw,
                    "smd_weighted": smd_w,
                }
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["variable", "type", "level"]).reset_index(drop=True)
