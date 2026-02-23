from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


def _weighted_mean(x: np.ndarray, w: np.ndarray) -> float:
    return float(np.sum(w * x) / np.sum(w))


def _weighted_var(x: np.ndarray, w: np.ndarray) -> float:
    mu = _weighted_mean(x, w)
    return float(np.sum(w * (x - mu) ** 2) / np.sum(w))


def standardized_mean_difference(
    x: np.ndarray, t: np.ndarray, *, w: Optional[np.ndarray] = None
) -> float:
    x = x.astype(float)
    t = t.astype(int)
    if w is None:
        x1 = x[t == 1]
        x0 = x[t == 0]
        m1 = float(np.mean(x1))
        m0 = float(np.mean(x0))
        v1 = float(np.var(x1, ddof=0))
        v0 = float(np.var(x0, ddof=0))
    else:
        w = w.astype(float)
        x1 = x[t == 1]
        x0 = x[t == 0]
        w1 = w[t == 1]
        w0 = w[t == 0]
        m1 = _weighted_mean(x1, w1)
        m0 = _weighted_mean(x0, w0)
        v1 = _weighted_var(x1, w1)
        v0 = _weighted_var(x0, w0)

    pooled = np.sqrt((v1 + v0) / 2.0)
    if pooled == 0:
        return 0.0
    return float((m1 - m0) / pooled)


def balance_table(
    features: pd.DataFrame,
    *,
    treatment_indicator: pd.Series,
    weights: Optional[pd.Series] = None,
) -> pd.DataFrame:
    t = treatment_indicator.to_numpy(dtype=int)
    w = None if weights is None else weights.to_numpy(dtype=float)

    rows = []
    for col in features.columns:
        x = pd.to_numeric(features[col], errors="coerce").to_numpy(dtype=float)
        smd_unw = standardized_mean_difference(x, t, w=None)
        smd_w = standardized_mean_difference(x, t, w=w) if w is not None else np.nan
        rows.append({"feature": col, "smd_unweighted": smd_unw, "smd_weighted": smd_w})
    out = pd.DataFrame(rows).sort_values("smd_unweighted", key=lambda s: s.abs(), ascending=False)
    return out.reset_index(drop=True)


def love_plot(
    balance: pd.DataFrame,
    *,
    outpath: str | Path,
    threshold: float = 0.1,
    title: str = "Covariate balance (SMD)",
) -> None:
    import matplotlib.pyplot as plt

    df = balance.copy()
    df = df.sort_values("smd_unweighted", key=lambda s: s.abs(), ascending=True)

    y = np.arange(len(df))
    plt.figure(figsize=(7.5, max(4.0, 0.18 * len(df))))
    plt.scatter(df["smd_unweighted"], y, label="Unweighted", s=18)
    if df["smd_weighted"].notna().any():
        plt.scatter(df["smd_weighted"], y, label="IPTW", s=18)

    plt.axvline(threshold, color="black", linewidth=1, linestyle="--")
    plt.axvline(-threshold, color="black", linewidth=1, linestyle="--")
    plt.axvline(0, color="black", linewidth=1)

    plt.yticks(y, df["feature"])
    plt.xlabel("Standardized mean difference")
    plt.title(title)
    plt.legend(loc="best")
    plt.tight_layout()

    outpath = Path(outpath)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outpath, dpi=200)
    plt.close()

