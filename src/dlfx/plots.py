from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


def _ensure_parent(path: str | Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def plot_hist_overlap(
    *,
    x: np.ndarray,
    group: np.ndarray,
    outpath: str | Path,
    weights: Optional[np.ndarray] = None,
    bins: int = 40,
    title: str,
    xlabel: str,
    labels: tuple[str, str] = ("PPI", "H2RA"),
) -> None:
    import matplotlib.pyplot as plt

    outpath = _ensure_parent(outpath)
    x = np.asarray(x, dtype=float)
    group = np.asarray(group, dtype=int)
    w = None if weights is None else np.asarray(weights, dtype=float)

    mask1 = group == 1
    mask0 = group == 0
    w1 = None if w is None else w[mask1]
    w0 = None if w is None else w[mask0]

    plt.figure(figsize=(7.2, 4.2))
    plt.hist(x[mask0], bins=bins, weights=w0, alpha=0.55, label=labels[1], density=True)
    plt.hist(x[mask1], bins=bins, weights=w1, alpha=0.55, label=labels[0], density=True)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel("Density")
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def plot_weight_hist(
    *,
    weights: np.ndarray,
    group: np.ndarray,
    outpath: str | Path,
    title: str = "IPTW distribution",
    labels: tuple[str, str] = ("PPI", "H2RA"),
) -> None:
    import matplotlib.pyplot as plt

    outpath = _ensure_parent(outpath)
    weights = np.asarray(weights, dtype=float)
    group = np.asarray(group, dtype=int)

    plt.figure(figsize=(7.2, 4.2))
    for g, lab, color in [(0, labels[1], "#1f77b4"), (1, labels[0], "#ff7f0e")]:
        ww = weights[group == g]
        if ww.size == 0:
            continue
        plt.hist(ww, bins=60, alpha=0.55, label=lab, density=True, color=color)
    plt.title(title)
    plt.xlabel("Weight")
    plt.ylabel("Density")
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def weighted_km_curve(
    *,
    durations: np.ndarray,
    events: np.ndarray,
    weights: np.ndarray,
) -> pd.DataFrame:
    """
    Weighted Kaplan–Meier curve using frequency-style weights.

    Returns a dataframe with columns:
      - time
      - survival
      - cuminc (1 - survival)
      - risk_weight (risk set weight just before time)
      - event_weight (event weight at time)
    """
    t = np.asarray(durations, dtype=float)
    e = np.asarray(events, dtype=int)
    w = np.asarray(weights, dtype=float)

    mask = np.isfinite(t) & np.isfinite(w) & np.isfinite(e)
    t = np.maximum(t[mask], 0.0)
    e = e[mask]
    w = w[mask]

    if t.size == 0:
        return pd.DataFrame(columns=["time", "survival", "cuminc", "risk_weight", "event_weight"])

    df = pd.DataFrame({"time": t, "event": e, "w": w})
    df = df.sort_values("time", ascending=True).reset_index(drop=True)
    total_w = float(df["w"].sum())
    if total_w <= 0:
        return pd.DataFrame(columns=["time", "survival", "cuminc", "risk_weight", "event_weight"])

    g = (
        df.assign(w_total=lambda d: d["w"], w_event=lambda d: d["w"] * (d["event"] == 1))
        .groupby("time", as_index=False)[["w_total", "w_event"]]
        .sum()
        .sort_values("time", ascending=True)
        .reset_index(drop=True)
    )

    surv = 1.0
    risk = total_w
    rows = [{"time": 0.0, "survival": 1.0, "cuminc": 0.0, "risk_weight": risk, "event_weight": 0.0}]
    for _, row in g.iterrows():
        time = float(row["time"])
        d = float(row["w_event"])
        if risk > 0 and d > 0:
            surv *= max(0.0, 1.0 - (d / risk))
        rows.append(
            {
                "time": time,
                "survival": float(surv),
                "cuminc": float(1.0 - surv),
                "risk_weight": float(risk),
                "event_weight": float(d),
            }
        )
        risk -= float(row["w_total"])

    return pd.DataFrame(rows)


def plot_km_curves(
    *,
    df: pd.DataFrame,
    duration_col: str,
    event_col: str,
    weight_col: str,
    group_col: str,
    outpath: str | Path,
    horizon_days: float,
    title: str,
    labels: tuple[str, str] = ("PPI", "H2RA"),
) -> None:
    import matplotlib.pyplot as plt

    outpath = _ensure_parent(outpath)

    work = df[[duration_col, event_col, weight_col, group_col]].copy()
    work = work.dropna(subset=[duration_col, event_col, weight_col, group_col])
    work[duration_col] = pd.to_numeric(work[duration_col], errors="coerce")
    work[event_col] = pd.to_numeric(work[event_col], errors="coerce").fillna(0).astype(int)
    work[weight_col] = pd.to_numeric(work[weight_col], errors="coerce")

    plt.figure(figsize=(7.2, 4.6))
    for g, lab, color in [(0, labels[1], "#1f77b4"), (1, labels[0], "#ff7f0e")]:
        sub = work[work[group_col].astype(int) == g]
        if sub.empty:
            continue
        curve = weighted_km_curve(
            durations=sub[duration_col].to_numpy(dtype=float),
            events=sub[event_col].to_numpy(dtype=int),
            weights=sub[weight_col].to_numpy(dtype=float),
        )
        curve = curve[curve["time"] <= horizon_days]
        plt.step(curve["time"], curve["survival"], where="post", label=lab, color=color)

    plt.axvline(horizon_days, color="black", linestyle="--", linewidth=1)
    plt.ylim(0, 1.0)
    plt.xlim(0, horizon_days)
    plt.xlabel("Days since index")
    plt.ylabel("Survival")
    plt.title(title)
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def plot_ratio_forest(
    effects: pd.DataFrame,
    *,
    outpath: str | Path,
    title: str = "Effect estimates (ratio scale)",
) -> None:
    """
    Forest plot for effect ratios (HR/RR) with 95% CI on log scale.

    Expected columns in effects:
      - outcome_label
      - ratio
      - ratio_lo
      - ratio_hi
    """
    import matplotlib.pyplot as plt

    outpath = _ensure_parent(outpath)

    df = effects.dropna(subset=["ratio", "ratio_lo", "ratio_hi"]).copy()
    if df.empty:
        return
    df = df.iloc[::-1].reset_index(drop=True)
    y = np.arange(len(df))

    plt.figure(figsize=(7.5, max(3.6, 0.5 * len(df))))
    plt.errorbar(
        x=df["ratio"],
        y=y,
        xerr=[df["ratio"] - df["ratio_lo"], df["ratio_hi"] - df["ratio"]],
        fmt="o",
        color="black",
        ecolor="black",
        capsize=3,
    )
    plt.axvline(1.0, color="black", linewidth=1)
    plt.xscale("log")
    plt.yticks(y, df["outcome_label"])
    plt.xlabel("Ratio (log scale)")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()

