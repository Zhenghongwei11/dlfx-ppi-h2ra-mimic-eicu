#!/usr/bin/env python3
"""
Publication figure board for dlfx ICU PPI-vs-H2RA target trial emulation.
Generates a composite 3×2 board with full provenance metadata.

Usage:
    python scripts/make_publication_board.py --run-dir output/synth_run3
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde

# ── Global rcParams (aligned with PLOT_STYLE_GUIDE.md) ─────────────────────
mpl.rcParams.update({
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
    "font.family": "sans-serif",
    "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans"],
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.labelsize": 9,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.linewidth": 0.8,
    "axes.grid": False,
    "lines.linewidth": 1.4,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.05,
    "figure.dpi": 150,
})

# Semantic colors
C_PPI = "#4878CF"
C_H2RA = "#E8853D"
C_NEUTRAL = "#999999"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _panel_label(ax: plt.Axes, label: str) -> None:
    ax.text(-0.12, 1.12, label, transform=ax.transAxes,
            fontsize=14, fontweight="bold", va="top", ha="left")


def _kish_ess(w: np.ndarray) -> float:
    s1 = float(np.sum(w))
    s2 = float(np.sum(w ** 2))
    return (s1 ** 2) / s2 if s2 > 0 else 0.0


def _save_fig(fig: plt.Figure, stem: Path, meta: dict[str, Any]) -> None:
    stem.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(stem.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(stem.with_suffix(".png"), dpi=300, bbox_inches="tight")
    plt.close(fig)
    meta["output_pdf"] = str(stem.with_suffix(".pdf"))
    meta["output_png"] = str(stem.with_suffix(".png"))
    meta["generated_utc"] = datetime.now(timezone.utc).isoformat()
    meta["software"] = {
        "python": sys.version,
        "matplotlib": mpl.__version__,
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "platform": platform.platform(),
    }
    with open(stem.with_suffix(".meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, default=str)


def weighted_km_curve(durations, events, weights):
    """Compute weighted Kaplan-Meier curve."""
    t = np.asarray(durations, dtype=float)
    e = np.asarray(events, dtype=int)
    w = np.asarray(weights, dtype=float)
    mask = np.isfinite(t) & np.isfinite(w) & np.isfinite(e)
    t, e, w = t[mask], e[mask], w[mask]
    if t.size == 0:
        return pd.DataFrame(columns=["time", "survival", "n_risk"])
    df = pd.DataFrame({"time": np.maximum(t, 0), "event": e, "w": w})
    df = df.sort_values("time").reset_index(drop=True)
    total_w = float(df["w"].sum())
    total_n = len(df)
    g = (df.assign(w_total=df["w"], w_event=df["w"] * (df["event"] == 1))
         .groupby("time", as_index=False)[["w_total", "w_event"]].sum()
         .sort_values("time").reset_index(drop=True))
    surv, risk = 1.0, total_w
    rows = [{"time": 0.0, "survival": 1.0, "n_risk": total_n}]
    remaining_n = total_n
    for _, row in g.iterrows():
        d = float(row["w_event"])
        if risk > 0 and d > 0:
            surv *= max(0.0, 1.0 - d / risk)
        rows.append({"time": float(row["time"]), "survival": float(surv), "n_risk": remaining_n})
        # approximate remaining subjects
        n_at = int((df["time"] == row["time"]).sum())
        remaining_n -= n_at
        risk -= float(row["w_total"])
    return pd.DataFrame(rows)


def make_board(run_dir: Path, outdir: Path) -> None:
    tables_dir = run_dir / "tables"
    df = pd.read_parquet(tables_dir / "analysis_table_used.parquet")
    bal = pd.read_csv(tables_dir / "balance_smd.csv")
    effects = pd.read_csv(tables_dir / "effect_estimates.csv")

    t = df["treatment_treated"].to_numpy(dtype=int)
    ps = df["ps"].to_numpy(dtype=float)
    w = df["iptw"].to_numpy(dtype=float)

    n_ppi = int(np.sum(t == 1))
    n_h2ra = int(np.sum(t == 0))
    ess_ppi = _kish_ess(w[t == 1])
    ess_h2ra = _kish_ess(w[t == 0])

    fig = plt.figure(figsize=(7.2, 9.6))
    gs = fig.add_gridspec(3, 2, wspace=0.38, hspace=0.50)

    # ── Panel a: Love plot ──
    ax_a = fig.add_subplot(gs[0, 0])
    bal_sorted = bal.sort_values("smd_unweighted", key=lambda s: s.abs(), ascending=True)
    y_a = np.arange(len(bal_sorted))
    ax_a.scatter(bal_sorted["smd_unweighted"].abs(), y_a, s=22, color=C_NEUTRAL,
                 label="Unweighted", alpha=0.7, zorder=2)
    if bal_sorted["smd_weighted"].notna().any():
        ax_a.scatter(bal_sorted["smd_weighted"].abs(), y_a, s=22, color=C_PPI,
                     label="IPTW", zorder=3)
    ax_a.axvline(0.1, color="#D65F5F", lw=0.9, ls="--", label="|SMD|=0.1")
    ax_a.axvline(0, color="black", lw=0.6)
    ax_a.set_yticks(y_a, bal_sorted["feature"].str.replace("_", " ", regex=False), fontsize=6.5)
    ax_a.set_xlabel("|Standardized mean difference|")
    ax_a.legend(frameon=False, fontsize=7, loc="lower right")
    ax_a.text(0.98, 0.02, f"n={n_ppi+n_h2ra}", transform=ax_a.transAxes,
              ha="right", va="bottom", fontsize=7, color="#555555")
    _panel_label(ax_a, "a")

    # ── Panel b: PS overlap (IPTW-weighted) ──
    ax_b = fig.add_subplot(gs[0, 1])
    ps_ppi = ps[t == 1]
    ps_h2ra = ps[t == 0]
    w_ppi = w[t == 1]
    w_h2ra = w[t == 0]

    bins = np.linspace(0, 1, 60)
    ax_b.hist(ps_h2ra, bins=bins, weights=w_h2ra, alpha=0.45, color=C_H2RA,
              label=f"H2RA (n={n_h2ra})", density=True)
    ax_b.hist(ps_ppi, bins=bins, weights=w_ppi, alpha=0.45, color=C_PPI,
              label=f"PPI (n={n_ppi})", density=True)
    ax_b.set_xlabel("Propensity score")
    ax_b.set_ylabel("Density (IPTW-weighted)")
    ax_b.legend(frameon=False, fontsize=7)
    _panel_label(ax_b, "b")

    # ── Panel c: IPTW distribution ──
    ax_c = fig.add_subplot(gs[1, 0])
    ax_c.hist(w[t == 0], bins=60, alpha=0.45, color=C_H2RA, density=True, label="H2RA")
    ax_c.hist(w[t == 1], bins=60, alpha=0.45, color=C_PPI, density=True, label="PPI")
    ax_c.set_xlabel("IPTW weight")
    ax_c.set_ylabel("Density")
    ax_c.legend(frameon=False, fontsize=7)
    ax_c.text(0.98, 0.95, f"ESS: PPI={ess_ppi:.0f}, H2RA={ess_h2ra:.0f}",
              transform=ax_c.transAxes, ha="right", va="top", fontsize=7, color="#555555")
    _panel_label(ax_c, "c")

    # ── Panel d: KM primary outcome (cigib_strict) ──
    ax_d = fig.add_subplot(gs[1, 1])
    primary = effects[effects["outcome"] == "cigib_strict"]
    horizon_primary = 14.0
    if not primary.empty:
        horizon_primary = float(primary.iloc[0]["horizon_days"])

    for g_val, label, color in [(1, "PPI", C_PPI), (0, "H2RA", C_H2RA)]:
        sub = df[df["treatment_treated"] == g_val].dropna(
            subset=["cigib_strict_time_days", "cigib_strict_event", "iptw"])
        if sub.empty:
            continue
        curve = weighted_km_curve(
            sub["cigib_strict_time_days"].to_numpy(),
            sub["cigib_strict_event"].to_numpy(),
            sub["iptw"].to_numpy(),
        )
        curve = curve[curve["time"] <= horizon_primary]
        ax_d.step(curve["time"], curve["survival"], where="post", label=label, color=color, lw=1.5)

    ax_d.axvline(horizon_primary, color="black", ls="--", lw=0.8, alpha=0.5)
    ax_d.set_ylim(0, 1.02)
    ax_d.set_xlim(0, horizon_primary)
    ax_d.set_xlabel("Days since index")
    ax_d.set_ylabel("Survival probability")
    # Risk table annotation
    if not primary.empty:
        hr = float(primary.iloc[0]["ratio"])
        lo = float(primary.iloc[0]["ratio_lo"])
        hi = float(primary.iloc[0]["ratio_hi"])
        ax_d.text(0.98, 0.05, f"HR={hr:.2f} [{lo:.2f}, {hi:.2f}]",
                  transform=ax_d.transAxes, fontsize=6.5, color="#333333", ha="right",
                  bbox=dict(facecolor="white", alpha=0.8, edgecolor="#cccccc", linewidth=0.5))
    ax_d.legend(frameon=False, fontsize=7, loc="upper right")
    _panel_label(ax_d, "d")

    # ── Panel e: KM secondary outcome (death) ──
    ax_e = fig.add_subplot(gs[2, 0])
    death_row = effects[effects["outcome"] == "death"]
    horizon_death = 28.0
    if not death_row.empty:
        horizon_death = float(death_row.iloc[0]["horizon_days"])

    for g_val, label, color in [(1, "PPI", C_PPI), (0, "H2RA", C_H2RA)]:
        sub = df[df["treatment_treated"] == g_val].dropna(
            subset=["death_time_days", "death_event_28d", "iptw"])
        if sub.empty:
            continue
        curve = weighted_km_curve(
            sub["death_time_days"].to_numpy(),
            sub["death_event_28d"].to_numpy(),
            sub["iptw"].to_numpy(),
        )
        curve = curve[curve["time"] <= horizon_death]
        ax_e.step(curve["time"], curve["survival"], where="post", label=label, color=color, lw=1.5)

    ax_e.axvline(horizon_death, color="black", ls="--", lw=0.8, alpha=0.5)
    ax_e.set_ylim(0, 1.02)
    ax_e.set_xlim(0, horizon_death)
    ax_e.set_xlabel("Days since index")
    ax_e.set_ylabel("Survival probability")
    if not death_row.empty:
        hr = float(death_row.iloc[0]["ratio"])
        lo = float(death_row.iloc[0]["ratio_lo"])
        hi = float(death_row.iloc[0]["ratio_hi"])
        ax_e.text(0.98, 0.05, f"HR={hr:.2f} [{lo:.2f}, {hi:.2f}]",
                  transform=ax_e.transAxes, fontsize=6.5, color="#333333", ha="right",
                  bbox=dict(facecolor="white", alpha=0.8, edgecolor="#cccccc", linewidth=0.5))
    ax_e.legend(frameon=False, fontsize=7, loc="upper right")
    _panel_label(ax_e, "e")

    # ── Panel f: Forest plot with ESS ──
    ax_f = fig.add_subplot(gs[2, 1])
    ef = effects.dropna(subset=["ratio", "ratio_lo", "ratio_hi"]).copy()
    if not ef.empty:
        ef = ef.iloc[::-1].reset_index(drop=True)
        y_f = np.arange(len(ef))
        xerr_lo = ef["ratio"] - ef["ratio_lo"]
        xerr_hi = ef["ratio_hi"] - ef["ratio"]

        ax_f.errorbar(ef["ratio"], y_f,
                      xerr=[xerr_lo, xerr_hi],
                      fmt="none", ecolor="#555555", capsize=3, lw=0.8, zorder=2)

        for i, (_, row) in enumerate(ef.iterrows()):
            col = "#D65F5F" if row["ratio"] > 1 else C_PPI
            ax_f.scatter(row["ratio"], y_f[i], s=55, color=col, zorder=3,
                         edgecolors="white", lw=0.4)

        ax_f.axvline(1.0, color="black", lw=0.9)
        ax_f.set_xscale("log")
        ax_f.set_yticks(y_f, ef["outcome_label"], fontsize=7)
        ax_f.set_xlabel("Effect ratio (log scale)")

        for i, (_, row) in enumerate(ef.iterrows()):
            effect_type = row.get("effect_type", "")
            label = "HR" if effect_type == "hr" else "RR"
            ax_f.annotate(
                f"{label}={row['ratio']:.2f} [{row['ratio_lo']:.2f}, {row['ratio_hi']:.2f}]",
                xy=(1.02, y_f[i]), xycoords=("axes fraction", "data"),
                fontsize=5, color="#555555", va="center", annotation_clip=False,
            )

    ax_f.set_xlabel(
        f"Effect ratio (log scale)\nESS: PPI={ess_ppi:.0f}, H2RA={ess_h2ra:.0f}  |  n={n_ppi}+{n_h2ra}",
        fontsize=7)
    _panel_label(ax_f, "f")

    # Input checksums
    input_files = {
        "analysis_table": str(tables_dir / "analysis_table_used.parquet"),
        "balance_smd": str(tables_dir / "balance_smd.csv"),
        "effect_estimates": str(tables_dir / "effect_estimates.csv"),
    }
    checksums = {}
    for k, p in input_files.items():
        pp = Path(p)
        if pp.exists():
            checksums[k] = _sha256(pp)

    meta = {
        "figure": "dlfx_Publication_Board",
        "project": "ICU PPI vs H2RA target trial emulation",
        "panels": [
            "a:love_plot_balance",
            "b:ps_overlap_weighted",
            "c:iptw_distribution",
            "d:km_primary_cigib_strict",
            "e:km_secondary_mortality",
            "f:forest_effect_estimates",
        ],
        "inputs": input_files,
        "input_checksums": checksums,
        "sample_sizes": {
            "n_ppi": n_ppi, "n_h2ra": n_h2ra,
            "ess_ppi": round(ess_ppi, 1), "ess_h2ra": round(ess_h2ra, 1),
        },
        "parameters": {
            "smd_threshold": 0.1,
            "km_horizon_primary": horizon_primary,
            "km_horizon_death": horizon_death,
        },
    }
    _save_fig(fig, outdir / "dlfx_Publication_Board", meta)
    print(f"[ok] dlfx publication board → {outdir}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, default=Path("output/synth_run3"))
    parser.add_argument("--outdir", type=Path, default=None)
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    outdir = args.outdir.resolve() if args.outdir else (run_dir / "publication")
    outdir.mkdir(parents=True, exist_ok=True)
    make_board(run_dir, outdir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
