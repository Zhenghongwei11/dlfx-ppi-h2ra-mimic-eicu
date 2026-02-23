#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate Figure 4 panels and supplement tables from sensitivity_summary.tsv.")
    p.add_argument(
        "--summary",
        default=str(ROOT / "output" / "multicohort_run" / "combined" / "sensitivity_summary.tsv"),
        help="Path to combined sensitivity summary TSV (Source Data 6).",
    )
    p.add_argument(
        "--outdir",
        default=str(ROOT / "output" / "multicohort_run" / "combined"),
        help="Output directory for figures and supplement tables.",
    )
    p.add_argument(
        "--include-g1-panel",
        action="store_true",
        help="Include G1 subgroup panel (MV driver mv_1 vs mv_0) in the Figure 4 composite.",
    )
    return p.parse_args()


def _ensure_dir(p: str | Path) -> Path:
    p = Path(p)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _panel_df(df: pd.DataFrame, *, rows: list[tuple[str, str]], title_prefix: str) -> pd.DataFrame:
    """
    Build a forest-ready frame with columns: outcome_label, ratio, ratio_lo, ratio_hi.
    `rows` are tuples: (label, sensitivity_id filter key).
    """
    out = []
    for label, sid in rows:
        sub = df[(df["sensitivity_id"] == sid) & (df["cohort"] == "pooled")].copy()
        if sub.empty:
            continue
        # Prefer strict CIGIB rows for panel A.
        r = sub[sub["outcome"] == "cigib_strict"]
        if r.empty:
            continue
        rr = r.iloc[0]
        out.append(
            {
                "outcome_label": f"{title_prefix}{label}",
                "ratio": float(rr["ratio"]),
                "ratio_lo": float(rr["ratio_lo"]),
                "ratio_hi": float(rr["ratio_hi"]),
            }
        )
    return pd.DataFrame(out)


def main() -> None:
    args = parse_args()
    summary_path = Path(args.summary)
    outdir = _ensure_dir(args.outdir)

    df = pd.read_csv(summary_path, sep="\t")

    # Supplement tables (submission-facing tables should be formatted later; these are reproducible anchors).
    strict = df[df["outcome"].isin(["cigib_strict", "cigib_strict_competing_risk_death"])].copy()
    strict.to_csv(outdir / "supplement_table_S1_strict_cigib.tsv", sep="\t", index=False)

    secondary = df[df["outcome"].isin(["death", "ugib_broad", "cdi"])].copy()
    secondary.to_csv(outdir / "supplement_table_S2_secondary_outcomes.tsv", sep="\t", index=False)

    # Figure 4 composite: Panel A (robustness sensitivities, strict CIGIB HR) + Panel B (competing risk).
    # We intentionally EXCLUDE S5 (negative-control early window) from main Figure 4 robustness forest;
    # it belongs in the supplement as a falsification test.
    panel_a_rows = [
        ("Mainline (24h)", "S6_COMPETING_RISK_DEATH"),
        ("Landmark 12h", "S1_LANDMARK_12H"),
        ("Landmark 6h", "S2_LANDMARK_6H"),
        ("Alt exposure definition", "S3_ALT_EXPOSURE_SOURCE"),
        ("Exclude early bleed proxy", "S4_EXCLUDE_EARLY_BLEED"),
    ]
    a = _panel_df(df, rows=panel_a_rows, title_prefix="")

    # Panel B: competing risk row (CIF-RR at 14d).
    bsub = df[(df["sensitivity_id"] == "S6_COMPETING_RISK_DEATH") & (df["cohort"] == "pooled")].copy()
    b = bsub[bsub["outcome"] == "cigib_strict_competing_risk_death"].copy()
    b_row = None
    if not b.empty:
        rr = b.iloc[0]
        b_row = {
            "outcome_label": "Competing risk: death (CIF-RR at 14d)",
            "ratio": float(rr["ratio"]),
            "ratio_lo": float(rr["ratio_lo"]),
            "ratio_hi": float(rr["ratio_hi"]),
        }

    # Optional Panel C: G1 subgroup (MV driver).
    c = None
    if bool(args.include_g1_panel):
        csub = df[
            (df["sensitivity_id"] == "G1_SUBGROUP_SUP_DRIVER")
            & (df["cohort"] == "pooled")
            & (df["outcome"] == "cigib_strict")
        ].copy()
        labels = {"mv_1": "MV=1", "mv_0": "MV=0"}
        out = []
        for level in ["mv_1", "mv_0"]:
            r = csub[csub["subgroup_level"] == level]
            if r.empty:
                continue
            rr = r.iloc[0]
            out.append(
                {
                    "outcome_label": f"Indication driver subgroup: {labels.get(level, level)}",
                    "ratio": float(rr["ratio"]),
                    "ratio_lo": float(rr["ratio_lo"]),
                    "ratio_hi": float(rr["ratio_hi"]),
                }
            )
        c = pd.DataFrame(out)

    # Write panel anchor tables
    a.to_csv(outdir / "figure4_panelA_strict_cigib.tsv", sep="\t", index=False)
    if b_row is not None:
        pd.DataFrame([b_row]).to_csv(outdir / "figure4_panelB_competing_risk.tsv", sep="\t", index=False)
    if c is not None:
        c.to_csv(outdir / "figure4_panelC_g1_subgroup.tsv", sep="\t", index=False)

    # Plot (simple, publication draft; journal styling can be adjusted later).
    import matplotlib.pyplot as plt

    def forest(ax, d: pd.DataFrame, title: str) -> None:
        if d is None or d.empty:
            ax.axis("off")
            return
        dd = d.dropna(subset=["ratio", "ratio_lo", "ratio_hi"]).copy()
        dd = dd.iloc[::-1].reset_index(drop=True)
        y = range(len(dd))
        ax.errorbar(
            dd["ratio"],
            list(y),
            xerr=[dd["ratio"] - dd["ratio_lo"], dd["ratio_hi"] - dd["ratio"]],
            fmt="o",
            color="black",
            ecolor="black",
            capsize=3,
        )
        ax.axvline(1.0, color="black", linewidth=1)
        ax.set_xscale("log")
        ax.set_yticks(list(y))
        ax.set_yticklabels(dd["outcome_label"])
        ax.set_xlabel("Ratio (log scale)")
        ax.set_title(title)

    n_panels = 2 + (1 if (bool(args.include_g1_panel) and c is not None and not c.empty) else 0)
    fig_h = 3.2 + 0.45 * max(len(a), 1) + (2.6 if n_panels == 3 else 0.0)
    fig, axes = plt.subplots(n_panels, 1, figsize=(9.2, fig_h), gridspec_kw={"height_ratios": [max(len(a), 1), 1, 2] if n_panels == 3 else [max(len(a), 1), 1]})
    if n_panels == 2:
        ax_a, ax_b = axes
    else:
        ax_a, ax_b, ax_c = axes

    forest(ax_a, a, "Figure 4A. Sensitivity analyses (strict CIGIB; pooled HR)")
    if b_row is not None:
        forest(ax_b, pd.DataFrame([b_row]), "Figure 4B. Competing risk sensitivity (death)")
    else:
        ax_b.axis("off")

    if n_panels == 3:
        forest(ax_c, c, "Figure 4C. Planned subgroup (SUP indication driver)")

    plt.tight_layout()
    png = outdir / "figure4_sensitivity.png"
    pdf = outdir / "figure4_sensitivity.pdf"
    fig.savefig(png, dpi=300)
    fig.savefig(pdf)
    plt.close(fig)

    print(f"Wrote: {png}")
    print(f"Wrote: {pdf}")
    print(f"Wrote: {outdir/'supplement_table_S1_strict_cigib.tsv'}")
    print(f"Wrote: {outdir/'supplement_table_S2_secondary_outcomes.tsv'}")


if __name__ == "__main__":
    main()
