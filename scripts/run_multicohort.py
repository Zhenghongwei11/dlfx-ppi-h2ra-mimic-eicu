#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dlfx.audit import record_file, utc_now_iso, write_json
from dlfx.io import read_table, write_table
from dlfx.meta import combine_effect_tables
from dlfx.plots import plot_ratio_forest
from dlfx.study import load_config, run_study


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run MIMIC + eICU and generate combined validation outputs.")
    p.add_argument("--primary", required=True, help="Primary cohort table (e.g., MIMIC) (.csv or .parquet).")
    p.add_argument("--external", required=True, help="External cohort table (e.g., eICU) (.csv or .parquet).")
    p.add_argument("--outdir", required=True, help="Output directory (will create subfolders).")
    p.add_argument("--label-primary", default="mimic", help="Label for primary cohort.")
    p.add_argument("--label-external", default="eicu", help="Label for external cohort.")
    p.add_argument(
        "--config",
        default=str(ROOT / "configs" / "study_default.yaml"),
        help="Path to YAML config (default: configs/study_default.yaml).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    out_primary = outdir / args.label_primary
    out_external = outdir / args.label_external
    out_combined = outdir / "combined"
    out_combined.mkdir(parents=True, exist_ok=True)

    cfg = load_config(args.config)
    run_study(input_path=args.primary, outdir=out_primary, config=cfg, repo_root=ROOT)
    run_study(input_path=args.external, outdir=out_external, config=cfg, repo_root=ROOT)

    eff_a = pd.read_csv(out_primary / "tables" / "effect_estimates.csv")
    eff_b = pd.read_csv(out_external / "tables" / "effect_estimates.csv")
    combined = combine_effect_tables(eff_a, eff_b, label_a=args.label_primary, label_b=args.label_external)
    write_table(combined, out_combined / "effect_estimates_combined.csv")

    # Forest plot: we plot pooled if present, otherwise cohort-specific points are in the CSV.
    pooled = combined.rename(columns={"pooled_ratio": "ratio", "pooled_ratio_lo": "ratio_lo", "pooled_ratio_hi": "ratio_hi"})[
        ["outcome_label", "ratio", "ratio_lo", "ratio_hi"]
    ].dropna()
    if not pooled.empty:
        plot_ratio_forest(pooled, outpath=out_combined / "forest_ratio_pooled.png", title="Pooled effect (random effects)")

    audit = {
        "run_started_utc": utc_now_iso(),
        "inputs": {
            "primary": record_file(args.primary).__dict__,
            "external": record_file(args.external).__dict__,
        },
        "outputs": {
            "primary_dir": str(out_primary),
            "external_dir": str(out_external),
            "combined_dir": str(out_combined),
        },
        "run_finished_utc": utc_now_iso(),
    }
    write_json(audit, outdir / "audit_multicohort.json")
    print(f"Wrote multicohort outputs to: {outdir}")


if __name__ == "__main__":
    main()

