#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import numpy as np
import pandas as pd

from dlfx.io import write_table
from dlfx.meta import combine_effect_tables
from dlfx.plots import plot_ratio_forest


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Rebuild combined effect tables + sensitivity_summary.tsv from existing per-cohort outputs, "
            "without re-extraction or re-running models."
        )
    )
    p.add_argument(
        "--multicohort-root",
        default=str(ROOT / "output" / "multicohort_run"),
        help="Root multicohort output directory (contains combined/ and sensitivity/).",
    )
    p.add_argument(
        "--label-primary",
        default="mimic",
        help="Primary cohort label under each run directory (default: mimic).",
    )
    p.add_argument(
        "--label-external",
        default="eicu",
        help="External cohort label under each run directory (default: eicu).",
    )
    p.add_argument(
        "--summary-out",
        default=str(ROOT / "output" / "multicohort_run" / "combined" / "sensitivity_summary.tsv"),
        help="Output TSV path for rebuilt sensitivity summary.",
    )
    return p.parse_args()


def _count_parquet(path: Path) -> int:
    import duckdb  # type: ignore

    con = duckdb.connect(database=":memory:")
    lit = "'" + str(path).replace("'", "''") + "'"
    return int(con.execute(f"select count(*) from read_parquet({lit});").fetchone()[0])


def _sanitize_effects_for_combine(eff: pd.DataFrame) -> pd.DataFrame:
    """
    Avoid propagating undefined ratios like 0/0 -> inf into combined outputs.
    Keep per-cohort outputs untouched; only sanitize for combined tables & summaries.
    """
    out = eff.copy()
    if {"effect_type", "ratio", "risk_treated", "risk_control"}.issubset(out.columns):
        rr = out["effect_type"].astype(str) == "rr"
        both_zero = (out["risk_treated"].fillna(np.nan) == 0) & (out["risk_control"].fillna(np.nan) == 0)
        bad = rr & both_zero & np.isinf(out["ratio"].astype(float))
        if bad.any():
            out.loc[bad, ["ratio", "ratio_lo", "ratio_hi"]] = np.nan
            note = out.get("note")
            if note is not None:
                out.loc[bad, "note"] = note.fillna("").astype(str) + " No events in either arm; RR undefined."
    return out


def _rebuild_combined(run_dir: Path, *, label_a: str, label_b: str) -> bool:
    eff_a_path = run_dir / label_a / "tables" / "effect_estimates.csv"
    eff_b_path = run_dir / label_b / "tables" / "effect_estimates.csv"
    if not (eff_a_path.exists() and eff_b_path.exists()):
        return False

    eff_a = pd.read_csv(eff_a_path)
    eff_b = pd.read_csv(eff_b_path)
    eff_a = _sanitize_effects_for_combine(eff_a)
    eff_b = _sanitize_effects_for_combine(eff_b)

    combined = combine_effect_tables(eff_a, eff_b, label_a=label_a, label_b=label_b)
    (run_dir / "combined").mkdir(parents=True, exist_ok=True)
    write_table(combined, run_dir / "combined" / "effect_estimates_combined.csv")

    pooled = (
        combined.rename(columns={"pooled_ratio": "ratio", "pooled_ratio_lo": "ratio_lo", "pooled_ratio_hi": "ratio_hi"})[
            ["outcome_label", "ratio", "ratio_lo", "ratio_hi"]
        ]
        .dropna()
        .copy()
    )
    if not pooled.empty:
        plot_ratio_forest(pooled, outpath=run_dir / "combined" / "forest_ratio_pooled.png", title="Pooled effect (random effects)")
    return True


def _tidy_combined(
    combined_csv: Path,
    *,
    sensitivity_id: str,
    landmark_hours: int,
    n_mimic: int,
    n_eicu: int,
    subgroup_name: str | None = None,
    subgroup_level: str | None = None,
) -> list[dict]:
    df = pd.read_csv(combined_csv)
    rows: list[dict] = []
    for _, r in df.iterrows():
        for cohort, n, cols in [
            ("mimic", n_mimic, ("ratio_mimic", "ratio_lo_mimic", "ratio_hi_mimic", "effect_type_mimic")),
            ("eicu", n_eicu, ("ratio_eicu", "ratio_lo_eicu", "ratio_hi_eicu", "effect_type_eicu")),
        ]:
            ratio = r.get(cols[0])
            if ratio is None or (isinstance(ratio, float) and pd.isna(ratio)):
                continue
            rows.append(
                {
                    "sensitivity_id": sensitivity_id,
                    "landmark_hours": int(landmark_hours),
                    "subgroup_name": subgroup_name,
                    "subgroup_level": subgroup_level,
                    "cohort": cohort,
                    "n": int(n),
                    "outcome": r["outcome"],
                    "outcome_label": r["outcome_label"],
                    "effect_type": r.get(cols[3]),
                    "ratio": ratio,
                    "ratio_lo": r.get(cols[1]),
                    "ratio_hi": r.get(cols[2]),
                    "tau2": None,
                }
            )

        pooled = r.get("pooled_ratio")
        if pooled is not None and not (isinstance(pooled, float) and pd.isna(pooled)):
            rows.append(
                {
                    "sensitivity_id": sensitivity_id,
                    "landmark_hours": int(landmark_hours),
                    "subgroup_name": subgroup_name,
                    "subgroup_level": subgroup_level,
                    "cohort": "pooled",
                    "n": None,
                    "outcome": r["outcome"],
                    "outcome_label": r["outcome_label"],
                    "effect_type": "pooled_random_effects",
                    "ratio": pooled,
                    "ratio_lo": r.get("pooled_ratio_lo"),
                    "ratio_hi": r.get("pooled_ratio_hi"),
                    "tau2": r.get("pooled_tau2"),
                }
            )
    return rows


def main() -> None:
    args = parse_args()
    root = Path(args.multicohort_root)
    label_a = str(args.label_primary)
    label_b = str(args.label_external)

    # 1) Rebuild combined tables wherever per-cohort effects exist.
    rebuilt = 0
    seen_run_dirs: set[Path] = set()
    for eff in root.rglob("tables/effect_estimates.csv"):
        # run_dir is two levels up: <run>/<label>/tables/effect_estimates.csv
        run_dir = eff.parents[2]
        if run_dir in seen_run_dirs:
            continue
        seen_run_dirs.add(run_dir)
        if _rebuild_combined(run_dir, label_a=label_a, label_b=label_b):
            rebuilt += 1

    # 2) Rebuild sensitivity_summary.tsv from existing sensitivity runs.
    sens_root = root / "sensitivity"
    all_rows: list[dict] = []
    if sens_root.exists():
        for outdir in sorted([p for p in sens_root.iterdir() if p.is_dir()]):
            audit_path = outdir / "audit" / "sensitivity_audit.json"
            if not audit_path.exists():
                continue
            audit = json.loads(audit_path.read_text(encoding="utf-8"))
            sid = str(audit.get("sensitivity_id", outdir.name))
            lh = int(audit.get("landmark_hours", 24))

            subgroup = audit.get("subgroup")
            if subgroup:
                sub_name = str(subgroup.get("name"))
                levels_dir = outdir / "levels"
                for lvl in sorted([p for p in levels_dir.iterdir() if p.is_dir()]):
                    combined_csv = lvl / "combined" / "effect_estimates_combined.csv"
                    if not combined_csv.exists():
                        continue
                    n_m = _count_parquet(lvl / label_a / "tables" / "analysis_table_used.parquet")
                    n_e = _count_parquet(lvl / label_b / "tables" / "analysis_table_used.parquet")
                    all_rows.extend(
                        _tidy_combined(
                            combined_csv,
                            sensitivity_id=sid,
                            landmark_hours=lh,
                            n_mimic=n_m,
                            n_eicu=n_e,
                            subgroup_name=sub_name,
                            subgroup_level=lvl.name,
                        )
                    )
                continue

            combined_csv = outdir / "combined" / "effect_estimates_combined.csv"
            if not combined_csv.exists():
                continue
            n_m = _count_parquet(outdir / label_a / "tables" / "analysis_table_used.parquet")
            n_e = _count_parquet(outdir / label_b / "tables" / "analysis_table_used.parquet")
            all_rows.extend(
                _tidy_combined(
                    combined_csv,
                    sensitivity_id=sid,
                    landmark_hours=lh,
                    n_mimic=n_m,
                    n_eicu=n_e,
                )
            )

    out_df = pd.DataFrame(all_rows)
    summary_out = Path(args.summary_out)
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    if not out_df.empty:
        out_df = out_df[
            [
                "sensitivity_id",
                "landmark_hours",
                "subgroup_name",
                "subgroup_level",
                "cohort",
                "n",
                "outcome",
                "outcome_label",
                "effect_type",
                "ratio",
                "ratio_lo",
                "ratio_hi",
                "tau2",
            ]
        ].sort_values(
            ["sensitivity_id", "subgroup_name", "subgroup_level", "cohort", "outcome"],
            na_position="last",
        )
    out_df.to_csv(summary_out, sep="\t", index=False)

    print(f"Rebuilt combined tables: {rebuilt}")
    print(f"Wrote rebuilt sensitivity summary: {summary_out}")


if __name__ == "__main__":
    main()
