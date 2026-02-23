#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dlfx.io import read_table
from dlfx.study import load_config


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate that an extracted analysis table matches the config.")
    p.add_argument("--input", required=True, help="Analysis table (.csv or .parquet).")
    p.add_argument(
        "--config",
        default=str(ROOT / "configs" / "study_default.yaml"),
        help="YAML config (default: configs/study_default.yaml).",
    )
    p.add_argument("--out", default=None, help="Optional JSON report path.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    df = read_table(args.input)

    report: dict = {"input": str(args.input), "n_rows": int(df.shape[0]), "n_cols": int(df.shape[1])}
    missing_cov = [c for c in cfg.covariates if c not in df.columns]
    report["missing_covariates"] = missing_cov

    outcome_checks = []
    for o in cfg.outcomes:
        ok_event = o.event_col in df.columns and pd.to_numeric(df[o.event_col], errors="coerce").notna().any()
        ok_time = True
        if o.time_col is not None:
            time_series = pd.to_numeric(df[o.time_col], errors="coerce") if o.time_col in df.columns else None
            if o.required:
                ok_time = time_series is not None and time_series.notna().all()
            else:
                ok_time = time_series is not None and time_series.notna().any()
        outcome_checks.append(
            {
                "name": o.name,
                "required": o.required,
                "event_col": o.event_col,
                "time_col": o.time_col,
                "event_col_ok": bool(ok_event),
                "time_col_ok": bool(ok_time),
            }
        )
    report["outcomes"] = outcome_checks

    # Hard fail conditions
    failures = []
    if cfg.treatment_col not in df.columns:
        failures.append(f"missing_treatment_col:{cfg.treatment_col}")
    for oc in outcome_checks:
        if oc["required"] and not oc["event_col_ok"]:
            failures.append(f"missing_required_outcome_event:{oc['name']}:{oc['event_col']}")
    report["failures"] = failures

    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text, encoding="utf-8")
    print(text)

    if failures:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
