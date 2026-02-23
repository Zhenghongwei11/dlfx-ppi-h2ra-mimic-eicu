#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

# Allow running without installing the package.
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from dlfx.balance import balance_table, love_plot
from dlfx.effects import fit_weighted_cox, weighted_binary_risks, weighted_km_risk_at
from dlfx.io import read_table, write_table
from dlfx.preprocess import TreatmentEncoding, encode_treatment, one_hot_balance_frame, split_covariates
from dlfx.ps import PSConfig, fit_propensity_score


OUTCOME_DEFAULTS = {
    "cigib_strict": {"event_col": "cigib_strict_event", "time_col": "cigib_strict_time_days", "horizon_days": 14.0},
    "ugib_broad": {"event_col": "ugib_broad_event", "time_col": "ugib_broad_time_days", "horizon_days": 14.0},
    "cdi": {"event_col": "cdi_event", "time_col": "cdi_time_days", "horizon_days": 14.0},
    "death": {"event_col": "death_event_28d", "time_col": "death_time_days", "horizon_days": 28.0},
}


DEFAULT_COVARIATES = [
    "age_years",
    "sex",
    "race",
    "sofa_24h",
    "hgb_min_24h",
    "platelet_min_24h",
    "inr_max_24h",
    "creatinine_max_24h",
    "lactate_max_24h",
]


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _weight_summary(df: pd.DataFrame, *, weight_col: str, treat_col: str) -> dict:
    w = df[weight_col].to_numpy(dtype=float)
    t = df[treat_col].to_numpy(dtype=int)
    out = {
        "n": int(df.shape[0]),
        "n_treated": int(np.sum(t == 1)),
        "n_control": int(np.sum(t == 0)),
        "weight_min": float(np.min(w)),
        "weight_p01": float(np.quantile(w, 0.01)),
        "weight_p50": float(np.quantile(w, 0.50)),
        "weight_p99": float(np.quantile(w, 0.99)),
        "weight_max": float(np.max(w)),
    }
    for label, mask in [("treated", t == 1), ("control", t == 0)]:
        ww = w[mask]
        ess = (float(np.sum(ww)) ** 2) / float(np.sum(ww**2))
        out[f"ess_{label}"] = float(ess)
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Target trial emulation scaffold: SUP PPI vs H2RA (IPTW + Cox/KM).")
    p.add_argument("--input", required=True, help="Path to analysis-ready table (.csv or .parquet).")
    p.add_argument("--outdir", required=True, help="Output directory.")
    p.add_argument(
        "--outcome",
        required=True,
        choices=sorted(OUTCOME_DEFAULTS.keys()),
        help="Outcome to analyze.",
    )
    p.add_argument("--event-col", default=None, help="Override event column name.")
    p.add_argument("--time-col", default=None, help="Override time-to-event column name (days from index).")
    p.add_argument("--horizon-days", type=float, default=None, help="Override risk horizon in days (e.g., 14).")
    p.add_argument("--treatment-col", default="treatment", help="Treatment column (ppi vs h2ra).")
    p.add_argument(
        "--covariates",
        default=",".join(DEFAULT_COVARIATES),
        help="Comma-separated baseline covariates used in PS model.",
    )
    p.add_argument("--no-splines", action="store_true", help="Disable spline expansion for continuous covariates.")
    p.add_argument("--trunc-lo", type=float, default=0.01, help="Lower quantile for weight truncation.")
    p.add_argument("--trunc-hi", type=float, default=0.99, help="Upper quantile for weight truncation.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    outdir = Path(args.outdir)
    _ensure_dir(outdir)

    df = read_table(args.input)
    df = df.copy()

    # Keep only the active comparator arms.
    df = df[df[args.treatment_col].isin(["ppi", "h2ra"])].reset_index(drop=True)
    if df.empty:
        raise SystemExit("No rows after filtering to treatment in {ppi,h2ra}.")

    t = encode_treatment(df, treatment_col=args.treatment_col, encoding=TreatmentEncoding())
    df["treatment_ppi"] = t

    covariates_raw = [c.strip() for c in args.covariates.split(",") if c.strip()]
    missing_cov = [c for c in covariates_raw if c not in df.columns]
    covariates = [c for c in covariates_raw if c in df.columns]
    if not covariates:
        raise SystemExit("No covariates found in input table. Check --covariates.")

    # Drop unusable covariates (all missing or no variance).
    dropped_cov = []
    usable = []
    for c in covariates:
        s = df[c]
        if pd.api.types.is_numeric_dtype(s):
            x = pd.to_numeric(s, errors="coerce")
            if not x.notna().any():
                dropped_cov.append((c, "all_missing"))
                continue
            if x.dropna().nunique() <= 1:
                dropped_cov.append((c, "no_variance"))
                continue
        else:
            x = s.astype("string")
            if not x.notna().any():
                dropped_cov.append((c, "all_missing"))
                continue
            if x.dropna().nunique() <= 1:
                dropped_cov.append((c, "no_variance"))
                continue
        usable.append(c)
    covariates = usable
    if not covariates:
        raise SystemExit("All covariates are missing or constant after filtering.")

    categorical, continuous = split_covariates(df, covariates)

    ps_config = PSConfig(
        spline_continuous=(not args.no_splines),
        weight_truncation=(args.trunc_lo, args.trunc_hi),
    )
    ps, w, _model = fit_propensity_score(
        df,
        treatment_indicator=t,
        covariates=covariates,
        categorical=categorical,
        continuous=continuous,
        config=ps_config,
    )
    df["ps"] = ps
    df["iptw"] = w

    # Balance diagnostics
    features = one_hot_balance_frame(df, categorical=categorical, continuous=continuous)
    bal = balance_table(features, treatment_indicator=t, weights=df["iptw"])
    write_table(bal, outdir / "balance_smd.csv")
    love_plot(bal, outpath=outdir / "love_plot.png")

    # Outcome setup
    defaults = OUTCOME_DEFAULTS[args.outcome]
    event_col = args.event_col or defaults["event_col"]
    time_col = args.time_col or defaults["time_col"]
    horizon = float(args.horizon_days) if args.horizon_days is not None else float(defaults["horizon_days"])

    if event_col not in df.columns:
        raise SystemExit(f"Missing event column: {event_col}")
    if time_col not in df.columns:
        time_col = None

    # Effect estimates
    results: dict = {
        "outcome": args.outcome,
        "event_col": event_col,
        "time_col": time_col,
        "horizon_days": horizon,
        "covariates_used": covariates,
        "covariates_missing_in_input": missing_cov,
        "covariates_dropped": [{"name": n, "reason": r} for n, r in dropped_cov],
        "ps": {
            "spline_continuous": ps_config.spline_continuous,
            "spline_n_knots": ps_config.spline_n_knots,
            "spline_degree": ps_config.spline_degree,
            "weight_truncation": list(ps_config.weight_truncation),
        },
        "weights": _weight_summary(df, weight_col="iptw", treat_col="treatment_ppi"),
    }

    # If time-to-event is available (non-null), run weighted Cox + weighted KM at horizon.
    has_time = False
    if time_col is not None:
        series = pd.to_numeric(df[time_col], errors="coerce")
        has_time = series.notna().any()
        if has_time:
            df[time_col] = series

    df[event_col] = pd.to_numeric(df[event_col], errors="coerce").fillna(0).astype(int)

    if has_time and time_col is not None:
        cox = fit_weighted_cox(
            df,
            duration_col=time_col,
            event_col=event_col,
            weight_col="iptw",
            treatment_indicator_col="treatment_ppi",
        )
        results["cox_hr"] = {"hr": cox.hr, "ci95": list(cox.hr_ci95)}

        risk_ppi = weighted_km_risk_at(
            df[df["treatment_ppi"] == 1],
            duration_col=time_col,
            event_col=event_col,
            weight_col="iptw",
            horizon_days=horizon,
        )
        risk_h2ra = weighted_km_risk_at(
            df[df["treatment_ppi"] == 0],
            duration_col=time_col,
            event_col=event_col,
            weight_col="iptw",
            horizon_days=horizon,
        )
        rd = float(risk_ppi - risk_h2ra)
        rr = float(risk_ppi / risk_h2ra) if risk_h2ra > 0 else float("inf")
        results["risk_at_horizon"] = {
            "risk_ppi": float(risk_ppi),
            "risk_h2ra": float(risk_h2ra),
            "rd": rd,
            "rr": rr,
            "note": "CI for KM risk not computed in this scaffold; use bootstrap if needed.",
        }
    else:
        risks = weighted_binary_risks(
            df,
            treatment_indicator_col="treatment_ppi",
            event_col=event_col,
            weight_col="iptw",
        )
        results["risk_binary"] = {
            "risk_ppi": risks.risk_treated,
            "risk_h2ra": risks.risk_control,
            "rd": risks.rd,
            "rr": risks.rr,
            "rd_ci95": list(risks.rd_ci95) if risks.rd_ci95 else None,
            "rr_ci95": list(risks.rr_ci95) if risks.rr_ci95 else None,
            "note": "Binary outcome assumed to be within a fixed follow-up window.",
        }

    (outdir / "analysis_table_used").mkdir(parents=True, exist_ok=True)
    write_table(df, outdir / "analysis_table_used" / "analysis_table.parquet")

    with open(outdir / "results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Wrote outputs to: {outdir}")


if __name__ == "__main__":
    main()
