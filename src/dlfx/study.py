from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np
import pandas as pd
import yaml
from decimal import Decimal

from .audit import collect_environment, record_file, safe_git_info, utc_now_iso, write_json
from .balance import balance_table, love_plot
from .effects import fit_weighted_cox, weighted_binary_risks, weighted_km_risk_at
from .io import read_table, write_table
from .plots import plot_hist_overlap, plot_km_curves, plot_ratio_forest, plot_weight_hist
from .preprocess import TreatmentEncoding, encode_treatment, one_hot_balance_frame, split_covariates
from .ps import PSConfig, fit_propensity_score
from .table1 import make_table1


@dataclass(frozen=True)
class OutcomeSpec:
    name: str
    label: str
    event_col: str
    time_col: Optional[str]
    horizon_days: float
    required: bool = False


@dataclass(frozen=True)
class StudyConfig:
    treatment_col: str
    treated_label: str
    control_label: str
    covariates: list[str]
    ps: PSConfig
    outcomes: list[OutcomeSpec]


def _coerce_decimals_to_float(df: pd.DataFrame, cols: Sequence[str]) -> None:
    """
    DuckDB can roundtrip NUMERIC columns to Parquet as Decimal objects, which Pandas
    reads as dtype=object. Treat those as numeric for modeling and Table 1.
    """
    for c in cols:
        if c not in df.columns:
            continue
        s = df[c]
        if s.dtype != object:
            continue
        # Fast check to avoid touching large object columns.
        head = s.dropna().head(32).tolist()
        if not head:
            continue
        if not any(isinstance(v, Decimal) for v in head):
            continue
        df[c] = pd.to_numeric(
            s.map(lambda v: float(v) if isinstance(v, Decimal) else v),
            errors="coerce",
        )


def load_config(path: str | Path) -> StudyConfig:
    path = Path(path)
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Config must be a YAML mapping.")

    treatment_col = str(raw.get("treatment_col", "treatment"))
    treated_label = str(raw.get("treated_label", "ppi"))
    control_label = str(raw.get("control_label", "h2ra"))

    covariates = raw.get("covariates", [])
    if not isinstance(covariates, list) or not all(isinstance(x, str) for x in covariates):
        raise ValueError("covariates must be a list of strings.")

    ps_raw = raw.get("ps", {}) or {}
    if not isinstance(ps_raw, dict):
        raise ValueError("ps must be a mapping.")
    ps = PSConfig(
        spline_continuous=bool(ps_raw.get("spline_continuous", True)),
        spline_n_knots=int(ps_raw.get("spline_n_knots", 5)),
        spline_degree=int(ps_raw.get("spline_degree", 3)),
        weight_truncation=tuple(ps_raw.get("weight_truncation", (0.01, 0.99))),  # type: ignore[arg-type]
    )

    outs = raw.get("outcomes", [])
    if not isinstance(outs, list) or not outs:
        raise ValueError("outcomes must be a non-empty list.")
    outcomes: list[OutcomeSpec] = []
    for o in outs:
        if not isinstance(o, dict):
            raise ValueError("Each outcome must be a mapping.")
        outcomes.append(
            OutcomeSpec(
                name=str(o["name"]),
                label=str(o.get("label", o["name"])),
                event_col=str(o["event_col"]),
                time_col=str(o["time_col"]) if o.get("time_col") is not None else None,
                horizon_days=float(o.get("horizon_days", 14.0)),
                required=bool(o.get("required", False)),
            )
        )

    return StudyConfig(
        treatment_col=treatment_col,
        treated_label=treated_label,
        control_label=control_label,
        covariates=list(covariates),
        ps=ps,
        outcomes=outcomes,
    )


def _weight_summary(df: pd.DataFrame, *, weight_col: str, treat_col: str) -> dict[str, Any]:
    w = df[weight_col].to_numpy(dtype=float)
    t = df[treat_col].to_numpy(dtype=int)
    out: dict[str, Any] = {
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


def run_study(
    *,
    input_path: str | Path,
    outdir: str | Path,
    config: StudyConfig,
    repo_root: Optional[str | Path] = None,
) -> dict[str, Any]:
    input_path = Path(input_path)
    outdir = Path(outdir)
    figures_dir = outdir / "figures"
    tables_dir = outdir / "tables"
    audit_dir = outdir / "audit"
    figures_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)

    df = read_table(input_path)
    df = df.copy()

    # Ensure numeric covariates are treated as numeric even if stored as Decimal objects.
    _coerce_decimals_to_float(df, list(config.covariates))

    # Active comparator filter
    df = df[df[config.treatment_col].isin([config.treated_label, config.control_label])].reset_index(drop=True)
    if df.empty:
        raise ValueError("No rows after filtering to active comparator arms.")

    t = encode_treatment(
        df,
        treatment_col=config.treatment_col,
        encoding=TreatmentEncoding(treated_label=config.treated_label, control_label=config.control_label),
    )
    df["treatment_treated"] = t

    covariates_raw = list(config.covariates)
    missing_cov = [c for c in covariates_raw if c not in df.columns]
    covariates = [c for c in covariates_raw if c in df.columns]

    # Drop unusable covariates (all missing or constant).
    dropped_cov: list[dict[str, str]] = []
    usable: list[str] = []
    for c in covariates:
        s = df[c]
        if pd.api.types.is_numeric_dtype(s):
            x = pd.to_numeric(s, errors="coerce")
            if not x.notna().any():
                dropped_cov.append({"name": c, "reason": "all_missing"})
                continue
            if x.dropna().nunique() <= 1:
                dropped_cov.append({"name": c, "reason": "no_variance"})
                continue
        else:
            x = s.astype("string")
            if not x.notna().any():
                dropped_cov.append({"name": c, "reason": "all_missing"})
                continue
            if x.dropna().nunique() <= 1:
                dropped_cov.append({"name": c, "reason": "no_variance"})
                continue
        usable.append(c)
    covariates = usable
    if not covariates:
        raise ValueError("No usable covariates remain after filtering.")

    categorical, continuous = split_covariates(df, covariates)

    ps, w, _model = fit_propensity_score(
        df,
        treatment_indicator=t,
        covariates=covariates,
        categorical=categorical,
        continuous=continuous,
        config=config.ps,
    )
    df["ps"] = ps
    df["iptw"] = w

    # Save analysis table used
    write_table(df, tables_dir / "analysis_table_used.parquet")

    # Balance
    features = one_hot_balance_frame(df, categorical=categorical, continuous=continuous)
    bal = balance_table(features, treatment_indicator=t, weights=df["iptw"])
    write_table(bal, tables_dir / "balance_smd.csv")
    balance_threshold = 0.1
    max_abs_smd_weighted = (
        float(bal["smd_weighted"].abs().max()) if bal["smd_weighted"].notna().any() else None
    )
    balance_summary: dict[str, Any] = {
        "threshold_max_abs_smd": balance_threshold,
        "max_abs_smd_weighted": max_abs_smd_weighted,
        "pass": (max_abs_smd_weighted is not None and max_abs_smd_weighted < balance_threshold),
    }

    love_plot(bal, outpath=figures_dir / "love_plot.png", threshold=balance_threshold)

    # Diagnostics plots
    plot_weight_hist(weights=df["iptw"].to_numpy(dtype=float), group=t.to_numpy(dtype=int), outpath=figures_dir / "weights_hist.png")
    plot_hist_overlap(
        x=df["ps"].to_numpy(dtype=float),
        group=t.to_numpy(dtype=int),
        weights=None,
        outpath=figures_dir / "ps_overlap_unweighted.png",
        title="Propensity score overlap (unweighted)",
        xlabel="Propensity score",
    )
    plot_hist_overlap(
        x=df["ps"].to_numpy(dtype=float),
        group=t.to_numpy(dtype=int),
        weights=df["iptw"].to_numpy(dtype=float),
        outpath=figures_dir / "ps_overlap_weighted.png",
        title="Propensity score overlap (IPTW-weighted)",
        xlabel="Propensity score",
    )

    # Table 1
    t1 = make_table1(
        df,
        covariates=covariates,
        treatment_indicator_col="treatment_treated",
        weight_col="iptw",
    )
    write_table(t1, tables_dir / "table1.csv")

    # Outcomes
    effect_rows: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for outcome in config.outcomes:
        if outcome.event_col not in df.columns:
            msg = f"missing_event_col:{outcome.event_col}"
            if outcome.required:
                raise ValueError(f"Required outcome {outcome.name} missing event column {outcome.event_col}.")
            skipped.append({"outcome": outcome.name, "reason": msg})
            continue

        event = pd.to_numeric(df[outcome.event_col], errors="coerce")
        if not event.notna().any():
            msg = f"all_missing_event_col:{outcome.event_col}"
            if outcome.required:
                raise ValueError(f"Required outcome {outcome.name} has all-missing event column {outcome.event_col}.")
            skipped.append({"outcome": outcome.name, "reason": msg})
            continue
        df[outcome.event_col] = event.fillna(0).astype(int)

        has_time = False
        time_col = outcome.time_col
        if time_col and time_col in df.columns:
            time_vals = pd.to_numeric(df[time_col], errors="coerce")
            if time_vals.notna().any():
                df[time_col] = time_vals
                has_time = True

        row: dict[str, Any] = {
            "outcome": outcome.name,
            "outcome_label": outcome.label,
            "event_col": outcome.event_col,
            "time_col": time_col if has_time else None,
            "horizon_days": float(outcome.horizon_days),
        }

        if has_time and time_col:
            cox = fit_weighted_cox(
                df,
                duration_col=time_col,
                event_col=outcome.event_col,
                weight_col="iptw",
                treatment_indicator_col="treatment_treated",
            )
            row.update(
                {
                    "effect_type": "hr",
                    "ratio": cox.hr,
                    "ratio_lo": cox.hr_ci95[0],
                    "ratio_hi": cox.hr_ci95[1],
                }
            )

            risk_t = weighted_km_risk_at(
                df[df["treatment_treated"] == 1],
                duration_col=time_col,
                event_col=outcome.event_col,
                weight_col="iptw",
                horizon_days=outcome.horizon_days,
            )
            risk_c = weighted_km_risk_at(
                df[df["treatment_treated"] == 0],
                duration_col=time_col,
                event_col=outcome.event_col,
                weight_col="iptw",
                horizon_days=outcome.horizon_days,
            )
            row.update(
                {
                    "risk_treated": float(risk_t),
                    "risk_control": float(risk_c),
                    "rd": float(risk_t - risk_c),
                    "rr_at_horizon": float(risk_t / risk_c) if risk_c > 0 else float("inf"),
                    "note": "Absolute-risk CI not computed in this scaffold; add bootstrap if needed.",
                }
            )

            plot_km_curves(
                df=df,
                duration_col=time_col,
                event_col=outcome.event_col,
                weight_col="iptw",
                group_col="treatment_treated",
                outpath=figures_dir / f"km_{outcome.name}.png",
                horizon_days=outcome.horizon_days,
                title=f"Weighted Kaplan–Meier: {outcome.label}",
            )
        else:
            risks = weighted_binary_risks(
                df,
                treatment_indicator_col="treatment_treated",
                event_col=outcome.event_col,
                weight_col="iptw",
            )
            row.update(
                {
                    "effect_type": "rr",
                    "ratio": float(risks.rr),
                    "ratio_lo": float(risks.rr_ci95[0]) if risks.rr_ci95 else None,
                    "ratio_hi": float(risks.rr_ci95[1]) if risks.rr_ci95 else None,
                    "risk_treated": float(risks.risk_treated),
                    "risk_control": float(risks.risk_control),
                    "rd": float(risks.rd),
                    "rd_lo": float(risks.rd_ci95[0]) if risks.rd_ci95 else None,
                    "rd_hi": float(risks.rd_ci95[1]) if risks.rd_ci95 else None,
                }
            )

        effect_rows.append(row)

    effects = pd.DataFrame(effect_rows)
    write_table(effects, tables_dir / "effect_estimates.csv")

    if not effects.empty:
        plot_ratio_forest(effects, outpath=figures_dir / "forest_ratio.png")

    def _code_manifest() -> list[dict[str, Any]]:
        if repo_root is None:
            return []
        root = Path(repo_root)
        include_dirs = ["configs", "protocol", "scripts", "sql", "src"]
        records: list[dict[str, Any]] = []
        for d in include_dirs:
            base = root / d
            if not base.exists():
                continue
            for p in sorted(base.rglob("*")):
                if p.is_file():
                    records.append(record_file(p).__dict__)
        return records

    # Audit manifest
    repo_root = Path(repo_root) if repo_root is not None else outdir.parent
    audit: dict[str, Any] = {
        "run_started_utc": utc_now_iso(),
        "input": record_file(input_path).__dict__,
        "code_manifest": _code_manifest(),
        "config": {
            "treatment_col": config.treatment_col,
            "treated_label": config.treated_label,
            "control_label": config.control_label,
            "covariates_requested": covariates_raw,
            "covariates_missing_in_input": missing_cov,
            "covariates_dropped": dropped_cov,
            "covariates_used": covariates,
            "ps": {
                "spline_continuous": config.ps.spline_continuous,
                "spline_n_knots": config.ps.spline_n_knots,
                "spline_degree": config.ps.spline_degree,
                "weight_truncation": list(config.ps.weight_truncation),
            },
            "outcomes": [o.__dict__ for o in config.outcomes],
        },
        "weights": _weight_summary(df, weight_col="iptw", treat_col="treatment_treated"),
        "balance": balance_summary,
        "skipped_outcomes": skipped,
        "environment": collect_environment(),
        "git": safe_git_info(repo_root),
    }

    # Output file manifest (hashes)
    output_files: list[dict[str, Any]] = []
    for p in sorted(outdir.rglob("*")):
        if p.is_file():
            output_files.append(record_file(p).__dict__)
    audit["outputs"] = output_files
    audit["run_finished_utc"] = utc_now_iso()

    write_json(audit, audit_dir / "run_audit.json")
    return audit
