#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd

from dlfx.effects import weighted_competing_risk_cif_rr_at
from dlfx.io import read_table, write_table
from dlfx.meta import combine_effect_tables


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the registered sensitivity suite (scripted batch).")
    p.add_argument(
        "--mimic-zip",
        default=str(ROOT / "data" / "raw" / "physionet" / "mimiciv-3.1.zip"),
        help="Path to mimiciv-3.1.zip.",
    )
    p.add_argument(
        "--eicu-zip",
        default=str(ROOT / "data" / "raw" / "physionet" / "eicu-crd-2.0.zip"),
        help="Path to eicu-crd-2.0.zip.",
    )
    p.add_argument(
        "--mimic-cache",
        default=str(ROOT / "data" / "raw" / "cache" / "mimic-iv-3.1"),
        help="Cache dir for MIMIC members (gitignored).",
    )
    p.add_argument(
        "--eicu-cache",
        default=str(ROOT / "data" / "raw" / "cache" / "eicu-2.0"),
        help="Cache dir for eICU members (gitignored).",
    )
    p.add_argument(
        "--out-root",
        default=str(ROOT / "output" / "multicohort_run" / "sensitivity"),
        help="Root output directory for per-sensitivity runs.",
    )
    p.add_argument(
        "--summary-out",
        default=str(ROOT / "output" / "multicohort_run" / "combined" / "sensitivity_summary.tsv"),
        help="Combined sensitivity summary TSV anchor (for storyboard).",
    )
    p.add_argument(
        "--config",
        default=str(ROOT / "configs" / "study_default.yaml"),
        help="Study YAML config (default: configs/study_default.yaml).",
    )
    p.add_argument(
        "--sensitivities",
        default="S1_LANDMARK_12H,S2_LANDMARK_6H,S3_ALT_EXPOSURE_SOURCE,S4_EXCLUDE_EARLY_BLEED,S5_NEG_CONTROL_WINDOW,S6_COMPETING_RISK_DEATH,G1_SUBGROUP_SUP_DRIVER,G2_SUBGROUP_LIVER_DX,G3_SUBGROUP_ANTITHROMBOTIC",
        help="Comma-separated sensitivity IDs to run.",
    )
    p.add_argument("--threads", type=int, default=4, help="DuckDB threads for extraction.")
    p.add_argument("--skip-validate", action="store_true", help="Skip analysis-table validation during extraction.")
    return p.parse_args()


def _count_parquet(path: Path) -> int:
    import duckdb  # type: ignore

    con = duckdb.connect(database=":memory:")
    lit = "'" + str(path).replace("'", "''") + "'"
    return int(con.execute(f"select count(*) from read_parquet({lit});").fetchone()[0])


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def _sql_lit(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _extract_tables(
    *,
    landmark_hours: int,
    mimic_zip: Path,
    mimic_cache: Path,
    eicu_zip: Path,
    eicu_cache: Path,
    outdir: Path,
    threads: int,
    skip_validate: bool,
) -> tuple[Path, Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    inputs_dir = outdir / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    mimic_out = inputs_dir / "mimic.parquet"
    eicu_out = inputs_dir / "eicu.parquet"

    mimic_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "extract_mimic_duckdb.py"),
        "--zip",
        str(mimic_zip),
        "--cache",
        str(mimic_cache),
        "--out",
        str(mimic_out),
        "--threads",
        str(int(threads)),
        "--landmark-hours",
        str(int(landmark_hours)),
        "--report",
        str(outdir / "extract_report_mimic.json"),
    ]
    eicu_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "extract_eicu_duckdb.py"),
        "--zip",
        str(eicu_zip),
        "--cache",
        str(eicu_cache),
        "--out",
        str(eicu_out),
        "--threads",
        str(int(threads)),
        "--landmark-hours",
        str(int(landmark_hours)),
        "--report",
        str(outdir / "extract_report_eicu.json"),
    ]
    if skip_validate:
        mimic_cmd.append("--no-validate")
        eicu_cmd.append("--no-validate")

    _run(mimic_cmd)
    _run(eicu_cmd)
    return mimic_out, eicu_out


def _exclude_early_prbc_mimic(*, cohort_parquet: Path, cache_dir: Path, out_parquet: Path, landmark_hours: int) -> Path:
    import duckdb  # type: ignore

    icustays = cache_dir / "icu" / "icustays.csv.gz"
    inputevents = cache_dir / "icu" / "inputevents.csv.gz"
    if not icustays.exists() or not inputevents.exists():
        raise SystemExit(f"Missing MIMIC cache files needed for S4: {icustays}, {inputevents}")

    con = duckdb.connect(database=":memory:")
    con.execute(f"create view cohort as select * from read_parquet({_sql_lit(cohort_parquet)});")
    con.execute(
        f"""
        create view icustays as
        select stay_id::bigint as stay_id, intime::timestamp as icu_intime
        from read_csv_auto({_sql_lit(icustays)}, header=true, strict_mode=false, null_padding=true);
        """
    )
    con.execute(
        f"""
        create view prbc as
        select
          try_cast(stay_id as bigint) as stay_id,
          try_cast(starttime as timestamp) as starttime,
          try_cast(itemid as integer) as itemid
        from read_csv_auto({_sql_lit(inputevents)}, header=true, strict_mode=false, null_padding=true)
        where try_cast(itemid as integer) in (220996, 225168, 226368, 227070);
        """
    )
    # Exclude stays with PRBC transfusion evidence during baseline window [icu_intime, index_time).
    con.execute(
        f"""
        create table cohort_filtered as
        with bad as (
          select distinct c.stay_id
          from cohort c
          join icustays s on s.stay_id = c.stay_id
          join prbc p on p.stay_id = c.stay_id
          where p.starttime >= s.icu_intime
            and p.starttime < c.index_time
        )
        select * from cohort
        where stay_id not in (select stay_id from bad);
        """
    )
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"copy cohort_filtered to {_sql_lit(out_parquet)} (format parquet);")
    return out_parquet


def _exclude_early_prbc_eicu(*, cohort_parquet: Path, cache_dir: Path, out_parquet: Path, landmark_hours: int) -> Path:
    import duckdb  # type: ignore

    medication = cache_dir / "medication.csv.gz"
    if not medication.exists():
        raise SystemExit(f"Missing eICU cache files needed for S4: {medication}")

    lm_minutes = int(landmark_hours * 60)
    con = duckdb.connect(database=":memory:")
    con.execute(f"create view cohort as select * from read_parquet({_sql_lit(cohort_parquet)});")
    con.execute(
        f"""
        create view medication as
        select
          patientunitstayid::bigint as stay_id,
          drugstartoffset::integer as drugstartoffset,
          drugname::varchar as drugname
        from read_csv_auto({_sql_lit(medication)}, header=true, strict_mode=false, null_padding=true);
        """
    )
    # Proxy: transfusion-related medication strings during baseline window [0, landmark).
    con.execute(
        f"""
        create table cohort_filtered as
        with bad as (
          select distinct c.stay_id
          from cohort c
          join medication m on m.stay_id = c.stay_id
          where m.drugstartoffset >= 0
            and m.drugstartoffset < {lm_minutes}
            and (
              m.drugname ilike '%packed%red%' or
              m.drugname ilike '%prbc%' or
              m.drugname ilike '%blood%' or
              m.drugname ilike '%rbc%'
            )
        )
        select * from cohort
        where stay_id not in (select stay_id from bad);
        """
    )
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"copy cohort_filtered to {_sql_lit(out_parquet)} (format parquet);")
    return out_parquet


def _alt_exposure_strict_start_mimic(*, cohort_parquet: Path, cache_dir: Path, out_parquet: Path, landmark_hours: int) -> Path:
    import duckdb  # type: ignore

    prescriptions = cache_dir / "hosp" / "prescriptions.csv.gz"
    if not prescriptions.exists():
        raise SystemExit(f"Missing MIMIC cache files needed for S3: {prescriptions}")

    con = duckdb.connect(database=":memory:")
    con.execute(f"create view cohort as select * from read_parquet({_sql_lit(cohort_parquet)});")
    con.execute(
        f"""
        create view prescriptions as
        select
          hadm_id::bigint as hadm_id,
          starttime::timestamp as starttime,
          stoptime::timestamp as stoptime,
          drug::varchar as drug
        from read_csv_auto({_sql_lit(prescriptions)}, header=true, strict_mode=false, null_padding=true);
        """
    )

    # Alternative exposure definition:
    # Require the medication start time to be within [icu_intime, icu_intime + landmark_hours).
    con.execute(
        f"""
        create table cohort_reexposed as
        with rx as (
          select
            c.stay_id,
            max(case
              when pr.drug ilike '%omeprazole%' or pr.drug ilike '%pantoprazole%' or pr.drug ilike '%esomeprazole%'
                or pr.drug ilike '%lansoprazole%' or pr.drug ilike '%rabeprazole%'
              then 1 else 0 end) as ppi_any,
            max(case
              when pr.drug ilike '%famotidine%' or pr.drug ilike '%ranitidine%'
                or pr.drug ilike '%cimetidine%' or pr.drug ilike '%nizatidine%'
              then 1 else 0 end) as h2ra_any
          from cohort c
          left join prescriptions pr
            on pr.hadm_id = c.hadm_id
           and pr.starttime >= c.icu_intime
           and pr.starttime < (c.icu_intime + interval '{int(landmark_hours)} hour')
          group by c.stay_id
        )
        select
          c.* exclude (ppi_any_24h, h2ra_any_24h, dual_ppi_h2ra_24h, treatment),
          coalesce(rx.ppi_any, 0) as ppi_any_24h,
          coalesce(rx.h2ra_any, 0) as h2ra_any_24h,
          case when coalesce(rx.ppi_any,0)=1 and coalesce(rx.h2ra_any,0)=1 then 1 else 0 end as dual_ppi_h2ra_24h,
          case
            when coalesce(rx.ppi_any,0)=1 and coalesce(rx.h2ra_any,0)=0 then 'ppi'
            when coalesce(rx.ppi_any,0)=0 and coalesce(rx.h2ra_any,0)=1 then 'h2ra'
            else null
          end as treatment
        from cohort c
        left join rx on rx.stay_id = c.stay_id;
        """
    )

    # Apply initiator/dual/neither exclusions consistent with mainline.
    con.execute(
        """
        create table cohort_filtered as
        select *
        from cohort_reexposed
        where dual_ppi_h2ra_24h = 0
          and treatment in ('ppi','h2ra');
        """
    )

    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"copy cohort_filtered to {_sql_lit(out_parquet)} (format parquet);")
    return out_parquet


def _tidy_combined(
    combined_csv: Path,
    *,
    sensitivity_id: str,
    landmark_hours: int,
    n_mimic: int,
    n_eicu: int,
    subgroup_name: str | None = None,
    subgroup_level: str | None = None,
) -> "list[dict]":
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
                    "landmark_hours": landmark_hours,
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
                    "landmark_hours": landmark_hours,
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


def _filter_parquet(*, input_parquet: Path, out_parquet: Path, where_sql: str) -> Path:
    import duckdb  # type: ignore

    con = duckdb.connect(database=":memory:")
    con.execute(f"create view t as select * from read_parquet({_sql_lit(input_parquet)});")
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"copy (select * from t where {where_sql}) to {_sql_lit(out_parquet)} (format parquet);")
    return out_parquet


def _truncate_time_window(
    *,
    input_parquet: Path,
    out_parquet: Path,
    time_col: str,
    event_col: str,
    horizon_days: float,
) -> Path:
    """
    Apply an administrative censoring window for a time-to-event outcome:
      - time := min(time, horizon)
      - event := 1 only if original event==1 and original time <= horizon

    Used for negative-control early windows.
    """
    import duckdb  # type: ignore

    hz = float(horizon_days)
    con = duckdb.connect(database=":memory:")
    con.execute(f"create view t as select * from read_parquet({_sql_lit(input_parquet)});")
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        copy (
          select
            * exclude ({event_col}, {time_col}),
            case
              when coalesce(try_cast({event_col} as integer), 0) = 1
               and coalesce(try_cast({time_col} as double), 1e9) <= {hz}
              then 1 else 0
            end as {event_col},
            least(coalesce(try_cast({time_col} as double), {hz}), {hz}) as {time_col}
          from t
        ) to {_sql_lit(out_parquet)} (format parquet);
        """
    )
    return out_parquet


def _append_competing_risk_row(*, analysis_parquet: Path, effects_csv: Path, seed: int = 0) -> None:
    cols = [
        "treatment_treated",
        "iptw",
        "cigib_strict_event",
        "cigib_strict_time_days",
        "death_event_28d",
        "death_time_days",
    ]
    df = read_table(analysis_parquet, columns=cols)
    est = weighted_competing_risk_cif_rr_at(
        df,
        treatment_indicator_col="treatment_treated",
        interest_event_col="cigib_strict_event",
        interest_time_col="cigib_strict_time_days",
        competing_event_col="death_event_28d",
        competing_time_col="death_time_days",
        weight_col="iptw",
        horizon_days=14.0,
        n_bootstrap=200,
        seed=int(seed),
    )

    eff = pd.read_csv(effects_csv)
    row = {
        "outcome": "cigib_strict_competing_risk_death",
        "outcome_label": "Strict CIGIB (competing risk: death)",
        "event_col": "cigib_strict_event",
        "time_col": "cigib_strict_time_days",
        "horizon_days": 14.0,
        "effect_type": "cif_rr",
        "ratio": float(est.rr),
        "ratio_lo": float(est.rr_ci95[0]) if est.rr_ci95 else None,
        "ratio_hi": float(est.rr_ci95[1]) if est.rr_ci95 else None,
        "risk_treated": float(est.risk_treated),
        "risk_control": float(est.risk_control),
        "rd": float(est.rd),
        "rr_at_horizon": float(est.rr),
        "note": "Weighted Aalen–Johansen CIF at 14d with death as competing event; stratified bootstrap (n=200) for CI.",
    }
    for c in eff.columns:
        row.setdefault(c, None)
    eff2 = pd.concat([eff, pd.DataFrame([row])[eff.columns]], ignore_index=True)
    eff2.to_csv(effects_csv, index=False)


def _rebuild_combined_effects(*, outdir: Path, label_a: str = "mimic", label_b: str = "eicu") -> None:
    eff_a = pd.read_csv(outdir / label_a / "tables" / "effect_estimates.csv")
    eff_b = pd.read_csv(outdir / label_b / "tables" / "effect_estimates.csv")
    combined = combine_effect_tables(eff_a, eff_b, label_a=label_a, label_b=label_b)
    write_table(combined, outdir / "combined" / "effect_estimates_combined.csv")


def main() -> None:
    args = parse_args()
    mimic_zip = Path(args.mimic_zip)
    eicu_zip = Path(args.eicu_zip)
    mimic_cache = Path(args.mimic_cache)
    eicu_cache = Path(args.eicu_cache)
    out_root = Path(args.out_root)
    summary_out = Path(args.summary_out)

    suite = [s.strip() for s in str(args.sensitivities).split(",") if s.strip()]
    sensitivity_defs: dict[str, dict] = {
        "S1_LANDMARK_12H": {"landmark_hours": 12, "config": str(args.config)},
        "S2_LANDMARK_6H": {"landmark_hours": 6, "config": str(args.config)},
        # Alternative exposure definition: require med start within baseline window (MIMIC prescriptions).
        "S3_ALT_EXPOSURE_SOURCE": {"landmark_hours": 24, "config": str(args.config), "filter": "mimic_strict_start"},
        # Negative-control: keep landmark at 24h but shorten horizon to 1 day for strict CIGIB.
        "S5_NEG_CONTROL_WINDOW": {
            "landmark_hours": 24,
            "config": str(ROOT / "configs" / "study_negative_control_1d.yaml"),
            "postprocess": "truncate_cigib_1d",
        },
        # Exclude early bleeding evidence proxy: PRBC evidence during baseline window.
        "S4_EXCLUDE_EARLY_BLEED": {"landmark_hours": 24, "config": str(args.config), "filter": "exclude_early_prbc"},
        # Competing-risk sensitivity (death as competing event): add AJ CIF row and rebuild combined table.
        "S6_COMPETING_RISK_DEATH": {"landmark_hours": 24, "config": str(args.config), "postprocess": "competing_risk"},
        # Subgroups (Protocol 8)
        "G1_SUBGROUP_SUP_DRIVER": {
            "landmark_hours": 24,
            "config": str(args.config),
            "subgroup": {
                "name": "sup_indication_mv_24h",
                "levels": [
                    ("mv_1", "sup_indication_mv_24h = 1"),
                    ("mv_0", "sup_indication_mv_24h = 0"),
                ],
            },
        },
        "G2_SUBGROUP_LIVER_DX": {
            "landmark_hours": 24,
            "config": str(args.config),
            "subgroup": {
                "name": "liver_disease",
                "levels": [
                    ("liver_1", "liver_disease = 1"),
                    ("liver_0", "liver_disease = 0"),
                ],
            },
        },
        "G3_SUBGROUP_ANTITHROMBOTIC": {
            "landmark_hours": 24,
            "config": str(args.config),
            "subgroup": {
                "name": "antithrombotic_any_24h",
                "levels": [
                    ("antith_1", "antithrombotic_any_24h = 1"),
                    ("antith_0", "antithrombotic_any_24h = 0"),
                ],
            },
        },
    }

    unknown = [s for s in suite if s not in sensitivity_defs]
    if unknown:
        raise SystemExit(f"Unknown sensitivity IDs (supported: {sorted(sensitivity_defs)}): {unknown}")

    all_rows: list[dict] = []

    for sid in suite:
        spec = sensitivity_defs[sid]
        lh = int(spec["landmark_hours"])
        cfg_path = str(spec["config"])
        outdir = out_root / sid
        mimic_out, eicu_out = _extract_tables(
            landmark_hours=lh,
            mimic_zip=mimic_zip,
            mimic_cache=mimic_cache,
            eicu_zip=eicu_zip,
            eicu_cache=eicu_cache,
            outdir=outdir,
            threads=int(args.threads),
            skip_validate=bool(args.skip_validate),
        )

        filt = spec.get("filter")
        if filt == "exclude_early_prbc":
            mimic_out = _exclude_early_prbc_mimic(
                cohort_parquet=mimic_out,
                cache_dir=mimic_cache,
                out_parquet=outdir / "inputs" / "mimic_filtered.parquet",
                landmark_hours=lh,
            )
            eicu_out = _exclude_early_prbc_eicu(
                cohort_parquet=eicu_out,
                cache_dir=eicu_cache,
                out_parquet=outdir / "inputs" / "eicu_filtered.parquet",
                landmark_hours=lh,
            )
        elif filt == "mimic_strict_start":
            mimic_out = _alt_exposure_strict_start_mimic(
                cohort_parquet=mimic_out,
                cache_dir=mimic_cache,
                out_parquet=outdir / "inputs" / "mimic_filtered.parquet",
                landmark_hours=lh,
            )

        subgroup = spec.get("subgroup")
        if subgroup:
            (outdir / "audit").mkdir(parents=True, exist_ok=True)
            sub_name = str(subgroup["name"])
            stacked_combined = []
            for level_id, where_sql in subgroup["levels"]:
                level_id = str(level_id)
                subrun = outdir / "levels" / level_id
                m_f = _filter_parquet(
                    input_parquet=mimic_out,
                    out_parquet=outdir / "inputs" / f"mimic_{level_id}.parquet",
                    where_sql=str(where_sql),
                )
                e_f = _filter_parquet(
                    input_parquet=eicu_out,
                    out_parquet=outdir / "inputs" / f"eicu_{level_id}.parquet",
                    where_sql=str(where_sql),
                )

                _run(
                    [
                        sys.executable,
                        str(ROOT / "scripts" / "run_multicohort.py"),
                        "--primary",
                        str(m_f),
                        "--external",
                        str(e_f),
                        "--outdir",
                        str(subrun),
                        "--config",
                        cfg_path,
                        "--label-primary",
                        "mimic",
                        "--label-external",
                        "eicu",
                    ]
                )

                n_m = _count_parquet(subrun / "mimic" / "tables" / "analysis_table_used.parquet")
                n_e = _count_parquet(subrun / "eicu" / "tables" / "analysis_table_used.parquet")
                combined_csv = subrun / "combined" / "effect_estimates_combined.csv"
                combined_df = pd.read_csv(combined_csv)
                combined_df["outcome"] = combined_df["outcome"].astype(str) + f"|{sub_name}={level_id}"
                combined_df["outcome_label"] = combined_df["outcome_label"].astype(str) + f" ({sub_name}={level_id})"
                stacked_combined.append(combined_df)

                rows = _tidy_combined(
                    combined_csv,
                    sensitivity_id=sid,
                    landmark_hours=lh,
                    n_mimic=n_m,
                    n_eicu=n_e,
                    subgroup_name=sub_name,
                    subgroup_level=level_id,
                )
                all_rows.extend(rows)

            if stacked_combined:
                (outdir / "combined").mkdir(parents=True, exist_ok=True)
                pd.concat(stacked_combined, ignore_index=True).to_csv(outdir / "combined" / "effect_estimates_combined.csv", index=False)

            (outdir / "audit" / "sensitivity_audit.json").write_text(
                json.dumps(
                    {
                        "sensitivity_id": sid,
                        "landmark_hours": lh,
                        "config": cfg_path,
                        "filter": filt,
                        "subgroup": subgroup,
                        "inputs": {"mimic": str(mimic_out), "eicu": str(eicu_out)},
                        "outputs": {"dir": str(outdir), "combined_effects": str(outdir / "combined" / "effect_estimates_combined.csv")},
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            continue

        _run(
            [
                sys.executable,
                str(ROOT / "scripts" / "run_multicohort.py"),
                "--primary",
                str(mimic_out),
                "--external",
                str(eicu_out),
                "--outdir",
                str(outdir),
                "--config",
                cfg_path,
                "--label-primary",
                "mimic",
                "--label-external",
                "eicu",
            ]
        )

        post = spec.get("postprocess")
        if post == "truncate_cigib_1d":
            mimic_out = _truncate_time_window(
                input_parquet=mimic_out,
                out_parquet=outdir / "inputs" / "mimic_trunc_1d.parquet",
                time_col="cigib_strict_time_days",
                event_col="cigib_strict_event",
                horizon_days=1.0,
            )
            eicu_out = _truncate_time_window(
                input_parquet=eicu_out,
                out_parquet=outdir / "inputs" / "eicu_trunc_1d.parquet",
                time_col="cigib_strict_time_days",
                event_col="cigib_strict_event",
                horizon_days=1.0,
            )

            _run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "run_multicohort.py"),
                    "--primary",
                    str(mimic_out),
                    "--external",
                    str(eicu_out),
                    "--outdir",
                    str(outdir),
                    "--config",
                    cfg_path,
                    "--label-primary",
                    "mimic",
                    "--label-external",
                    "eicu",
                ]
            )

        if post == "competing_risk":
            _append_competing_risk_row(
                analysis_parquet=outdir / "mimic" / "tables" / "analysis_table_used.parquet",
                effects_csv=outdir / "mimic" / "tables" / "effect_estimates.csv",
                seed=7,
            )
            _append_competing_risk_row(
                analysis_parquet=outdir / "eicu" / "tables" / "analysis_table_used.parquet",
                effects_csv=outdir / "eicu" / "tables" / "effect_estimates.csv",
                seed=13,
            )
            _rebuild_combined_effects(outdir=outdir, label_a="mimic", label_b="eicu")

        # Per-sensitivity audit (what changed).
        audit = {
            "sensitivity_id": sid,
            "landmark_hours": lh,
            "config": cfg_path,
            "filter": filt,
            "postprocess": post,
            "inputs": {"mimic": str(mimic_out), "eicu": str(eicu_out)},
            "outputs": {"dir": str(outdir), "combined_effects": str(outdir / "combined" / "effect_estimates_combined.csv")},
        }
        (outdir / "audit").mkdir(parents=True, exist_ok=True)
        (outdir / "audit" / "sensitivity_audit.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")

        n_m = _count_parquet(outdir / "mimic" / "tables" / "analysis_table_used.parquet")
        n_e = _count_parquet(outdir / "eicu" / "tables" / "analysis_table_used.parquet")
        rows = _tidy_combined(
            outdir / "combined" / "effect_estimates_combined.csv",
            sensitivity_id=sid,
            landmark_hours=lh,
            n_mimic=n_m,
            n_eicu=n_e,
        )
        all_rows.extend(rows)

    summary_out.parent.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(all_rows)
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
    ].sort_values(["sensitivity_id", "cohort", "outcome"])

    # If a full-suite summary already exists, merge-update only the sensitivities we just ran.
    # This prevents accidental loss of previously computed rows when running a subset.
    if summary_out.exists():
        try:
            prev = pd.read_csv(summary_out, sep="\t")
            if "sensitivity_id" in prev.columns:
                prev = prev[~prev["sensitivity_id"].isin(suite)].copy()
                out_df = pd.concat([prev, out_df], ignore_index=True)
        except Exception:
            # Fall back to overwriting if the previous file is unreadable.
            pass

    out_df = out_df.sort_values(
        ["sensitivity_id", "subgroup_name", "subgroup_level", "cohort", "outcome"],
        na_position="last",
    )
    out_df.to_csv(summary_out, sep="\t", index=False)
    print(f"Wrote sensitivity summary: {summary_out}")


if __name__ == "__main__":
    main()
