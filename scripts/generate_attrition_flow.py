#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate cohort flow / attrition tables (MIMIC + eICU).")
    p.add_argument("--landmark-hours", type=int, default=24, help="Landmark in hours (default: 24).")

    p.add_argument(
        "--mimic-cache",
        default=str(ROOT / "data" / "raw" / "cache" / "mimic-iv-3.1"),
        help="Cache dir holding extracted MIMIC csv.gz members (gitignored).",
    )
    p.add_argument(
        "--eicu-cache",
        default=str(ROOT / "data" / "raw" / "cache" / "eicu-2.0"),
        help="Cache dir holding extracted eICU csv.gz members (gitignored).",
    )

    p.add_argument(
        "--mimic-outdir",
        default=str(ROOT / "output" / "mimic_run"),
        help="Per-cohort output dir where tables/attrition_flow.tsv will be written.",
    )
    p.add_argument(
        "--eicu-outdir",
        default=str(ROOT / "output" / "eicu_run"),
        help="Per-cohort output dir where tables/attrition_flow.tsv will be written.",
    )
    p.add_argument(
        "--multicohort-outdir",
        default=str(ROOT / "output" / "multicohort_run"),
        help="Multicohort output dir; combined/attrition_flow.tsv will be written.",
    )

    p.add_argument(
        "--check-mimic-analysis-table",
        default=str(ROOT / "output" / "multicohort_run" / "mimic" / "tables" / "analysis_table_used.parquet"),
        help="If this Parquet exists, final flow n must equal its row count.",
    )
    p.add_argument(
        "--check-eicu-analysis-table",
        default=str(ROOT / "output" / "multicohort_run" / "eicu" / "tables" / "analysis_table_used.parquet"),
        help="If this Parquet exists, final flow n must equal its row count.",
    )
    return p.parse_args()


def _sql_string_literal(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def _count_parquet(path: Path) -> int:
    import duckdb  # type: ignore

    con = duckdb.connect(database=":memory:")
    lit = _sql_string_literal(path)
    return int(con.execute(f"select count(*) from read_parquet({lit});").fetchone()[0])


def compute_mimic_flow(cache_dir: Path, landmark_hours: int) -> list[dict]:
    import duckdb  # type: ignore

    icu_dir = cache_dir / "icu"
    hosp_dir = cache_dir / "hosp"

    icustays = icu_dir / "icustays.csv.gz"
    chartevents = icu_dir / "chartevents.csv.gz"
    admissions = hosp_dir / "admissions.csv.gz"
    patients = hosp_dir / "patients.csv.gz"
    labevents = hosp_dir / "labevents.csv.gz"
    d_labitems = hosp_dir / "d_labitems.csv.gz"
    prescriptions = hosp_dir / "prescriptions.csv.gz"

    missing = [p for p in [icustays, chartevents, admissions, patients, labevents, d_labitems, prescriptions] if not p.exists()]
    if missing:
        raise SystemExit(f"MIMIC cache missing files under {cache_dir}: {missing}")

    con = duckdb.connect(database=":memory:")
    con.execute("create schema if not exists mimiciv_icu;")
    con.execute("create schema if not exists mimiciv_hosp;")

    con.execute(
        f"""
        create view mimiciv_icu.icustays as
        select
          subject_id::bigint as subject_id,
          hadm_id::bigint as hadm_id,
          stay_id::bigint as stay_id,
          intime::timestamp as intime,
          outtime::timestamp as outtime
        from read_csv_auto({_sql_string_literal(icustays)}, header=true, delim=',', strict_mode=false, null_padding=true);
        """
    )
    con.execute(
        f"""
        create view mimiciv_hosp.admissions as
        select
          subject_id::bigint as subject_id,
          hadm_id::bigint as hadm_id,
          admittime::timestamp as admittime,
          deathtime::timestamp as deathtime,
          race::varchar as race
        from read_csv_auto({_sql_string_literal(admissions)}, header=true, delim=',', strict_mode=false, null_padding=true);
        """
    )
    con.execute(
        f"""
        create view mimiciv_hosp.patients as
        select
          subject_id::bigint as subject_id,
          gender::varchar as gender,
          anchor_age::integer as anchor_age,
          anchor_year::integer as anchor_year,
          dod::date as dod
        from read_csv_auto({_sql_string_literal(patients)}, header=true, delim=',', strict_mode=false, null_padding=true);
        """
    )
    con.execute(
        f"""
        create view mimiciv_hosp.d_labitems as
        select
          itemid::integer as itemid,
          label::varchar as label
        from read_csv_auto({_sql_string_literal(d_labitems)}, header=true, delim=',', strict_mode=false, null_padding=true);
        """
    )

    # Resolve lab itemids once (small table) and then read labevents with an itemid filter to reduce scan cost.
    platelet_ids = [int(r[0]) for r in con.execute("select distinct itemid from mimiciv_hosp.d_labitems where label ilike 'Platelet%';").fetchall()]
    inr_ids = [int(r[0]) for r in con.execute("select distinct itemid from mimiciv_hosp.d_labitems where label ilike 'INR%' or label ilike '%INR%';").fetchall()]
    if not platelet_ids or not inr_ids:
        raise SystemExit(f"Failed to resolve required lab itemids from {d_labitems}. Platelet={platelet_ids}, INR={inr_ids}")
    lab_itemids = sorted(set(platelet_ids + inr_ids))
    lab_itemids_sql = ",".join(str(x) for x in lab_itemids)

    con.execute(
        f"""
        create view mimiciv_hosp.labevents as
        select
          try_cast(hadm_id as bigint) as hadm_id,
          try_cast(itemid as integer) as itemid,
          try_cast(charttime as timestamp) as charttime,
          try_cast(valuenum as double) as valuenum
        from read_csv_auto({_sql_string_literal(labevents)}, header=true, delim=',', strict_mode=false, null_padding=true, all_varchar=true)
        where try_cast(itemid as integer) in ({lab_itemids_sql});
        """
    )
    con.execute(
        f"""
        create view mimiciv_hosp.prescriptions as
        select
          hadm_id::bigint as hadm_id,
          starttime::timestamp as starttime,
          stoptime::timestamp as stoptime,
          drug::varchar as drug
        from read_csv_auto({_sql_string_literal(prescriptions)}, header=true, delim=',', strict_mode=false, null_padding=true);
        """
    )
    con.execute(
        f"""
        create view mimiciv_icu.chartevents as
        select stay_id::bigint as stay_id, charttime::timestamp as charttime, itemid::integer as itemid, value::varchar as value
        from read_csv_auto({_sql_string_literal(chartevents)}, header=true, delim=',', strict_mode=false, null_padding=true)
        where itemid in (223848, 223849, 229314) and value is not null;
        """
    )

    con.execute(
        f"""
        create table mimic_flow as
        with
        icu_stays as (
          select
            ie.subject_id,
            ie.hadm_id,
            ie.stay_id,
            ie.intime as icu_intime,
            ie.outtime as icu_outtime,
            row_number() over (partition by ie.subject_id order by ie.intime) as rn_icu
          from mimiciv_icu.icustays ie
        ),
        base as (
          select
            s.subject_id as patient_id,
            s.hadm_id,
            s.stay_id,
            s.icu_intime,
            s.icu_outtime,
            (s.icu_intime + interval '{int(landmark_hours)} hour') as index_time,
            (case when s.rn_icu = 1 then 1 else 0 end) as is_first_icu_stay,
            (case when s.icu_outtime >= (s.icu_intime + interval '{int(landmark_hours)} hour') then 1 else 0 end) as in_icu_at_landmark
          from icu_stays s
          where s.rn_icu = 1
        ),
        demo as (
          select
            b.*,
            p.gender as sex,
            a.race,
            (p.anchor_age + (date_part('year', a.admittime) - p.anchor_year))::double as age_years,
            p.dod as dod_date,
            a.deathtime as hosp_deathtime
          from base b
          join mimiciv_hosp.admissions a on a.hadm_id = b.hadm_id
          join mimiciv_hosp.patients p on p.subject_id = b.patient_id
        ),
        alive as (
          select
            d.*,
            (case when d.age_years >= 18 then 1 else 0 end) as adult,
            (case
              when d.hosp_deathtime is not null and d.hosp_deathtime < d.index_time then 0
              when d.dod_date is not null and d.dod_date < d.index_time::date then 0
              else 1
            end) as alive_at_landmark
          from demo d
        ),
        labs_0_lm as (
          select
            a.stay_id,
            min(case when le.itemid in ({",".join(str(x) for x in platelet_ids)}) then le.valuenum end) as platelet_min,
            max(case when le.itemid in ({",".join(str(x) for x in inr_ids)}) then le.valuenum end) as inr_max
          from alive a
          join mimiciv_hosp.labevents le
            on le.hadm_id = a.hadm_id
           and le.charttime >= a.icu_intime
           and le.charttime < (a.icu_intime + interval '{int(landmark_hours)} hour')
          where le.valuenum is not null
          group by a.stay_id
        ),
        coagulopathy as (
          select
            a.stay_id,
            (case
              when l.platelet_min is not null and l.platelet_min < 50 then 1
              when l.inr_max is not null and l.inr_max > 1.5 then 1
              else 0
            end) as sup_indication_coagulopathy
          from alive a
          left join labs_0_lm l on l.stay_id = a.stay_id
        ),
        mv as (
          select
            a.stay_id,
            (case when count(ce.stay_id) > 0 then 1 else 0 end) as sup_indication_mv
          from alive a
          left join mimiciv_icu.chartevents ce
            on ce.stay_id = a.stay_id
           and ce.charttime >= a.icu_intime
           and ce.charttime < (a.icu_intime + interval '{int(landmark_hours)} hour')
          group by a.stay_id
        ),
        sup as (
          select
            a.*,
            coalesce(mv.sup_indication_mv, 0) as sup_indication_mv,
            co.sup_indication_coagulopathy,
            (case when coalesce(mv.sup_indication_mv,0)=1 or co.sup_indication_coagulopathy=1 then 1 else 0 end) as eligible_sup_high_risk
          from alive a
          left join mv on mv.stay_id = a.stay_id
          left join coagulopathy co on co.stay_id = a.stay_id
        ),
        rx as (
          select
            s.stay_id,
            max(case
              when pr.drug ilike '%omeprazole%' or pr.drug ilike '%pantoprazole%' or pr.drug ilike '%esomeprazole%'
                or pr.drug ilike '%lansoprazole%' or pr.drug ilike '%rabeprazole%'
              then 1 else 0 end) as ppi_any,
            max(case
              when pr.drug ilike '%famotidine%' or pr.drug ilike '%ranitidine%'
                or pr.drug ilike '%cimetidine%' or pr.drug ilike '%nizatidine%'
              then 1 else 0 end) as h2ra_any
          from sup s
          left join mimiciv_hosp.prescriptions pr
            on pr.hadm_id = s.hadm_id
           and pr.starttime < (s.icu_intime + interval '{int(landmark_hours)} hour')
           and (pr.stoptime is null or pr.stoptime >= s.icu_intime)
          group by s.stay_id
        ),
        exposure as (
          select
            s.*,
            coalesce(r.ppi_any, 0) as ppi_any,
            coalesce(r.h2ra_any, 0) as h2ra_any,
            (case when coalesce(r.ppi_any,0)=1 and coalesce(r.h2ra_any,0)=1 then 1 else 0 end) as dual,
            (case
              when coalesce(r.ppi_any,0)=1 and coalesce(r.h2ra_any,0)=0 then 'ppi'
              when coalesce(r.ppi_any,0)=0 and coalesce(r.h2ra_any,0)=1 then 'h2ra'
              else null
            end) as treatment
          from sup s
          left join rx r on r.stay_id = s.stay_id
        )
        select
          count(*) filter (where adult=1 and is_first_icu_stay=1) as n_adult_first_stay,
          count(*) filter (where adult=1 and is_first_icu_stay=1 and in_icu_at_landmark=1 and alive_at_landmark=1) as n_landmark_alive_in_icu,
          count(*) filter (where adult=1 and is_first_icu_stay=1 and in_icu_at_landmark=1 and alive_at_landmark=1 and eligible_sup_high_risk=1) as n_sup_high_risk,
          count(*) filter (where adult=1 and is_first_icu_stay=1 and in_icu_at_landmark=1 and alive_at_landmark=1 and eligible_sup_high_risk=1 and dual=1) as n_excl_dual,
          count(*) filter (where adult=1 and is_first_icu_stay=1 and in_icu_at_landmark=1 and alive_at_landmark=1 and eligible_sup_high_risk=1 and ppi_any=0 and h2ra_any=0) as n_excl_neither,
          count(*) filter (where adult=1 and is_first_icu_stay=1 and in_icu_at_landmark=1 and alive_at_landmark=1 and eligible_sup_high_risk=1 and dual=0 and treatment in ('ppi','h2ra')) as n_final
        from exposure;
        """
    )

    row = con.execute("select * from mimic_flow;").fetchone()
    n_adult_first, n_landmark, n_sup, n_dual, n_neither, n_final = map(int, row)
    return [
        {"dataset": "mimic", "step_id": "S1_ADULT_ICU", "step_label": "Adult (>=18) ICU stays", "n": n_adult_first, "notes": "MIMIC: first ICU stay per patient (one stay per patient)."},
        {"dataset": "mimic", "step_id": "S2_LANDMARK_ALIVE_IN_ICU", "step_label": f"Alive and in ICU at landmark ({landmark_hours}h)", "n": n_landmark, "notes": ""},
        {"dataset": "mimic", "step_id": "S3_SUP_HIGH_RISK", "step_label": f"Meets SUP high-risk indication within 0-{landmark_hours}h", "n": n_sup, "notes": ""},
        {"dataset": "mimic", "step_id": "E1_EXCL_DUAL", "step_label": f"Excluded: dual exposure (PPI+H2RA) within 0-{landmark_hours}h", "n": n_dual, "notes": "Non-cumulative; excluded count among S3."},
        {"dataset": "mimic", "step_id": "E2_EXCL_NEITHER", "step_label": f"Excluded: no exposure (neither PPI nor H2RA) within 0-{landmark_hours}h", "n": n_neither, "notes": "Non-cumulative; excluded count among S3."},
        {"dataset": "mimic", "step_id": "S4_FINAL", "step_label": "Final cohort: initiators (PPI only or H2RA only)", "n": n_final, "notes": ""},
    ]


def compute_eicu_flow(cache_dir: Path, landmark_hours: int) -> list[dict]:
    import duckdb  # type: ignore

    patient = cache_dir / "patient.csv.gz"
    lab = cache_dir / "lab.csv.gz"
    medication = cache_dir / "medication.csv.gz"
    diagnosis = cache_dir / "diagnosis.csv.gz"

    missing = [p for p in [patient, lab, medication, diagnosis] if not p.exists()]
    if missing:
        raise SystemExit(f"eICU cache missing files under {cache_dir}: {missing}")

    lm = int(landmark_hours * 60)

    con = duckdb.connect(database=":memory:")
    con.execute(
        f"""
        create view patient as
        select
          patientunitstayid::bigint as stay_id,
          uniquepid::varchar as patient_id,
          gender::varchar as sex,
          age::varchar as age,
          ethnicity::varchar as race,
          unitdischargeoffset::integer as unitdischargeoffset,
          unitdischargestatus::varchar as unitdischargestatus
        from read_csv_auto({_sql_string_literal(patient)}, header=true, delim=',', strict_mode=false, null_padding=true);
        """
    )
    con.execute(
        f"""
        create view lab as
        select
          patientunitstayid::bigint as stay_id,
          labresultoffset::integer as labresultoffset,
          labname::varchar as labname,
          labresult::varchar as labresult
        from read_csv_auto({_sql_string_literal(lab)}, header=true, delim=',', strict_mode=false, null_padding=true);
        """
    )
    con.execute(
        f"""
        create view medication as
        select
          patientunitstayid::bigint as stay_id,
          drugstartoffset::integer as drugstartoffset,
          drugname::varchar as drugname
        from read_csv_auto({_sql_string_literal(medication)}, header=true, delim=',', strict_mode=false, null_padding=true);
        """
    )
    con.execute(
        f"""
        create view diagnosis as
        select
          patientunitstayid::bigint as stay_id,
          diagnosisoffset::integer as diagnosisoffset,
          diagnosisstring::varchar as diagnosisstring,
          icd9code::varchar as icd9code
        from read_csv_auto({_sql_string_literal(diagnosis)}, header=true, delim=',', strict_mode=false, null_padding=true);
        """
    )

    con.execute(
        f"""
        create table eicu_flow as
        with
        base as (
          select
            p.stay_id,
            p.patient_id,
            case
              when try_cast(p.age as integer) is not null then try_cast(p.age as integer)::double
              when position('>' in p.age) > 0 then 90::double
              else null::double
            end as age_years,
            p.unitdischargeoffset,
            p.unitdischargestatus
          from patient p
          where coalesce(try_cast(p.age as integer), 90) >= 18
        ),
        labs_0_lm as (
          select
            b.stay_id,
            min(case when l.labname ilike '%platelet%' then try_cast(l.labresult as double) end) as platelet_min,
            max(case when l.labname ilike '%inr%' then try_cast(l.labresult as double) end) as inr_max
          from base b
          left join lab l
            on l.stay_id = b.stay_id
           and l.labresultoffset >= 0
           and l.labresultoffset < {lm}
          group by b.stay_id
        ),
        coagulopathy as (
          select
            b.stay_id,
            (case
              when l.platelet_min is not null and l.platelet_min < 50 then 1
              when l.inr_max is not null and l.inr_max > 1.5 then 1
              else 0
            end) as sup_indication_coagulopathy
          from base b
          left join labs_0_lm l on l.stay_id = b.stay_id
        ),
        mv as (
          select
            b.stay_id,
            0 as sup_indication_mv
          from base b
        ),
        sup as (
          select
            b.*,
            mv.sup_indication_mv,
            co.sup_indication_coagulopathy,
            (case when mv.sup_indication_mv=1 or co.sup_indication_coagulopathy=1 then 1 else 0 end) as eligible_sup_high_risk
          from base b
          left join mv on mv.stay_id=b.stay_id
          left join coagulopathy co on co.stay_id=b.stay_id
        ),
        rx as (
          select
            s.stay_id,
            max(case
              when m.drugname ilike '%omeprazole%' or m.drugname ilike '%pantoprazole%' or m.drugname ilike '%esomeprazole%'
                or m.drugname ilike '%lansoprazole%' or m.drugname ilike '%rabeprazole%'
              then 1 else 0 end) as ppi_any,
            max(case
              when m.drugname ilike '%famotidine%' or m.drugname ilike '%ranitidine%'
                or m.drugname ilike '%cimetidine%' or m.drugname ilike '%nizatidine%'
              then 1 else 0 end) as h2ra_any
          from sup s
          left join medication m
            on m.stay_id = s.stay_id
           and m.drugstartoffset >= 0
           and m.drugstartoffset < {lm}
          group by s.stay_id
        ),
        exposure as (
          select
            s.*,
            coalesce(r.ppi_any,0) as ppi_any,
            coalesce(r.h2ra_any,0) as h2ra_any,
            (case when coalesce(r.ppi_any,0)=1 and coalesce(r.h2ra_any,0)=1 then 1 else 0 end) as dual,
            (case
              when coalesce(r.ppi_any,0)=1 and coalesce(r.h2ra_any,0)=0 then 'ppi'
              when coalesce(r.ppi_any,0)=0 and coalesce(r.h2ra_any,0)=1 then 'h2ra'
              else null
            end) as treatment
          from sup s
          left join rx r on r.stay_id = s.stay_id
        )
        select
          count(*) as n_adult,
          count(*) filter (where unitdischargeoffset >= {lm}) as n_landmark_alive_in_icu,
          count(*) filter (where unitdischargeoffset >= {lm} and eligible_sup_high_risk=1) as n_sup_high_risk,
          count(*) filter (where unitdischargeoffset >= {lm} and eligible_sup_high_risk=1 and dual=1) as n_excl_dual,
          count(*) filter (where unitdischargeoffset >= {lm} and eligible_sup_high_risk=1 and ppi_any=0 and h2ra_any=0) as n_excl_neither,
          count(*) filter (where unitdischargeoffset >= {lm} and eligible_sup_high_risk=1 and dual=0 and treatment in ('ppi','h2ra')) as n_final
        from exposure;
        """
    )
    row = con.execute("select * from eicu_flow;").fetchone()
    n_adult, n_landmark, n_sup, n_dual, n_neither, n_final = map(int, row)
    return [
        {"dataset": "eicu", "step_id": "S1_ADULT_ICU", "step_label": "Adult (>=18) ICU stays", "n": n_adult, "notes": "eICU flow is per patientunitstayid; first-stay restriction not applied."},
        {"dataset": "eicu", "step_id": "S2_LANDMARK_ALIVE_IN_ICU", "step_label": f"Alive and in ICU at landmark ({landmark_hours}h)", "n": n_landmark, "notes": ""},
        {"dataset": "eicu", "step_id": "S3_SUP_HIGH_RISK", "step_label": f"Meets SUP high-risk indication within 0-{landmark_hours}h", "n": n_sup, "notes": "MV proxy currently not implemented for eICU; eligibility is coagulopathy-only."},
        {"dataset": "eicu", "step_id": "E1_EXCL_DUAL", "step_label": f"Excluded: dual exposure (PPI+H2RA) within 0-{landmark_hours}h", "n": n_dual, "notes": "Non-cumulative; excluded count among S3."},
        {"dataset": "eicu", "step_id": "E2_EXCL_NEITHER", "step_label": f"Excluded: no exposure (neither PPI nor H2RA) within 0-{landmark_hours}h", "n": n_neither, "notes": "Non-cumulative; excluded count among S3."},
        {"dataset": "eicu", "step_id": "S4_FINAL", "step_label": "Final cohort: initiators (PPI only or H2RA only)", "n": n_final, "notes": ""},
    ]


def main() -> None:
    args = parse_args()
    landmark_hours = int(args.landmark_hours)
    if landmark_hours <= 0 or landmark_hours > 72:
        raise SystemExit("--landmark-hours must be in [1, 72].")

    mimic_cache = Path(args.mimic_cache)
    eicu_cache = Path(args.eicu_cache)

    flow = []
    flow.extend(compute_mimic_flow(mimic_cache, landmark_hours))
    flow.extend(compute_eicu_flow(eicu_cache, landmark_hours))

    import pandas as pd

    df = pd.DataFrame(flow, columns=["dataset", "step_id", "step_label", "n", "notes"])

    mimic_out = Path(args.mimic_outdir) / "tables"
    eicu_out = Path(args.eicu_outdir) / "tables"
    mimic_out.mkdir(parents=True, exist_ok=True)
    eicu_out.mkdir(parents=True, exist_ok=True)
    df[df["dataset"] == "mimic"].to_csv(mimic_out / "attrition_flow.tsv", sep="\t", index=False)
    df[df["dataset"] == "eicu"].to_csv(eicu_out / "attrition_flow.tsv", sep="\t", index=False)

    # Combined pivot for storyboard anchor.
    combined_out = Path(args.multicohort_outdir) / "combined"
    combined_out.mkdir(parents=True, exist_ok=True)
    pivot = (
        df.pivot_table(index=["step_id", "step_label"], columns="dataset", values="n", aggfunc="first")
        .reset_index()
        .rename(columns={"mimic": "n_mimic", "eicu": "n_eicu"})
    )
    pivot.to_csv(combined_out / "attrition_flow.tsv", sep="\t", index=False)

    # Guardrail checks against the actual analysis tables used in the run (if present).
    mimic_check = Path(args.check_mimic_analysis_table)
    eicu_check = Path(args.check_eicu_analysis_table)
    if mimic_check.exists():
        expected = _count_parquet(mimic_check)
        got = int(df[(df["dataset"] == "mimic") & (df["step_id"] == "S4_FINAL")]["n"].iloc[0])
        if got != expected:
            raise SystemExit(f"MIMIC final flow n={got} does not match {mimic_check} n={expected}.")
    if eicu_check.exists():
        expected = _count_parquet(eicu_check)
        got = int(df[(df["dataset"] == "eicu") & (df["step_id"] == "S4_FINAL")]["n"].iloc[0])
        if got != expected:
            raise SystemExit(f"eICU final flow n={got} does not match {eicu_check} n={expected}.")

    print(f"Wrote per-cohort flow tables to: {mimic_out}/attrition_flow.tsv and {eicu_out}/attrition_flow.tsv")
    print(f"Wrote combined flow table to: {combined_out}/attrition_flow.tsv")


if __name__ == "__main__":
    main()
