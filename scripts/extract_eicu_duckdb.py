#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import zipfile
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def _sql_string_literal(path: Path) -> str:
    # DuckDB doesn't support prepared parameters for CREATE VIEW statements.
    return "'" + str(path).replace("'", "''") + "'"


def _extract_member(zf: zipfile.ZipFile, member: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return
    with zf.open(member) as src, dest.open("wb") as dst:
        shutil.copyfileobj(src, dst)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Extract eICU-CRD analysis-ready cohort table using DuckDB (from PhysioNet zip)."
    )
    p.add_argument(
        "--zip",
        default=str(ROOT / "data" / "raw" / "physionet" / "eicu-crd-2.0.zip"),
        help="Path to eicu-crd-2.0.zip (PhysioNet archive).",
    )
    p.add_argument(
        "--out",
        default=str(ROOT / "data" / "eicu_sup_ppi_h2ra.parquet"),
        help="Output Parquet path (MUST NOT be committed).",
    )
    p.add_argument(
        "--cache",
        default=str(ROOT / "data" / "raw" / "cache" / "eicu-2.0"),
        help="Cache dir for extracted .csv.gz members (gitignored).",
    )
    p.add_argument("--threads", type=int, default=4, help="DuckDB threads.")
    p.add_argument(
        "--landmark-hours",
        type=int,
        default=24,
        help="Landmark time in hours since ICU admission (default: 24). Used for sensitivity analyses (e.g., 12, 6).",
    )
    p.add_argument("--report", default=None, help="Optional JSON report path.")
    p.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip running scripts/validate_analysis_table.py after export.",
    )
    return p.parse_args()


def main() -> None:
    try:
        import duckdb  # type: ignore
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"duckdb is required. Install with: pip install -r requirements.txt\nError: {e}")

    args = parse_args()
    landmark_hours = int(args.landmark_hours)
    if landmark_hours <= 0 or landmark_hours > 72:
        raise SystemExit("--landmark-hours must be in [1, 72].")
    landmark_minutes = int(landmark_hours * 60)
    followup_end_14m = landmark_minutes + 14 * 1440
    followup_end_28m = landmark_minutes + 28 * 1440
    zip_path = Path(args.zip)
    out_path = Path(args.out)
    cache_dir = Path(args.cache)

    if not zip_path.exists():
        raise SystemExit(f"Missing archive: {zip_path}")

    required = [
        "eicu-collaborative-research-database-2.0/patient.csv.gz",
        "eicu-collaborative-research-database-2.0/lab.csv.gz",
        "eicu-collaborative-research-database-2.0/medication.csv.gz",
        "eicu-collaborative-research-database-2.0/diagnosis.csv.gz",
        "eicu-collaborative-research-database-2.0/respiratoryCare.csv.gz",
    ]

    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        missing = [m for m in required if m not in names]
        if missing:
            raise SystemExit(f"Archive missing required members: {missing}")
        for m in required:
            dest = cache_dir / Path(m).name
            _extract_member(zf, m, dest)

    patient = cache_dir / "patient.csv.gz"
    lab = cache_dir / "lab.csv.gz"
    medication = cache_dir / "medication.csv.gz"
    diagnosis = cache_dir / "diagnosis.csv.gz"
    respiratory_care = cache_dir / "respiratoryCare.csv.gz"

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={int(args.threads)};")

    patient_lit = _sql_string_literal(patient)
    lab_lit = _sql_string_literal(lab)
    medication_lit = _sql_string_literal(medication)
    diagnosis_lit = _sql_string_literal(diagnosis)
    respiratory_care_lit = _sql_string_literal(respiratory_care)

    con.execute(
        f"""
        create view patient as
        select
          patientunitstayid::bigint as patientunitstayid,
          uniquepid::varchar as uniquepid,
          gender::varchar as gender,
          age::varchar as age,
          ethnicity::varchar as ethnicity,
          unitadmittime24::varchar as unitadmittime24,
          unitdischargeoffset::integer as unitdischargeoffset,
          unitdischargestatus::varchar as unitdischargestatus
        from read_csv_auto(
          {patient_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )
    con.execute(
        f"""
        create view lab as
        select
          patientunitstayid::bigint as patientunitstayid,
          labresultoffset::integer as labresultoffset,
          labname::varchar as labname,
          labresult::varchar as labresult
        from read_csv_auto(
          {lab_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )
    con.execute(
        f"""
        create view medication as
        select
          patientunitstayid::bigint as patientunitstayid,
          drugstartoffset::integer as drugstartoffset,
          drugname::varchar as drugname
        from read_csv_auto(
          {medication_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )
    con.execute(
        f"""
        create view diagnosis as
        select
          patientunitstayid::bigint as patientunitstayid,
          diagnosisoffset::integer as diagnosisoffset,
          diagnosisstring::varchar as diagnosisstring,
          icd9code::varchar as icd9code
        from read_csv_auto(
          {diagnosis_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )
    con.execute(
        f"""
        create view respiratoryCare as
        select
          patientunitstayid::bigint as patientunitstayid,
          ventstartoffset::integer as ventstartoffset,
          ventendoffset::integer as ventendoffset,
          airwaytype::varchar as airwaytype
        from read_csv_auto(
          {respiratory_care_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )

    # Build analysis-ready table (duckdb SQL).
    sql = """
        create table dlfx_eicu_sup_ppi_h2ra as
        with
        base as (
          select
            p.patientunitstayid as stay_id,
            p.uniquepid as patient_id,
            1440 as landmark_minutes,
            case
              when try_cast(p.age as integer) is not null then try_cast(p.age as integer)::double
              when position('>' in p.age) > 0 then 90::double
              else null::double
            end as age_years,
            p.gender as sex,
            p.ethnicity as race,
            p.unitdischargeoffset as unitdischargeoffset,
            p.unitdischargestatus as unitdischargestatus
          from patient p
          where coalesce(try_cast(p.age as integer), 90) >= 18
        ),
        labs_24h as (
          select
            b.stay_id,
            min(case when l.labname ilike '%hemoglobin%' then try_cast(l.labresult as double) end) as hgb_min_24h,
            min(case when l.labname ilike '%platelet%' then try_cast(l.labresult as double) end) as platelet_min_24h,
            max(case when l.labname ilike '%inr%' then try_cast(l.labresult as double) end) as inr_max_24h
          from base b
          left join lab l
            on l.patientunitstayid = b.stay_id
           and l.labresultoffset >= 0
           and l.labresultoffset < 1440
          group by b.stay_id
        ),
        coagulopathy as (
          select
            b.stay_id,
            l.platelet_min_24h,
            l.inr_max_24h,
            case
              when l.platelet_min_24h is not null and l.platelet_min_24h < 50 then 1
              when l.inr_max_24h is not null and l.inr_max_24h > 1.5 then 1
              else 0
            end as sup_indication_coagulopathy_24h
          from base b
          left join labs_24h l on l.stay_id = b.stay_id
        ),
        mv_24h as (
          select
            b.stay_id,
            case when max(case when rc.patientunitstayid is not null then 1 else 0 end) = 1 then 1 else 0 end as sup_indication_mv_24h
          from base b
          left join respiratoryCare rc
            on rc.patientunitstayid = b.stay_id
           and rc.ventstartoffset is not null
           and rc.ventstartoffset < b.landmark_minutes
           and coalesce(nullif(rc.ventendoffset, 0), 999999) > 0
          group by b.stay_id
        ),
        sup_flags as (
          select
            b.*,
            c.platelet_min_24h,
            c.inr_max_24h,
            mv.sup_indication_mv_24h,
            c.sup_indication_coagulopathy_24h,
            case
              when mv.sup_indication_mv_24h = 1 or c.sup_indication_coagulopathy_24h = 1 then 1 else 0
            end as eligible_sup_high_risk
          from base b
          left join coagulopathy c on c.stay_id = b.stay_id
          left join mv_24h mv on mv.stay_id = b.stay_id
        ),
        rx_24h as (
          select
            s.stay_id,
            max(case
              when m.drugname ilike '%omeprazole%' or m.drugname ilike '%pantoprazole%' or m.drugname ilike '%esomeprazole%'
                or m.drugname ilike '%lansoprazole%' or m.drugname ilike '%rabeprazole%'
              then 1 else 0 end) as ppi_any_24h,
            max(case
              when m.drugname ilike '%famotidine%' or m.drugname ilike '%ranitidine%'
                or m.drugname ilike '%cimetidine%' or m.drugname ilike '%nizatidine%'
              then 1 else 0 end) as h2ra_any_24h
          from sup_flags s
          left join medication m
            on m.patientunitstayid = s.stay_id
           and m.drugstartoffset >= 0
           and m.drugstartoffset < 1440
          group by s.stay_id
        ),
        exposure as (
          select
            s.*,
            coalesce(r.ppi_any_24h, 0) as ppi_any_24h,
            coalesce(r.h2ra_any_24h, 0) as h2ra_any_24h,
            case when coalesce(r.ppi_any_24h,0)=1 and coalesce(r.h2ra_any_24h,0)=1 then 1 else 0 end as dual_ppi_h2ra_24h,
            case
              when coalesce(r.ppi_any_24h,0)=1 and coalesce(r.h2ra_any_24h,0)=0 then 'ppi'
              when coalesce(r.ppi_any_24h,0)=0 and coalesce(r.h2ra_any_24h,0)=1 then 'h2ra'
              else null
            end as treatment
          from sup_flags s
          left join rx_24h r on r.stay_id = s.stay_id
        ),
        dx as (
          select
            e.stay_id,
            max(case
              when d.icd9code like '578%' then 1
              when d.diagnosisstring ilike '%gastrointestinal%hemorrhage%' then 1
              when d.diagnosisstring ilike '%gi%bleed%' then 1
              when d.diagnosisstring ilike '%upper%gi%bleed%' then 1
              else 0
            end) as ugib_event,
            min(case
              when (
                d.icd9code like '578%'
                or d.diagnosisstring ilike '%gastrointestinal%hemorrhage%'
                or d.diagnosisstring ilike '%gi%bleed%'
                or d.diagnosisstring ilike '%upper%gi%bleed%'
              )
              and d.diagnosisoffset >= 1440
              then d.diagnosisoffset
              else null
            end)::double / 1440.0 - 1.0 as ugib_time_days,
            max(case
              when d.icd9code like '00845%' then 1
              when d.diagnosisstring ilike '%clostrid%' then 1
              else 0
            end) as cdi_event,
            min(case
              when (d.icd9code like '00845%' or d.diagnosisstring ilike '%clostrid%')
               and d.diagnosisoffset >= 1440
              then d.diagnosisoffset
              else null
            end)::double / 1440.0 - 1.0 as cdi_time_days
          from exposure e
          left join diagnosis d on d.patientunitstayid = e.stay_id
          group by e.stay_id
        ),
        liver_dx as (
          -- Baseline comorbidity proxy: liver disease (ICD-9 or diagnosis string).
          select
            e.stay_id,
            max(case
              when d.icd9code like '571%' then 1
              when d.icd9code like '572%' then 1
              when d.diagnosisstring ilike '%cirrhosis%' then 1
              when d.diagnosisstring ilike '%liver failure%' then 1
              when d.diagnosisstring ilike '%hepatic failure%' then 1
              when d.diagnosisstring ilike '%portal hypertension%' then 1
              else 0
            end) as liver_disease
          from exposure e
          left join diagnosis d on d.patientunitstayid = e.stay_id
          group by e.stay_id
        ),
        antithrombotic_24h as (
          -- Baseline-window antithrombotic exposure proxy (string matching).
          select
            e.stay_id,
            max(case
              when m.drugname ilike '%warfarin%' then 1
              when m.drugname ilike '%heparin%' then 1
              when m.drugname ilike '%enoxaparin%' then 1
              when m.drugname ilike '%dalteparin%' then 1
              when m.drugname ilike '%fondaparinux%' then 1
              when m.drugname ilike '%apixaban%' then 1
              when m.drugname ilike '%rivaroxaban%' then 1
              when m.drugname ilike '%dabigatran%' then 1
              when m.drugname ilike '%edoxaban%' then 1
              when m.drugname ilike '%clopidogrel%' then 1
              when m.drugname ilike '%ticagrelor%' then 1
              when m.drugname ilike '%prasugrel%' then 1
              when m.drugname ilike '%aspirin%' then 1
              else 0 end) as antithrombotic_any_24h
          from exposure e
          left join medication m
            on m.patientunitstayid = e.stay_id
           and m.drugstartoffset >= 0
           and m.drugstartoffset < e.landmark_minutes
          group by e.stay_id
        ),
        death as (
          select
            e.stay_id,
            case
              when e.unitdischargestatus ilike 'Expired' and e.unitdischargeoffset <= (1440 + 28*1440) then 1
              else 0
            end as death_event_28d,
            case
              when e.unitdischargestatus ilike 'Expired' and e.unitdischargeoffset >= 1440
              then (e.unitdischargeoffset::double / 1440.0) - 1.0
              else null::double
            end as death_time_days
          from exposure e
        ),
        final as (
          select
            'eicu' as dataset,
            e.stay_id,
            e.patient_id,
            null::bigint as hadm_id,
            null::timestamp as icu_intime,
            null::timestamp as icu_outtime,
            null::timestamp as index_time,
            e.age_years,
            e.sex,
            e.race,
            1 as is_first_icu_stay,
            case when e.unitdischargeoffset >= 1440 then 1 else 0 end as alive_in_icu_at_landmark,
            case when e.unitdischargeoffset >= 1440 then 1 else 0 end as alive_at_landmark,
            e.sup_indication_mv_24h,
            e.sup_indication_coagulopathy_24h,
            e.eligible_sup_high_risk,
            coalesce(liver.liver_disease, 0) as liver_disease,
            coalesce(a24.antithrombotic_any_24h, 0) as antithrombotic_any_24h,
            e.platelet_min_24h,
            e.inr_max_24h,
            l.hgb_min_24h,
            null::double as hgb_max_24h,
            null::double as creatinine_min_24h,
            null::double as creatinine_max_24h,
            null::double as lactate_min_24h,
            null::double as lactate_max_24h,
            null::double as sofa_24h,
            e.treatment,
            e.ppi_any_24h,
            e.h2ra_any_24h,
            e.dual_ppi_h2ra_24h,
            case when dx.ugib_time_days is not null then coalesce(dx.ugib_event,0) else 0 end as cigib_strict_event,
            coalesce(
              dx.ugib_time_days,
              (least(coalesce(e.unitdischargeoffset, (1440 + 14*1440)), (1440 + 14*1440))::double / 1440.0) - 1.0
            ) as cigib_strict_time_days,
            case when dx.ugib_time_days is not null then coalesce(dx.ugib_event,0) else 0 end as ugib_broad_event,
            coalesce(
              dx.ugib_time_days,
              (least(coalesce(e.unitdischargeoffset, (1440 + 14*1440)), (1440 + 14*1440))::double / 1440.0) - 1.0
            ) as ugib_broad_time_days,
            coalesce(dx.cdi_event,0) as cdi_event,
            dx.cdi_time_days as cdi_time_days,
            coalesce(death.death_event_28d,0) as death_event_28d,
            coalesce(
              death.death_time_days,
              (least(coalesce(e.unitdischargeoffset, (1440 + 28*1440)), (1440 + 28*1440))::double / 1440.0) - 1.0
            ) as death_time_days
          from exposure e
          left join labs_24h l on l.stay_id = e.stay_id
          left join dx on dx.stay_id = e.stay_id
          left join liver_dx liver on liver.stay_id = e.stay_id
          left join antithrombotic_24h a24 on a24.stay_id = e.stay_id
          left join death on death.stay_id = e.stay_id
        )
        select *
        from final
        where
          alive_in_icu_at_landmark = 1
          and eligible_sup_high_risk = 1
          and dual_ppi_h2ra_24h = 0
          and treatment in ('ppi', 'h2ra');
        """

    # Parameterize landmark for sensitivity analyses (default 24h).
    # We keep the template SQL readable (24h baseline) and adjust the few required constants.
    sql = sql.replace("1440 as landmark_minutes", f"{landmark_minutes} as landmark_minutes")
    sql = sql.replace("labresultoffset < 1440", f"labresultoffset < {landmark_minutes}")
    sql = sql.replace("drugstartoffset < 1440", f"drugstartoffset < {landmark_minutes}")
    sql = sql.replace("diagnosisoffset >= 1440", f"diagnosisoffset >= {landmark_minutes}")
    sql = sql.replace("unitdischargeoffset >= 1440", f"unitdischargeoffset >= {landmark_minutes}")
    sql = sql.replace(
        "when e.unitdischargestatus ilike 'Expired' and e.unitdischargeoffset <= (1440 + 28*1440)",
        f"when e.unitdischargestatus ilike 'Expired' and e.unitdischargeoffset <= ({followup_end_28m})",
    )
    sql = sql.replace(
        "then (e.unitdischargeoffset::double / 1440.0) - 1.0",
        f"then (e.unitdischargeoffset - {landmark_minutes})::double / 1440.0",
    )
    sql = sql.replace(
        "end)::double / 1440.0 - 1.0 as ugib_time_days",
        f"end - {landmark_minutes})::double / 1440.0 as ugib_time_days",
    )
    sql = sql.replace(
        "end)::double / 1440.0 - 1.0 as cdi_time_days",
        f"end - {landmark_minutes})::double / 1440.0 as cdi_time_days",
    )
    sql = sql.replace(
        "(least(coalesce(e.unitdischargeoffset, (1440 + 14*1440)), (1440 + 14*1440))::double / 1440.0) - 1.0",
        f"(least(coalesce(e.unitdischargeoffset, ({followup_end_14m})), ({followup_end_14m})) - {landmark_minutes})::double / 1440.0",
    )
    sql = sql.replace(
        "(least(coalesce(e.unitdischargeoffset, (1440 + 28*1440)), (1440 + 28*1440))::double / 1440.0) - 1.0",
        f"(least(coalesce(e.unitdischargeoffset, ({followup_end_28m})), ({followup_end_28m})) - {landmark_minutes})::double / 1440.0",
    )

    con.execute(sql)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_lit = _sql_string_literal(out_path)
    con.execute(f"copy dlfx_eicu_sup_ppi_h2ra to {out_lit} (format parquet);")

    n_rows = int(con.execute("select count(*) from dlfx_eicu_sup_ppi_h2ra;").fetchone()[0])
    cols = [r[1] for r in con.execute("pragma table_info('dlfx_eicu_sup_ppi_h2ra');").fetchall()]
    report = {
        "dataset": "eicu",
        "zip": str(zip_path),
        "out": str(out_path),
        "cache": str(cache_dir),
        "landmark_hours": landmark_hours,
        "n_rows": n_rows,
        "n_cols": len(cols),
        "columns": cols,
        "notes": [
            "Output is gitignored (data/**). Do not commit patient-level data.",
            "eICU MV proxy uses respiratoryCare.ventstartoffset/ventendoffset overlap with baseline window.",
            "UGIB/CDI definitions use diagnosis codes/strings and diagnosisoffset for timing (censored at discharge/14d where needed).",
            "Subgroup support columns included: liver_disease, antithrombotic_any_24h.",
        ],
    }
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        Path(args.report).write_text(text, encoding="utf-8")
    print(text)

    if not args.no_validate:
        import subprocess

        subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "validate_analysis_table.py"), "--input", str(out_path)],
            check=False,
        )


if __name__ == "__main__":
    main()
