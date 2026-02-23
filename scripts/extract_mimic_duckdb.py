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
        description="Extract MIMIC-IV analysis-ready cohort table using DuckDB (from PhysioNet zip)."
    )
    p.add_argument(
        "--zip",
        default=str(ROOT / "data" / "raw" / "physionet" / "mimiciv-3.1.zip"),
        help="Path to mimiciv-3.1.zip (PhysioNet archive).",
    )
    p.add_argument(
        "--out",
        default=str(ROOT / "data" / "mimic_sup_ppi_h2ra.parquet"),
        help="Output Parquet path (MUST NOT be committed).",
    )
    p.add_argument(
        "--cache",
        default=str(ROOT / "data" / "raw" / "cache" / "mimic-iv-3.1"),
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
    zip_path = Path(args.zip)
    out_path = Path(args.out)
    cache_dir = Path(args.cache)

    if not zip_path.exists():
        raise SystemExit(f"Missing archive: {zip_path}")

    required = [
        "mimic-iv-3.1/icu/icustays.csv.gz",
        "mimic-iv-3.1/icu/chartevents.csv.gz",
        "mimic-iv-3.1/icu/inputevents.csv.gz",
        "mimic-iv-3.1/hosp/admissions.csv.gz",
        "mimic-iv-3.1/hosp/patients.csv.gz",
        "mimic-iv-3.1/hosp/labevents.csv.gz",
        "mimic-iv-3.1/hosp/d_labitems.csv.gz",
        "mimic-iv-3.1/hosp/prescriptions.csv.gz",
        "mimic-iv-3.1/hosp/diagnoses_icd.csv.gz",
    ]

    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        missing = [m for m in required if m not in names]
        if missing:
            raise SystemExit(f"Archive missing required members: {missing}")

        for m in required:
            dest = cache_dir / m.replace("mimic-iv-3.1/", "")
            _extract_member(zf, m, dest)

    icu_dir = cache_dir / "icu"
    hosp_dir = cache_dir / "hosp"
    icustays = icu_dir / "icustays.csv.gz"
    chartevents = icu_dir / "chartevents.csv.gz"
    inputevents = icu_dir / "inputevents.csv.gz"
    admissions = hosp_dir / "admissions.csv.gz"
    patients = hosp_dir / "patients.csv.gz"
    labevents = hosp_dir / "labevents.csv.gz"
    d_labitems = hosp_dir / "d_labitems.csv.gz"
    prescriptions = hosp_dir / "prescriptions.csv.gz"
    diagnoses_icd = hosp_dir / "diagnoses_icd.csv.gz"

    con = duckdb.connect(database=":memory:")
    con.execute(f"PRAGMA threads={int(args.threads)};")

    icustays_lit = _sql_string_literal(icustays)
    chartevents_lit = _sql_string_literal(chartevents)
    inputevents_lit = _sql_string_literal(inputevents)
    admissions_lit = _sql_string_literal(admissions)
    patients_lit = _sql_string_literal(patients)
    labevents_lit = _sql_string_literal(labevents)
    d_labitems_lit = _sql_string_literal(d_labitems)
    prescriptions_lit = _sql_string_literal(prescriptions)
    diagnoses_icd_lit = _sql_string_literal(diagnoses_icd)

    con.execute("create schema if not exists mimiciv_icu;")
    con.execute("create schema if not exists mimiciv_hosp;")

    # Read only the columns we actually use downstream to keep parsing overhead lower.
    con.execute(
        f"""
        create view mimiciv_icu.icustays as
        select
          subject_id::bigint as subject_id,
          hadm_id::bigint as hadm_id,
          stay_id::bigint as stay_id,
          intime::timestamp as intime,
          outtime::timestamp as outtime
        from read_csv_auto(
          {icustays_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )
    con.execute(
        f"""
        create view mimiciv_hosp.admissions as
        select
          subject_id::bigint as subject_id,
          hadm_id::bigint as hadm_id,
          admittime::timestamp as admittime,
          dischtime::timestamp as dischtime,
          deathtime::timestamp as deathtime,
          race::varchar as race
        from read_csv_auto(
          {admissions_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
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
        from read_csv_auto(
          {patients_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )
    con.execute(
        f"""
        create view mimiciv_hosp.d_labitems as
        select
          itemid::integer as itemid,
          label::varchar as label
        from read_csv_auto(
          {d_labitems_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )
    con.execute(
        f"""
        create view mimiciv_hosp.labevents as
        select
          try_cast(hadm_id as bigint) as hadm_id,
          try_cast(itemid as integer) as itemid,
          try_cast(charttime as timestamp) as charttime,
          try_cast(valuenum as double) as valuenum
        from read_csv_auto(
          {labevents_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true,
          all_varchar=true
        );
        """,
    )
    con.execute(
        f"""
        create view mimiciv_hosp.prescriptions as
        select
          hadm_id::bigint as hadm_id,
          starttime::timestamp as starttime,
          stoptime::timestamp as stoptime,
          drug::varchar as drug
        from read_csv_auto(
          {prescriptions_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )
    con.execute(
        f"""
        create view mimiciv_hosp.diagnoses_icd as
        select
          hadm_id::bigint as hadm_id,
          icd_code::varchar as icd_code,
          icd_version::integer as icd_version
        from read_csv_auto(
          {diagnoses_icd_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        );
        """,
    )

    # For MV proxy, we only need ventilator mode/type items.
    con.execute(
        f"""
        create view mimiciv_icu.chartevents as
        select
          try_cast(stay_id as bigint) as stay_id,
          try_cast(charttime as timestamp) as charttime,
          try_cast(itemid as integer) as itemid,
          value::varchar as value
        from read_csv_auto(
          {chartevents_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        )
        where try_cast(itemid as integer) in (223848, 223849, 229314) and value is not null;
        """,
    )

    # For strict CIGIB proxy, we only need PRBC-related inputevents.
    con.execute(
        f"""
        create view mimiciv_icu.inputevents as
        select
          try_cast(stay_id as bigint) as stay_id,
          try_cast(starttime as timestamp) as starttime,
          try_cast(itemid as integer) as itemid
        from read_csv_auto(
          {inputevents_lit},
          header=true,
          delim=',',
          strict_mode=false,
          null_padding=true
        )
        where try_cast(itemid as integer) in (220996, 225168, 226368, 227070);
        """,
    )

    sql_path = ROOT / "sql" / "mimic" / "build_analysis_table.sql"
    sql = sql_path.read_text(encoding="utf-8")
    # This SQL file is written as a Postgres template with a 24h landmark; for sensitivity analyses
    # we adjust the landmark and the baseline/exposure window to match landmark_hours.
    sql = sql.replace("interval '24 hour'", f"interval '{landmark_hours} hour'")
    con.execute(sql)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_lit = _sql_string_literal(out_path)
    con.execute(f"copy dlfx_mimic_sup_ppi_h2ra to {out_lit} (format parquet);")

    # Report
    n_rows = int(con.execute("select count(*) from dlfx_mimic_sup_ppi_h2ra;").fetchone()[0])
    cols = [r[1] for r in con.execute("pragma table_info('dlfx_mimic_sup_ppi_h2ra');").fetchall()]
    report = {
        "dataset": "mimic",
        "zip": str(zip_path),
        "out": str(out_path),
        "cache": str(cache_dir),
        "landmark_hours": landmark_hours,
        "n_rows": n_rows,
        "n_cols": len(cols),
        "columns": cols,
        "notes": [
            "Output is gitignored (data/**). Do not commit patient-level data.",
            "MV proxy uses ventilator mode/type documentation in chartevents (heuristic).",
            "Strict CIGIB proxy uses UGIB ICD + PRBC transfusion evidence from inputevents.",
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
