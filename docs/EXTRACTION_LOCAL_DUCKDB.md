# Local Extraction (DuckDB) — From PhysioNet ZIPs to `data/*.parquet`

This repo can run the full analysis pipeline once you have an **analysis-ready table** (one row per ICU stay) exported to Parquet/CSV.

On this machine:
- PhysioNet raw archives are present under `data/raw/physionet/`.
- BigQuery `physionet-data` access is not currently available for the active Google account.
- PostgreSQL CLI (`psql`) is not installed.

So the default extraction route is **local DuckDB**.

## Prerequisites
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## MIMIC-IV (discovery cohort)
Build and export the analysis-ready table:
```bash
python3 scripts/extract_mimic_duckdb.py \
  --zip data/raw/physionet/mimiciv-3.1.zip \
  --out data/mimic_sup_ppi_h2ra.parquet
```

Validate that the output has the required columns for the pipeline:
```bash
python3 scripts/validate_analysis_table.py --input data/mimic_sup_ppi_h2ra.parquet
```

Run the study:
```bash
python3 scripts/run_study.py --input data/mimic_sup_ppi_h2ra.parquet --outdir output/mimic_run
```

## eICU-CRD (external validation)
```bash
python3 scripts/extract_eicu_duckdb.py \
  --zip data/raw/physionet/eicu-crd-2.0.zip \
  --out data/eicu_sup_ppi_h2ra.parquet
python3 scripts/validate_analysis_table.py --input data/eicu_sup_ppi_h2ra.parquet
```

## Runtime notes
- These extracts are large and may take a long time (minutes to hours) depending on CPU/RAM/disk.
- Outputs under `data/` and `output/` are gitignored; do not commit patient-level data.

